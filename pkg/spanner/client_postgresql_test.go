package spanner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cloudspanner "cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/uuid"
)

const postgreSQLSchema = `CREATE TABLE singers (
  singer_id bigint NOT NULL PRIMARY KEY,
  first_name varchar(1024),
  last_name varchar(1024)
);`

func TestCreatePostgreSQLDatabaseAndLoadDDL(t *testing.T) {
	client := testPostgreSQLClientWithSchema(t, postgreSQLSchema)

	loadedDDL, _, err := client.LoadDDL(t.Context())
	if err != nil {
		t.Fatalf("failed to load PostgreSQL DDL: %v", err)
	}
	if !strings.Contains(strings.ToLower(string(loadedDDL)), "create table singers") {
		t.Errorf("loaded DDL does not contain the singers table: %s", loadedDDL)
	}
}

func TestApplyPostgreSQLDDLFile(t *testing.T) {
	client := testPostgreSQLClientWithSchema(t, postgreSQLSchema)

	ddl := []byte("ALTER TABLE singers ADD COLUMN nickname varchar(32);")
	if err := client.ApplyDDLFile(t.Context(), "alter.sql", ddl, nil); err != nil {
		t.Fatalf("failed to apply PostgreSQL DDL: %v", err)
	}

	loadedDDL, _, err := client.LoadDDL(t.Context())
	if err != nil {
		t.Fatalf("failed to load PostgreSQL DDL: %v", err)
	}
	if !strings.Contains(strings.ToLower(string(loadedDDL)), "nickname") {
		t.Errorf("loaded DDL does not contain the added column: %s", loadedDDL)
	}
}

func TestPostgreSQLMigrationWorkflow(t *testing.T) {
	ctx := t.Context()
	client := testPostgreSQLClientWithSchema(t, postgreSQLSchema)

	_, err := client.spannerClient.Apply(ctx, []*cloudspanner.Mutation{
		cloudspanner.Insert("singers", []string{"singer_id", "first_name", "last_name"}, []interface{}{int64(1), "Ada", "Lovelace"}),
	})
	if err != nil {
		t.Fatalf("failed to seed PostgreSQL database: %v", err)
	}

	dml := []byte("UPDATE singers SET last_name = 'Byron' WHERE singer_id = 1;")
	updated, err := client.ApplyDMLFile(ctx, "update.sql", dml, false, PriorityTypeUnspecified)
	if err != nil {
		t.Fatalf("failed to apply PostgreSQL DML: %v", err)
	}
	if updated != 1 {
		t.Fatalf("want one updated row, got %d", updated)
	}

	if err := client.EnsureMigrationTable(ctx, migrationTable); err != nil {
		t.Fatalf("failed to create PostgreSQL migration table: %v", err)
	}
	if err := client.SetSchemaMigrationVersion(ctx, 0, true, migrationTable); err != nil {
		t.Fatalf("failed to set PostgreSQL migration version: %v", err)
	}
	version, dirty, err := client.GetSchemaMigrationVersion(ctx, migrationTable)
	if err != nil {
		t.Fatalf("failed to get manually set PostgreSQL migration version: %v", err)
	}
	if version != 0 || !dirty {
		t.Fatalf("want migration version 0 dirty, got version %d dirty %t", version, dirty)
	}
	if err := client.SetSchemaMigrationVersion(ctx, 0, false, migrationTable); err != nil {
		t.Fatalf("failed to clear PostgreSQL migration dirty state: %v", err)
	}

	migrationDir := t.TempDir()
	files := map[string]string{
		"000001_add_nickname.sql": "ALTER TABLE singers ADD COLUMN nickname varchar(32);",
		"000002_insert.up.sql":    "INSERT INTO singers (singer_id, first_name, last_name, nickname) VALUES (2, 'Grace', 'Hopper', 'Amazing Grace');",
		"000003_update.up.sql":    "UPDATE singers SET last_name = 'Murray' WHERE singer_id = 2;",
	}
	for filename, contents := range files {
		if err := os.WriteFile(filepath.Join(migrationDir, filename), []byte(contents), 0600); err != nil {
			t.Fatalf("failed to write migration %s: %v", filename, err)
		}
	}

	migrations, err := ReadMigrationsWithDialect(ctx, migrationDir, databasepb.DatabaseDialect_POSTGRESQL)
	if err != nil {
		t.Fatalf("failed to read PostgreSQL migrations: %v", err)
	}
	if err := client.ExecuteMigrations(ctx, migrations, len(migrations), migrationTable, PriorityTypeUnspecified, nil); err != nil {
		t.Fatalf("failed to execute PostgreSQL migrations: %v", err)
	}

	version, dirty, err = client.GetSchemaMigrationVersion(ctx, migrationTable)
	if err != nil {
		t.Fatalf("failed to get PostgreSQL migration version: %v", err)
	}
	if version != 3 || dirty {
		t.Fatalf("want migration version 3 clean, got version %d dirty %t", version, dirty)
	}

	row := client.spannerClient.Single().Query(ctx, cloudspanner.Statement{
		SQL: "SELECT first_name, last_name, nickname FROM singers WHERE singer_id = 2",
	})
	defer row.Stop()
	result, err := row.Next()
	if err != nil {
		t.Fatalf("failed to read migrated PostgreSQL row: %v", err)
	}
	var firstName, lastName, nickname string
	if err := result.Columns(&firstName, &lastName, &nickname); err != nil {
		t.Fatalf("failed to decode migrated PostgreSQL row: %v", err)
	}
	if firstName != "Grace" || lastName != "Murray" || nickname != "Amazing Grace" {
		t.Fatalf("unexpected migrated row: name=%q last_name=%q nickname=%q", firstName, lastName, nickname)
	}

	if err := client.EnsureMigrationTable(ctx, customMigrationTable); err != nil {
		t.Fatalf("failed to create custom PostgreSQL migration table: %v", err)
	}
	if err := client.SetSchemaMigrationVersion(ctx, 9, false, customMigrationTable); err != nil {
		t.Fatalf("failed to set custom PostgreSQL migration version: %v", err)
	}
	if err := client.TruncateAllTables(ctx, strings.ToLower(migrationTable)); err != nil {
		t.Fatalf("failed to truncate PostgreSQL tables: %v", err)
	}
	version, dirty, err = client.GetSchemaMigrationVersion(ctx, migrationTable)
	if err != nil {
		t.Fatalf("failed to read default migration version after truncate: %v", err)
	}
	if version != 3 || dirty {
		t.Fatalf("truncate changed default migration version: version %d dirty %t", version, dirty)
	}
	if _, _, err := client.GetSchemaMigrationVersion(ctx, customMigrationTable); err == nil {
		t.Fatal("custom migration version survived truncation")
	}

	if err := client.SetSchemaMigrationVersion(ctx, 9, false, customMigrationTable); err != nil {
		t.Fatalf("failed to restore custom PostgreSQL migration version: %v", err)
	}
	if err := client.TruncateAllTables(ctx, customMigrationTable); err != nil {
		t.Fatalf("failed to truncate PostgreSQL tables with custom migration table: %v", err)
	}
	version, dirty, err = client.GetSchemaMigrationVersion(ctx, customMigrationTable)
	if err != nil {
		t.Fatalf("failed to read custom migration version after truncate: %v", err)
	}
	if version != 9 || dirty {
		t.Fatalf("truncate changed custom migration version: version %d dirty %t", version, dirty)
	}
	if _, _, err := client.GetSchemaMigrationVersion(ctx, migrationTable); err == nil {
		t.Fatal("default migration version survived custom-table truncation")
	}
	count := client.spannerClient.Single().Query(ctx, cloudspanner.Statement{
		SQL: "SELECT COUNT(*) FROM singers",
	})
	defer count.Stop()
	countRow, err := count.Next()
	if err != nil {
		t.Fatalf("failed to count PostgreSQL rows after truncate: %v", err)
	}
	var rowCount int64
	if err := countRow.Columns(&rowCount); err != nil {
		t.Fatalf("failed to decode PostgreSQL row count: %v", err)
	}
	if rowCount != 0 {
		t.Errorf("want zero PostgreSQL rows after truncate, got %d", rowCount)
	}
}

func testPostgreSQLClientWithSchema(t *testing.T, schema string) *Client {
	t.Helper()
	ctx := t.Context()
	if os.Getenv(envSpannerEmulatorHost) == "" {
		t.Fatal("test must use spanner emulator")
	}
	projectID := os.Getenv(envSpannerProjectID)
	if projectID == "" {
		t.Fatalf("must set %s", envSpannerProjectID)
	}
	instanceID := os.Getenv(envSpannerInstanceID)
	if instanceID == "" {
		t.Fatalf("must set %s", envSpannerInstanceID)
	}

	config := &Config{
		Project:  projectID,
		Instance: instanceID,
		Database: fmt.Sprintf("test-%s", uuid.NewString()[:18]),
		Dialect:  databasepb.DatabaseDialect_POSTGRESQL,
	}
	client, err := NewClient(ctx, config)
	if err != nil {
		t.Fatalf("failed to create spanner client: %v", err)
	}
	created := false
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if created {
			if err := client.DropDatabase(cleanupCtx); err != nil {
				t.Errorf("failed to drop test database: %v", err)
			}
		}
		if err := client.Close(); err != nil {
			t.Errorf("failed to close client: %v", err)
		}
	})

	if err := client.CreateDatabase(ctx, "schema.sql", []byte(schema), nil); err != nil {
		t.Fatalf("failed to create PostgreSQL database: %v", err)
	}
	created = true
	if err := client.Close(); err != nil {
		t.Fatalf("failed to close initial client: %v", err)
	}
	client, err = NewClient(ctx, config)
	if err != nil {
		t.Fatalf("failed to reconnect to PostgreSQL database: %v", err)
	}
	return client
}
