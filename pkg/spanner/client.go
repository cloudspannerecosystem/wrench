// Copyright (c) 2020 Mercari, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package spanner

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"sort"
	"time"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	ddlStatementsSeparator             = ";"
	upgradeIndicator                   = "wrench_upgrade_indicator"
	historyStr                         = "History"
	FirstRun                           = UpgradeStatus("FirstRun")
	ExistingMigrationsNoUpgrade        = UpgradeStatus("NoUpgrade")
	ExistingMigrationsUpgradeStarted   = UpgradeStatus("Started")
	ExistingMigrationsUpgradeCompleted = UpgradeStatus("Completed")
	createUpgradeIndicatorFormatString = `CREATE TABLE %s (Dummy INT64 NOT NULL) PRIMARY KEY(Dummy)`
)
var (
	createUpgradeIndicatorSql          = fmt.Sprintf(createUpgradeIndicatorFormatString, upgradeIndicator)
)

type UpgradeStatus string

type table struct {
	TableName string `spanner:"table_name"`
}

type Client struct {
	config             *Config
	spannerClient      *spanner.Client
	spannerAdminClient *admin.DatabaseAdminClient
}

type MigrationHistoryRecord struct {
	Version  int64     `spanner:"Version"`
	Dirty    bool      `spanner:"Dirty"`
	Created  time.Time `spanner:"Created"`
	Modified time.Time `spanner:"Modified"`
}

func NewClient(ctx context.Context, config *Config) (*Client, error) {
	opts := make([]option.ClientOption, 0)
	if config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}

	spannerClient, err := spanner.NewClient(ctx, config.URL(), opts...)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeCreateClient,
			err:  err,
		}
	}

	spannerAdminClient, err := admin.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		spannerClient.Close()
		return nil, &Error{
			Code: ErrorCodeCreateClient,
			err:  err,
		}
	}

	return &Client{
		config:             config,
		spannerClient:      spannerClient,
		spannerAdminClient: spannerAdminClient,
	}, nil
}

func (c *Client) CreateDatabase(ctx context.Context, ddl []byte) error {
	statements := toStatements(ddl)

	createReq := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", c.config.Project, c.config.Instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", c.config.Database),
		ExtraStatements: statements,
	}

	op, err := c.spannerAdminClient.CreateDatabase(ctx, createReq)
	if err != nil {
		return &Error{
			Code: ErrorCodeCreateDatabase,
			err:  err,
		}
	}

	_, err = op.Wait(ctx)
	if err != nil {
		return &Error{
			Code: ErrorCodeWaitOperation,
			err:  err,
		}
	}

	return nil
}

func (c *Client) DropDatabase(ctx context.Context) error {
	req := &databasepb.DropDatabaseRequest{Database: c.config.URL()}

	if err := c.spannerAdminClient.DropDatabase(ctx, req); err != nil {
		return &Error{
			Code: ErrorCodeDropDatabase,
			err:  err,
		}
	}

	return nil
}

func (c *Client) TruncateAllTables(ctx context.Context) error {
	var m []*spanner.Mutation

	ri := c.spannerClient.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT table_name FROM information_schema.tables WHERE table_catalog = '' AND table_schema = ''",
	})
	err := ri.Do(func(row *spanner.Row) error {
		t := &table{}
		if err := row.ToStruct(t); err != nil {
			return err
		}

		if t.TableName == "SchemaMigrations" {
			return nil
		}

		m = append(m, spanner.Delete(t.TableName, spanner.AllKeys()))
		return nil
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeTruncateAllTables,
			err:  err,
		}
	}

	_, err = c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite(m)
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeTruncateAllTables,
			err:  err,
		}
	}

	return nil
}

func (c *Client) LoadDDL(ctx context.Context) ([]byte, error) {
	req := &databasepb.GetDatabaseDdlRequest{Database: c.config.URL()}

	res, err := c.spannerAdminClient.GetDatabaseDdl(ctx, req)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeLoadSchema,
			err:  err,
		}
	}

	var schema []byte
	last := len(res.Statements) - 1
	for index, statement := range res.Statements {
		if index != last {
			statement += ddlStatementsSeparator + "\n\n"
		} else {
			statement += ddlStatementsSeparator + "\n"
		}

		schema = append(schema[:], []byte(statement)[:]...)
	}

	return schema, nil
}

func (c *Client) ApplyDDLFile(ctx context.Context, ddl []byte) error {
	return c.ApplyDDL(ctx, toStatements(ddl))
}

func (c *Client) ApplyDDL(ctx context.Context, statements []string) error {
	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.config.URL(),
		Statements: statements,
	}

	op, err := c.spannerAdminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return &Error{
			Code: ErrorCodeUpdateDDL,
			err:  err,
		}
	}

	err = op.Wait(ctx)
	if err != nil {
		return &Error{
			Code: ErrorCodeWaitOperation,
			err:  err,
		}
	}

	return nil
}

func (c *Client) ApplyDMLFile(ctx context.Context, ddl []byte, partitioned bool) (int64, error) {
	statements := toStatements(ddl)

	if partitioned {
		return c.ApplyPartitionedDML(ctx, statements)
	}
	return c.ApplyDML(ctx, statements)
}

func (c *Client) ApplyDML(ctx context.Context, statements []string) (int64, error) {
	numAffectedRows := int64(0)
	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		for _, s := range statements {
			num, err := tx.Update(ctx, spanner.Statement{
				SQL: s,
			})
			if err != nil {
				return err
			}
			numAffectedRows += num
		}
		return nil
	})
	if err != nil {
		return 0, &Error{
			Code: ErrorCodeUpdateDML,
			err:  err,
		}
	}

	return numAffectedRows, nil
}

func (c *Client) ApplyPartitionedDML(ctx context.Context, statements []string) (int64, error) {
	numAffectedRows := int64(0)

	for _, s := range statements {
		num, err := c.spannerClient.PartitionedUpdate(ctx, spanner.Statement{
			SQL: s,
		})
		if err != nil {
			return numAffectedRows, &Error{
				Code: ErrorCodeUpdatePartitionedDML,
				err:  err,
			}
		}

		numAffectedRows += num
	}

	return numAffectedRows, nil
}
func (c *Client) UpgradeExecuteMigrations(ctx context.Context, migrations Migrations, limit int, tableName string) error {
	err := c.backfillMigrations(ctx, migrations, tableName)
	if err != nil {
		return err
	}

	err = c.ExecuteMigrations(ctx, migrations, limit, tableName)
	if err != nil {
		return err
	}

	return c.markUpgradeComplete(ctx)
}

func (c *Client) backfillMigrations(ctx context.Context, migrations Migrations, tableName string) error {
	v, d, err := c.GetSchemaMigrationVersion(ctx, tableName)
	if err != nil {
		return err
	}

	historyTableName := tableName + historyStr
	_, err = c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, rw *spanner.ReadWriteTransaction) error {
		for i := range migrations {
			if v > migrations[i].Version {
				if err := c.upsertVersionHistory(ctx, rw, int64(migrations[i].Version), false, historyTableName); err != nil {
					return err
				}
			} else if v == migrations[i].Version {
				if err := c.upsertVersionHistory(ctx, rw, int64(migrations[i].Version), d, historyTableName); err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) upsertVersionHistory(ctx context.Context, rw *spanner.ReadWriteTransaction, version int64, dirty bool, historyTableName string) error {
	_, err := rw.ReadRow(ctx, historyTableName, spanner.Key{version}, []string{"Version", "Dirty", "Created", "Modified"})
	if err != nil {
		// insert
		if spanner.ErrCode(err) == codes.NotFound {
			return rw.BufferWrite([]*spanner.Mutation{
				spanner.Insert(historyTableName,
					[]string{"Version", "Dirty", "Created", "Modified"},
					[]interface{}{version, dirty, spanner.CommitTimestamp, spanner.CommitTimestamp})})
		}
		return err
	}

	// update
	return rw.BufferWrite([]*spanner.Mutation{
		spanner.Update(historyTableName,
			[]string{"Version", "Dirty", "Modified"},
			[]interface{}{version, dirty, spanner.CommitTimestamp})})
}

func (c *Client) markUpgradeComplete(ctx context.Context) error {
	err := c.ApplyDDL(ctx, []string{"DROP TABLE " + upgradeIndicator})
	if err != nil {
		return &Error{
			Code: ErrorCodeCompleteUpgrade,
			err:  err,
		}
	}

	return nil
}

func (c *Client) GetMigrationHistory(ctx context.Context, versionTableName string) ([]MigrationHistoryRecord, error) {
	history := make([]MigrationHistoryRecord, 0)
	stmt := spanner.NewStatement("SELECT Version, Dirty, Created, Modified FROM " + versionTableName + historyStr)
	err := c.spannerClient.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		version := MigrationHistoryRecord{}
		if err := r.ToStruct(&version); err != nil {
			return err
		}
		history = append(history, version)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return history, nil
}

func (c *Client) ExecuteMigrations(ctx context.Context, migrations Migrations, limit int, tableName string) error {
	sort.Sort(migrations)

	version, dirty, err := c.GetSchemaMigrationVersion(ctx, tableName)
	if err != nil {
		var se *Error
		if !errors.As(err, &se) || se.Code != ErrorCodeNoMigration {
			return &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  err,
			}
		}
	}

	if dirty {
		return &Error{
			Code: ErrorCodeMigrationVersionDirty,
			err:  fmt.Errorf("Database version: %d is dirty, please fix it.", version),
		}
	}

	history, err := c.GetMigrationHistory(ctx, tableName)
	if err != nil {
		return &Error{
			Code: ErrorCodeExecuteMigrations,
			err:  err,
		}
	}
	applied := make(map[int64]bool)
	for i := range history {
		applied[history[i].Version] = true
	}

	var count int
	for _, m := range migrations {
		if limit == 0 {
			break
		}

		if applied[int64(m.Version)] {
			continue
		}

		if err := c.SetSchemaMigrationVersion(ctx, m.Version, true, tableName); err != nil {
			return &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  err,
			}
		}

		switch m.kind {
		case statementKindDDL:
			if err := c.ApplyDDL(ctx, m.Statements); err != nil {
				return &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  err,
				}
			}
		case statementKindDML:
			if _, err := c.ApplyPartitionedDML(ctx, m.Statements); err != nil {
				return &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  err,
				}
			}
		default:
			return &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  fmt.Errorf("Unknown query type, version: %d", m.Version),
			}
		}

		if m.Name != "" {
			fmt.Printf("%d/up %s\n", m.Version, m.Name)
		} else {
			fmt.Printf("%d/up\n", m.Version)
		}

		if err := c.SetSchemaMigrationVersion(ctx, m.Version, false, tableName); err != nil {
			return &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  err,
			}
		}

		count++
		if limit > 0 && count == limit {
			break
		}
	}

	if count == 0 {
		fmt.Println("no change")
	}

	return nil
}

func (c *Client) GetSchemaMigrationVersion(ctx context.Context, tableName string) (uint, bool, error) {
	stmt := spanner.Statement{
		SQL: `SELECT Version, Dirty FROM ` + tableName + ` LIMIT 1`,
	}
	iter := c.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		if err == iterator.Done {
			return 0, false, &Error{
				Code: ErrorCodeNoMigration,
				err:  errors.New("No migration."),
			}
		}
		return 0, false, &Error{
			Code: ErrorCodeGetMigrationVersion,
			err:  err,
		}
	}

	var (
		v     int64
		dirty bool
	)
	if err := row.Columns(&v, &dirty); err != nil {
		return 0, false, &Error{
			Code: ErrorCodeGetMigrationVersion,
			err:  err,
		}
	}

	return uint(v), dirty, nil
}

func (c *Client) SetSchemaMigrationVersion(ctx context.Context, version uint, dirty bool, tableName string) error {
	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		m := []*spanner.Mutation{
			spanner.Delete(tableName, spanner.AllKeys()),
			spanner.Insert(
				tableName,
				[]string{"Version", "Dirty"},
				[]interface{}{int64(version), dirty},
			),
		}
		if err := tx.BufferWrite(m); err != nil {
			return err
		}

		return c.upsertVersionHistory(ctx, tx, int64(version), dirty, tableName+historyStr)
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeSetMigrationVersion,
			err:  err,
		}
	}

	return nil
}

func (c *Client) Close() error {
	c.spannerClient.Close()
	if err := c.spannerAdminClient.Close(); err != nil {
		return &Error{
			err:  err,
			Code: ErrorCodeCloseClient,
		}
	}

	return nil
}

func (c *Client) EnsureMigrationTable(ctx context.Context, tableName string) error {
	fmtErr := func(err error) *Error {
		return &Error{
			Code: ErrorCodeEnsureMigrationTables,
			err:  err,
		}
	}
	status, err := c.DetermineUpgradeStatus(ctx, tableName)
	if err != nil {
		return fmtErr(err)
	}

	switch status {
	case FirstRun:
		if err := c.createVersionTable(ctx, tableName); err != nil {
			return fmtErr(err)
		}
		if err := c.createHistoryTable(ctx, tableName+historyStr); err != nil {
			return fmtErr(err)
		}
	case ExistingMigrationsNoUpgrade:
		if err := c.createUpgradeIndicatorTable(ctx); err != nil {
			return fmtErr(err)
		}
		if err := c.createHistoryTable(ctx, tableName+historyStr); err != nil {
			return fmtErr(err)
		}
	}

	return nil
}

func (c *Client) DetermineUpgradeStatus(ctx context.Context, tableName string) (UpgradeStatus, error) {
	stmt := spanner.NewStatement(`SELECT table_name FROM information_schema.tables WHERE table_catalog = '' AND table_schema = ''
AND table_name in (@version, @history, @indicator)`)
	stmt.Params["version"] = tableName
	stmt.Params["history"] = tableName + historyStr
	stmt.Params["indicator"] = upgradeIndicator
	iter := c.spannerClient.Single().Query(ctx, stmt)

	tables := make(map[string]bool)
	err := iter.Do(func(r *spanner.Row) error {
		t := &table{}
		if err := r.ToStruct(t); err != nil {
			return err
		}
		tables[t.TableName] = true
		return nil
	})
	if err != nil {
		return "", err
	}

	switch {
	case len(tables) == 0:
		return FirstRun, nil
	case len(tables) == 1 && tables[tableName]:
		return ExistingMigrationsNoUpgrade, nil
	case len(tables) == 2 && tables[tableName] && tables[tableName+historyStr]:
		return ExistingMigrationsUpgradeCompleted, nil
	case len(tables) > 1 && tables[tableName] && tables[upgradeIndicator]:
		return ExistingMigrationsUpgradeStarted, nil
	default:
		return "", fmt.Errorf("undetermined state of schema version tables %+v", tables)
	}
}

func (c *Client) tableExists(ctx context.Context, tableName string) bool {
	ri := c.spannerClient.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT table_name FROM information_schema.tables WHERE table_catalog = '' AND table_name = @table",
		Params: map[string]interface{}{"table": tableName},
	})
	defer ri.Stop()
	_, err := ri.Next()
	return err != iterator.Done
}

func (c *Client) createHistoryTable(ctx context.Context, historyTableName string) error {
	if c.tableExists(ctx, historyTableName) {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
    Version INT64 NOT NULL,
	Dirty BOOL NOT NULL,
	Created TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	Modified TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
	) PRIMARY KEY(Version)`, historyTableName)

	return c.ApplyDDL(ctx, []string{stmt})
}

func (c *Client) createUpgradeIndicatorTable(ctx context.Context) error {
	if c.tableExists(ctx, upgradeIndicator) {
		return nil
	}

	stmt := fmt.Sprintf(createUpgradeIndicatorFormatString, upgradeIndicator)

	return c.ApplyDDL(ctx, []string{stmt})
}

func (c *Client) createVersionTable(ctx context.Context, tableName string) error {
	if c.tableExists(ctx, tableName) {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
    Version INT64 NOT NULL,
    Dirty    BOOL NOT NULL
	) PRIMARY KEY(Version)`, tableName)

	return c.ApplyDDL(ctx, []string{stmt})
}
