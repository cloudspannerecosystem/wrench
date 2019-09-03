package spanner

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"golang.org/x/xerrors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	ddlStatementsSeparator = ";"
)

type Client struct {
	config             *Config
	spannerClient      *spanner.Client
	spannerAdminClient *admin.DatabaseAdminClient
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
	} else {
		return c.ApplyDML(ctx, statements)
	}
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

func (c *Client) ExecuteMigrations(ctx context.Context, migrations Migrations, limit int, tableName string) error {
	sort.Sort(migrations)

	version, dirty, err := c.GetSchemaMigrationVersion(ctx, tableName)
	if err != nil {
		se := &Error{}
		if !xerrors.As(err, &se) || se.Code != ErrorCodeNoMigration {
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

	var count int
	for _, m := range migrations {
		if limit == 0 {
			break
		}

		if m.Version <= version {
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

	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
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

	return 0, false, &Error{
		Code: ErrorCodeNoMigration,
		err:  errors.New("No migration."),
	}
}

func (c *Client) SetSchemaMigrationVersion(ctx context.Context, version uint, dirty bool, tableName string) error {
	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		m := []*spanner.Mutation{
			spanner.Delete(tableName, spanner.AllKeys()),
			spanner.Insert(
				tableName,
				[]string{"Version", "Dirty"},
				[]interface{}{int64(version), dirty},
			)}
		return tx.BufferWrite(m)
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeSetMigrationVersion,
			err:  err,
		}
	}

	return nil
}

func (c *Client) EnsureMigrationTable(ctx context.Context, tableName string) error {
	iter := c.spannerClient.Single().Read(ctx, tableName, spanner.AllKeys(), []string{"Version"})
	err := iter.Do(func(r *spanner.Row) error {
		return nil
	})
	if err == nil {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
    Version INT64 NOT NULL,
    Dirty    BOOL NOT NULL
	) PRIMARY KEY(Version)`, tableName)

	return c.ApplyDDL(ctx, []string{stmt})
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
