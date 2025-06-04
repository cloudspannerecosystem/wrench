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
	"sort"

	"cloud.google.com/go/spanner"
	databasev1 "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/hashicorp/go-multierror"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	ddlStatementsSeparator = ";"
)

type table struct {
	TableName string `spanner:"table_name"`
}

type Client struct {
	config             *Config
	spannerClient      *spanner.Client
	spannerAdminClient *databasev1.DatabaseAdminClient
}

func NewClient(ctx context.Context, config *Config) (*Client, error) {
	var opts []option.ClientOption

	// The options passed by config are evaluated first.
	// Most options are last win so the options can be overridden by another option.
	opts = append(opts, config.ClientOptions...)

	if config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}

	spannerClient, err := spanner.NewClientWithConfig(ctx, config.URL(),
		spanner.ClientConfig{
			SessionPoolConfig: spanner.SessionPoolConfig{
				MaxOpened:                         spanner.DefaultSessionPoolConfig.MaxOpened,
				MinOpened:                         1,
				MaxIdle:                           spanner.DefaultSessionPoolConfig.MaxIdle,
				HealthCheckWorkers:                spanner.DefaultSessionPoolConfig.HealthCheckWorkers,
				HealthCheckInterval:               spanner.DefaultSessionPoolConfig.HealthCheckInterval,
				TrackSessionHandles:               false,
				InactiveTransactionRemovalOptions: spanner.DefaultSessionPoolConfig.InactiveTransactionRemovalOptions,
			},
		},
		opts...)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeCreateClient,
			err:  fmt.Errorf("failed to create spanner client: %w", err),
		}
	}

	spannerAdminClient, err := databasev1.NewDatabaseAdminClient(ctx, opts...)
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

func (c *Client) CreateDatabase(ctx context.Context, filename string, ddl []byte, protoDescriptors []byte) error {
	statements, err := ddlToStatements(filename, ddl)
	if err != nil {
		return &Error{
			Code: ErrorCodeLoadSchema,
			err:  err,
		}
	}

	createReq := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", c.config.Project, c.config.Instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", c.config.Database),
		ExtraStatements: statements,
		ProtoDescriptors: protoDescriptors,
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
	var stms []spanner.Statement

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

		stms = append(stms, spanner.NewStatement(fmt.Sprintf("DELETE FROM `%s` WHERE true", t.TableName)))
		return nil
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeTruncateAllTables,
			err:  err,
		}
	}

	g := &multierror.Group{}
	for _, stmt := range stms {
		stmt := stmt
		g.Go(func() error {
			_, err := c.spannerClient.PartitionedUpdate(ctx, stmt)
			return err
		})
	}
	if err := g.Wait(); err != nil {
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

func (c *Client) ApplyDDLFile(ctx context.Context, filename string, ddl []byte, protoDescriptors []byte) error {
	statements, err := ddlToStatements(filename, ddl)
	if err != nil {
		return err
	}

	return c.ApplyDDL(ctx, statements, protoDescriptors)
}

func (c *Client) ApplyDDL(ctx context.Context, statements []string, protoDescriptors []byte) error {
	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.config.URL(),
		Statements: statements,
		ProtoDescriptors: protoDescriptors,
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

type PriorityType int

const (
	PriorityTypeUnspecified PriorityType = iota
	PriorityTypeHigh
	PriorityTypeMedium
	PriorityTypeLow
)

func (c *Client) ApplyDMLFile(ctx context.Context, filename string, ddl []byte, partitioned bool, priority PriorityType) (int64, error) {
	statements, err := dmlToStatements(filename, ddl)
	if err != nil {
		return 0, err
	}

	if partitioned {
		return c.ApplyPartitionedDML(ctx, statements, priority)
	}
	return c.ApplyDML(ctx, statements, priority)
}

func (c *Client) ApplyDML(ctx context.Context, statements []string, priority PriorityType) (int64, error) {
	p := priorityPBOf(priority)
	numAffectedRows := int64(0)
	_, err := c.spannerClient.ReadWriteTransactionWithOptions(
		ctx,
		func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			stmts := make([]spanner.Statement, len(statements))
			for i, s := range statements {
				stmts[i] = spanner.Statement{SQL: s}
			}
			counts, err := tx.BatchUpdateWithOptions(ctx, stmts, spanner.QueryOptions{
				Priority: p,
			})
			if err != nil {
				return err
			}

			for _, num := range counts {
				numAffectedRows += num
			}

			return nil
		},
		spanner.TransactionOptions{
			CommitPriority: p,
		},
	)
	if err != nil {
		return 0, &Error{
			Code: ErrorCodeUpdateDML,
			err:  err,
		}
	}

	return numAffectedRows, nil
}

func (c *Client) ApplyPartitionedDML(ctx context.Context, statements []string, priority PriorityType) (int64, error) {
	p := priorityPBOf(priority)
	numAffectedRows := int64(0)
	for _, s := range statements {
		num, err := c.spannerClient.PartitionedUpdateWithOptions(ctx, spanner.Statement{
			SQL: s,
		}, spanner.QueryOptions{
			Priority: p,
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

func (c *Client) ExecuteMigrations(ctx context.Context, migrations Migrations, limit int, tableName string, protoDescriptors []byte) error {
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
			err:  fmt.Errorf("database version: %d is dirty, please fix it", version),
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
			if err := c.ApplyDDL(ctx, m.Statements, protoDescriptors); err != nil {
				return &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  err,
				}
			}
		case statementKindDML:
			if _, err := c.ApplyDML(ctx, m.Statements, PriorityTypeUnspecified); err != nil {
				return &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  err,
				}
			}
		case statementKindPartitionedDML:
			if _, err := c.ApplyPartitionedDML(ctx, m.Statements, PriorityTypeUnspecified); err != nil {
				return &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  err,
				}
			}
		default:
			return &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  fmt.Errorf("unknown query type, version: %d", m.Version),
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
		if errors.Is(err, iterator.Done) {
			return 0, false, &Error{
				Code: ErrorCodeNoMigration,
				err:  errors.New("no migration"),
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
	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(_ context.Context, tx *spanner.ReadWriteTransaction) error {
		m := []*spanner.Mutation{
			spanner.Delete(tableName, spanner.AllKeys()),
			spanner.Insert(
				tableName,
				[]string{"Version", "Dirty"},
				[]interface{}{int64(version), dirty},
			),
		}
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

	return c.ApplyDDL(ctx, []string{stmt}, nil)
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

func priorityPBOf(priority PriorityType) sppb.RequestOptions_Priority {
	switch priority {
	case PriorityTypeHigh:
		return sppb.RequestOptions_PRIORITY_HIGH
	case PriorityTypeMedium:
		return sppb.RequestOptions_PRIORITY_MEDIUM
	case PriorityTypeLow:
		return sppb.RequestOptions_PRIORITY_LOW
	case PriorityTypeUnspecified:
		return sppb.RequestOptions_PRIORITY_UNSPECIFIED
	default:
		return sppb.RequestOptions_PRIORITY_UNSPECIFIED
	}
}
