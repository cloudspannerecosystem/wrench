package cmd

import (
	"context"
	"path/filepath"

	"github.com/mercari/wrench/pkg/spanner"
	"github.com/spf13/cobra"
)

const (
	flagNameProject       = "project"
	flagNameInstance      = "instance"
	flagNameDatabase      = "database"
	flagNameDirectory     = "directory"
	flagCredentialsFile   = "credentials_file"
	flagNameSchemaFile    = "schema_file"
	flagDDLFile           = "ddl"
	flagDMLFile           = "dml"
	flagPartitioned       = "partitioned"
	defaultSchemaFileName = "schema.sql"
)

func newSpannerClient(ctx context.Context, c *cobra.Command) (*spanner.Client, error) {
	config := &spanner.Config{
		Project:         c.Flag(flagNameProject).Value.String(),
		Instance:        c.Flag(flagNameInstance).Value.String(),
		Database:        c.Flag(flagNameDatabase).Value.String(),
		CredentialsFile: c.Flag(flagCredentialsFile).Value.String(),
	}

	client, err := spanner.NewClient(ctx, config)
	if err != nil {
		return nil, &Error{
			err: err,
			cmd: c,
		}
	}

	return client, nil
}

func schemaFilePath(c *cobra.Command) string {
	filename := c.Flag(flagNameSchemaFile).Value.String()
	if filename == "" {
		filename = defaultSchemaFileName
	}
	return filepath.Join(c.Flag(flagNameDirectory).Value.String(), filename)
}
