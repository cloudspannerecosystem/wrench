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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/cloudspannerecosystem/wrench/pkg/spanner"
)

const (
	migrationsDirName  = "migrations"
	migrationTableName = "SchemaMigrations"
)

const (
	createMigrationFileLayout = "20060102150405"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate database",
}

func init() {
	migrateCreateCmd := &cobra.Command{
		Use:   "create NAME",
		Short: "Create a set of sequential up migrations in directory",
		RunE:  migrateCreate,
	}
	migrateUpCmd := &cobra.Command{
		Use:   "up [N]",
		Short: "Apply all or N up migrations",
		RunE:  migrateUp,
	}
	migrateVersionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print current migration version",
		RunE:  migrateVersion,
	}
	migrateSetCmd := &cobra.Command{
		Use:   "set V",
		Short: "Set version V but don't run migration (ignores dirty state)",
		RunE:  migrateSet,
	}

	migrateCmd.AddCommand(
		migrateCreateCmd,
		migrateUpCmd,
		migrateVersionCmd,
		migrateSetCmd,
	)

	migrateCmd.PersistentFlags().String(flagNameDirectory, "", "Directory that migration files placed (required)")
}

func migrateCreate(c *cobra.Command, args []string) error {
	name := ""

	if len(args) > 0 {
		name = args[0]
	}

	dir := filepath.Join(c.Flag(flagNameDirectory).Value.String(), migrationsDirName)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
			return &Error{
				cmd: c,
				err: err,
			}
		}
	}

	filename, err := createMigrationFile(dir, name, 6)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	fmt.Printf("%s is created\n", filename)

	return nil
}

func migrateUp(c *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(c.Context(), timeout)
	defer cancel()

	limit := -1
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil {
			return &Error{
				cmd: c,
				err: err,
			}
		}
		limit = n
	}

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	if err = client.EnsureMigrationTable(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	dir := filepath.Join(c.Flag(flagNameDirectory).Value.String(), migrationsDirName)
	migrations, err := spanner.LoadMigrations(dir)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	return client.ExecuteMigrations(ctx, migrations, limit, migrationTableName)
}

func migrateVersion(c *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(c.Context(), timeout)
	defer cancel()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	if err = client.EnsureMigrationTable(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	v, _, err := client.GetSchemaMigrationVersion(ctx, migrationTableName)
	if err != nil {
		var se *spanner.Error
		if errors.As(err, &se) && se.Code == spanner.ErrorCodeNoMigration {
			fmt.Println("No migrations.")
			return nil
		}
		return &Error{
			cmd: c,
			err: err,
		}
	}

	fmt.Println(v)

	return nil
}

func migrateSet(c *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(c.Context(), timeout)
	defer cancel()

	if len(args) == 0 {
		return &Error{
			cmd: c,
			err: errors.New("Parameters are not passed."),
		}
	}
	version, err := strconv.Atoi(args[0])
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	if err = client.EnsureMigrationTable(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	if err := client.SetSchemaMigrationVersion(ctx, uint(version), false, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	return nil
}

func createMigrationFile(dir string, name string, digits int) (string, error) {
	if name != "" && !spanner.MigrationNameRegex.MatchString(name) {
		return "", errors.New("invalid migration file name")
	}

	fileTimestampStr := time.Now().Format(createMigrationFileLayout)
	var filename string
	filename = filepath.Join(dir, fmt.Sprintf("%s.sql", fileTimestampStr))
	if name != "" {
		filename = filepath.Join(dir, fmt.Sprintf("%s_%s.sql", fileTimestampStr, name))
	}

	fp, err := os.Create(filename)
	defer func() {
		if defErr := fp.Close(); defErr != nil {
			err = defErr
		}
	}()
	if err != nil {
		return "", err
	}

	return filename, nil
}
