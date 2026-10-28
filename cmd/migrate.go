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
	"strings"

	"github.com/cloudspannerecosystem/wrench/internal/fs"
	"github.com/cloudspannerecosystem/wrench/pkg/spanner"
	"github.com/spf13/cobra"
)

const (
	migrationsDirName = "migrations"
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
		Short: "Print highest recorded migration version",
		RunE:  migrateVersion,
	}
	migrateSetCmd := &cobra.Command{
		Use:   "set V",
		Short: "Set version V but don't run migration (ignores dirty state)",
		RunE:  migrateSet,
	}

	migrateUpCmd.Flags().String(flagProtoDescriptorFile, "", "Proto descriptor file to be used with migrations")
	migrateUpCmd.Flags().String("only", "", "Apply only the pending migration with this version")

	migrateCmd.AddCommand(
		migrateCreateCmd,
		migrateUpCmd,
		migrateVersionCmd,
		migrateSetCmd,
	)

	migrateCmd.PersistentFlags().String(flagNameDirectory, "", "Directory that migration files placed (required)")
	migrateCmd.PersistentFlags().String(flagMigrationTableName, defaultMigrationTableName, "Name of the migration tracking table")

	migrateUpCmd.PersistentFlags().StringVar(&priority, flagPriority, "", "The priority to apply DML (optional)")
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

	filename, err := createMigrationFile(c.Context(), dir, name, 6)
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

	limit, only, onlySet, err := migrateUpSelection(c, args)
	if err != nil {
		return &Error{cmd: c, err: err}
	}

	priorityType, err := priorityTypeOf(priority)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	migrationTableName, err := getMigrationTableName(c)
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
	defer func() {
		if err := client.Close(); err != nil {
			c.PrintErrf("failed to close Spanner client: %v\n", err)
		}
	}()

	if err = client.EnsureMigrationTable(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	dir := filepath.Join(c.Flag(flagNameDirectory).Value.String(), migrationsDirName)
	migrations, err := spanner.ReadMigrations(ctx, dir)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	var protoDescriptor []byte
	protoDescriptorFile := protoDescriptorFilePath(c)
	if protoDescriptorFile != "" {
		protoDescriptor, err = fs.ReadFile(ctx, protoDescriptorFile)
		if err != nil {
			return &Error{
				err: err,
				cmd: c,
			}
		}
	}

	if onlySet {
		return client.ExecuteMigrationVersion(ctx, migrations, only, migrationTableName, priorityType, protoDescriptor)
	}
	return client.ExecuteMigrations(ctx, migrations, limit, migrationTableName, priorityType, protoDescriptor)
}

func migrateUpSelection(c *cobra.Command, args []string) (int, uint, bool, error) {
	if c.Flags().Changed("only") {
		if len(args) > 0 {
			return 0, 0, false, errors.New("--only cannot be used with a migration count")
		}
		value, err := c.Flags().GetString("only")
		if err != nil {
			return 0, 0, false, err
		}
		version, err := strconv.ParseUint(value, 10, 63)
		if err != nil {
			return 0, 0, false, fmt.Errorf("invalid migration version %q: must be a nonnegative Spanner INT64", value)
		}
		if version > uint64(^uint(0)) {
			return 0, 0, false, fmt.Errorf("migration version %q exceeds supported range", value)
		}
		return -1, uint(version), true, nil
	}

	limit := -1
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil {
			return 0, 0, false, err
		}
		limit = n
	}
	return limit, 0, false, nil
}

func migrateVersion(c *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(c.Context(), timeout)
	defer cancel()

	migrationTableName, err := getMigrationTableName(c)
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
	defer func() {
		if err := client.Close(); err != nil {
			c.PrintErrf("failed to close Spanner client: %v\n", err)
		}
	}()

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
			err: errors.New("parameters are not passed"),
		}
	}
	version, err := strconv.ParseUint(args[0], 10, 63)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}
	if version > uint64(^uint(0)) {
		return &Error{cmd: c, err: fmt.Errorf("migration version %d exceeds supported range", version)}
	}

	migrationTableName, err := getMigrationTableName(c)
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
	defer func() {
		if err := client.Close(); err != nil {
			c.PrintErrf("failed to close Spanner client: %v\n", err)
		}
	}()

	if err = client.EnsureMigrationTable(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	if c.Flag(flagNameDirectory).Value.String() == "" {
		if err := client.SetMigrationBaseline(ctx, uint(version), migrationTableName); err != nil {
			return &Error{cmd: c, err: err}
		}
		return nil
	}

	dir := filepath.Join(c.Flag(flagNameDirectory).Value.String(), migrationsDirName)
	migrations, err := spanner.ReadMigrations(ctx, dir)
	if err != nil {
		return &Error{cmd: c, err: err}
	}
	if err := client.ResetMigrationHistory(ctx, migrations, uint(version), migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	return nil
}

func createMigrationFile(ctx context.Context, dir string, name string, digits int) (string, error) {
	if name != "" && !spanner.MigrationNameRegex.MatchString(name) {
		return "", errors.New("invalid migration file name")
	}

	ms, err := spanner.ReadMigrations(ctx, dir)
	if err != nil {
		return "", err
	}

	var v uint = 1
	if len(ms) > 0 {
		v = ms[len(ms)-1].Version + 1
	}
	vStr := fmt.Sprint(v)

	padding := digits - len(vStr)
	if padding > 0 {
		vStr = strings.Repeat("0", padding) + vStr
	}

	var filename string
	if name == "" {
		filename = filepath.Join(dir, fmt.Sprintf("%s.sql", vStr))
	} else {
		filename = filepath.Join(dir, fmt.Sprintf("%s_%s.sql", vStr, name))
	}

	fp, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	if err := fp.Close(); err != nil {
		return "", err
	}

	return filename, nil
}
