package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mercari/wrench/pkg/spanner"
	"github.com/spf13/cobra"
	"golang.org/x/xerrors"
)

const (
	migrationsDirName  = "migrations"
	migrationTableName = "SchemaMigrations"
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
	ctx := context.Background()

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

func migrateVersion(c *cobra.Command, args []string) error {
	ctx := context.Background()

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
		se := &spanner.Error{}
		if xerrors.As(err, &se) && se.Code == spanner.ErrorCodeNoMigration {
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
	ctx := context.Background()

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
		return "", errors.New("Invalid migration file name.")
	}

	ms, err := spanner.LoadMigrations(dir)
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
	fp.Close()

	return filename, nil
}
