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
	"io/fs"
	"os"
	"runtime/debug"
	"time"

	wrenchfs "github.com/cloudspannerecosystem/wrench/internal/fs"
	"github.com/spf13/cobra"
)

var (
	version         = ""
	versionTemplate = `{{.Version}}
`
)

var (
	project         string
	instance        string
	database        string
	directory       string
	schemaFile      string
	credentialsFile string
	timeout         time.Duration
)

// CustomFileSystemFunc is a function that returns a custom fs.FS.
// This variable allows customizing what kind of fs.FS should be use in wrench CLI execution.
// e.g. embed.FS for use.
var CustomFileSystemFunc func() fs.FS

var rootCmd = &cobra.Command{
	Use: "wrench",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if CustomFileSystemFunc != nil {
			ctx := cmd.Context()
			ctx = wrenchfs.WithContext(ctx, CustomFileSystemFunc())
			cmd.SetContext(ctx)
		}

		return nil
	},
}

func Execute(ctx context.Context) error {
	return rootCmd.ExecuteContext(ctx)
}

func init() {
	cobra.EnableCommandSorting = false

	rootCmd.SilenceUsage = true
	rootCmd.SilenceErrors = true

	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(dropCmd)
	rootCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(loadCmd)
	rootCmd.AddCommand(applyCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(truncateCmd)
	rootCmd.AddCommand(instanceCmd)

	rootCmd.PersistentFlags().StringVar(&project, flagNameProject, spannerProjectID(), "GCP project id (optional. if not set, will use $SPANNER_PROJECT_ID or $GOOGLE_CLOUD_PROJECT value)")
	rootCmd.PersistentFlags().StringVar(&instance, flagNameInstance, spannerInstanceID(), "Cloud Spanner instance name (optional. if not set, will use $SPANNER_INSTANCE_ID value)")
	rootCmd.PersistentFlags().StringVar(&database, flagNameDatabase, spannerDatabaseID(), "Cloud Spanner database name (optional. if not set, will use $SPANNER_DATABASE_ID value)")
	rootCmd.PersistentFlags().StringVar(&directory, flagNameDirectory, "", "Directory that schema file placed (required)")
	rootCmd.PersistentFlags().StringVar(&schemaFile, flagNameSchemaFile, "", "Name of schema file (optional. if not set, will use default 'schema.sql' file name)")
	rootCmd.PersistentFlags().StringVar(&credentialsFile, flagCredentialsFile, "", "Specify Credentials File")
	rootCmd.PersistentFlags().DurationVar(&timeout, flagTimeout, time.Hour, "Context timeout")

	rootCmd.Version = versionInfo()
	rootCmd.SetVersionTemplate(versionTemplate)
}

func spannerProjectID() string {
	projectID := os.Getenv("SPANNER_PROJECT_ID")
	if projectID != "" {
		return projectID
	}
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func spannerInstanceID() string {
	return os.Getenv("SPANNER_INSTANCE_ID")
}

func spannerDatabaseID() string {
	return os.Getenv("SPANNER_DATABASE_ID")
}

func versionInfo() string {
	if version != "" {
		return version
	}

	// For those who "go install" yo
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return info.Main.Version
}
