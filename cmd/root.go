package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	Version         = "unknown"
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
)

var rootCmd = &cobra.Command{
	Use: "wrench",
}

func Execute() error {
	return rootCmd.Execute()
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

	rootCmd.PersistentFlags().StringVar(&project, flagNameProject, spannerProjectID(), "GCP project id (optional. if not set, will use $SPANNER_PROJECT_ID or $GOOGLE_CLOUD_PROJECT value)")
	rootCmd.PersistentFlags().StringVar(&instance, flagNameInstance, spannerInstanceID(), "Cloud Spanner instance name (optional. if not set, will use $SPANNER_INSTANCE_ID value)")
	rootCmd.PersistentFlags().StringVar(&database, flagNameDatabase, spannerDatabaseID(), "Cloud Spanner database name (optional. if not set, will use $SPANNER_DATABASE_ID value)")
	rootCmd.PersistentFlags().StringVar(&directory, flagNameDirectory, "", "Directory that schema file placed (required)")
	rootCmd.PersistentFlags().StringVar(&schemaFile, flagNameSchemaFile, "", "Name of schema file (optional. if not set, will use default 'schema.sql' file name)")
	rootCmd.PersistentFlags().StringVar(&credentialsFile, flagCredentialsFile, "", "Specify Credentials File")

	rootCmd.Version = Version
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
