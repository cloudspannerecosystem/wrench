package cmd

import (
	"context"
	"io/ioutil"

	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create database with tables described in schema file",
	RunE:  create,
}

func create(c *cobra.Command, _ []string) error {
	ctx := context.Background()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	ddl, err := ioutil.ReadFile(schemaFilePath(c))
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	err = client.CreateDatabase(ctx, ddl)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	return nil
}
