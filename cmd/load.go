package cmd

import (
	"context"
	"io/ioutil"

	"github.com/spf13/cobra"
)

var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Load schema from server to file",
	RunE:  load,
}

func load(c *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	ddl, err := client.LoadDDL(ctx)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	err = ioutil.WriteFile(schemaFilePath(c), ddl, 0664)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	return nil
}
