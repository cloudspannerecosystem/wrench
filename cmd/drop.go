package cmd

import (
	"context"

	"github.com/spf13/cobra"
)

var dropCmd = &cobra.Command{
	Use:   "drop",
	Short: "Drop database",
	RunE:  drop,
}

func drop(c *cobra.Command, _ []string) error {
	ctx := context.Background()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.DropDatabase(ctx)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	return nil
}
