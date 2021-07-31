package cmd

import (
	"github.com/spf13/cobra"
)

var node int32

var instanceCmd = &cobra.Command{
	Use: "instance",
}

var instanceCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a instance",
	RunE:  instanceCreate,
}

func instanceCreate(c *cobra.Command, _ []string) error {
	ctx := c.Context()

	client, err := newSpannerAdminClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.CreateInstance(ctx, node)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	return nil
}

func init() {
	instanceCmd.AddCommand(instanceCreateCmd)

	instanceCmd.PersistentFlags().Int32Var(&node, flagNode, 1, "TODO: ")
}
