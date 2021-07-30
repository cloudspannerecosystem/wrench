package cmd

import (
	"context"

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
	ctx := context.Background()

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

var instanceDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a instance",
	Long:  "Delete a instance. This command will delete databases immediately and irrevocably disappear",
	RunE:  instanceDelete,
}

func instanceDelete(c *cobra.Command, _ []string) error {
	ctx := context.Background()

	client, err := newSpannerAdminClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.DeleteInstance(ctx, instance)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	return nil
}

func init() {
	instanceCreateCmd.Flags().Int32Var(&node, flagNode, 1, "TODO: ")
	instanceCmd.AddCommand(instanceCreateCmd)

	instanceCmd.AddCommand(instanceDeleteCmd)
}
