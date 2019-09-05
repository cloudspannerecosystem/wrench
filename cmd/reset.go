package cmd

import (
	"errors"

	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Equivalent to drop and then create",
	RunE:  reset,
}

func reset(c *cobra.Command, args []string) error {
	if err := drop(c, args); err != nil {
		return errorReset(c, err)
	}

	if err := create(c, args); err != nil {
		return errorReset(c, err)
	}

	return nil
}

func errorReset(c *cobra.Command, err error) error {
	if ue := errors.Unwrap(err); ue != nil {
		return &Error{
			cmd: c,
			err: ue,
		}
	}

	return &Error{
		cmd: c,
		err: err,
	}
}
