package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

type Error struct {
	err error
	cmd *cobra.Command
}

func (e *Error) Error() string {
	return fmt.Sprintf("Error command: %s, version: %s", e.cmd.Name(), Version)
}

func (e *Error) Unwrap() error {
	return e.err
}
