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

package cmd

import (
	"bufio"
	"errors"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const flagInteractive = "interactive"

func addInteractiveFlag(c *cobra.Command) {
	c.Flags().BoolP(flagInteractive, "i", false, "Ask for confirmation before running this destructive command")
}

// confirmDestructive asks the user to confirm action when --interactive is set.
// It returns nil without prompting when the flag is not set.
func confirmDestructive(c *cobra.Command, action string) error {
	interactive, err := c.Flags().GetBool(flagInteractive)
	if err != nil || !interactive {
		return nil
	}

	in := c.InOrStdin()
	if in == os.Stdin && !isTerminal(os.Stdin) {
		return errors.New("--interactive requires a terminal on stdin")
	}

	c.PrintErrf("About to %s database projects/%s/instances/%s/databases/%s.\nContinue? [y/N]: ",
		action,
		c.Flag(flagNameProject).Value.String(),
		c.Flag(flagNameInstance).Value.String(),
		c.Flag(flagNameDatabase).Value.String(),
	)

	answer, _ := bufio.NewReader(in).ReadString('\n')
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return nil
	}
	return errors.New("aborted")
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
