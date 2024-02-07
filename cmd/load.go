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
	"os"

	"github.com/spf13/cobra"
)

var (
	skipChangeStream bool
)
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Load schema from server to file",
	RunE:  load,
}

func load(c *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(c.Context(), timeout)
	defer cancel()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	ddl, err := client.LoadDDL(ctx, skipChangeStream)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	err = os.WriteFile(schemaFilePath(c), ddl, 0o664)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	return nil
}

func init() {
	loadCmd.PersistentFlags().BoolVar(&skipChangeStream, flagSkipChangeStream, false, "Skip Change Stream to load")
}
