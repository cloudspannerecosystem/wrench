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
	"errors"
	"fmt"
	"github.com/cloudspannerecosystem/wrench/internal/fs"
	"github.com/cloudspannerecosystem/wrench/pkg/spanner"
	"github.com/spf13/cobra"
)

var (
	ddlFile     string
	dmlFile     string
	partitioned bool
	priority    string
)

var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply DDL file to database",
	RunE:  apply,
}

func apply(c *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(c.Context(), timeout)
	defer cancel()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	if ddlFile != "" {
		if dmlFile != "" {
			return errors.New("cannot specify DDL and DML at same time")
		}

		ddl, err := fs.ReadFile(ctx, ddlFile)
		if err != nil {
			return &Error{
				err: err,
				cmd: c,
			}
		}

		var protoDescriptor []byte
		protoDescriptorFile := protoDescriptorFilePath(c)
		if protoDescriptorFile != "" {
			protoDescriptor, err = fs.ReadFile(ctx, protoDescriptorFile)
			if err != nil {
				return &Error{
					err: err,
					cmd: c,
				}
			}
		}

		err = client.ApplyDDLFile(ctx, ddlFile, ddl, protoDescriptor)
		if err != nil {
			return &Error{
				err: err,
				cmd: c,
			}
		}

		return nil
	}

	if dmlFile == "" {
		return errors.New("Must specify DDL or DML.")
	}

	// apply dml
	dml, err := fs.ReadFile(ctx, dmlFile)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	p, err := priorityTypeOf(priority)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	numAffectedRows, err := client.ApplyDMLFile(ctx, dmlFile, dml, partitioned, p)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}
	fmt.Printf("%d rows affected.\n", numAffectedRows)

	return nil
}

const (
	priorityTypeHigh   = "high"
	priorityTypeMedium = "medium"
	priorityTypeLow    = "low"
)

func priorityTypeOf(prioirty string) (spanner.PriorityType, error) {
	switch prioirty {
	case priorityTypeHigh:
		return spanner.PriorityTypeHigh, nil
	case priorityTypeMedium:
		return spanner.PriorityTypeMedium, nil
	case priorityTypeLow:
		return spanner.PriorityTypeLow, nil
	case "":
		return spanner.PriorityTypeUnspecified, nil
	default:
		return 0, fmt.Errorf(
			"%s is unsupported priority, it must be one of %s, %s, or %s",
			priority, priorityTypeHigh, priorityTypeMedium, priorityTypeLow,
		)
	}
}

func init() {
	applyCmd.PersistentFlags().StringVar(&ddlFile, flagDDLFile, "", "DDL file to be applied")
	applyCmd.PersistentFlags().StringVar(&dmlFile, flagDMLFile, "", "DML file to be applied")
	applyCmd.PersistentFlags().BoolVar(&partitioned, flagPartitioned, false, "Whether given DML should be executed as a Partitioned-DML or not")
	applyCmd.PersistentFlags().StringVar(&priority, flagPriority, "", "The priority to apply DML(optional)")
}
