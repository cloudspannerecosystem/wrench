package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/spf13/cobra"
)

var (
	ddlFile     string
	dmlFile     string
	partitioned bool
)

var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply DDL file to database",
	RunE:  apply,
}

func apply(c *cobra.Command, _ []string) error {
	ctx := context.Background()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	if ddlFile != "" {
		if dmlFile != "" {
			return errors.New("Cannot specify DDL and DML at same time.")
		}

		ddl, err := ioutil.ReadFile(ddlFile)
		if err != nil {
			return &Error{
				err: err,
				cmd: c,
			}
		}

		err = client.ApplyDDLFile(ctx, ddl)
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
	dml, err := ioutil.ReadFile(dmlFile)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	numAffectedRows, err := client.ApplyDMLFile(ctx, dml, partitioned)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}
	fmt.Printf("%d rows affected.\n", numAffectedRows)

	return nil
}

func init() {
	applyCmd.PersistentFlags().StringVar(&ddlFile, flagDDLFile, "", "DDL file to be applied")
	applyCmd.PersistentFlags().StringVar(&dmlFile, flagDMLFile, "", "DML file to be applied")
	applyCmd.PersistentFlags().BoolVar(&partitioned, flagPartitioned, false, "Whether given DML should be executed as a Partitioned-DML or not")
}
