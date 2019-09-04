package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/mercari/wrench/cmd"
	"github.com/mercari/wrench/pkg/spanner"
)

func main() {
	execute()
}

func execute() {
	handleError(cmd.Execute())
}

func handleError(err error) {
	if err != nil {
		fmt.Fprint(os.Stderr, (fmt.Sprintf("%s\n\t%s\n", err.Error(), errorDetails(err))))
		os.Exit(1)
	}
}

func errorDetails(err error) string {
	var se *spanner.Error
	if errors.As(err, &se) {
		switch se.Code {
		case spanner.ErrorCodeCreateClient:
			return fmt.Sprintf("Failed to connect to Cloud Spanner, %s", se.Error())
		case spanner.ErrorCodeExecuteMigrations, spanner.ErrorCodeMigrationVersionDirty:
			return fmt.Sprintf("Failed to execute migration, %s", se.Error())
		default:
			return fmt.Sprintf("Failed to execute the operation to Cloud Spanner, %s", se.Error())
		}
	}

	var pe *os.PathError
	if errors.As(err, &pe) {
		return fmt.Sprintf("Invalid file path, %s", pe.Error())
	}

	if err := errors.Unwrap(err); err != nil {
		return fmt.Sprintf("%s", err.Error())
	}

	return "Unknown error..."
}
