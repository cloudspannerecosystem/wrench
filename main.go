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
		return err.Error()
	}

	return "Unknown error..."
}
