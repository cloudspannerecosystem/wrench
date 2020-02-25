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

package cmd_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cloudspannerecosystem/wrench/cmd"
)

func TestCreateMigrationFile(t *testing.T) {
	testdatadir := filepath.Join("testdata", "migrations")

	testcases := []struct {
		filename     string
		digits       int
		wantFilename string
	}{
		{
			filename:     "foo",
			digits:       6,
			wantFilename: filepath.Join(testdatadir, "000003_foo.sql"),
		},
		{
			filename:     "bar",
			digits:       0,
			wantFilename: filepath.Join(testdatadir, "3_bar.sql"),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.filename, func(t *testing.T) {
			filename, err := cmd.CreateMigrationFile(testdatadir, tc.filename, tc.digits)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.Remove(filename)
			}()

			if tc.wantFilename != filename {
				t.Errorf("filename want %v, but got %v", tc.wantFilename, filename)
			}
		})
	}

	t.Run("invalid name", func(t *testing.T) {
		_, err := cmd.CreateMigrationFile(testdatadir, "あああ", 6)
		if err.Error() != "Invalid migration file name." {
			t.Errorf("err want `invalid name`, but got `%v`", err)
		}
	})
}
