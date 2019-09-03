package cmd_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mercari/wrench/cmd"
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
