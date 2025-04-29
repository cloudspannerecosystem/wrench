package cmd

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestDirectoryFlagNilAndCustomFileSystemFuncIsNil(t *testing.T) {
	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		return nil
	}

	testcases := []struct {
		name  string
		flags []string
		want  bool
	}{
		{
			name:  "directory flag is nil and CustomFileSystemFunc is nil",
			flags: []string{},
			want:  true,
		},
		{
			name:  "directory flag is set and CustomFileSystemFunc is nil",
			flags: []string{"--directory", "test"},
			want:  false,
		},
		{
			name:  "directory flag is nil and CustomFileSystemFunc is set",
			flags: []string{},
			want:  false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			rootCmd.SetArgs(tc.flags)
			err := rootCmd.Execute()
			if (err != nil) != tc.want {
				t.Errorf("Execute() error = %v, wantErr %v", err, tc.want)
			}
		})
	}
}
