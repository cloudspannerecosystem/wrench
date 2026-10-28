package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestMigrateUpSelection(t *testing.T) {
	up, _, err := migrateCmd.Find([]string{"up"})
	if err != nil || up.Flags().Lookup("only") == nil {
		t.Fatalf("migrate up must register --only: %v", err)
	}

	tests := []struct {
		name     string
		args     []string
		only     string
		setOnly  bool
		limit    int
		version  uint
		wantOnly bool
		wantErr  string
	}{
		{name: "all", limit: -1},
		{name: "count", args: []string{"2"}, limit: 2},
		{name: "zero count", args: []string{"0"}, limit: 0},
		{name: "only", only: "42", setOnly: true, limit: -1, version: 42, wantOnly: true},
		{name: "only with count", args: []string{"2"}, only: "42", setOnly: true, wantErr: "--only cannot be used with a migration count"},
		{name: "invalid only", only: "abc", setOnly: true, wantErr: "invalid migration version"},
		{name: "zero only", only: "0", setOnly: true, limit: -1, version: 0, wantOnly: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := &cobra.Command{}
			command.Flags().String("only", "", "")
			if test.setOnly {
				if err := command.Flags().Set("only", test.only); err != nil {
					t.Fatal(err)
				}
			}
			limit, version, only, err := migrateUpSelection(command, test.args)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("expected error containing %q, got %v", test.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if limit != test.limit || version != test.version || only != test.wantOnly {
				t.Fatalf("got limit=%d version=%d only=%t; want limit=%d version=%d only=%t",
					limit, version, only, test.limit, test.version, test.wantOnly)
			}
		})
	}
}
