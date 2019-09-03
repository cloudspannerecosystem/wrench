package spanner_test

import (
	"path/filepath"
	"testing"

	"github.com/mercari/wrench/pkg/spanner"
)

func TestLoadMigrations(t *testing.T) {
	ms, err := spanner.LoadMigrations(filepath.Join("testdata", "migrations"))
	if err != nil {
		t.Fatal(err)
	}

	if len(ms) != 3 {
		t.Fatalf("migrations length want 3, but got %v", len(ms))
	}

	testcases := []struct {
		idx         int
		wantVersion uint
		wantName    string
	}{
		{
			idx:         0,
			wantVersion: 2,
			wantName:    "test",
		},
		{
			idx:         1,
			wantVersion: 3,
			wantName:    "",
		},
	}

	for _, tc := range testcases {
		if ms[tc.idx].Version != tc.wantVersion {
			t.Errorf("migrations[%d].version want %v, but got %v", tc.idx, tc.wantVersion, ms[tc.idx].Version)
		}

		if ms[tc.idx].Name != tc.wantName {
			t.Errorf("migrations[%d].name want %v, but got %v", tc.idx, tc.wantName, ms[tc.idx].Name)
		}
	}
}
