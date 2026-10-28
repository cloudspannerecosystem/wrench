package spanner

import (
	"strings"
	"testing"
)

func TestHistoryFromRecords(t *testing.T) {
	tests := []struct {
		name      string
		records   []migrationRecord
		legacy    bool
		current   uint
		applied   []uint
		dirty     bool
		wantError string
	}{
		{name: "empty legacy table", legacy: true},
		{name: "legacy version", records: []migrationRecord{{version: 4}}, legacy: true, current: 4, applied: []uint{4}},
		{name: "history with gap", records: []migrationRecord{{version: -1}, {version: 2}, {version: 4}},
			current: 4, applied: []uint{2, 4}},
		{name: "zero version", records: []migrationRecord{{version: -1}, {version: 0}}, applied: []uint{0}},
		{name: "dirty lower version", records: []migrationRecord{{version: -1}, {version: 2, dirty: true}, {version: 4}},
			current: 4, applied: []uint{2, 4}, dirty: true},
		{name: "marker only", records: []migrationRecord{{version: -1}}},
		{name: "ambiguous rows", records: []migrationRecord{{version: 2}, {version: 4}}, wantError: "without a history marker"},
		{name: "negative version", records: []migrationRecord{{version: -2}}, wantError: "invalid migration version"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			history, err := historyFromRecords(test.records)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("expected error containing %q, got %v", test.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if history.legacy != test.legacy || history.current != test.current || (history.dirty != nil) != test.dirty {
				t.Fatalf("got legacy=%t current=%d dirty=%t", history.legacy, history.current, history.dirty != nil)
			}
			if len(history.applied) != len(test.applied) {
				t.Fatalf("got %d applied versions, want %d", len(history.applied), len(test.applied))
			}
			for _, version := range test.applied {
				if _, ok := history.applied[version]; !ok {
					t.Fatalf("version %d is missing", version)
				}
			}
		})
	}
}

func TestLegacyBackfill(t *testing.T) {
	migrations := Migrations{{Version: 0}, {Version: 2}, {Version: 3}, {Version: 5}}
	applied := map[uint]struct{}{3: {}}
	versions := legacyBackfill(migrations, 3, applied)
	if len(versions) != 2 || versions[0] != 0 || versions[1] != 2 {
		t.Fatalf("got backfill versions %v, want [0 2]", versions)
	}
}
