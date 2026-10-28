package spanner

import (
	"strings"
	"testing"
)

func TestPendingMigration(t *testing.T) {
	migrations := Migrations{{Version: 2}, {Version: 4}, {Version: 6}}

	applied := map[uint]struct{}{2: {}}
	selected, err := pendingMigration(migrations, 4, applied)
	if err != nil || selected != migrations[1] {
		t.Fatalf("expected migration version 4, got %v, %v", selected, err)
	}

	_, err = pendingMigration(migrations, 3, applied)
	if err == nil || !strings.Contains(err.Error(), "migration version 3 was not found") {
		t.Fatalf("expected missing version error, got %v", err)
	}

	_, err = pendingMigration(migrations, 2, applied)
	if err == nil || !strings.Contains(err.Error(), "migration version 2 is already applied") {
		t.Fatalf("expected applied version error, got %v", err)
	}

	applied[6] = struct{}{}
	selected, err = pendingMigration(migrations, 4, applied)
	if err != nil || selected != migrations[1] {
		t.Fatalf("expected lower pending migration, got %v, %v", selected, err)
	}
}
