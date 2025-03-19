package cmd

import (
	"os"
	"testing"
)

func Test_spannerProjectID(t *testing.T) {
	t.Setenv("SPANNER_PROJECT_ID", "spanner-project")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "gc-project")

	if got := spannerProjectID(); got != "spanner-project" {
		t.Errorf("spannerProjectID() = %v, want %v", got, "env-project")
	}

	os.Unsetenv("SPANNER_PROJECT_ID")
	if got := spannerProjectID(); got != "gc-project" {
		t.Errorf("spannerProjectID() = %v, want %v", got, "")
	}
}

func Test_spannerInstanceID(t *testing.T) {
	t.Setenv("SPANNER_INSTANCE_ID", "spanner-instance")

	instance = ""
	if got := spannerInstanceID(); got != "spanner-instance" {
		t.Errorf("spannerInstanceID() = %v, want %v", got, "spanner-instance")
	}
}

func Test_spannerDatabaseID(t *testing.T) {
	t.Setenv("SPANNER_DATABASE_ID", "spanner-database")

	database = ""
	if got := spannerDatabaseID(); got != "spanner-database" {
		t.Errorf("spannerDatabaseID() = %v, want %v", got, "spanner-database")
	}
}
