package cmd

import (
	"testing"

	"github.com/cloudspannerecosystem/wrench/pkg/spanner"
)

func TestPriorityTypeOf(t *testing.T) {
	tests := map[string]struct {
		priority  string
		want      spanner.PriorityType
		wantError bool
	}{
		"priority high": {
			priority: priorityTypeHigh,
			want:     spanner.PriorityTypeHigh,
		},
		"priority medium": {
			priority: priorityTypeMedium,
			want:     spanner.PriorityTypeMedium,
		},
		"priority low": {
			priority: priorityTypeLow,
			want:     spanner.PriorityTypeLow,
		},
		"unspecified": {
			priority: "",
			want:     spanner.PriorityTypeUnspecified,
		},
		"invalid": {
			priority:  "lower",
			wantError: true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := priorityTypeOf(test.priority)
			if (err != nil) != test.wantError {
				if test.wantError {
					t.Fatal("want error, but got nil")
				}
				t.Fatalf("want no error, but got %v", err)
			}
			if got != test.want {
				t.Fatalf("want %d, but got %d", test.want, got)
			}
		})
	}
}
