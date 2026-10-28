package spanner

import (
	"testing"

	"google.golang.org/api/option"
)

func TestResolveCredentialsType(t *testing.T) {
	tests := []struct {
		name      string
		input     option.CredentialsType
		want      option.CredentialsType
		wantError bool
	}{
		{
			name: "defaults to service account",
			want: option.ServiceAccount,
		},
		{
			name:  "service account",
			input: option.ServiceAccount,
			want:  option.ServiceAccount,
		},
		{
			name:  "authorized user",
			input: option.AuthorizedUser,
			want:  option.AuthorizedUser,
		},
		{
			name:      "unsupported type",
			input:     option.ExternalAccount,
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveCredentialsType(test.input)
			if test.wantError {
				if err == nil {
					t.Fatal("expected an unsupported credentials type error")
				}
				return
			}
			if err != nil {
				t.Fatalf("failed to resolve credentials type: %v", err)
			}
			if got != test.want {
				t.Errorf("want credentials type %q, got %q", test.want, got)
			}
		})
	}
}

func TestCredentialsClientOptionWithoutFile(t *testing.T) {
	got, err := credentialsClientOption(&Config{CredentialsType: option.ExternalAccount})
	if err != nil {
		t.Fatalf("failed to build credentials option without a file: %v", err)
	}
	if got != nil {
		t.Fatal("expected no credentials option without a credentials file")
	}
}
