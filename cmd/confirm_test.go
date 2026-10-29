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

package cmd_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/wrench/cmd"
	"github.com/spf13/cobra"
)

func newConfirmCmd(input string, args ...string) (*cobra.Command, *bytes.Buffer) {
	c := &cobra.Command{Use: "x"}
	c.Flags().String("project", "p", "")
	c.Flags().String("instance", "i", "")
	c.Flags().String("database", "d", "")
	cmd.AddInteractiveFlag(c)
	c.SetIn(strings.NewReader(input))
	var stderr bytes.Buffer
	c.SetErr(&stderr)
	if err := c.ParseFlags(args); err != nil {
		panic(err)
	}
	return c, &stderr
}

func TestConfirmDestructive(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		args    []string
		wantErr bool
		prompt  bool
	}{
		{name: "no flag skips prompt", input: "", args: nil},
		{name: "yes", input: "y\n", args: []string{"-i"}, prompt: true},
		{name: "YES", input: "YES\n", args: []string{"--interactive"}, prompt: true},
		{name: "no", input: "n\n", args: []string{"-i"}, wantErr: true, prompt: true},
		{name: "empty", input: "\n", args: []string{"-i"}, wantErr: true, prompt: true},
		{name: "eof", input: "", args: []string{"-i"}, wantErr: true, prompt: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, stderr := newConfirmCmd(tc.input, tc.args...)
			err := cmd.ConfirmDestructive(c, "drop")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			gotPrompt := strings.Contains(stderr.String(), "projects/p/instances/i/databases/d")
			if gotPrompt != tc.prompt {
				t.Fatalf("prompt shown = %v, want %v (stderr %q)", gotPrompt, tc.prompt, stderr.String())
			}
		})
	}
}
