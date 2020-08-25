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
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package spanner

import (
	"path/filepath"
	"testing"
)

const (
	TestStmtDDL            = "ALTER TABLE Singers ADD COLUMN Foo STRING(MAX)"
	TestStmtPartitionedDML = "UPDATE Singers SET FirstName = \"Bar\" WHERE SingerID = \"1\""
	TestStmtDML            = "INSERT INTO Singers(FirstName) VALUES(\"Bar\")"
)

func TestLoadMigrations(t *testing.T) {
	ms, err := LoadMigrations(filepath.Join("testdata", "migrations"))
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

func Test_getStatementKind(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      statementKind
	}{
		{
			"ALTER statement is DDL",
			TestStmtDDL,
			statementKindDDL,
		},
		{
			"UPDATE statement is PartitionedDML",
			TestStmtPartitionedDML,
			statementKindPartitionedDML,
		},
		{
			"INSERT statement is DML",
			TestStmtDML,
			statementKindDML,
		},
		{
			"lowercase insert statement is DML",
			TestStmtDML,
			statementKindDML,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getStatementKind(tt.statement); got != tt.want {
				t.Errorf("getStatementKind() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_inspectStatementsKind(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		want       statementKind
		wantErr    bool
	}{
		{
			"Only DDL returns DDL",
			[]string{TestStmtDDL, TestStmtDDL},
			statementKindDDL,
			false,
		},
		{
			"Only PartitionedDML returns PartitionedDML",
			[]string{TestStmtPartitionedDML, TestStmtPartitionedDML},
			statementKindPartitionedDML,
			false,
		},
		{
			"Only DML returns DML",
			[]string{TestStmtDML, TestStmtDML},
			statementKindDDL,
			false,
		},
		{
			"DML and DDL returns error",
			[]string{TestStmtDDL, TestStmtDML},
			"",
			true,
		},
		{
			"DML and PartitionedDML returns error",
			[]string{TestStmtDML, TestStmtPartitionedDML},
			"",
			true,
		},
		{
			"DDL and PartitionedDML returns error",
			[]string{TestStmtDDL, TestStmtPartitionedDML},
			"",
			true,
		},
		{
			"no statements defaults to DDL as before",
			[]string{},
			statementKindDDL,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := inspectStatementsKind(tt.statements)
			if (err != nil) != tt.wantErr {
				t.Errorf("inspectStatementsKind() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("inspectStatementsKind() got = %v, want %v", got, tt.want)
			}
		})
	}
}
