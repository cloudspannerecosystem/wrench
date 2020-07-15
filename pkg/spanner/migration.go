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
	"bytes"
	"errors"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	statementsSeparator = ";"
)

var (
	// migrationFileRegex matches the following patterns
	// 001.sql
	// 001_name.sql
	// 001_name.up.sql
	migrationFileRegex = regexp.MustCompile(`^([0-9]+)(?:_([a-zA-Z0-9_\-]+))?(\.up)?\.sql$`)

	MigrationNameRegex = regexp.MustCompile(`[a-zA-Z0-9_\-]+`)

	dmlAnyRegex = regexp.MustCompile("^(UPDATE|DELETE|INSERT)[\t\n\f\r ].*")

	// INSERT statements are not supported for partitioned DML. Although not every DML can be partitioned
	// as it must be idempotent. This probably isn't solvable with more regexes.
	notPartitionedDmlRegex = regexp.MustCompile("INSERT")

)

const (
	statementKindDDL statementKind = "DDL"
	statementKindDML statementKind = "DML"
	statementKindPartitionedDML statementKind = "PartitionedDML"
)

type (
	// migration represents the parsed migration file. e.g. version_name.sql
	Migration struct {
		// Version is the version of the migration
		Version uint

		// Name is the name of the migration
		Name string

		// Statements is the migration statements
		Statements []string

		kind statementKind
	}

	Migrations []*Migration

	statementKind string
)

func (ms Migrations) Len() int {
	return len(ms)
}

func (ms Migrations) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms Migrations) Less(i, j int) bool {
	return ms[i].Version < ms[j].Version
}

func LoadMigrations(dir string) (Migrations, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var migrations Migrations
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		matches := migrationFileRegex.FindStringSubmatch(f.Name())
		if len(matches) != 4 {
			continue
		}

		version, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			continue
		}

		file, err := ioutil.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			continue
		}

		statements := toStatements(file)
		kind, err := inspectStatementsKind(statements)
		if err != nil {
			return nil, err
		}

		migrations = append(migrations, &Migration{
			Version:    uint(version),
			Name:       matches[2],
			Statements: statements,
			kind:       kind,
		})
	}

	return migrations, nil
}

func toStatements(file []byte) []string {
	contents := bytes.Split(file, []byte(statementsSeparator))

	statements := make([]string, 0, len(contents))
	for _, c := range contents {
		if statement := strings.TrimSpace(string(c)); statement != "" {
			statements = append(statements, statement)
		}
	}

	return statements
}

func inspectStatementsKind(statements []string) (statementKind, error) {
	kindMap := map[statementKind]uint64{
		statementKindDDL: 0,
		statementKindDML: 0,
		statementKindPartitionedDML: 0,
	}

	for _, s := range statements {
		kindMap[getStatementKind(s)]++
	}

	if distinctKind(kindMap, statementKindDDL) {
		return statementKindDDL, nil
	}

	if distinctKind(kindMap, statementKindDML) {
		return statementKindDML, nil
	}

	if distinctKind(kindMap, statementKindPartitionedDML) {
		return statementKindPartitionedDML, nil
	}

	return "", errors.New("Cannot specify DDL and DML in the same migration file")
}

func distinctKind(kindMap map[statementKind]uint64, kind statementKind) bool {
	target := kindMap[kind]

	var total uint64
	for k:= range kindMap {
		total = total + kindMap[k]
	}

	return target == total
}

func getStatementKind(statement string) statementKind {
	if isPartitionedDMLOnly(statement){
		return statementKindPartitionedDML
	}

	if isDMLAny(statement){
		return statementKindDML
	}

	return statementKindDDL
}

func isPartitionedDMLOnly(statement string) bool {
	return isDMLAny(statement) && !notPartitionedDmlRegex.Match([]byte(statement))
}

func isDMLAny(statement string) bool {
	return dmlAnyRegex.Match([]byte(statement))
}
