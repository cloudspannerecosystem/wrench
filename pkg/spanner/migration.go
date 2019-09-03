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

	dmlRegex = regexp.MustCompile("^(UPDATE|DELETE)[\t\n\f\r ].*")
)

const (
	statementKindDDL statementKind = "DDL"
	statementKindDML statementKind = "DML"
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
	}

	for _, s := range statements {
		if isDML(s) {
			kindMap[statementKindDML]++
		} else {
			kindMap[statementKindDDL]++
		}
	}

	if kindMap[statementKindDML] > 0 {
		if kindMap[statementKindDDL] > 0 {
			return "", errors.New("Cannot specify DDL and DML at same migration file.")
		}

		return statementKindDML, nil
	}

	return statementKindDDL, nil
}

func isDML(statement string) bool {
	return dmlRegex.Match([]byte(statement))
}
