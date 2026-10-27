package spanner

import (
	"fmt"
	"strings"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/apstndb/gsqlutils"
	"github.com/cloudspannerecosystem/memefish"
	sqlparser "github.com/googleapis/go-sql-spanner/parser"
)

// Parser integrations are kept here so migration code depends only on local types.
func toStatements(filename string, data []byte) ([]string, error) {
	return toStatementsWithDialect(filename, data, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
}

func toStatementsWithDialect(filename string, data []byte, dialect databasepb.DatabaseDialect) ([]string, error) {
	parser, err := newStatementParser(dialect)
	if err != nil {
		return nil, err
	}

	multiple, statements, err := parser.Split(string(data))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filename, err)
	}
	if multiple {
		return statements, nil
	}
	if dialect == databasepb.DatabaseDialect_POSTGRESQL {
		statement := strings.TrimSpace(string(data))
		statement = strings.TrimSuffix(statement, ";")
		stripped, err := gsqlutils.SimpleStripComments("", statement)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(stripped) == "" {
			return nil, nil
		}
		return []string{strings.TrimSpace(statement)}, nil
	}

	// Split returns no statements when the input contains only one statement.
	// Keep Memefish for that case so it removes the terminator and validates input.
	rawStmts, err := memefish.SplitRawStatements(filename, string(data))
	if err != nil {
		return nil, err
	}

	var result []string
	for _, rawStmt := range rawStmts {
		stripped, err := gsqlutils.SimpleStripComments("", rawStmt.Statement)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(stripped) != "" {
			result = append(result, rawStmt.Statement)
		}
	}
	return result, nil
}

func newStatementParser(dialect databasepb.DatabaseDialect) (*sqlparser.StatementParser, error) {
	return sqlparser.NewStatementParser(dialect, 0)
}

type migrationDMLType uint8

const (
	migrationDMLTypeOther migrationDMLType = iota
	migrationDMLTypeInsert
	migrationDMLTypeUpdate
	migrationDMLTypeDelete
)

func classifyStatements(statements []string, dialect databasepb.DatabaseDialect) ([]migrationDMLType, error) {
	parser, err := newStatementParser(dialect)
	if err != nil {
		return nil, err
	}

	classifications := make([]migrationDMLType, len(statements))
	for i, statement := range statements {
		info := parser.DetectStatementType(statement)
		switch info.DmlType {
		case sqlparser.DmlTypeInsert:
			classifications[i] = migrationDMLTypeInsert
		case sqlparser.DmlTypeUpdate:
			classifications[i] = migrationDMLTypeUpdate
		case sqlparser.DmlTypeDelete:
			classifications[i] = migrationDMLTypeDelete
		}
	}
	return classifications, nil
}
