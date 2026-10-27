package spanner

import (
	"fmt"
	"strings"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/apstndb/gsqlutils"
	"github.com/cloudspannerecosystem/memefish"
	sqlparser "github.com/googleapis/go-sql-spanner/parser"
)

func toStatementsWithDialect(filename string, data []byte, dialect databasepb.DatabaseDialect) ([]string, error) {
	parser, err := newStatementParser(dialect)
	if err != nil {
		return nil, err
	}

	multiple, statements, err := parser.Split(string(data))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filename, err)
	}
	// The parser returns split statements only for multi-statement input. Single
	// statements need dialect-specific handling below.
	if multiple {
		return statements, nil
	}
	if dialect == databasepb.DatabaseDialect_POSTGRESQL {
		// Split returns no statement for single-statement input. Memefish only parses
		// GoogleSQL, so trim the terminator and inspect a comment-stripped copy here.
		statement := strings.TrimSpace(string(data))
		statement = strings.TrimSuffix(statement, ";")
		stripped, err := gsqlutils.SimpleStripComments("", statement)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(stripped) == "" {
			// Ignore input that contains comments but no SQL statement.
			return nil, nil
		}
		return []string{strings.TrimSpace(statement)}, nil
	}

	// Memefish removes the terminator and validates the GoogleSQL single-statement case.
	rawStmts, err := memefish.SplitRawStatements(filename, string(data))
	if err != nil {
		return nil, err
	}

	var result []string
	for _, rawStmt := range rawStmts {
		// Strip comments only to detect comment-only statements; append the original
		// statement so DDL comments reach Spanner unchanged.
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
	// The migration runner uses ordinary DML for INSERT and partitioned DML for
	// UPDATE and DELETE.
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
