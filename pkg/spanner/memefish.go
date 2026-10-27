package spanner

import (
	"github.com/apstndb/gsqlutils"
	"github.com/cloudspannerecosystem/memefish"
)

// Directly use of memefish/gsqlutils is permitted only in this file.
func toStatements(filename string, data []byte) ([]string, error) {
	rawStmts, err := memefish.SplitRawStatements(filename, string(data))
	if err != nil {
		return nil, err
	}

	var result []string
	for _, rawStmt := range rawStmts {
		result = append(result, rawStmt.Statement)
	}
	return result, nil
}

func isDML(statement string) bool {
	token, err := gsqlutils.FirstNonHintToken("", statement)
	if err != nil {
		return false
	}
	return token.IsKeywordLike("INSERT")
}

func isPartitionedDML(statement string) bool {
	// It is better than regular expression because PDML can be prefixed by statement hints.
	token, err := gsqlutils.FirstNonHintToken("", statement)
	if err != nil {
		return false
	}
	return token.IsKeywordLike("UPDATE") || token.IsKeywordLike("DELETE")
}
