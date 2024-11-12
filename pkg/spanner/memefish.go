package spanner

import (
	"github.com/apstndb/gsqlutils"
)

// Directly use of memefish is permitted only in this file.
func toStatements(filename string, data []byte) ([]string, error) {
	rawStmts, err := gsqlutils.SeparateInputPreserveCommentsWithStatus(filename, string(data))
	if err != nil {
		return nil, err
	}

	var result []string
	for _, rawStmt := range rawStmts {
		stripped, err := gsqlutils.SimpleStripComments("", rawStmt.Statement)
		if err != nil {
			return nil, err
		}
		result = append(result, stripped)
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
	token, err := gsqlutils.FirstNonHintToken("", statement)
	if err != nil {
		return false
	}
	return token.IsKeywordLike("UPDATE") || token.IsKeywordLike("DELETE")
}
