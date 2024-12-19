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

	// need to strip comments because memefish.SplitRawStatements preserve comments, but UpdateDDL doesn't support comments.
	var result []string
	for _, rawStmt := range rawStmts {
		stripped, err := gsqlutils.SimpleStripComments("", rawStmt.Statement)
		if err != nil {
			return nil, err
		}
		if len(stripped) != 0 {
			result = append(result, stripped)
		}
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
