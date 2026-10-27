package spanner

import (
	"strings"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

func quoteIdentifier(identifier string, dialect databasepb.DatabaseDialect) string {
	if dialect == databasepb.DatabaseDialect_POSTGRESQL {
		return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
	}
	return "`" + identifier + "`"
}
