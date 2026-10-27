package spanner

import (
	"strings"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

// quoteIdentifier uses double quotes with doubled embedded quotes for PostgreSQL,
// and backticks for GoogleSQL.
func quoteIdentifier(identifier string, dialect databasepb.DatabaseDialect) string {
	if dialect == databasepb.DatabaseDialect_POSTGRESQL {
		return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
	}
	return "`" + identifier + "`"
}
