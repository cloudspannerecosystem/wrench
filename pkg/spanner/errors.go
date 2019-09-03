package spanner

import "google.golang.org/grpc/status"

type ErrorCode int

const (
	ErrorCodeCreateClient = iota + 1
	ErrorCodeCloseClient
	ErrorCodeCreateDatabase
	ErrorCodeDropDatabase
	ErrorCodeLoadSchema
	ErrorCodeUpdateDDL
	ErrorCodeUpdateDML
	ErrorCodeUpdatePartitionedDML
	ErrorCodeExecuteMigrations
	ErrorCodeGetMigrationVersion
	ErrorCodeSetMigrationVersion
	ErrorCodeNoMigration
	ErrorCodeMigrationVersionDirty
	ErrorCodeWaitOperation
)

type Error struct {
	Code ErrorCode
	err  error
}

func (e *Error) Error() string {
	if st, ok := status.FromError(e.err); ok {
		return st.Message()
	}

	return e.err.Error()
}
