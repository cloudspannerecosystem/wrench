package spanner

import (
	"context"
	"fmt"
	"math"

	"cloud.google.com/go/spanner"
)

// A negative Version cannot occur in a migration filename. This row marks a
// table whose other rows record individual applied migrations.
const migrationHistoryMarker int64 = -1

type migrationRecord struct {
	version int64
	dirty   bool
}

type migrationHistory struct {
	applied map[uint]struct{}
	dirty   *uint
	legacy  bool
	current uint
}

func historyFromRecords(records []migrationRecord) (migrationHistory, error) {
	history := migrationHistory{applied: make(map[uint]struct{}), legacy: true}
	for _, record := range records {
		if record.version == migrationHistoryMarker {
			if record.dirty {
				return history, fmt.Errorf("migration history marker is dirty")
			}
			history.legacy = false
			continue
		}
		if record.version < 0 {
			return history, fmt.Errorf("invalid migration version %d in table", record.version)
		}
		if uint64(record.version) > uint64(^uint(0)) {
			return history, fmt.Errorf("migration version %d exceeds supported range", record.version)
		}
		version := uint(record.version)
		history.applied[version] = struct{}{}
		if version > history.current {
			history.current = version
		}
		if record.dirty {
			history.dirty = &version
		}
	}
	if history.legacy && len(records) > 1 {
		return history, fmt.Errorf("migration table has multiple rows without a history marker")
	}
	return history, nil
}

func (c *Client) readMigrationHistory(ctx context.Context, tableName string) (migrationHistory, error) {
	iter := c.spannerClient.Single().Read(ctx, tableName, spanner.AllKeys(), []string{"Version", "Dirty"})
	defer iter.Stop()
	var records []migrationRecord
	err := iter.Do(func(row *spanner.Row) error {
		var record migrationRecord
		if err := row.Columns(&record.version, &record.dirty); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	})
	if err != nil {
		return migrationHistory{}, err
	}
	return historyFromRecords(records)
}

func (c *Client) initializeMigrationHistory(ctx context.Context, tableName string, migrations Migrations, history *migrationHistory) error {
	if !history.legacy {
		return nil
	}
	backfill := legacyBackfill(migrations, history.current, history.applied)
	mutations := []*spanner.Mutation{spanner.Insert(tableName, []string{"Version", "Dirty"}, []interface{}{migrationHistoryMarker, false})}
	if len(history.applied) > 0 {
		for _, version := range backfill {
			if version > math.MaxInt64 {
				return fmt.Errorf("migration version %d exceeds Spanner INT64 range", version)
			}
			mutations = append(mutations, spanner.Insert(tableName, []string{"Version", "Dirty"},
				[]interface{}{int64(version), false}))
		}
	}
	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(_ context.Context, tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite(mutations)
	})
	if err != nil {
		return err
	}
	history.legacy = false
	for _, version := range backfill {
		history.applied[version] = struct{}{}
	}
	return nil
}

func legacyBackfill(migrations Migrations, current uint, applied map[uint]struct{}) []uint {
	var versions []uint
	for _, migration := range migrations {
		if migration.Version > current {
			continue
		}
		if _, exists := applied[migration.Version]; exists {
			continue
		}
		versions = append(versions, migration.Version)
	}
	return versions
}
