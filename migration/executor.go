package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rubenv/sql-migrate/dialect"
)

type Direction int

const (
	Up Direction = iota + 1
	Down
)

const (
	defaultTableName = "migrations"
)

// Executor provides database parameters for a migration execution
type Executor struct {
	// TableName name of the table used to store migration info.
	TableName string
	// SchemaName schema that the migration table be referenced.
	SchemaName string
	// IgnoreUnknown skips the check to see if there is a migration
	// ran in the database that is not in Source.
	//
	// This should be used sparingly as it is removing a safety check.
	IgnoreUnknown bool
	// DisableCreateTable disable the creation of the migration table
	DisableCreateTable bool
}

type SqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Insert(list ...interface{}) error
	Delete(list ...interface{}) (int64, error)
}

type MigrationRecord struct {
	Id        string    `db:"id"`
	AppliedAt time.Time `db:"applied_at"`
}

// getTableName returns a parametrized Migration object
func (ms *Executor) getTableName() string {
	if ms.TableName == "" {
		return defaultTableName
	}

	return ms.TableName
}

// Exec Returns the number of applied migrations.
func (ms *Executor) Exec(db *sql.DB, dialect dialect.DialectType, m Source, dir Direction) (int, error) {
	return ms.ExecMaxContext(context.Background(), db, dialect, m, dir, 0)
}

// ExecContext Returns the number of applied migrations.
func (ms *Executor) ExecContext(ctx context.Context, db *sql.DB, dialect dialect.DialectType, m Source, dir Direction) (int, error) {
	return ms.ExecMaxContext(ctx, db, dialect, m, dir, 0)
}

// ExecMax Returns the number of applied migrations.
func (ms *Executor) ExecMax(db *sql.DB, dialect dialect.DialectType, m Source, dir Direction, max int) (int, error) {
	return ms.ExecMaxContext(context.Background(), db, dialect, m, dir, max)
}

// ExecMaxContext Returns the number of applied migrations, but applies with an input context.
func (ms *Executor) ExecMaxContext(ctx context.Context, db *sql.DB, dialect dialect.DialectType, m Source, dir Direction, max int) (int, error) {
	migrations, dbMap, err := ms.PlanMigration(db, dialect, m, dir, max)
	if err != nil {
		return 0, err
	}

	return ms.applyMigrations(ctx, dir, migrations, dbMap)
}

// ExecVersion Returns the number of applied migrations.
func (ms *Executor) ExecVersion(db *sql.DB, dialect dialect.DialectType, m Source, dir Direction, version int64) (int, error) {
	return ms.ExecVersionContext(context.Background(), db, dialect, m, dir, version)
}

func (ms *Executor) ExecVersionContext(ctx context.Context, db *sql.DB, dialect dialect.DialectType, m Source, dir Direction, version int64) (int, error) {
	migrations, dbMap, err := ms.PlanMigrationToVersion(db, dialect, m, dir, version)
	if err != nil {
		return 0, err
	}

	return ms.applyMigrations(ctx, dir, migrations, dbMap)
}

// Applies the planned migrations and returns the number of applied migrations.
func (*Executor) applyMigrations(ctx context.Context, dir Direction, migrations []*PlannedMigration, dbMap *dialect.DbMap) (int, error) {
	applied := 0
	for _, migration := range migrations {
		var executor SqlExecutor
		var err error

		if migration.DisableTransaction {
			executor = dbMap.WithContext(ctx)
		} else {
			e, err := dbMap.Begin()
			if err != nil {
				return applied, newTxError(migration, err)
			}
			executor = e.WithContext(ctx)
		}

		for _, stmt := range migration.Queries {
			// remove the semicolon from stmt, fix ORA-00922 issue in database oracle
			stmt = strings.TrimSuffix(stmt, "\n")
			stmt = strings.TrimSuffix(stmt, " ")
			stmt = strings.TrimSuffix(stmt, ";")
			if _, err := executor.Exec(stmt); err != nil {
				if trans, ok := executor.(*dialect.Transaction); ok {
					_ = trans.Rollback()
				}

				return applied, newTxError(migration, err)
			}
		}

		switch dir {
		case Up:
			err = executor.Insert(&MigrationRecord{
				Id:        migration.Id,
				AppliedAt: time.Now(),
			})
			if err != nil {
				if trans, ok := executor.(*dialect.Transaction); ok {
					_ = trans.Rollback()
				}

				return applied, newTxError(migration, err)
			}
		case Down:
			_, err := executor.Delete(&MigrationRecord{
				Id: migration.Id,
			})
			if err != nil {
				if trans, ok := executor.(*dialect.Transaction); ok {
					_ = trans.Rollback()
				}

				return applied, newTxError(migration, err)
			}
		default:
			panic("Not possible")
		}

		if trans, ok := executor.(*dialect.Transaction); ok {
			if err := trans.Commit(); err != nil {
				return applied, newTxError(migration, err)
			}
		}

		applied++
	}

	return applied, nil
}

// PlanMigration Plan a migration.
func (ms *Executor) PlanMigration(db *sql.DB, dialectType dialect.DialectType, m Source, dir Direction, max int) ([]*PlannedMigration, *dialect.DbMap, error) {
	return ms.planMigrationCommon(db, dialectType, m, dir, max, -1)
}

// PlanMigrationToVersion Plan a migration to version.
func (ms *Executor) PlanMigrationToVersion(db *sql.DB, dialectType dialect.DialectType, m Source, dir Direction, version int64) ([]*PlannedMigration, *dialect.DbMap, error) {
	return ms.planMigrationCommon(db, dialectType, m, dir, 0, version)
}

// planMigrationCommon A common method to plan a migration.
func (ms *Executor) planMigrationCommon(db *sql.DB, dialectType dialect.DialectType, m Source, dir Direction, max int, version int64) ([]*PlannedMigration, *dialect.DbMap, error) {
	dbMap, err := ms.getMigrationDbMap(db, dialectType)
	if err != nil {
		return nil, nil, err
	}

	migrations, err := m.FindMigrations()
	if err != nil {
		return nil, nil, err
	}

	var migrationRecords []MigrationRecord
	_, err = dbMap.Select(&migrationRecords, fmt.Sprintf("SELECT * FROM %s", dbMap.Dialect.QuotedTableForQuery(ms.SchemaName, ms.getTableName())))
	if err != nil {
		return nil, nil, err
	}

	// Sort migrations that have been run by Id.
	var existingMigrations []*Migration
	for _, migrationRecord := range migrationRecords {
		existingMigrations = append(existingMigrations, &Migration{
			Id: migrationRecord.Id,
		})
	}
	sort.Sort(byId(existingMigrations))

	// Make sure all migrations in the database are among the found migrations which
	// are to be applied.
	if !ms.IgnoreUnknown {
		migrationsSearch := make(map[string]struct{})
		for _, migration := range migrations {
			migrationsSearch[migration.Id] = struct{}{}
		}
		for _, existingMigration := range existingMigrations {
			if _, ok := migrationsSearch[existingMigration.Id]; !ok {
				return nil, nil, newPlanError(existingMigration, "unknown migration in database")
			}
		}
	}

	// Get last migration that was run
	record := &Migration{}
	if len(existingMigrations) > 0 {
		record = existingMigrations[len(existingMigrations)-1]
	}

	result := make([]*PlannedMigration, 0)

	// Add missing migrations up to the last run migration.
	// This can happen for example when merges happened.
	if len(existingMigrations) > 0 {
		result = append(result, ToCatchup(migrations, existingMigrations, record)...)
	}

	// Figure out which migrations to apply
	toApply := ToApply(migrations, record.Id, dir)
	toApplyCount := len(toApply)

	if version >= 0 {
		targetIndex := 0
		for targetIndex < len(toApply) {
			tempVersion := toApply[targetIndex].VersionInt()
			if dir == Up && tempVersion > version || dir == Down && tempVersion < version {
				return nil, nil, newPlanError(&Migration{}, fmt.Errorf("unknown migration with version id %d in database", version).Error())
			}
			if tempVersion == version {
				toApplyCount = targetIndex + 1
				break
			}
			targetIndex++
		}
		if targetIndex == len(toApply) {
			return nil, nil, newPlanError(&Migration{}, fmt.Errorf("unknown migration with version id %d in database", version).Error())
		}
	} else if max > 0 && max < toApplyCount {
		toApplyCount = max
	}
	for _, v := range toApply[0:toApplyCount] {
		if dir == Up {
			result = append(result, &PlannedMigration{
				Migration:          v,
				Queries:            v.Up,
				DisableTransaction: v.DisableTransactionUp,
			})
		} else if dir == Down {
			result = append(result, &PlannedMigration{
				Migration:          v,
				Queries:            v.Down,
				DisableTransaction: v.DisableTransactionDown,
			})
		}
	}

	return result, dbMap, nil
}

func (ms *Executor) GetMigrationRecords(db *sql.DB, dialectType dialect.DialectType) ([]*MigrationRecord, error) {
	dbMap, err := ms.getMigrationDbMap(db, dialectType)
	if err != nil {
		return nil, err
	}

	var records []*MigrationRecord
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s ASC", dbMap.Dialect.QuotedTableForQuery(ms.SchemaName, ms.getTableName()), dbMap.Dialect.QuoteField("id"))
	_, err = dbMap.Select(&records, query)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (ms *Executor) getMigrationDbMap(db *sql.DB, dialectType dialect.DialectType) (*dialect.DbMap, error) {
	d, ok := dialect.Dialects[dialectType]
	if !ok {
		return nil, fmt.Errorf("unknown dialect: %s", dialectType)
	}

	// When using the mysql driver, make sure that the parseTime option is
	// configured, otherwise it won't map time columns to time.Time. See
	// https://github.com/rubenv/sql-migrate/issues/2
	if dialectType == dialect.MySQL {
		var out *time.Time

		err := db.QueryRow("SELECT NOW()").Scan(&out)
		if err != nil {
			if err.Error() == "sql: Scan error on column index 0: unsupported driver -> Scan pair: []uint8 -> *time.Time" ||
				err.Error() == "sql: Scan error on column index 0: unsupported Scan, storing driver.Value type []uint8 into type *time.Time" ||
				err.Error() == "sql: Scan error on column index 0, name \"NOW()\": unsupported Scan, storing driver.Value type []uint8 into type *time.Time" {
				return nil, errors.New(`Cannot parse dates.
Make sure that the parseTime option is supplied to your database connection.
Check https://github.com/go-sql-driver/mysql#parsetime for more info.`)
			}

			return nil, err
		}
	}

	// Create migration database map
	dbMap := &dialect.DbMap{Db: db, Dialect: d}
	table := dbMap.AddTableWithNameAndSchema(MigrationRecord{}, ms.SchemaName, ms.getTableName()).SetKeys(false, "Id")

	if dialectType == "oci8" || dialectType == "godror" {
		table.ColMap("Id").SetMaxSize(4000)
	}

	if ms.DisableCreateTable {
		return dbMap, nil
	}

	err := dbMap.CreateTablesIfNotExists()
	if err != nil {
		// Oracle database does not support `if not exists`, so use `ORA-00955:` error code
		// to check if the table exists.
		if (dialectType == "oci8" || dialectType == "godror") && strings.Contains(err.Error(), "ORA-00955:") {
			return dbMap, nil
		}
		return nil, err
	}

	return dbMap, nil
}

func ToCatchup(migrations, existingMigrations []*Migration, lastRun *Migration) []*PlannedMigration {
	missing := make([]*PlannedMigration, 0)
	for _, migration := range migrations {
		found := false
		for _, existing := range existingMigrations {
			if existing.Id == migration.Id {
				found = true
				break
			}
		}
		if !found && migration.Less(lastRun) {
			missing = append(missing, &PlannedMigration{
				Migration:          migration,
				Queries:            migration.Up,
				DisableTransaction: migration.DisableTransactionUp,
			})
		}
	}
	return missing
}

// Filter a slice of migrations into ones that should be applied.
func ToApply(migrations []*Migration, current string, direction Direction) []*Migration {
	index := -1
	if current != "" {
		for index < len(migrations)-1 {
			index++
			if migrations[index].Id == current {
				break
			}
		}
	}

	if direction == Up {
		return migrations[index+1:]
	} else if direction == Down {
		if index == -1 {
			return []*Migration{}
		}

		// Add in reverse order
		toApply := make([]*Migration, index+1)
		for i := 0; i < index+1; i++ {
			toApply[index-i] = migrations[i]
		}
		return toApply
	}

	panic("Not possible")
}
