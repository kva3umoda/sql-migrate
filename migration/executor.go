package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	`github.com/kva3umoda/sql-migrate/dialect`
)

type Direction int

const (
	Up Direction = iota + 1
	Down
)

const (
	defaultTableName = "migrations"
)

// MigrationExecutor provides database parameters for a migration execution
type MigrationExecutor struct {
	// TableName name of the table used to store migration info.
	TableName string
	// SchemaName schema that the migration table be referenced.
	SchemaName string
	// IgnoreUnknown skips the check to see if there is a migration
	// ran in the database that is not in Source.
	//
	// This should be used sparingly as it is removing a safety check.
	IgnoreUnknown bool
	// CreateTable disable the creation of the migration table
	CreateTable bool
	// CreateSchema disable the creation of the migration schema
	CreateSchema bool
}

type MigrationRecord struct {
	Id        string    `db:"id"`
	AppliedAt time.Time `db:"applied_at"`
}

// getTableName returns a parametrized Migration object
func (ex *MigrationExecutor) getTableName() string {
	if ex.TableName == "" {
		return defaultTableName
	}

	return ex.TableName
}

// Exec Returns the number of applied migrations.
func (ex *MigrationExecutor) Exec(db *sql.DB, dialect dialect.DialectType, m Source, dir Direction) (int, error) {
	return ex.ExecMaxContext(context.Background(), db, dialect, m, dir, 0)
}

// ExecContext Returns the number of applied migrations.
func (ex *MigrationExecutor) ExecContext(
	ctx context.Context,
	db *sql.DB,
	dialect dialect.DialectType,
	m Source,
	dir Direction,
) (int, error) {
	return ex.ExecMaxContext(ctx, db, dialect, m, dir, 0)
}

// ExecMax Returns the number of applied migrations.
func (ex *MigrationExecutor) ExecMax(
	db *sql.DB,
	dialect dialect.DialectType,
	m Source,
	dir Direction,
	max int,
) (int, error) {
	return ex.ExecMaxContext(context.Background(), db, dialect, m, dir, max)
}

// ExecMaxContext Returns the number of applied migrations, but applies with an input context.
func (ex *MigrationExecutor) ExecMaxContext(
	ctx context.Context,
	db *sql.DB,
	dialect dialect.DialectType,
	m Source,
	dir Direction,
	max int,
) (int, error) {
	migrations, rep, err := ex.PlanMigration(ctx, db, dialect, m, dir, max)
	if err != nil {
		return 0, err
	}

	return ex.applyMigrations(ctx, dir, migrations, rep)
}

// ExecVersion Returns the number of applied migrations.
func (ex *MigrationExecutor) ExecVersion(
	db *sql.DB,
	dialect dialect.DialectType,
	m Source,
	dir Direction,
	version int64,
) (int, error) {
	return ex.ExecVersionContext(context.Background(), db, dialect, m, dir, version)
}

func (ex *MigrationExecutor) ExecVersionContext(
	ctx context.Context,
	db *sql.DB,
	dialect dialect.DialectType,
	m Source,
	dir Direction,
	version int64,
) (int, error) {
	migrations, rep, err := ex.PlanMigrationToVersion(ctx, db, dialect, m, dir, version)
	if err != nil {
		return 0, err
	}

	return ex.applyMigrations(ctx, dir, migrations, rep)
}

// Applies the planned migrations and returns the number of applied migrations.
func (ex *MigrationExecutor) applyMigrations(
	ctx context.Context,
	dir Direction,
	migrations []*PlannedMigration,
	rep *Repository,
) (int, error) {
	applied := 0
	for _, migration := range migrations {
		err := ex.applyMigration(ctx, dir, rep, migration)
		if err != nil {
			return applied, err
		}

		applied++
	}

	return applied, nil
}

func (ex *MigrationExecutor) applyMigration(
	ctx context.Context,
	dir Direction,
	rep *Repository,
	migration *PlannedMigration,
) (err error) {
	if !migration.DisableTransaction {
		var tx *sql.Tx
		tx, ctx, err = rep.BeginTx(ctx)
		if err != nil {
			return newTxError(migration, err)
		}

		defer func() {
			if err != nil {
				_ = tx.Rollback()

				return
			}

			err = tx.Commit()
			if err != nil {
				err = newTxError(migration, err)
			}
		}()
	}

	for _, stmt := range migration.Queries {
		// remove the semicolon from stmt, fix ORA-00922 issue in database oracle
		stmt = strings.TrimSuffix(stmt, "\n")
		stmt = strings.TrimSuffix(stmt, " ")
		stmt = strings.TrimSuffix(stmt, ";")

		_, err = rep.ExecContext(ctx, stmt)
		if err != nil {
			return newTxError(migration, err)
		}
	}

	switch dir {
	case Up:
		err = rep.SaveMigration(ctx, Record{Id: migration.Id, AppliedAt: time.Now()})
	case Down:
		err = rep.DeleteMigration(ctx, migration.Id)
	default:
		panic("Not possible")
	}

	if err != nil {
		return newTxError(migration, err)
	}

	return nil
}

// PlanMigration Plan a migration.
func (ex *MigrationExecutor) PlanMigration(
	ctx context.Context,
	db *sql.DB,
	dialectType dialect.DialectType,
	m Source,
	dir Direction,
	max int,
) ([]*PlannedMigration, *Repository, error) {
	return ex.planMigrationCommon(ctx, db, dialectType, m, dir, max, -1)
}

// PlanMigrationToVersion Plan a migration to version.
func (ex *MigrationExecutor) PlanMigrationToVersion(
	ctx context.Context,
	db *sql.DB,
	dialectType dialect.DialectType,
	m Source,
	dir Direction,
	version int64,
) ([]*PlannedMigration, *Repository, error) {
	return ex.planMigrationCommon(ctx, db, dialectType, m, dir, 0, version)
}

// planMigrationCommon A common method to plan a migration.
func (ex *MigrationExecutor) planMigrationCommon(
	ctx context.Context,
	db *sql.DB,
	dialectType dialect.DialectType,
	m Source,
	dir Direction,
	max int,
	version int64,
) ([]*PlannedMigration, *Repository, error) {
	rep, err := ex.getMigrationRepository(ctx, db, dialectType)
	if err != nil {
		return nil, nil, err
	}

	migrations, err := m.FindMigrations()
	if err != nil {
		return nil, nil, err
	}

	migrationRecords, err := rep.ListMigration(ctx)
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
	if !ex.IgnoreUnknown {
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

	return result, rep, nil
}

func (ex *MigrationExecutor) GetMigrationRecords(ctx context.Context, db *sql.DB, dialectType dialect.DialectType) ([]Record, error) {
	rep, err := ex.getMigrationRepository(ctx, db, dialectType)
	if err != nil {
		return nil, err
	}

	records, err := rep.ListMigration(ctx)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (ex *MigrationExecutor) getMigrationRepository(ctx context.Context, db *sql.DB, dialectType dialect.DialectType) (*Repository, error) {
	d, ok := dialect.Dialects[dialectType]
	if !ok {
		return nil, fmt.Errorf("unknown Dialect: %s", dialectType)
	}

	// Create migration database map
	rep := NewRepository(db, d, ex.SchemaName, ex.getTableName())

	if ex.CreateSchema && strings.TrimSpace(ex.SchemaName) != "" {
		err := rep.CreateSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	if ex.CreateTable {
		err := rep.CreateTable(ctx)
		if err != nil {
			return nil, err
		}
	}

	return rep, nil
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
