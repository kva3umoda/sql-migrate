package migrate

import (
	"context"
	"database/sql"
	"fmt"

	`github.com/kva3umoda/sql-migrate/dialect`
)

var migrateExecutor = NewMigrationExecutor()

// SetTable Set the name of the table used to store migration info.
// Should be called before any other call such as (Exec, ExecMax, ...).
func SetTable(name string) {
	if name != "" {
		migrateExecutor.TableName = name
	}
}

// SetSchema sets the name of a schema that the migration table be referenced.
func SetSchema(name string) {
	if name != "" {
		migrateExecutor.SchemaName = name
	}
}

// SetCreateSchema sets the boolean to enable the creation of the migration schema
func SetCreateSchema(enable bool) {
	migrateExecutor.CreateSchema = enable
}

// SetCreateTable sets the boolean to enable the creation of the migration table
func SetCreateTable(enable bool) {
	migrateExecutor.CreateTable = enable
}

// SetIgnoreUnknown sets the flag that skips database check to see if there is a
// migration in the database that is not in migration source.
//
// This should be used sparingly as it is removing a safety check.
func SetIgnoreUnknown(v bool) {
	migrateExecutor.IgnoreUnknown = v
}

func SetLogger(logger Logger) {
	migrateExecutor.Logger = logger
}

type DialectName string

const (
	SQLite3    DialectName = "sqlite3"
	Postgres   DialectName = "postgres"
	MySQL      DialectName = "mysql"
	MSSQL      DialectName = "mssql"
	OCI8       DialectName = "oci8"
	GoDrOr     DialectName = "godror"
	Snowflake  DialectName = "snowflake"
	ClickHouse DialectName = "clickhouse"
)

func GetDialect(name DialectName) (dialect.Dialect, error) {
	switch name {
	case SQLite3:
		return dialect.NewSqliteDialect(), nil
	case Postgres:
		return dialect.NewPostgresDialect(), nil
	case MySQL:
		return dialect.NewMySQLDialect("InnoDB", "UTF8"), nil
	case MSSQL:
		return dialect.NewSqliteDialect(), nil
	case OCI8:
		return dialect.NewOracleDialect(), nil
	case GoDrOr:
		return dialect.NewOracleDialect(), nil
	case Snowflake:
		return dialect.NewSnowflakeDialect(), nil
	case ClickHouse:
		return dialect.NewClickhouseDialect("", "TinyLog"), nil
	}

	return nil, fmt.Errorf("unknown dialect: %s", name)
}

// Exec Execute a set of migrations
// Returns the number of applied migrations.
func Exec(db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection) (int, error) {
	return ExecMaxContext(context.Background(), db, dialect, m, dir, 0)
}

// ExecContext Execute a set of migrations with an input context.
// Returns the number of applied migrations.
func ExecContext(ctx context.Context, db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection) (int, error) {
	return ExecMaxContext(ctx, db, dialect, m, dir, 0)
}

// ExecMax Execute a set of migrations
// Will apply at most `max` migrations. Pass 0 for no limit (or use Exec).
// Returns the number of applied migrations.
func ExecMax(db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection, max int) (int, error) {
	return migrateExecutor.ExecMax(db, dialect, m, dir, max)
}

// ExecMaxContext Execute a set of migrations with an input context.
// Will apply at most `max` migrations. Pass 0 for no limit (or use Exec).
// Returns the number of applied migrations.
func ExecMaxContext(ctx context.Context, db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection, max int) (int, error) {
	return migrateExecutor.ExecMaxContext(ctx, db, dialect, m, dir, max)
}

// ExecVersion Execute a set of migrations
// Will apply at the target `version` of migration. Cannot be a negative value.
// Returns the number of applied migrations.
func ExecVersion(db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection, version int64) (int, error) {
	return ExecVersionContext(context.Background(), db, dialect, m, dir, version)
}

// ExecVersionContext Execute a set of migrations with an input context.
// Will apply at the target `version` of migration. Cannot be a negative value.
// Returns the number of applied migrations.
func ExecVersionContext(ctx context.Context, db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection, version int64) (int, error) {
	if version < 0 {
		return 0, fmt.Errorf("target version %d should not be negative", version)
	}
	return migrateExecutor.ExecVersionContext(ctx, db, dialect, m, dir, version)
}

// PlanMigration Plan a migration.
func PlanMigration(db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection, max int) ([]*PlannedMigration, *MigrationRepository, error) {
	return migrateExecutor.PlanMigration(context.Background(), db, dialect, m, dir, max)
}

// PlanMigrationToVersion Plan a migration to version.
func PlanMigrationToVersion(db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection, version int64) ([]*PlannedMigration, *MigrationRepository, error) {
	return migrateExecutor.PlanMigrationToVersion(context.Background(), db, dialect, m, dir, version)
}

// SkipMax Skip a set of migrations
// Will skip at most `max` migrations. Pass 0 for no limit.
// Returns the number of skipped migrations.
func SkipMax(db *sql.DB, dialect dialect.Dialect, m MigrationSource, dir MigrationDirection, max int) (int, error) {
	return migrateExecutor.SkipMax(context.Background(), db, dialect, m, dir, max)
}

func GetMigrationRecords(db *sql.DB, dialect dialect.Dialect) ([]MigrationRecord, error) {
	return migrateExecutor.GetMigrationRecords(context.Background(), db, dialect)
}
