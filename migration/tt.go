package migrate

import (
	"database/sql"

	"github.com/rubenv/sql-migrate/dialect"
)

type MigrationDirection int

const (
	Up MigrationDirection = iota + 1
	Down
)

var MigrationDialects = map[string]dialect.Dialect{
	"sqlite3":   dialect.SqliteDialect{},
	"postgres":  dialect.PostgresDialect{},
	"mysql":     dialect.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"},
	"mssql":     dialect.SqlServerDialect{},
	"oci8":      dialect.OracleDialect{},
	"godror":    dialect.OracleDialect{},
	"snowflake": dialect.SnowflakeDialect{},
}

type SqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Insert(list ...interface{}) error
	Delete(list ...interface{}) (int64, error)
}
