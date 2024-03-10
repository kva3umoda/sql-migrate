// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

const (
	ColumnID        = "id"
	ColumnAppliedAt = "applied_at"
)

type DataKind int

const (
	Bool DataKind = iota
	Int
	Int8
	Int16
	Int32
	Int64
	Uint
	Uint8
	Uint16
	Uint32
	Uint64
	Float32
	Float64
	String
	Datetime
)

type DialectType string

const (
	SQLite3    DialectType = "sqlite3"
	Postgres   DialectType = "postgres"
	MySQL      DialectType = "mysql"
	MSSQL      DialectType = "mssql"
	OCI8       DialectType = "oci8"
	GoDrOr     DialectType = "godror"
	Snowflake  DialectType = "snowflake"
	ClickHouse DialectType = "clickhouse"
)

var Dialects = map[DialectType]Dialect{
	SQLite3:    NewSqliteDialect(),
	Postgres:   NewPostgresDialect(),
	MySQL:      NewMySQLDialect("InnoDB", "UTF8"),
	MSSQL:      NewSqliteDialect(),
	OCI8:       NewOracleDialect(),
	GoDrOr:     NewOracleDialect(),
	Snowflake:  NewSnowflakeDialect(),
	ClickHouse: &ClickHouseDialect{},
}

// The Dialect interface encapsulates behaviors that differ across
// SQL databases.  At present the Dialect is only used by CreateTables()
// but this could change in the future
type Dialect interface {
	// QueryCreateMigrateSchema returns the query - create schema if not exists
	QueryCreateMigrateSchema(schemaName string) string
	// QueryCreateMigrateTable returns the query - create table if not exists
	QueryCreateMigrateTable(schemaName, tableName string) string
	// QueryDeleteMigrate returns the query - delete migration by id
	QueryDeleteMigrate(schemaName, tableName string) string
	// QuerySelectMigrate returns the query - select all migrations order by id ASC
	QuerySelectMigrate(schemaName, tableName string) string
	// QueryInsertMigrate returns the query - insert migration
	QueryInsertMigrate(schemaName, tableName string) string
}
