// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

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
	SQLite3:   &SqliteDialect{},
	Postgres:  &PostgresDialect{},
	MySQL:     &MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"},
	MSSQL:     &SqlServerDialect{},
	OCI8:      &OracleDialect{},
	GoDrOr:    &OracleDialect{},
	Snowflake: &SnowflakeDialect{},
}

// The Dialect interface encapsulates behaviors that differ across
// SQL databases.  At present the Dialect is only used by CreateTables()
// but this could change in the future
type Dialect interface {
	// adds a suffix to any query, usually ";"
	QuerySuffix() string

	// CreateTableSuffix string to append to "create table" statement for vendor specific
	// table attributes
	CreateTableSuffix() string

	// ToSqlType returns the SQL column type to use when creating a
	// table of the given Go Type.  maxsize can be used to switch based on
	// size.  For example, in MySQL []byte could map to BLOB, MEDIUMBLOB,
	// or LONGBLOB depending on the maxsize
	ToSqlType(kind DataKind) string

	// BindVar bind variable string to use when forming SQL statements
	// in many dbs it is "?", but Postgres appears to use $1
	// i is a zero based index of the bind variable in this statement
	BindVar(i int) string

	// QuoteField Handles quoting of a field name to ensure that it doesn't raise any
	// SQL parsing exceptions by using a reserved word as a field name.
	QuoteField(field string) string

	// QuotedTableForQuery Handles building up of a schema.database string that is compatible with
	// the given dialect
	// schema - The schema that <table> lives in
	// table - The table name
	QuotedTableForQuery(schema string, table string) string

	// IfSchemaNotExists Existence clause for table creation / deletion
	IfSchemaNotExists(command, schema string) string
	IfTableNotExists(command, schema, table string) string
}
