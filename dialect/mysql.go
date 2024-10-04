package dialect

import (
	"fmt"
	"strings"
)

var _ Dialect = (*MySQLDialect)(nil)

// MySQLDialect Implementation of Dialect for MySQL databases.
type MySQLDialect struct {
	// engine is the storage engine to use "InnoDB" vs "MyISAM" for example
	engine string
	// encoding is the character encoding to use for created tables
	encoding string
}

func NewMySQLDialect(engine, encoding string) *MySQLDialect {
	return &MySQLDialect{
		engine:   engine,
		encoding: encoding,
	}
}

func (d *MySQLDialect) QueryCreateMigrateSchema(schemaName string) string {
	return fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s;",
		d.quoteField(schemaName))
}

func (d *MySQLDialect) QueryCreateMigrateTable(schemaName, tableName string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id text primary key, applied_at datetime not null) engine=%s charset=%s;",
		d.quotedTableForQuery(schemaName, tableName),
		d.engine, d.encoding,
	)
}

func (d *MySQLDialect) QueryDeleteMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"DELETE FROM %s WHERE id = ?",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *MySQLDialect) QuerySelectMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s ORDER BY id ASC",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *MySQLDialect) QueryInsertMigrate(schemaName, tableName string) string {
	return fmt.Sprintf("INSERT INTO %s(id, applied_at) VALUES (?, ?)",
		d.quotedTableForQuery(schemaName, tableName))
}

func (d *MySQLDialect) quoteField(f string) string {
	return "`" + f + "`"
}

func (d *MySQLDialect) quotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.quoteField(table)
	}

	return schema + "." + d.quoteField(table)
}
