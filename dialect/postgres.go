package dialect

import (
	"fmt"
	"strings"
)

var _ Dialect = (*PostgresDialect)(nil)

// PostgresDialect Implementation of Dialect for PostgreSQL databases.
type PostgresDialect struct {
}

func NewPostgresDialect() *PostgresDialect {
	return &PostgresDialect{}
}

func (d *PostgresDialect) QueryCreateMigrateSchema(schemaName string) string {
	return fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s;",
		d.quoteField(schemaName))
}

func (d *PostgresDialect) QueryCreateMigrateTable(schemaName, tableName string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id text primary key, applied_at timestamp without time zone not null);",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *PostgresDialect) QueryDeleteMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"DELETE FROM %s WHERE id = $1",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *PostgresDialect) QuerySelectMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s ORDER BY id ASC",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *PostgresDialect) QueryInsertMigrate(schemaName, tableName string) string {
	return fmt.Sprintf("INSERT INTO %s(id, applied_at) VALUES ($1, $2)",
		d.quotedTableForQuery(schemaName, tableName))
}

func (d *PostgresDialect) quoteField(f string) string {
	return `"` + f + `"`
}

func (d *PostgresDialect) quotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.quoteField(table)
	}

	return d.quoteField(schema) + "." + d.quoteField(table)
}
