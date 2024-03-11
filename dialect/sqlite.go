package dialect

import (
	"fmt"
)

var _ Dialect = (*SqliteDialect)(nil)

type SqliteDialect struct {
}

func NewSqliteDialect() *SqliteDialect {
	return &SqliteDialect{}
}

func (d *SqliteDialect) QueryCreateMigrateSchema(_ string) string {
	return ";"
}

func (d *SqliteDialect) QueryCreateMigrateTable(schemaName, tableName string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id text primary key, applied_at datetime not null);",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SqliteDialect) QueryDeleteMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"DELETE FROM %s WHERE id = ?",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SqliteDialect) QuerySelectMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s ORDER BY id ASC",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SqliteDialect) QueryInsertMigrate(schemaName, tableName string) string {
	return fmt.Sprintf("INSERT INTO %s(id, applied_at) VALUES (?, ?)",
		d.quotedTableForQuery(schemaName, tableName))
}

func (d *SqliteDialect) quoteField(f string) string {
	return `"` + f + `"`
}

// sqlite does not have schemas like PostgreSQL does, so just escape it like normal
func (d *SqliteDialect) quotedTableForQuery(_ string, table string) string {
	return d.quoteField(table)
}
