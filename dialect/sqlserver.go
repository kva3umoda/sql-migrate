// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"fmt"
	"strings"
)

// Implementation of Dialect for Microsoft SQL Server databases.
// Use gorp.SqlServerDialect{"2005"} for legacy datatypes.
// Tested with driver: github.com/denisenkom/go-mssqldb

var _ Dialect = (*SnowflakeDialect)(nil)

type SqlServerDialect struct {
}

func NewSqlServerDialect() *SqlServerDialect {
	return &SqlServerDialect{}
}

func (d *SqlServerDialect) QueryCreateMigrateSchema(schemaName string) string {
	return fmt.Sprintf(
		"if schema_id(N'%s') is null CREATE SCHEMA IF NOT EXISTS %s;",
		schemaName, schemaName)
}

func (d *SqlServerDialect) QueryCreateMigrateTable(schemaName, tableName string) string {
	var schemaClause string
	if strings.TrimSpace(schemaName) != "" {
		schemaClause = fmt.Sprintf("%s.", schemaName)
	}

	return fmt.Sprintf(
		"if object_id('%s%s') is null CREATE TABLE IF NOT EXISTS %s (id nvarchar(255) primary key, applied_at datetime2 not null);",
		schemaClause, tableName,
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SqlServerDialect) QueryDeleteMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"DELETE FROM %s WHERE id = ?",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SqlServerDialect) QuerySelectMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s ORDER BY id ASC",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SqlServerDialect) QueryInsertMigrate(schemaName, tableName string) string {
	return fmt.Sprintf("INSERT INTO %s(id, applied_at) VALUES (?, ?)",
		d.quotedTableForQuery(schemaName, tableName))
}

func (d *SqlServerDialect) quoteField(f string) string {
	return "[" + strings.Replace(f, "]", "]]", -1) + "]"
}

func (d *SqlServerDialect) quotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.quoteField(table)
	}
	return d.quoteField(schema) + "." + d.quoteField(table)
}
