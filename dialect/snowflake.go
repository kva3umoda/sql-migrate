// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"fmt"
	"strings"
)

var _ Dialect = (*SnowflakeDialect)(nil)

type SnowflakeDialect struct {
}

func NewSnowflakeDialect() *SnowflakeDialect {
	return &SnowflakeDialect{}
}

func (d *SnowflakeDialect) QueryCreateMigrateSchema(schemaName string) string {
	return fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s;",
		schemaName)
}

func (d *SnowflakeDialect) QueryCreateMigrateTable(schemaName, tableName string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id varchar(255) primary key, applied_at timestamp not null);",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SnowflakeDialect) QueryDeleteMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"DELETE FROM %s WHERE id = ?",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SnowflakeDialect) QuerySelectMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s ORDER BY id ASC",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *SnowflakeDialect) QueryInsertMigrate(schemaName, tableName string) string {
	return fmt.Sprintf("INSERT INTO %s(id, applied_at) VALUES (?, ?)",
		d.quotedTableForQuery(schemaName, tableName))
}

func (d *SnowflakeDialect) quoteField(f string) string {
	return `"` + f + `"`
}

func (d *SnowflakeDialect) quotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.quoteField(table)
	}

	return schema + "." + d.quoteField(table)
}
