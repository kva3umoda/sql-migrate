// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"fmt"
	"strings"
)

var _ Dialect = (*OracleDialect)(nil)

// Implementation of Dialect for Oracle databases.
type OracleDialect struct{}

func NewOracleDialect() *OracleDialect {
	return &OracleDialect{}
}

func (d *OracleDialect) QueryCreateMigrateSchema(schemaName string) string {
	return fmt.Sprintf(
		"CREATE SCHEMA %s;",
		schemaName)
}

func (d *OracleDialect) QueryCreateMigrateTable(schemaName, tableName string) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (id varchar2(255) primary key, applied_at timestamp not null);",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *OracleDialect) QueryDeleteMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"DELETE FROM %s WHERE id = :1",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *OracleDialect) QuerySelectMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s ORDER BY id ASC",
		d.quotedTableForQuery(schemaName, tableName),
	)
}

func (d *OracleDialect) QueryInsertMigrate(schemaName, tableName string) string {
	return fmt.Sprintf("INSERT INTO %s(id, applied_at) VALUES (:1, :2)",
		d.quotedTableForQuery(schemaName, tableName))
}

func (d *OracleDialect) quoteField(f string) string {
	return `"` + strings.ToUpper(f) + `"`
}

func (d *OracleDialect) quotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.quoteField(table)
	}

	return schema + "." + d.quoteField(table)
}
