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

func (d *OracleDialect) QuerySuffix() string { return ";" }

func (d *OracleDialect) ToSqlType(kind DataKind) string {
	switch kind {
	case Bool:
		return "boolean"
	case Int, Int8, Int16, Int32, Uint, Uint8, Uint16, Uint32:
		return "integer"
	case Int64, Uint64:
		return "bigint"
	case Float64:
		return "double precision"
	case Float32:
		return "real"
	case Datetime:
		return "timestamp with time zone"
	case String:
		return "varchar2(4000)"
	}

	panic("unsupported type")
}

// Returns suffix
func (d *OracleDialect) CreateTableSuffix() string {
	return ""
}

func (d *OracleDialect) TruncateClause() string {
	return "truncate"
}

// Returns "$(i+1)"
func (d *OracleDialect) BindVar(i int) string {
	return fmt.Sprintf(":%d", i+1)
}

func (d *OracleDialect) QuoteField(f string) string {
	return `"` + strings.ToUpper(f) + `"`
}

func (d *OracleDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}

	return schema + "." + d.QuoteField(table)
}

func (d *OracleDialect) IfSchemaNotExists(command, schema string) string {
	return command
}

func (d *OracleDialect) IfTableExists(command, schema, table string) string {
	return command
}

func (d *OracleDialect) IfTableNotExists(command, schema, table string) string {
	return command
}
