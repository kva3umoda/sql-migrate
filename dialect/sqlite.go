// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"fmt"
)

var _ Dialect = (*SqliteDialect)(nil)

type SqliteDialect struct {
	suffix string
}

func (d *SqliteDialect) QuerySuffix() string { return ";" }

func (d *SqliteDialect) ToSqlType(kind DataKind) string {
	switch kind {
	case Bool:
		return "integer"
	case Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64:
		return "integer"
	case Float64, Float32:
		return "real"
	case Datetime:
		return "datetime"
	case String:
		return "text"
	}

	panic(fmt.Sprintf("unsupported type: %d", kind))
}

// Returns suffix
func (d *SqliteDialect) CreateTableSuffix() string {
	return d.suffix
}

func (d *SqliteDialect) DropIndexSuffix() string {
	return ""
}

// Returns "?"
func (d *SqliteDialect) BindVar(i int) string {
	return "?"
}

func (d *SqliteDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

// sqlite does not have schemas like PostgreSQL does, so just escape it like normal
func (d *SqliteDialect) QuotedTableForQuery(schema string, table string) string {
	return d.QuoteField(table)
}

func (d *SqliteDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s if not exists", command)
}

func (d *SqliteDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s if not exists", command)
}
