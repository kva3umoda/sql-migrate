// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"fmt"
	"strings"
)

var _ Dialect = (*PostgresDialect)(nil)

type PostgresDialect struct {
	suffix          string
	LowercaseFields bool
}

func (d *PostgresDialect) QuerySuffix() string { return ";" }

func (d *PostgresDialect) ToSqlType(kind DataKind) string {
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
		return "text"
	}

	panic(fmt.Sprintf("unsupported type: %d", kind))
}

// Returns suffix
func (d *PostgresDialect) CreateTableSuffix() string {
	return d.suffix
}

func (d *PostgresDialect) TruncateClause() string {
	return "truncate"
}

// Returns "$(i+1)"
func (d *PostgresDialect) BindVar(i int) string {
	return fmt.Sprintf("$%d", i+1)
}

func (d *PostgresDialect) QuoteField(f string) string {
	if d.LowercaseFields {
		return `"` + strings.ToLower(f) + `"`
	}
	return `"` + f + `"`
}

func (d *PostgresDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}

	return schema + "." + d.QuoteField(table)
}

func (d *PostgresDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s if not exists", command)
}

func (d *PostgresDialect) IfTableExists(command, schema, table string) string {
	return fmt.Sprintf("%s if exists", command)
}

func (d *PostgresDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s if not exists", command)
}
