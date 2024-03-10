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
	suffix          string
	LowercaseFields bool
}

func (d *SnowflakeDialect) QuerySuffix() string { return ";" }

func (d *SnowflakeDialect) ToSqlType(kind DataKind) string {
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
		return "varchar(4000)"
	}

	panic(fmt.Sprintf("unsupported type: %d", kind))
}

// Returns suffix
func (d *SnowflakeDialect) CreateTableSuffix() string {
	return d.suffix
}

func (d *SnowflakeDialect) TruncateClause() string {
	return "truncate"
}

// Returns "$(i+1)"
func (d *SnowflakeDialect) BindVar(i int) string {
	return "?"
}

func (d *SnowflakeDialect) QuoteField(f string) string {
	if d.LowercaseFields {
		return `"` + strings.ToLower(f) + `"`
	}
	return `"` + f + `"`
}

func (d *SnowflakeDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}

	return schema + "." + d.QuoteField(table)
}

func (d *SnowflakeDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s if not exists", command)
}

func (d *SnowflakeDialect) IfTableExists(command, schema, table string) string {
	return fmt.Sprintf("%s if exists", command)
}

func (d *SnowflakeDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s if not exists", command)
}
