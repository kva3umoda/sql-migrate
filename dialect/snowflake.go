// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"fmt"
	"reflect"
	"strings"
)

type SnowflakeDialect struct {
	suffix          string
	LowercaseFields bool
}

func (d *SnowflakeDialect) QuerySuffix() string { return ";" }

func (d *SnowflakeDialect) ToSqlType(val reflect.Kind) string {
	switch val.Kind() {
	case reflect.Ptr:
		return d.ToSqlType(val.Elem(), maxsize)
	case reflect.Bool:
		return "boolean"
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32:

		return "integer"
	case reflect.Int64, reflect.Uint64:
		return "bigint"
	case reflect.Float64:
		return "double precision"
	case reflect.Float32:
		return "real"
	case reflect.Slice:
		if val.Elem().Kind() == reflect.Uint8 {
			return "binary"
		}
	}

	switch val.Name() {
	case "NullInt64":
		return "bigint"
	case "NullFloat64":
		return "double precision"
	case "NullBool":
		return "boolean"
	case "Time", "NullTime":
		return "timestamp with time zone"
	}

	if maxsize > 0 {
		return fmt.Sprintf("varchar(%d)", maxsize)
	} else {
		return "text"
	}

}

// Returns suffix
func (d *SnowflakeDialect) CreateTableSuffix() string {
	return d.suffix
}

func (d *SnowflakeDialect) CreateIndexSuffix() string {
	return ""
}

func (d *SnowflakeDialect) DropIndexSuffix() string {
	return ""
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
