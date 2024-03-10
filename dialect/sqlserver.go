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

type SqlServerDialect struct {

	// If set to "2005" legacy datatypes will be used
	Version string
}

func (d *SqlServerDialect) ToSqlType(kind DataKind) string {
	switch kind {
	case Bool:
		return "bit"
	case Int8:
		return "tinyint"
	case Uint8:
		return "smallint"
	case Int16:
		return "smallint"
	case Uint16:
		return "int"
	case Int, Int32:
		return "int"
	case Uint, Uint32:
		return "bigint"
	case Int64:
		return "bigint"
	case Uint64:
		return "numeric(20,0)"
	case Float32:
		return "float(24)"
	case Float64:
		return "float(53)"
	case Datetime:
		if d.Version == "2005" {
			return "datetime"
		}

		return "datetime2"
	case String:
		return "nvarchar(255)"
	}

	panic(fmt.Sprintf("unsupported type: %d", kind))
}

func (d *SqlServerDialect) CreateTableSuffix() string { return ";" }

// Returns "?"
func (d *SqlServerDialect) BindVar(i int) string {
	return "?"
}

func (d *SqlServerDialect) QuoteField(f string) string {
	return "[" + strings.Replace(f, "]", "]]", -1) + "]"
}

func (d *SqlServerDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}
	return d.QuoteField(schema) + "." + d.QuoteField(table)
}

func (d *SqlServerDialect) QuerySuffix() string { return ";" }

func (d *SqlServerDialect) IfSchemaNotExists(command, schema string) string {
	s := fmt.Sprintf("if schema_id(N'%s') is null %s", schema, command)

	return s
}

func (d *SqlServerDialect) IfTableNotExists(command, schema, table string) string {
	var schemaClause string
	if strings.TrimSpace(schema) != "" {
		schemaClause = fmt.Sprintf("%s.", schema)
	}

	s := fmt.Sprintf("if object_id('%s%s') is null %s", schemaClause, table, command)
	return s
}
