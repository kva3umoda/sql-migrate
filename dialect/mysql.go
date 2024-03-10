// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"fmt"
	"strings"
	"time"
)

var _ Dialect = (*MySQLDialect)(nil)

// Implementation of Dialect for MySQL databases.
type MySQLDialect struct {

	// Engine is the storage engine to use "InnoDB" vs "MyISAM" for example
	Engine string

	// Encoding is the character encoding to use for created tables
	Encoding string
}

func (d *MySQLDialect) QuerySuffix() string { return ";" }

func (d *MySQLDialect) ToSqlType(kind DataKind) string {
	switch kind {
	case Bool:
		return "boolean"
	case Int8:
		return "tinyint"
	case Uint8:
		return "tinyint unsigned"
	case Int16:
		return "smallint"
	case Uint16:
		return "smallint unsigned"
	case Int32:
		return "int"
	case Uint32:
		return "int unsigned"
	case Int, Int64:
		return "bigint"
	case Uint, Uint64:
		return "bigint unsigned"
	case Float64, Float32:
		return "double"
	case Datetime:
		return "datetime"
	case String:
		return "text"
	}

	panic("unsupported type")
}

// Returns engine=%s charset=%s  based on values stored on struct
func (d *MySQLDialect) CreateTableSuffix() string {
	if d.Engine == "" || d.Encoding == "" {
		msg := "undefined"

		if d.Engine == "" {
			msg += " MySQLDialect.Engine"
		}
		if d.Engine == "" && d.Encoding == "" {
			msg += ","
		}
		if d.Encoding == "" {
			msg += " MySQLDialect.Encoding"
		}

		msg += ". Check that your MySQLDialect was correctly initialized when declared."

		panic(msg)
	}

	return fmt.Sprintf(" engine=%s charset=%s", d.Engine, d.Encoding)
}

func (d *MySQLDialect) TruncateClause() string {
	return "truncate"
}

func (d *MySQLDialect) SleepClause(s time.Duration) string {
	return fmt.Sprintf("sleep(%f)", s.Seconds())
}

// Returns "?"
func (d *MySQLDialect) BindVar(i int) string {
	return "?"
}

func (d *MySQLDialect) QuoteField(f string) string {
	return "`" + f + "`"
}

func (d *MySQLDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}

	return schema + "." + d.QuoteField(table)
}

func (d *MySQLDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s if not exists", command)
}

func (d *MySQLDialect) IfTableExists(command, schema, table string) string {
	return fmt.Sprintf("%s if exists", command)
}

func (d *MySQLDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s if not exists", command)
}
