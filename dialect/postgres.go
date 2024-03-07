// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"fmt"
	"strings"
	"time"
)

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

	panic("unsupported type")
}

// Returns suffix
func (d *PostgresDialect) CreateTableSuffix() string {
	return d.suffix
}

func (d *PostgresDialect) CreateIndexSuffix() string {
	return "using"
}

func (d *PostgresDialect) DropIndexSuffix() string {
	return ""
}

func (d *PostgresDialect) TruncateClause() string {
	return "truncate"
}

func (d *PostgresDialect) SleepClause(s time.Duration) string {
	return fmt.Sprintf("pg_sleep(%f)", s.Seconds())
}

// Returns "$(i+1)"
func (d *PostgresDialect) BindVar(i int) string {
	return fmt.Sprintf("$%d", i+1)
}

func (d *PostgresDialect) InsertAutoIncrToTarget(exec SqlExecutor, insertSql string, target any, params ...any) error {
	rows, err := exec.Query(insertSql, params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return fmt.Errorf("No serial value returned for insert: %s Encountered error: %s", insertSql, rows.Err())
	}
	if err := rows.Scan(target); err != nil {
		return err
	}
	if rows.Next() {
		return fmt.Errorf("more than two serial value returned for insert: %s", insertSql)
	}
	return rows.Err()
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
