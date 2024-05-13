package dialect

import (
	"fmt"
	"strings"
)

var _ Dialect = (*ClickhouseDialect)(nil)

type ClickhouseEngine string

const (
	TinyLogEngine ClickhouseEngine = "TinyLog"
)

type ClickhouseDialect struct {
	engine      ClickhouseEngine
	clusterName string
}

func NewClickhouseDialect(clusterName string, engine ClickhouseEngine) *ClickhouseDialect {
	return &ClickhouseDialect{
		clusterName: clusterName,
		engine:      engine,
	}
}

func (c *ClickhouseDialect) QueryCreateMigrateSchema(databaseName string) string {
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", databaseName)
}

func (c *ClickhouseDialect) QueryCreateMigrateTable(database, tableName string) string {
	if c.clusterName != "" {
		return fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (id String, applied_at DateTime) ENGINE = %s;",
			c.quotedTableForQuery(database, tableName), c.clusterName, c.engine,
		)
	}

	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id String, applied_at DateTime) ENGINE = %s;",
		c.quotedTableForQuery(database, tableName), c.engine,
	)
}

func (c *ClickhouseDialect) QueryDeleteMigrate(database, tableName string) string {
	return ";"
}

func (c *ClickhouseDialect) QuerySelectMigrate(database, tableName string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s ORDER BY id ASC",
		c.quotedTableForQuery(database, tableName),
	)
}

func (c *ClickhouseDialect) QueryInsertMigrate(database, tableName string) string {
	return fmt.Sprintf("INSERT INTO %s(id, applied_at) VALUES (?, ?)",
		c.quotedTableForQuery(database, tableName))
}

func (c *ClickhouseDialect) quoteField(f string) string {
	return `"` + f + `"`
}

func (c *ClickhouseDialect) quotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return c.quoteField(table)
	}

	return c.quoteField(schema) + "." + c.quoteField(table)
}
