package dialect

import (
	`fmt`
)

var _ Dialect = (*ClickhouseDialect)(nil)

type ClickhouseDialect struct {
	engine      string
	clusterName string
}

func NewClickhouseDialect(clusterName, engine string) *ClickhouseDialect {
	return &ClickhouseDialect{
		clusterName: clusterName,
		engine:      engine,
	}
}

func (c *ClickhouseDialect) QueryCreateMigrateSchema(schemaName string) string {
	//TODO implement me
	return ";"
}

func (c *ClickhouseDialect) QueryCreateMigrateTable(schemaName, tableName string) string {
	if c.clusterName != "" {
		return fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (id String, applied_at DateTime) ENGINE = %s PRIMARY KEY(id);",
			tableName, c.clusterName, c.engine,
		)
	}

	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id String, applied_at DateTime) ENGINE = %s PRIMARY KEY(id);",
		tableName, c.engine,
	)
}

func (c *ClickhouseDialect) QueryDeleteMigrate(schemaName, tableName string) string {
	return ";"
}

func (c *ClickhouseDialect) QuerySelectMigrate(schemaName, tableName string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s ORDER BY id ASC",
		tableName,
	)
}

func (c *ClickhouseDialect) QueryInsertMigrate(schemaName, tableName string) string {
	return fmt.Sprintf("INSERT INTO %s(id, applied_at) VALUES (?, ?)", tableName)
}
