package dialect

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
	panic("implement me")
}

func (c *ClickhouseDialect) QueryCreateMigrateTable(schemaName, tableName string) string {
	//TODO implement me
	panic("implement me")
}

func (c *ClickhouseDialect) QueryDeleteMigrate(schemaName, tableName string) string {
	//TODO implement me
	panic("implement me")
}

func (c *ClickhouseDialect) QuerySelectMigrate(schemaName, tableName string) string {
	//TODO implement me
	panic("implement me")
}

func (c *ClickhouseDialect) QueryInsertMigrate(schemaName, tableName string) string {
	//TODO implement me
	panic("implement me")
}
