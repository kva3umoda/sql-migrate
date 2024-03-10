package dialect

// The Dialect interface encapsulates behaviors that differ across
// SQL databases.
type Dialect interface {
	// QueryCreateMigrateSchema returns the query - create schema if not exists
	QueryCreateMigrateSchema(schemaName string) string
	// QueryCreateMigrateTable returns the query - create table if not exists
	QueryCreateMigrateTable(schemaName, tableName string) string
	// QueryDeleteMigrate returns the query - delete migration by id
	QueryDeleteMigrate(schemaName, tableName string) string
	// QuerySelectMigrate returns the query - select all migrations order by id ASC
	QuerySelectMigrate(schemaName, tableName string) string
	// QueryInsertMigrate returns the query - insert migration
	QueryInsertMigrate(schemaName, tableName string) string
}
