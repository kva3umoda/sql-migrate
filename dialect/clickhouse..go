package dialect

import (
	`fmt`
)

var _ Dialect = (*ClickHouseDialect)(nil)

var DefaultClickhouseTableEngine = "TinyLog"

type ClickHouseDialect struct {
	Engine      string
	ClusterName string
}

func NewClickHouseDialect(engine string) *ClickHouseDialect {
	return &ClickHouseDialect{
		Engine: engine,
	}
}

func (c *ClickHouseDialect) QuerySuffix() string {
	return ";"
}

func (c *ClickHouseDialect) CreateTableSuffix() string {
	return fmt.Sprintf(" Engine=%s", c.Engine)
}

func (c *ClickHouseDialect) ToSqlType(kind DataKind) string {
	switch kind {
	case Bool:
		return "UInt8"
	case Int8:
		return "Int8"
	case Int16:
		return "Int16"
	case Int32:
		return "Int32"
	case Int, Int64:
		return "Int64"
	case Uint8:
		return "UInt8"
	case Uint16:
		return "UInt16"
	case Uint32:
		return "UInt32"
	case Uint, Uint64:
		return "UInt64"
	case Float32:
		return "Float32"
	case Float64:
		return "Float64"
	case Datetime:
		return "DateTime('UTC')"
	case String:
		return "varchar(255)"
	}

	panic(fmt.Sprintf("unsupported type: %d", kind))
}

func (c *ClickHouseDialect) BindVar(i int) string {
	return "?"
}

func (c *ClickHouseDialect) QuoteField(field string) string {
	return field
}

func (c *ClickHouseDialect) QuotedTableForQuery(schema string, table string) string {
	//TODO implement me
	panic("implement me")
}

func (c *ClickHouseDialect) IfSchemaNotExists(command, schema string) string {
	//TODO implement me
	panic("implement me")
}

func (c *ClickHouseDialect) IfTableNotExists(command, schema, table string) string {
	//TODO implement me
	panic("implement me")
}
