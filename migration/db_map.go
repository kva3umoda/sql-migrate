package migrate

import (
	`bytes`
	`context`
	`database/sql`
	`database/sql/driver`
	`fmt`
	`io`
	`reflect`
	`strings`
	`time`

	`github.com/rubenv/sql-migrate/dialect`
	`github.com/rubenv/sql-migrate/logger`
)

type Column struct {
	ColumnName string
	DataType   dialect.DataKind
	IsPK       bool
}

type DbMap struct {
	ctx       context.Context
	db        *sql.DB
	Dialect   dialect.Dialect
	logger    logger.Logger
	logPrefix string
}

func NewDbMap(db *sql.DB, dialect dialect.Dialect) *DbMap {
	return &DbMap{
		db:      db,
		Dialect: dialect,
	}
}

func (db *DbMap) CreateTableIfNotExists(schemaName, tableName string, columns []Column) error {
	query := &bytes.Buffer{}

	if strings.TrimSpace(schemaName) != "" {
		db.queryCreateSchemaIfNotExists(query, schemaName)
	}

	db.queryCreateTableIfNotExists(query, schemaName, tableName, columns)

	_, err := db.Exec(query.String())
	if err != nil {
		return err
	}

	return nil
}

func (db *DbMap) queryCreateSchemaIfNotExists(w io.StringWriter, schemaName string) {
	schemaCreate := "create schema"

	_, _ = w.WriteString(db.Dialect.IfSchemaNotExists(schemaCreate, schemaName))
	_, _ = w.WriteString(fmt.Sprintf(" %s;", schemaName))
}

func (db *DbMap) queryCreateTableIfNotExists(w io.StringWriter, schemaName, tableName string, columns []Column) {
	tableCreate := "create table"
	_, _ = w.WriteString(db.Dialect.IfTableNotExists(tableCreate, schemaName, tableName))
	_, _ = w.WriteString(fmt.Sprintf(" %s (", db.Dialect.QuotedTableForQuery(schemaName, tableName)))

	for i, col := range columns {
		if i > 0 {
			_, _ = w.WriteString(", ")
		}

		stype := db.Dialect.ToSqlType(col.DataType)
		_, _ = w.WriteString(fmt.Sprintf("%s %s", db.Dialect.QuoteField(col.ColumnName), stype))

		if col.IsPK {
			_, _ = w.WriteString(" not null")
		}

		if col.IsPK {
			_, _ = w.WriteString(" primary key")
		}
	}

	_, _ = w.WriteString(") ")
	_, _ = w.WriteString(db.Dialect.CreateTableSuffix())
	_, _ = w.WriteString(db.Dialect.QuerySuffix())
}

// Exec runs an arbitrary SQL statement.  args represent the bind parameters.
// This is equivalent to running:  Exec() using database/sql
func (db *DbMap) Exec(query string, args ...any) (sql.Result, error) {
	if db.logger != nil {
		now := time.Now()
		defer db.trace(now, query, args...)
	}

	res, err := db.db.ExecContext(db.ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (m *DbMap) trace(started time.Time, query string, args ...any) {

	if m.logger != nil {
		var margs = argsString(args...)
		m.logger.Printf("%s%s [%s] (%v)", m.logPrefix, query, margs, (time.Now().Sub(started)))
	}
}

func argsString(args ...any) string {
	var margs string
	for i, a := range args {
		v := argValue(a)
		switch v.(type) {
		case string:
			v = fmt.Sprintf("%q", v)
		default:
			v = fmt.Sprintf("%v", v)
		}
		margs += fmt.Sprintf("%d:%s", i+1, v)
		if i+1 < len(args) {
			margs += " "
		}
	}
	return margs
}

func argValue(a any) any {
	v, ok := a.(driver.Valuer)
	if !ok {
		return a
	}
	vV := reflect.ValueOf(v)
	if vV.Kind() == reflect.Ptr && vV.IsNil() {
		return nil
	}
	ret, err := v.Value()
	if err != nil {
		return a
	}
	return ret
}
