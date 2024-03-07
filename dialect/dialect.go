// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dialect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// SqlTyper is a type that returns its database type.  Most of the
// time, the type can just use "database/sql/driver".Valuer; but when
// it returns nil for its empty value, it needs to implement SqlTyper
// to have its column type detected properly during table creation.
type SqlTyper interface {
	SqlType() driver.Value
}

// legacySqlTyper prevents breaking clients who depended on the previous
// SqlTyper interface
type legacySqlTyper interface {
	SqlType() driver.Valuer
}

// for fields that exists in DB table, but not exists in struct
type dummyField struct{}

// Scan implements the Scanner interface.
func (nt *dummyField) Scan(value interface{}) error {
	return nil
}

var zeroVal reflect.Value

type DialectType string

const (
	SQLite3    DialectType = "sqlite3"
	Postgres   DialectType = "postgres"
	MySQL      DialectType = "mysql"
	MSSQL      DialectType = "mssql"
	OCI8       DialectType = "oci8"
	GoDrOr     DialectType = "godror"
	Snowflake  DialectType = "snowflake"
	ClickHouse DialectType = "clickhouse"
)

var Dialects = map[DialectType]Dialect{
	SQLite3:   &SqliteDialect{},
	Postgres:  &PostgresDialect{},
	MySQL:     &MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"},
	MSSQL:     &SqlServerDialect{},
	OCI8:      &OracleDialect{},
	GoDrOr:    &OracleDialect{},
	Snowflake: &SnowflakeDialect{},
}

// The Dialect interface encapsulates behaviors that differ across
// SQL databases.  At present the Dialect is only used by CreateTables()
// but this could change in the future
type Dialect interface {
	// adds a suffix to any query, usually ";"
	QuerySuffix() string

	// ToSqlType returns the SQL column type to use when creating a
	// table of the given Go Type.  maxsize can be used to switch based on
	// size.  For example, in MySQL []byte could map to BLOB, MEDIUMBLOB,
	// or LONGBLOB depending on the maxsize
	ToSqlType(val reflect.Type, maxsize int) string

	// string to append to "create table" statement for vendor specific
	// table attributes
	CreateTableSuffix() string

	// string to append to "create index" statement
	CreateIndexSuffix() string

	// string to append to "drop index" statement
	DropIndexSuffix() string

	// string to truncate tables
	TruncateClause() string

	// bind variable string to use when forming SQL statements
	// in many dbs it is "?", but Postgres appears to use $1
	//
	// i is a zero based index of the bind variable in this statement
	//
	BindVar(i int) string

	// Handles quoting of a field name to ensure that it doesn't raise any
	// SQL parsing exceptions by using a reserved word as a field name.
	QuoteField(field string) string

	// Handles building up of a schema.database string that is compatible with
	// the given dialect
	//
	// schema - The schema that <table> lives in
	// table - The table name
	QuotedTableForQuery(schema string, table string) string

	// Existence clause for table creation / deletion
	IfSchemaNotExists(command, schema string) string
	IfTableExists(command, schema, table string) string
	IfTableNotExists(command, schema, table string) string
}

// TargetQueryInserter is implemented by dialects that can perform
// assignment of integer primary key type by executing a query
// like "select sequence.currval from dual".
type TargetQueryInserter interface {
	// TargetQueryInserter runs an insert operation and assigns the
	// automatically generated primary key retrived by the query
	// extracted from the GeneratedIdQuery field of the id column.
	InsertQueryToTarget(exec SqlExecutor, insertSql, idSql string, target any, params ...any) error
}

type SqlExecutor interface {
	WithContext(ctx context.Context) SqlExecutor
	Get(i any, keys ...any) (any, error)
	Insert(list ...any) error
	Update(list ...any) (int64, error)
	Delete(list ...any) (int64, error)
	Exec(query string, args ...any) (sql.Result, error)
	Select(i any, query string, args ...any) ([]any, error)
	SelectInt(query string, args ...any) (int64, error)
	SelectNullInt(query string, args ...any) (sql.NullInt64, error)
	SelectFloat(query string, args ...any) (float64, error)
	SelectNullFloat(query string, args ...any) (sql.NullFloat64, error)
	SelectStr(query string, args ...any) (string, error)
	SelectNullStr(query string, args ...any) (sql.NullString, error)
	SelectOne(holder any, query string, args ...any) error
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

type TypeConverter interface {
	// ToDb converts val to another type. Called before INSERT/UPDATE operations
	ToDb(val any) (any, error)

	// FromDb returns a CustomScanner appropriate for this type. This will be used
	// to hold values returned from SELECT queries.
	//
	// In particular the CustomScanner returned should implement a Binder
	// function appropriate for the Go type you wish to convert the db value to
	//
	// If bool==false, then no custom scanner will be used for this field.
	FromDb(target any) (CustomScanner, bool)
}

// DynamicTable allows the users of gorp to dynamically
// use different database table names during runtime
// while sharing the same golang struct for in-memory data
type DynamicTable interface {
	TableName() string
	SetTableName(string)
}

// Compile-time check that DbMap and Transaction implement the SqlExecutor
// interface.
var _, _ SqlExecutor = &DbMap{}, &Transaction{}

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

// Calls the Exec function on the executor, but attempts to expand any eligible named
// query arguments first.
func maybeExpandNamedQueryAndExec(e SqlExecutor, query string, args ...any) (sql.Result, error) {
	dbMap := extractDbMap(e)

	if len(args) == 1 {
		query, args = maybeExpandNamedQuery(dbMap, query, args)
	}

	return exec(e, query, args...)
}

func extractDbMap(e SqlExecutor) *DbMap {
	switch m := e.(type) {
	case *DbMap:
		return m
	case *Transaction:
		return m.dbmap
	}
	return nil
}

// executor exposes the sql.DB and sql.Tx functions so that it can be used
// on internal functions that need to be agnostic to the underlying object.
type executor interface {
	Exec(query string, args ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func extractExecutorAndContext(e SqlExecutor) (executor, context.Context) {
	switch m := e.(type) {
	case *DbMap:
		return m.Db, m.ctx
	case *Transaction:
		return m.tx, m.ctx
	}
	return nil, nil
}

// maybeExpandNamedQuery checks the given arg to see if it's eligible to be used
// as input to a named query.  If so, it rewrites the query to use
// dialect-dependent bindvars and instantiates the corresponding slice of
// parameters by extracting data from the map / struct.
// If not, returns the input values unchanged.
func maybeExpandNamedQuery(m *DbMap, query string, args []any) (string, []any) {
	var (
		arg    = args[0]
		argval = reflect.ValueOf(arg)
	)
	if argval.Kind() == reflect.Ptr {
		argval = argval.Elem()
	}

	if argval.Kind() == reflect.Map && argval.Type().Key().Kind() == reflect.String {
		return expandNamedQuery(m, query, func(key string) reflect.Value {
			return argval.MapIndex(reflect.ValueOf(key))
		})
	}
	if argval.Kind() != reflect.Struct {
		return query, args
	}
	if _, ok := arg.(time.Time); ok {
		// time.Time is driver.Value
		return query, args
	}
	if _, ok := arg.(driver.Valuer); ok {
		// driver.Valuer will be converted to driver.Value.
		return query, args
	}

	return expandNamedQuery(m, query, argval.FieldByName)
}

var keyRegexp = regexp.MustCompile(`:[[:word:]]+`)

// expandNamedQuery accepts a query with placeholders of the form ":key", and a
// single arg of Kind Struct or Map[string].  It returns the query with the
// dialect's placeholders, and a slice of args ready for positional insertion
// into the query.
func expandNamedQuery(m *DbMap, query string, keyGetter func(key string) reflect.Value) (string, []any) {
	var (
		n    int
		args []any
	)
	return keyRegexp.ReplaceAllStringFunc(query, func(key string) string {
		val := keyGetter(key[1:])
		if !val.IsValid() {
			return key
		}
		args = append(args, val.Interface())
		newVar := m.Dialect.BindVar(n)
		n++
		return newVar
	}), args
}

func columnToFieldIndex(m *DbMap, t reflect.Type, name string, cols []string) ([][]int, error) {
	colToFieldIndex := make([][]int, len(cols))

	// check if type t is a mapped table - if so we'll
	// check the table for column aliasing below
	tableMapped := false
	table := tableOrNil(m, t, name)
	if table != nil {
		tableMapped = true
	}

	// Loop over column names and find field in i to bind to
	// based on column name. all returned columns must match
	// a field in the i struct
	missingColNames := []string{}
	for x := range cols {
		colName := strings.ToLower(cols[x])
		field, found := t.FieldByNameFunc(func(fieldName string) bool {
			field, _ := t.FieldByName(fieldName)
			cArguments := strings.Split(field.Tag.Get("db"), ",")
			fieldName = cArguments[0]

			if fieldName == "-" {
				return false
			} else if fieldName == "" {
				fieldName = field.Name
			}
			if tableMapped {
				colMap := colMapOrNil(table, fieldName)
				if colMap != nil {
					fieldName = colMap.ColumnName
				}
			}
			return colName == strings.ToLower(fieldName)
		})
		if found {
			colToFieldIndex[x] = field.Index
		}
		if colToFieldIndex[x] == nil {
			missingColNames = append(missingColNames, colName)
		}
	}
	if len(missingColNames) > 0 {
		return colToFieldIndex, &NoFieldInTypeError{
			TypeName:        t.Name(),
			MissingColNames: missingColNames,
		}
	}
	return colToFieldIndex, nil
}

func fieldByName(val reflect.Value, fieldName string) *reflect.Value {
	// try to find field by exact match
	f := val.FieldByName(fieldName)

	if f != zeroVal {
		return &f
	}

	// try to find by case insensitive match - only the Postgres driver
	// seems to require this - in the case where columns are aliased in the sql
	fieldNameL := strings.ToLower(fieldName)
	fieldCount := val.NumField()
	t := val.Type()
	for i := 0; i < fieldCount; i++ {
		sf := t.Field(i)
		if strings.ToLower(sf.Name) == fieldNameL {
			f := val.Field(i)
			return &f
		}
	}

	return nil
}

// toSliceType returns the element type of the given object, if the object is a
// "*[]*Element" or "*[]Element". If not, returns nil.
// err is returned if the user was trying to pass a pointer-to-slice but failed.
func toSliceType(i any) (reflect.Type, error) {
	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		// If it's a slice, return a more helpful error message
		if t.Kind() == reflect.Slice {
			return nil, fmt.Errorf("gorp: cannot SELECT into a non-pointer slice: %v", t)
		}
		return nil, nil
	}
	if t = t.Elem(); t.Kind() != reflect.Slice {
		return nil, nil
	}
	return t.Elem(), nil
}

func toType(i any) (reflect.Type, error) {
	t := reflect.TypeOf(i)

	// If a Pointer to a type, follow
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("gorp: cannot SELECT into this type: %v", reflect.TypeOf(i))
	}
	return t, nil
}

type foundTable struct {
	table   *TableMap
	dynName *string
}

func tableFor(m *DbMap, t reflect.Type, i any) (*foundTable, error) {
	if dyn, isDynamic := i.(DynamicTable); isDynamic {
		tableName := dyn.TableName()
		table, err := m.DynamicTableFor(tableName, true)
		if err != nil {
			return nil, err
		}
		return &foundTable{
			table:   table,
			dynName: &tableName,
		}, nil
	}
	table, err := m.TableFor(t, true)
	if err != nil {
		return nil, err
	}
	return &foundTable{table: table}, nil
}

func get(m *DbMap, exec SqlExecutor, i any,
	keys ...any) (any, error) {

	t, err := toType(i)
	if err != nil {
		return nil, err
	}

	foundTable, err := tableFor(m, t, i)
	if err != nil {
		return nil, err
	}
	table := foundTable.table

	plan := table.bindGet()

	v := reflect.New(t)
	if foundTable.dynName != nil {
		retDyn := v.Interface().(DynamicTable)
		retDyn.SetTableName(*foundTable.dynName)
	}

	dest := make([]any, len(plan.argFields))

	conv := m.TypeConverter
	custScan := make([]CustomScanner, 0)

	for x, fieldName := range plan.argFields {
		f := v.Elem().FieldByName(fieldName)
		target := f.Addr().Interface()
		if conv != nil {
			scanner, ok := conv.FromDb(target)
			if ok {
				target = scanner.Holder
				custScan = append(custScan, scanner)
			}
		}
		dest[x] = target
	}

	row := exec.QueryRow(plan.query, keys...)
	err = row.Scan(dest...)
	if err != nil {
		if err == sql.ErrNoRows {
			err = nil
		}
		return nil, err
	}

	for _, c := range custScan {
		err = c.Bind()
		if err != nil {
			return nil, err
		}
	}

	if v, ok := v.Interface().(HasPostGet); ok {
		err := v.PostGet(exec)
		if err != nil {
			return nil, err
		}
	}

	return v.Interface(), nil
}

func delete(m *DbMap, exec SqlExecutor, list ...any) (int64, error) {
	count := int64(0)
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, true)
		if err != nil {
			return -1, err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreDelete); ok {
			err = v.PreDelete(exec)
			if err != nil {
				return -1, err
			}
		}

		bi, err := table.bindDelete(elem)
		if err != nil {
			return -1, err
		}

		res, err := exec.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		if rows == 0 && bi.existingVersion > 0 {
			return lockError(m, exec, table.TableName,
				bi.existingVersion, elem, bi.keys...)
		}

		count += rows

		if v, ok := eval.(HasPostDelete); ok {
			err := v.PostDelete(exec)
			if err != nil {
				return -1, err
			}
		}
	}

	return count, nil
}

func update(m *DbMap, exec SqlExecutor, colFilter ColumnFilter, list ...any) (int64, error) {
	count := int64(0)
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, true)
		if err != nil {
			return -1, err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreUpdate); ok {
			err = v.PreUpdate(exec)
			if err != nil {
				return -1, err
			}
		}

		bi, err := table.bindUpdate(elem, colFilter)
		if err != nil {
			return -1, err
		}

		res, err := exec.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		if rows == 0 && bi.existingVersion > 0 {
			return lockError(m, exec, table.TableName,
				bi.existingVersion, elem, bi.keys...)
		}

		if bi.versField != "" {
			elem.FieldByName(bi.versField).SetInt(bi.existingVersion + 1)
		}

		count += rows

		if v, ok := eval.(HasPostUpdate); ok {
			err = v.PostUpdate(exec)
			if err != nil {
				return -1, err
			}
		}
	}
	return count, nil
}

func insert(m *DbMap, exec SqlExecutor, list ...any) error {
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, false)
		if err != nil {
			return err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreInsert); ok {
			err := v.PreInsert(exec)
			if err != nil {
				return err
			}
		}

		bi, err := table.bindInsert(elem)
		if err != nil {
			return err
		}

		if bi.autoIncrIdx > -1 {
			f := elem.FieldByName(bi.autoIncrFieldName)
			switch inserter := m.Dialect.(type) {
			case TargetQueryInserter:
				var idQuery = table.ColMap(bi.autoIncrFieldName).GeneratedIdQuery
				if idQuery == "" {
					return fmt.Errorf("gorp: cannot set %s value if its ColumnMap.GeneratedIdQuery is empty", bi.autoIncrFieldName)
				}
				err := inserter.InsertQueryToTarget(exec, bi.query, idQuery, f.Addr().Interface(), bi.args...)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("gorp: cannot use autoincrement fields on dialects that do not implement an autoincrementing interface")
			}
		} else {
			_, err := exec.Exec(bi.query, bi.args...)
			if err != nil {
				return err
			}
		}

		if v, ok := eval.(HasPostInsert); ok {
			err := v.PostInsert(exec)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func exec(e SqlExecutor, query string, args ...any) (sql.Result, error) {
	executor, ctx := extractExecutorAndContext(e)

	if ctx != nil {
		return executor.ExecContext(ctx, query, args...)
	}

	return executor.Exec(query, args...)
}

func prepare(e SqlExecutor, query string) (*sql.Stmt, error) {
	executor, ctx := extractExecutorAndContext(e)

	if ctx != nil {
		return executor.PrepareContext(ctx, query)
	}

	return executor.Prepare(query)
}

func queryRow(e SqlExecutor, query string, args ...any) *sql.Row {
	executor, ctx := extractExecutorAndContext(e)

	if ctx != nil {
		return executor.QueryRowContext(ctx, query, args...)
	}

	return executor.QueryRow(query, args...)
}

func query(e SqlExecutor, query string, args ...any) (*sql.Rows, error) {
	executor, ctx := extractExecutorAndContext(e)

	if ctx != nil {
		return executor.QueryContext(ctx, query, args...)
	}

	return executor.Query(query, args...)
}

func begin(m *DbMap) (*sql.Tx, error) {
	if m.ctx != nil {
		return m.Db.BeginTx(m.ctx, nil)
	}

	return m.Db.Begin()
}
