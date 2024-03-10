package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/rubenv/sql-migrate/dialect"
	"github.com/rubenv/sql-migrate/logger"
)

const (
	columnID        = "id"
	columnAppliedAt = "applied_at"
)

type Record struct {
	Id        string
	AppliedAt time.Time
}

type Repository struct {
	dialect    dialect.Dialect
	db         *sql.DB
	schemaName string
	tableName  string

	logger    logger.Logger
	logPrefix string
}

func NewRepository(db *sql.DB, dialect dialect.Dialect, schemaName, tableName string) *Repository {
	return &Repository{
		db:         db,
		dialect:    dialect,
		schemaName: schemaName,
		tableName:  tableName,
		logger:     logger.DefaultLogger(),
	}
}

func (r *Repository) CreateSchemaIfNotExists(ctx context.Context) error {
	query := &bytes.Buffer{}

	if strings.TrimSpace(r.schemaName) != "" {
		r.queryCreateSchemaIfNotExists(query, r.schemaName)
	}

	r.queryCreateTableIfNotExists(query, r.schemaName, r.tableName, columns)

	_, err := r.Exec(ctx, query.String())
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) ListMigration(ctx context.Context) ([]*Record, error) {
	records := make([]*Record, 0, 10)
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s ASC",
		r.dialect.QuotedTableForQuery(r.schemaName, r.tableName),
		r.dialect.QuoteField(columnID),
	)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		rec := new(Record)

		err = rows.Scan(&rec.Id, &rec.AppliedAt)
		if err != nil {
			return nil, err
		}

		records = append(records, rec)
	}

	return records, nil
}

// Exec runs an arbitrary SQL statement.  args represent the bind parameters.
// This is equivalent to running:  Exec() using database/sql
func (r *Repository) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	defer r.trace(time.Now(), query, args...)

	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (r *Repository) queryCreateSchemaIfNotExists(w io.StringWriter, schemaName string) {
	schemaCreate := "create schema"

	_, _ = w.WriteString(r.dialect.IfSchemaNotExists(schemaCreate, schemaName))
	_, _ = w.WriteString(fmt.Sprintf(" %s;", schemaName))
}

func (r *Repository) queryCreateTableIfNotExists(w io.StringWriter, schemaName, tableName string, columns []Column) {
	tableCreate := "create table"
	_, _ = w.WriteString(r.dialect.IfTableNotExists(tableCreate, schemaName, tableName))
	_, _ = w.WriteString(fmt.Sprintf(" %s (", r.dialect.QuotedTableForQuery(schemaName, tableName)))

	for i, col := range columns {
		if i > 0 {
			_, _ = w.WriteString(", ")
		}

		stype := r.dialect.ToSqlType(col.DataType)
		_, _ = w.WriteString(fmt.Sprintf("%s %s", r.dialect.QuoteField(col.ColumnName), stype))

		if col.IsPK {
			_, _ = w.WriteString(" not null")
		}

		if col.IsPK {
			_, _ = w.WriteString(" primary key")
		}
	}

	_, _ = w.WriteString(") ")
	_, _ = w.WriteString(r.dialect.CreateTableSuffix())
	_, _ = w.WriteString(r.dialect.QuerySuffix())
}

func (r *Repository) trace(started time.Time, query string, args ...any) {
	var margs = argsString(args...)
	r.logger.Printf("%s%s [%s] (%v)", r.logPrefix, query, margs, (time.Now().Sub(started)))
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
