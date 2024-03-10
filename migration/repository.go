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

type transactionKey struct{}

type Record struct {
	Id        string
	AppliedAt time.Time
}

type SqlExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
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

func (r *Repository) BeginTx(ctx context.Context) (*sql.Tx, context.Context, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, ctx, err
	}

	return tx, context.WithValue(ctx, transactionKey{}, tx), nil
}

func (r *Repository) CreateTableIfNotExists(ctx context.Context) error {
	query := &bytes.Buffer{}

	if strings.TrimSpace(r.schemaName) != "" {
		r.queryCreateSchemaIfNotExists(query, r.schemaName)
	}

	r.queryCreateTableIfNotExists(query, r.schemaName, r.tableName)

	_, err := r.ExecContext(ctx, query.String())
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) SaveMigration(ctx context.Context, record Record) error {
	query := fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES (%s, %s)",
		r.dialect.QuotedTableForQuery(r.schemaName, r.tableName),
		r.dialect.QuoteField(columnID),
		r.dialect.QuoteField(columnAppliedAt),
		r.dialect.BindVar(0),
		r.dialect.BindVar(1),
	)

	_, err := r.ExecContext(ctx, query, record.Id, record.AppliedAt)

	return err
}

func (r *Repository) DeleteMigration(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
		r.dialect.QuotedTableForQuery(r.schemaName, r.tableName),
		r.dialect.QuoteField(columnID),
		r.dialect.BindVar(0),
	)

	_, err := r.ExecContext(ctx, query, id)

	return err
}

func (r *Repository) ListMigration(ctx context.Context) ([]Record, error) {
	records := make([]Record, 0, 10)
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s ASC",
		r.dialect.QuotedTableForQuery(r.schemaName, r.tableName),
		r.dialect.QuoteField(columnID),
	)

	rows, err := r.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var rec Record

	for rows.Next() {

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
func (r *Repository) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	defer r.trace(time.Now(), query, args...)

	res, err := r.use(ctx).ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (r *Repository) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := r.use(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (r *Repository) queryCreateSchemaIfNotExists(w io.StringWriter, schemaName string) {
	schemaCreate := "create schema"

	_, _ = w.WriteString(r.dialect.IfSchemaNotExists(schemaCreate, schemaName))
	_, _ = w.WriteString(fmt.Sprintf(" %s;", schemaName))
}

func (r *Repository) queryCreateTableIfNotExists(w io.StringWriter, schemaName, tableName string) {
	tableCreate := "create table"
	_, _ = w.WriteString(r.dialect.IfTableNotExists(tableCreate, schemaName, tableName))
	_, _ = w.WriteString(fmt.Sprintf(" %s (", r.dialect.QuotedTableForQuery(schemaName, tableName)))

	stype := r.dialect.ToSqlType(dialect.String)
	_, _ = w.WriteString(fmt.Sprintf("%s %s not null primary key", r.dialect.QuoteField(columnID), stype))
	_, _ = w.WriteString(", ")

	stype = r.dialect.ToSqlType(dialect.Datetime)
	_, _ = w.WriteString(fmt.Sprintf("%s %s not null", r.dialect.QuoteField(columnAppliedAt), stype))

	_, _ = w.WriteString(") ")
	_, _ = w.WriteString(r.dialect.CreateTableSuffix())
	_, _ = w.WriteString(r.dialect.QuerySuffix())
}

// extract - extract transaction from context.
func (r *Repository) use(ctx context.Context) SqlExecutor {
	tx, ok := ctx.Value(transactionKey{}).(*sql.Tx)
	if !ok {
		return r.db
	}

	return tx
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
