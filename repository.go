package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"

	`github.com/kva3umoda/sql-migrate/dialect`
)

type transactionKey struct{}

type MigrationRecord struct {
	Id        string
	AppliedAt time.Time
}

type SqlExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type MigrationRepository struct {
	dialect    dialect.Dialect
	db         *sql.DB
	schemaName string
	tableName  string

	logger    Logger
	logPrefix string
}

func NewMigrationRepository(db *sql.DB, dialect dialect.Dialect, schemaName, tableName string, logger Logger) *MigrationRepository {
	return &MigrationRepository{
		db:         db,
		dialect:    dialect,
		schemaName: schemaName,
		tableName:  tableName,
		logger:     logger,
	}
}

func (r *MigrationRepository) BeginTx(ctx context.Context) (*sql.Tx, context.Context, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, ctx, err
	}

	return tx, context.WithValue(ctx, transactionKey{}, tx), nil
}

func (r *MigrationRepository) CreateSchema(ctx context.Context) error {
	query := r.dialect.QueryCreateMigrateSchema(r.schemaName)

	_, err := r.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (r *MigrationRepository) CreateTable(ctx context.Context) error {
	query := r.dialect.QueryCreateMigrateTable(r.schemaName, r.tableName)

	_, err := r.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (r *MigrationRepository) SaveMigration(ctx context.Context, record MigrationRecord) error {
	query := r.dialect.QueryInsertMigrate(r.schemaName, r.tableName)
	_, err := r.ExecContext(ctx, query, record.Id, record.AppliedAt)

	return err
}

func (r *MigrationRepository) DeleteMigration(ctx context.Context, id string) error {
	query := r.dialect.QueryDeleteMigrate(r.schemaName, r.tableName)
	_, err := r.ExecContext(ctx, query, id)

	return err
}

func (r *MigrationRepository) ListMigration(ctx context.Context) ([]MigrationRecord, error) {
	records := make([]MigrationRecord, 0, 10)
	query := r.dialect.QuerySelectMigrate(r.schemaName, r.tableName)

	rows, err := r.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var rec MigrationRecord

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
func (r *MigrationRepository) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	defer r.trace(time.Now(), query, args...)

	res, err := r.use(ctx).ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (r *MigrationRepository) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	defer r.trace(time.Now(), query, args...)

	rows, err := r.use(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// extract - extract transaction from context.
func (r *MigrationRepository) use(ctx context.Context) SqlExecutor {
	tx, ok := ctx.Value(transactionKey{}).(*sql.Tx)
	if !ok {
		return r.db
	}

	return tx
}

func (r *MigrationRepository) trace(started time.Time, query string, args ...any) {
	var margs = argsString(args...)

	r.logger.Tracef("%s%s [%s] (%v)", r.logPrefix, query, margs, (time.Now().Sub(started)))
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
