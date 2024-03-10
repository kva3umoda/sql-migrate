package migrate

import (
	"fmt"
)

// PlanError happens where no migration plan could be created between the sets
// of already applied migrations and the currently found. For example, when the database
// contains a migration which is not among the migrations list found for an operation.
type PlanError struct {
	Migration    *Migration
	ErrorMessage string
}

func newPlanError(migration *Migration, errorMessage string) error {
	return &PlanError{
		Migration:    migration,
		ErrorMessage: errorMessage,
	}
}

func (p *PlanError) Error() string {
	return fmt.Sprintf("Unable to create migration plan because of %s: %s",
		p.Migration.Id, p.ErrorMessage)
}

// TxError is returned when any error is encountered during a database
// transaction. It contains the relevant *Migration and notes it's Id in the
// Error function output.
type TxError struct {
	Migration *Migration
	Err       error
}

func newTxError(migration *PlannedMigration, err error) error {
	return &TxError{
		Migration: migration.Migration,
		Err:       err,
	}
}

func (e *TxError) Error() string {
	return e.Err.Error() + " handling " + e.Migration.Id
}
