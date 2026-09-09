package postgres

import (
	"context"

	"github.com/SanjayDrop5528/models-go-engine/rdbms"
	"github.com/SanjayDrop5528/models-go-engine/rdbms/dialect"
)

// RDBMS returns an active *rdbms.DB instance tied to this adapter's DB and PostgreSQL dialect.
func (a *PostgresAdapter) RDBMS(ctx context.Context) (*rdbms.DB, error) {
	db, err := a.getDB(ctx)
	if err != nil {
		return nil, err
	}
	return rdbms.NewDB(db, dialect.NewPostgreSQL()), nil
}

// NewSelect returns a new rdbms.SelectQuery configured with the PostgreSQL dialect.
func (a *PostgresAdapter) NewSelect(ctx context.Context) (*rdbms.SelectQuery, error) {
	rdb, err := a.RDBMS(ctx)
	if err != nil {
		return nil, err
	}
	return rdb.NewSelect(), nil
}
