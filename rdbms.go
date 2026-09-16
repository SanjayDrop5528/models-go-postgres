// Package postgres implements the PostgreSQL storage adapter, query generator,
// DDL schema migrator, introspection engine, and Dataset Studio compiler.
//
// File: rdbms.go
// Usage:
//   This file provides fluent relational query integration methods on PostgresAdapter,
//   exposing *rdbms.DB and *rdbms.SelectQuery configured with the PostgreSQL dialect.
package postgres

import (
	"context"

	"github.com/SanjayDrop5528/models-go-engine/rdbms"
	"github.com/SanjayDrop5528/models-go-engine/rdbms/dialect"
)

// RDBMS returns an active *rdbms.DB instance tied to this adapter's DB and PostgreSQL dialect.
//
// Purpose:
//   Creates a fluent RDBMS query executor wired directly to the live PostgreSQL connection pool.
//
// Where it is used:
//   - Used by advanced query builders, report engines, and relational join queries.
//
// When can it be used:
//   - When executing complex multi-table SQL queries with type-safe dialect builders.
func (a *PostgresAdapter) RDBMS(ctx context.Context) (*rdbms.DB, error) {
	db, err := a.getDB(ctx)
	if err != nil {
		return nil, err
	}
	return rdbms.NewDB(db, dialect.NewPostgreSQL()), nil
}

// NewSelect returns a new rdbms.SelectQuery configured with the PostgreSQL dialect.
//
// Purpose:
//   Provides a chainable SELECT query builder targeted specifically for PostgreSQL syntax and quotation.
//
// Where it is used:
//   - Used in relational query construction and service query handlers.
//
// When can it be used:
//   - When writing fluent SELECT statements with joins, groupings, or subqueries.
func (a *PostgresAdapter) NewSelect(ctx context.Context) (*rdbms.SelectQuery, error) {
	rdb, err := a.RDBMS(ctx)
	if err != nil {
		return nil, err
	}
	return rdb.NewSelect(), nil
}
