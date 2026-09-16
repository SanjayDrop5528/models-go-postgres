// Package postgres implements the PostgreSQL storage adapter, query generator,
// DDL schema migrator, introspection engine, and Dataset Studio compiler.
//
// File: introspection.go
// Usage:
//
//	This file implements the PostgreSQL Introspector, which queries the PostgreSQL system
//	catalogs (information_schema.schemata, information_schema.tables, information_schema.columns,
//	information_schema.table_constraints, and key_column_usage) to discover live schemas,
//	tables, columns, data types, primary keys, and foreign keys for reverse-engineering into
//	normalized core Schema representations.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/schema"
)

// TableItem represents a table discovered in a database schema.
type TableItem struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

// Introspector queries PostgreSQL catalogs to construct a normalized core Schema.
type Introspector struct {
	db *sql.DB
}

// NewIntrospector creates an introspector instance.
//
// Purpose:
//
//	Initializes an Introspector wrapping a live PostgreSQL *sql.DB connection pool.
//
// Where it is used:
//   - Instantiated in PostgresAdapter.Connect and PostgresAdapter.GetSchema.
//
// When can it be used:
//   - Whenever examining live PostgreSQL database structure without ORMs.
func NewIntrospector(db *sql.DB) *Introspector {
	return &Introspector{db: db}
}

// DB returns the underlying database handle.
//
// Purpose:
//
//	Provides access to the raw *sql.DB for low-level transactions and catalog checks.
//
// Where it is used:
//   - Used by adapter methods requiring direct connection handles.
//
// When can it be used:
//   - When executing custom catalog inspection queries.
func (i *Introspector) DB() *sql.DB {
	return i.db
}

// ListSchemas queries PostgreSQL catalogs for all user-defined schemas.
//
// Purpose:
//
//	Discovers all user schemas excluding system schemas (pg_catalog, information_schema, pg_toast).
//
// Where it is used:
//   - Called by table discovery and multi-schema inspection routines.
//
// When can it be used:
//   - When enumerating available schemas in a multi-tenant PostgreSQL database.
func (i *Introspector) ListSchemas(ctx context.Context) ([]string, error) {
	if i.db == nil {
		return nil, nil
	}
	query := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY schema_name;
	`
	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed listing schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err == nil {
			schemas = append(schemas, s)
		}
	}
	return schemas, nil
}

// ListTables queries PostgreSQL catalogs for user base tables across target schemas.
// If schemas is empty or contains "ALL" / "*", it queries all user-defined schemas (excluding pg_catalog, information_schema, pg_toast).
//
// Purpose:
//
//	Discovers live database tables across designated schemas for catalog registration and live reverse engineering.
//
// Where it is used:
//   - Called by PostgresAdapter.ImportLiveMetadata and database schema sync APIs.
//
// When can it be used:
//   - When discovering existing database tables to map into the models engine.
func (i *Introspector) ListTables(ctx context.Context, schemas ...string) ([]TableItem, error) {
	if i.db == nil {
		return nil, nil
	}

	var targetSchemas []string
	for _, s := range schemas {
		sClean := strings.TrimSpace(s)
		if sClean != "" && !strings.EqualFold(sClean, "ALL") && sClean != "*" {
			targetSchemas = append(targetSchemas, sClean)
		}
	}

	var query string
	var args []any

	if len(targetSchemas) == 0 {
		query = `
			SELECT table_schema, table_name
			FROM information_schema.tables
			WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
			  AND table_type = 'BASE TABLE'
			ORDER BY table_schema, table_name;
		`
	} else {
		placeholders := make([]string, len(targetSchemas))
		for idx, s := range targetSchemas {
			placeholders[idx] = fmt.Sprintf("$%d", idx+1)
			args = append(args, s)
		}
		query = fmt.Sprintf(`
			SELECT table_schema, table_name
			FROM information_schema.tables
			WHERE table_schema IN (%s)
			  AND table_type = 'BASE TABLE'
			ORDER BY table_schema, table_name;
		`, strings.Join(placeholders, ", "))
	}

	rows, err := i.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed listing tables from information_schema: %w", err)
	}
	defer rows.Close()

	var tables []TableItem
	for rows.Next() {
		var item TableItem
		if err := rows.Scan(&item.Schema, &item.Name); err == nil {
			tables = append(tables, item)
		}
	}
	return tables, nil
}

// IntrospectTable inspects columns, primary keys, and indexes for a specific PostgreSQL table.
// If tableName contains "schema.table", it splits it automatically. If omitted, attempts lookup across user schemas.
//
// Purpose:
//
//	Constructs a complete, normalized Schema definition by querying table column metadata, data types,
//	nullability, primary keys, and foreign keys from PostgreSQL information_schema.
//
// Where it is used:
//   - Called by PostgresAdapter.GetSchema and live reverse-engineering pipelines.
//
// When can it be used:
//   - When reverse-engineering an existing PostgreSQL table into engine schema models.
func (i *Introspector) IntrospectTable(ctx context.Context, tableName string) (*schema.Schema, error) {
	schemaName := "public"
	tableOnly := tableName
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		schemaName = parts[0]
		tableOnly = parts[1]
		return i.IntrospectTableInSchema(ctx, schemaName, tableOnly)
	}

	// Try public schema first
	sc, err := i.IntrospectTableInSchema(ctx, "public", tableOnly)
	if err == nil && sc != nil && len(sc.Attributes) > 0 {
		return sc, nil
	}

	// Auto-lookup schema for table if not found in public
	if i.db != nil {
		var foundSchema string
		lookupErr := i.db.QueryRowContext(ctx, `
			SELECT table_schema 
			FROM information_schema.tables 
			WHERE table_name = $1 AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast') 
			LIMIT 1;
		`, tableOnly).Scan(&foundSchema)
		if lookupErr == nil && foundSchema != "" {
			return i.IntrospectTableInSchema(ctx, foundSchema, tableOnly)
		}
	}

	return i.IntrospectTableInSchema(ctx, schemaName, tableOnly)
}

// IntrospectTableInSchema inspects columns, primary keys, and indexes for a specific schema and table.
func (i *Introspector) IntrospectTableInSchema(ctx context.Context, schemaName, tableName string) (*schema.Schema, error) {
	if i.db == nil {
		return nil, nil
	}
	if schemaName == "" {
		schemaName = "public"
	}

	// 1. Fetch Columns
	colQuery := `
		SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2;
	`
	rows, err := i.db.QueryContext(ctx, colQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for '%s.%s': %w", schemaName, tableName, err)
	}
	defer rows.Close()

	fullName := tableName
	if schemaName != "" {
		fullName = fmt.Sprintf("%s.%s", schemaName, tableName)
	}

	s := &schema.Schema{
		Name:        fullName,
		StorageType: model.StorageRelational,
		Attributes:  make([]schema.SchemaAttribute, 0),
		Indexes:     make([]schema.SchemaIndex, 0),
		Relations:   make([]schema.SchemaRelation, 0),
	}

	hasRows := false
	for rows.Next() {
		hasRows = true
		var (
			colName    string
			dataType   string
			maxLen     sql.NullInt32
			numPrec    sql.NullInt32
			numScale   sql.NullInt32
			isNullable string
			colDefault sql.NullString
		)

		if err := rows.Scan(&colName, &dataType, &maxLen, &numPrec, &numScale, &isNullable, &colDefault); err != nil {
			return nil, fmt.Errorf("failed to scan column info for '%s.%s': %w", schemaName, tableName, err)
		}

		attr := schema.SchemaAttribute{
			Name:     colName,
			Type:     FromPostgresType(dataType),
			Nullable: isNullable == "YES",
		}

		if maxLen.Valid {
			attr.Length = int(maxLen.Int32)
		}
		if numPrec.Valid {
			attr.Precision = int(numPrec.Int32)
		}
		if numScale.Valid {
			attr.Scale = int(numScale.Int32)
		}
		if colDefault.Valid {
			attr.Default = colDefault.String
			if strings.Contains(colDefault.String, "nextval(") {
				attr.AutoIncrement = true
			}
		}

		s.Attributes = append(s.Attributes, attr)
	}

	if !hasRows {
		// Table does not exist in live DB
		return nil, nil
	}

	// 2. Fetch Primary Key
	pkQuery := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = $1 AND tc.table_name = $2;
	`
	pkRows, err := i.db.QueryContext(ctx, pkQuery, schemaName, tableName)
	if err == nil {
		defer pkRows.Close()
		var pkCols []string
		for pkRows.Next() {
			var col string
			if err := pkRows.Scan(&col); err == nil {
				pkCols = append(pkCols, col)
				for idx := range s.Attributes {
					if s.Attributes[idx].Name == col {
						s.Attributes[idx].PrimaryKey = true
					}
				}
			}
		}
		if len(pkCols) > 0 {
			s.PrimaryKey = &schema.SchemaKey{
				Name:    fmt.Sprintf("%s_pkey", tableName),
				Columns: pkCols,
			}
		}
	}

	// 3. Fetch Foreign Keys
	fkQuery := `
		SELECT
			kcu.column_name,
			ccu.table_name AS foreign_table,
			ccu.column_name AS foreign_column,
			tc.constraint_name
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu
		  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu
		  ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = $1 AND tc.table_name = $2;
	`
	fkRows, err := i.db.QueryContext(ctx, fkQuery, schemaName, tableName)
	if err == nil {
		defer fkRows.Close()
		for fkRows.Next() {
			var colName, foreignTable, foreignCol, constraintName string
			if err := fkRows.Scan(&colName, &foreignTable, &foreignCol, &constraintName); err == nil {
				s.Relations = append(s.Relations, schema.SchemaRelation{
					Name:          constraintName,
					Column:        colName,
					ForeignTable:  foreignTable,
					ForeignColumn: foreignCol,
					OnDelete:      "CASCADE",
					OnUpdate:      "CASCADE",
				})
			}
		}
	}

	return s, nil
}
