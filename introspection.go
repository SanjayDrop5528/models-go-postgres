package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"strings"
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
func NewIntrospector(db *sql.DB) *Introspector {
	return &Introspector{db: db}
}

func (i *Introspector) DB() *sql.DB {
	return i.db
}

// ListTables queries PostgreSQL catalogs for user base tables across target schemas.
// If schemas is empty, it queries all user-defined schemas (excluding pg_catalog, information_schema, pg_toast).
func (i *Introspector) ListTables(ctx context.Context, schemas ...string) ([]TableItem, error) {
	if i.db == nil {
		return nil, nil
	}

	var query string
	var args []any

	if len(schemas) == 0 {
		query = `
			SELECT table_schema, table_name
			FROM information_schema.tables
			WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
			  AND table_type = 'BASE TABLE'
			ORDER BY table_schema, table_name;
		`
	} else {
		placeholders := make([]string, len(schemas))
		for idx, s := range schemas {
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
// If tableName contains "schema.table", it splits it automatically. Defaults schema to "public" if omitted.
func (i *Introspector) IntrospectTable(ctx context.Context, tableName string) (*schema.Schema, error) {
	schemaName := "public"
	tableOnly := tableName
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		schemaName = parts[0]
		tableOnly = parts[1]
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

	s := &schema.Schema{
		Name:        tableName,
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
