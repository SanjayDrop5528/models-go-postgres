package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"strings"
)

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

// ListTables queries PostgreSQL catalogs for all user base tables in the public schema.
func (i *Introspector) ListTables(ctx context.Context) ([]string, error) {
	if i.db == nil {
		return nil, nil
	}
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		ORDER BY table_name;
	`
	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed listing tables from information_schema: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

// IntrospectTable inspects columns, primary keys, and indexes for a specific PostgreSQL table.
func (i *Introspector) IntrospectTable(ctx context.Context, tableName string) (*schema.Schema, error) {
	if i.db == nil {
		return nil, nil
	}

	// 1. Fetch Columns
	colQuery := `
		SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1;
	`
	rows, err := i.db.QueryContext(ctx, colQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
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
			return nil, fmt.Errorf("failed to scan column info: %w", err)
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
		WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public' AND tc.table_name = $1;
	`
	pkRows, err := i.db.QueryContext(ctx, pkQuery, tableName)
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
		WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public' AND tc.table_name = $1;
	`
	fkRows, err := i.db.QueryContext(ctx, fkQuery, tableName)
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
