package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/SanjayDrop5528/models-go-engine/model"
)

// JoinMetadataReader can be a *sql.DB or a *sql.Tx.
type JoinMetadataReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// VerifyJoinCatalog returns a copy of catalog with physical types, nullability,
// and unique keys read from PostgreSQL. Caller-owned DataModel records are not
// modified. Catalog entries must specify a schema, avoiding search_path guesses.
// Call again after schema changes; do not reuse stale constraint evidence.
func VerifyJoinCatalog(ctx context.Context, db JoinMetadataReader, catalog JoinCatalog) (JoinCatalog, error) {
	result := JoinCatalog{}
	for id, m := range catalog {
		if _, err := (&joinBuilder{catalog: catalog}).model(id); err != nil {
			return nil, err
		}
		if m.Config.Schema == "" {
			return nil, fmt.Errorf("model %q requires an explicit schema for verification", id)
		}
		cfg := *m.Config
		verified := JoinModel{Config: &cfg}
		byColumn := map[string]*model.DataModel{}
		fieldIDs := map[string]bool{}
		for _, f := range m.Fields {
			if f == nil || f.ModelID != id || f.ID == "" || f.ColumnName == "" {
				return nil, fmt.Errorf("invalid field ownership for model %q", id)
			}
			if _, ok := byColumn[f.ColumnName]; ok {
				return nil, fmt.Errorf("duplicate metadata column %q", f.ColumnName)
			}
			if fieldIDs[f.ID] {
				return nil, fmt.Errorf("duplicate metadata field ID %q", f.ID)
			}
			fieldIDs[f.ID] = true
			copyField := *f
			copyField.PhysicalDataType = ""
			verified.Fields = append(verified.Fields, &copyField)
			byColumn[f.ColumnName] = &copyField
		}
		rows, err := db.QueryContext(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r','p')
AND a.attnum > 0 AND NOT a.attisdropped`, cfg.Schema, cfg.Table)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var name, typ string
			var notnull bool
			if err = rows.Scan(&name, &typ, &notnull); err != nil {
				break
			}
			if f := byColumn[name]; f != nil {
				f.PhysicalDataType = typ
				f.IsNullable = !notnull
			}
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return nil, err
		}
		for _, f := range verified.Fields {
			if f.Status == model.DataModelStatusActive && f.PhysicalDataType == "" {
				return nil, fmt.Errorf("metadata field %q has no physical column in %s.%s", f.ID, cfg.Schema, cfg.Table)
			}
		}
		rows, err = db.QueryContext(ctx, `SELECT json_agg(a.attname ORDER BY k.ordinality)::text
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_index i ON i.indexrelid = con.conindid
CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY k(attnum, ordinality)
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
WHERE n.nspname = $1 AND c.relname = $2 AND con.contype IN ('p','u')
AND con.convalidated AND NOT con.condeferrable AND i.indisvalid AND i.indisunique
GROUP BY con.oid`, cfg.Schema, cfg.Table)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var raw string
			if err = rows.Scan(&raw); err != nil {
				break
			}
			var cols []string
			if err = json.Unmarshal([]byte(raw), &cols); err != nil {
				break
			}
			ids := []string{}
			for _, col := range cols {
				f := byColumn[col]
				if f == nil || f.Status != model.DataModelStatusActive {
					ids = nil
					break
				}
				ids = append(ids, f.ID)
			}
			if len(ids) > 0 {
				verified.VerifiedUniqueKeys = append(verified.VerifiedUniqueKeys, ids)
			}
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return nil, err
		}
		result[id] = verified
	}
	return result, nil
}
