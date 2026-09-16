package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/SanjayDrop5528/models-go-engine/adapter"
	"github.com/SanjayDrop5528/models-go-engine/execution"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/operation"
	"github.com/SanjayDrop5528/models-go-engine/plan"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"github.com/SanjayDrop5528/models-go-engine/schema"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresAdapter implements the core Adapter interface for PostgreSQL.
type PostgresAdapter struct {
	dsn          string
	schemas      []string
	db           *sql.DB
	ddlGen       *DDLGenerator
	queryBuilder *QueryBuilder
	introspector *Introspector
	mu           sync.RWMutex
	mockStore    map[string][]map[string]any
}

// NewPostgresAdapter creates a new PostgreSQL adapter instance.
func NewPostgresAdapter(dsn string) *PostgresAdapter {
	return &PostgresAdapter{
		dsn:          dsn,
		ddlGen:       NewDDLGenerator(),
		queryBuilder: &QueryBuilder{},
		mockStore:    make(map[string][]map[string]any),
	}
}

// WithSchemas configures specific PostgreSQL database schemas for introspection and operations.
func (a *PostgresAdapter) WithSchemas(schemas ...string) *PostgresAdapter {
	a.schemas = schemas
	return a
}

func (a *PostgresAdapter) Name() string {
	return "postgres"
}

// Capabilities returns PostgreSQL's supported feature matrix.
func (a *PostgresAdapter) Capabilities() adapter.Capabilities {
	return adapter.Capabilities{
		Category:                    adapter.StorageCategoryRelational,
		SupportsTransactions:        true,
		SupportsDDLMigration:        true,
		SupportsProcedures:          true,
		SupportsFunctions:           true,
		SupportsAggregationPipeline: false,
		SupportsJSONValidation:      true,
		SupportsIndexes:             true,
		SupportedSaveModes:          []string{"PROCEDURE", "FUNCTION", "QUERY"},
	}
}

// NativeClient returns the underlying *sql.DB connection handle.
func (a *PostgresAdapter) NativeClient() any {
	return a.DB()
}

func (a *PostgresAdapter) DB() *sql.DB {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db
}

// DatabaseName returns the target database name.
func (a *PostgresAdapter) DatabaseName() string {
	return a.GetDatabaseName()
}

// GetDatabaseName returns the database name extracted from the DSN string, or "in-memory mock" if no DSN.
func (a *PostgresAdapter) GetDatabaseName() string {
	u, err := url.Parse(a.dsn)
	if err != nil || u.Path == "" || u.Path == "/" {
		return "postgres"
	}
	return strings.TrimPrefix(u.Path, "/")
}

func (a *PostgresAdapter) createDatabaseIfNotExists(ctx context.Context) error {
	u, err := url.Parse(a.dsn)
	if err != nil || u.Path == "" || u.Path == "/" {
		return nil
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" || dbName == "postgres" {
		return nil
	}

	sysURL := *u
	sysURL.Path = "/postgres"
	sysDSN := sysURL.String()

	sysDB, err := sql.Open("pgx", sysDSN)
	if err != nil {
		return err
	}
	defer sysDB.Close()

	query := fmt.Sprintf("CREATE DATABASE %s", quoteIdent(dbName))
	log.Printf("[PostgreSQL] Auto-creating target database '%s'...", dbName)
	_, _ = sysDB.ExecContext(ctx, query)
	return nil
}

// getDB lazily and thread-safely connects to the live PostgreSQL database when a DSN is provided.
func (a *PostgresAdapter) getDB(ctx context.Context) (*sql.DB, error) {
	if strings.TrimSpace(a.dsn) == "" {
		return nil, nil // Offline mock fallback mode
	}

	a.mu.RLock()
	if a.db != nil {
		db := a.db
		a.mu.RUnlock()
		return db, nil
	}
	a.mu.RUnlock()

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.db != nil {
		return a.db, nil
	}

	log.Printf("[PostgreSQL] Connecting to live database at: %s...", a.dsn)
	db, err := sql.Open("pgx", a.dsn)
	if err != nil {
		return nil, fmt.Errorf("failed connecting to PostgreSQL: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	pingErr := db.PingContext(pingCtx)
	cancel()

	if pingErr != nil && (strings.Contains(pingErr.Error(), "does not exist") || strings.Contains(pingErr.Error(), "3D000")) {
		_ = db.Close()
		log.Printf("[PostgreSQL] Database does not exist. Auto-creating database...")
		if createErr := a.createDatabaseIfNotExists(ctx); createErr == nil {
			db, err = sql.Open("pgx", a.dsn)
			if err == nil {
				db.SetMaxOpenConns(25)
				db.SetMaxIdleConns(10)
				db.SetConnMaxLifetime(5 * time.Minute)
				pCtx, pCancel := context.WithTimeout(ctx, 5*time.Second)
				pingErr = db.PingContext(pCtx)
				pCancel()
			}
		}
	}

	if pingErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping live PostgreSQL: %w", pingErr)
	}

	log.Printf("[PostgreSQL] ✔ Connected successfully to live PostgreSQL database!")
	a.db = db
	a.introspector = NewIntrospector(db)
	_ = a.ensureMetadataTablesInternal(ctx, db)
	return a.db, nil
}

// EnsureMetadataTables creates 'model_configs' and 'data_models' system metadata tables if they do not exist.
func (a *PostgresAdapter) EnsureMetadataTables(ctx context.Context) error {
	db, err := a.getDB(ctx)
	if err != nil || db == nil {
		return err
	}
	return a.ensureMetadataTablesInternal(ctx, db)
}

func (a *PostgresAdapter) ensureMetadataTablesInternal(ctx context.Context, db *sql.DB) error {
	createSchema := `CREATE SCHEMA IF NOT EXISTS metadata_catalog;`
	if _, err := db.ExecContext(ctx, createSchema); err != nil {
		return fmt.Errorf("failed to create 'metadata_catalog' schema: %w", err)
	}

	createCfgTable := `
	CREATE TABLE IF NOT EXISTS metadata_catalog.model_configs (
		id VARCHAR(255) PRIMARY KEY,
		schema VARCHAR(255),
		name VARCHAR(255) NOT NULL,
		"table" VARCHAR(255),
		ref_name VARCHAR(255),
		is_table BOOLEAN DEFAULT TRUE,
		is_attribute_reference BOOLEAN DEFAULT FALSE,
		description TEXT,
		status VARCHAR(50) DEFAULT 'active',
		version INT DEFAULT 1,
		is_system BOOLEAN DEFAULT FALSE,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		created_by VARCHAR(255),
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_by VARCHAR(255)
	);`

	createDMTable := `
	CREATE TABLE IF NOT EXISTS metadata_catalog.data_models (
		id VARCHAR(255) PRIMARY KEY,
		model_id VARCHAR(255) NOT NULL,
		column_name VARCHAR(255),
		json_field VARCHAR(255),
		ref_name VARCHAR(255),
		description TEXT,
		data_type VARCHAR(100) NOT NULL,
		custom_type_id VARCHAR(255),
		custom_type VARCHAR(255),
		is_array BOOLEAN DEFAULT FALSE,
		is_nullable BOOLEAN DEFAULT TRUE,
		is_required BOOLEAN DEFAULT FALSE,
		is_primary_key BOOLEAN DEFAULT FALSE,
		is_unique BOOLEAN DEFAULT FALSE,
		is_immutable BOOLEAN DEFAULT FALSE,
		is_generated BOOLEAN DEFAULT FALSE,
		default_value TEXT,
		min NUMERIC,
		max NUMERIC,
		min_length INT,
		max_length INT,
		pattern TEXT,
		enum JSONB,
		precision INT,
		scale INT,
		items JSONB,
		is_orbital_reference BOOLEAN DEFAULT FALSE,
		orbital_reference_model_id VARCHAR(255),
		orbital_reference_field_id VARCHAR(255),
		orbital_reference_validation VARCHAR(100),
		reference JSONB,
		status VARCHAR(50) DEFAULT 'active',
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		created_by VARCHAR(255),
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_by VARCHAR(255)
	);`

	createDSTable := `
	CREATE TABLE IF NOT EXISTS metadata_catalog.dataset (
		id VARCHAR(100) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		reference_name VARCHAR(100) NOT NULL UNIQUE,
		driver VARCHAR(50) NOT NULL DEFAULT 'postgres',
		base_collection JSONB NOT NULL,
		join_collections JSONB DEFAULT '[]'::jsonb,
		custom_columns JSONB DEFAULT '[]'::jsonb,
		group_by_fields JSONB DEFAULT '[]'::jsonb,
		schematic_table JSONB DEFAULT '[]'::jsonb,
		filter JSONB DEFAULT '{}'::jsonb,
		filter_params JSONB DEFAULT '[]'::jsonb,
		selected_list JSONB DEFAULT '[]'::jsonb,
		save_mode VARCHAR(50) DEFAULT 'PROCEDURE',
		pipeline TEXT,
		reference_pipeline TEXT,
		status VARCHAR(20) DEFAULT 'ACTIVE',
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_dataset_reference_name ON metadata_catalog.dataset(reference_name);`

	if _, err := db.ExecContext(ctx, createCfgTable); err != nil {
		return fmt.Errorf("failed to create 'metadata_catalog.model_configs' table: %w", err)
	}
	if _, err := db.ExecContext(ctx, createDMTable); err != nil {
		return fmt.Errorf("failed to create 'metadata_catalog.data_models' table: %w", err)
	}
	if _, err := db.ExecContext(ctx, createDSTable); err != nil {
		return fmt.Errorf("failed to create 'metadata_catalog.dataset' table: %w", err)
	}

	log.Printf("[PostgreSQL] ✔ System metadata catalog tables ('metadata_catalog.model_configs', 'metadata_catalog.data_models', 'metadata_catalog.dataset') verified & active.")
	return nil
}

// ImportLiveMetadata introspects live PostgreSQL database tables and auto-populates model_configs & data_models.
func (a *PostgresAdapter) ImportLiveMetadata(ctx context.Context) ([]*model.ModelConfig, []*model.DataModel, error) {
	log.Printf("[PostgreSQL] [Import] Connecting to live database catalog (Database: '%s')...", a.GetDatabaseName())
	db, err := a.getDB(ctx)
	if err != nil {
		log.Printf("[PostgreSQL] ✖ [Import Error] Database connection failed: %v", err)
		return nil, nil, fmt.Errorf("postgres database connection failed: %w", err)
	}
	if db == nil {
		log.Println("[PostgreSQL] ⚠ [Import Info] No PostgreSQL DSN configured (offline mock mode).")
		return nil, nil, errors.New("no active postgres database connection")
	}

	if err := a.ensureMetadataTablesInternal(ctx, db); err != nil {
		log.Printf("[PostgreSQL] ✖ [Import Error] Failed auto-provisioning metadata tables: %v", err)
		return nil, nil, fmt.Errorf("postgres metadata table creation failed: %w", err)
	}

	introspector := NewIntrospector(db)
	tables, err := introspector.ListTables(ctx, a.schemas...)
	if err != nil {
		log.Printf("[PostgreSQL] ✖ [Import Error] Failed querying information_schema.tables: %v", err)
		return nil, nil, fmt.Errorf("failed listing postgres tables: %w", err)
	}

	targetSchemas := "all user schemas"
	if len(a.schemas) > 0 {
		targetSchemas = fmt.Sprintf("schema(s) '%s'", strings.Join(a.schemas, "', '"))
	}
	log.Printf("[PostgreSQL] [Import Introspection] Discovered %d live database table(s) across %s (Database: '%s').", len(tables), targetSchemas, a.GetDatabaseName())

	var configs []*model.ModelConfig
	var fields []*model.DataModel

	for _, item := range tables {
		tableName := item.Name
		schemaName := item.Schema

		if schemaName == "metadata_catalog" || tableName == "model_configs" || tableName == "data_models" || tableName == "dataset" || tableName == "schema_migrations" || tableName == "alembic_version" || tableName == "flyway_schema_history" {
			continue
		}

		schemaObj, err := introspector.IntrospectTableInSchema(ctx, schemaName, tableName)
		if err != nil || schemaObj == nil {
			log.Printf("[PostgreSQL] ⚠ [Import Warning] Could not introspect table '%s.%s': %v (skipping)", schemaName, tableName, err)
			continue
		}

		modelID := tableName
		if schemaName != "public" {
			modelID = fmt.Sprintf("%s_%s", schemaName, tableName)
		}

		modelName := tableName
		if schemaName != "" && schemaName != "public" {
			modelName = fmt.Sprintf("%s_%s", schemaName, tableName)
		}

		cfg := &model.ModelConfig{
			ID:                   modelID,
			Name:                 modelName,
			Table:                tableName,
			RefName:              tableName,
			Schema:               schemaName,
			IsAttributeReference: false,
			Description:          fmt.Sprintf("Auto-imported from PostgreSQL live table '%s.%s'", schemaName, tableName),
			Status:               model.ModelConfigStatusActive,
			Version:              1,
			CreatedAt:            time.Now(),
			UpdatedAt:            time.Now(),
		}
		configs = append(configs, cfg)

		pkCount := 0
		for _, attr := range schemaObj.Attributes {
			if attr.PrimaryKey {
				pkCount++
			}
			fieldID := fmt.Sprintf("%s_%s", modelID, attr.Name)
			dm := &model.DataModel{
				ID:           fieldID,
				ModelID:      modelID,
				ColumnName:   attr.Name,
				JSONField:    attr.Name,
				DataType:     attr.Type,
				IsNullable:   attr.Nullable,
				IsRequired:   !attr.Nullable && !attr.PrimaryKey,
				IsPrimaryKey: attr.PrimaryKey,
				IsUnique:     attr.Unique,
				DefaultValue: attr.Default,
				Status:       model.DataModelStatusActive,
				CreatedAt:    time.Now(),
				UpdatedAt:    time.Now(),
			}
			if attr.Length > 0 {
				dm.MaxLength = &attr.Length
			}
			if attr.Precision > 0 {
				dm.Precision = &attr.Precision
			}
			if attr.Scale > 0 {
				dm.Scale = &attr.Scale
			}

			// Attach Foreign Key / Orbital Reference metadata from Introspection
			for _, rel := range schemaObj.Relations {
				if rel.Column == attr.Name {
					dm.IsOrbitalReference = true
					targetModel := rel.ForeignTable
					targetField := rel.ForeignColumn
					dm.OrbitalReferenceModelID = &targetModel
					dm.OrbitalReferenceFieldID = &targetField
					dm.OrbitalReferenceValidation = model.OrbitalValidationExists
					dm.Reference = &model.OrbitalRefSpec{
						Model:     rel.ForeignTable,
						Attribute: rel.ForeignColumn,
						OnDelete:  rel.OnDelete,
						OnUpdate:  rel.OnUpdate,
					}
					log.Printf("[PostgreSQL] [Import FK] Introspected Foreign Key on '%s.%s' -> %s.%s (Constraint: %s)",
						tableName, attr.Name, rel.ForeignTable, rel.ForeignColumn, rel.Name)
					break
				}
			}

			fields = append(fields, dm)
		}

		log.Printf("[PostgreSQL] [Import Table] Introspected Table '%s' -> ModelConfig: ID='%s', Columns=%d, PKs=%d",
			tableName, modelID, len(schemaObj.Attributes), pkCount)
	}

	log.Printf("[PostgreSQL] ✔ [Import Success] Successfully introspected %d ModelConfig(s) and %d DataModel field(s) directly inside adapter.", len(configs), len(fields))
	return configs, fields, nil
}

func (a *PostgresAdapter) Connect(ctx context.Context) error {
	_, err := a.getDB(ctx)
	return err
}

func (a *PostgresAdapter) Ping(ctx context.Context) error {
	db, err := a.getDB(ctx)
	if err != nil {
		return err
	}
	if db != nil {
		return db.PingContext(ctx)
	}
	return nil
}

func (a *PostgresAdapter) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.db != nil {
		err := a.db.Close()
		a.db = nil
		return err
	}
	return nil
}

// GetSchema introspects the PostgreSQL catalog.
func (a *PostgresAdapter) GetSchema(ctx context.Context, ref model.ModelRef) (*schema.Schema, error) {
	if a.introspector == nil {
		return nil, nil
	}
	tableName := ref.StorageName
	if tableName == "" {
		tableName = ref.Name
	}
	return a.introspector.IntrospectTable(ctx, tableName)
}

// ValidateSchemaPlan checks for PostgreSQL-specific limitations.
func (a *PostgresAdapter) ValidateSchemaPlan(ctx context.Context, p *plan.SchemaPlan) error {
	_, err := a.ddlGen.GenerateStatements(p.Operations)
	return err
}

// PreviewSchemaChange compiles operations to native PostgreSQL SQL statements.
func (a *PostgresAdapter) PreviewSchemaChange(ctx context.Context, p *plan.SchemaPlan) (*plan.SchemaPreview, error) {
	statements, err := a.ddlGen.GenerateStatements(p.Operations)
	if err != nil {
		return nil, err
	}

	nativeActions := make([]plan.NativeAction, 0, len(statements))
	for i, stmt := range statements {
		op := p.Operations[i]
		nativeActions = append(nativeActions, plan.NativeAction{
			Type:        "SQL",
			Description: op.Description,
			Statement:   stmt,
			Destructive: op.Destructive,
		})
	}

	return &plan.SchemaPreview{
		ModelID:              p.ModelID,
		StorageName:          p.StorageName,
		Database:             "postgres",
		Changes:              p.Operations,
		NativeActions:        nativeActions,
		HasDestructive:       p.Destructive,
		RequiresConfirmation: p.Destructive,
		Warnings:             p.Warnings,
		Status:               "READY",
	}, nil
}

// ApplySchemaChange executes DDL migration statements inside a transaction.
func (a *PostgresAdapter) ApplySchemaChange(ctx context.Context, p *plan.SchemaPlan) error {
	statements, err := a.ddlGen.GenerateStatements(p.Operations)
	if err != nil {
		return err
	}

	if a.db == nil {
		// Mock / preview mode - success
		return nil
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin schema migration transaction: %w", err)
	}
	defer tx.Rollback()

	for _, stmt := range statements {
		for _, rawStmt := range strings.Split(stmt, "\n") {
			rawStmt = strings.TrimSpace(rawStmt)
			if rawStmt == "" {
				continue
			}
			if strings.Contains(rawStmt, "FOREIGN KEY") {
				log.Printf("[Schema] [DDL] [Foreign Key] Adding FK constraint for table '%s': %s", p.StorageName, rawStmt)
			} else if strings.Contains(rawStmt, "CREATE TABLE") {
				log.Printf("[Schema] [DDL] [Table Creation] Creating table '%s'...", p.StorageName)
			} else {
				log.Printf("[Schema] [DDL] Executing statement for table '%s': %s", p.StorageName, rawStmt)
			}

			if _, err := tx.ExecContext(ctx, rawStmt); err != nil {
				if strings.Contains(rawStmt, "FOREIGN KEY") && (strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "42P01")) {
					log.Printf("[Schema] [DDL] [Foreign Key Notice] FK constraint deferred for table '%s' (referenced target table not created yet): %v", p.StorageName, err)
					continue
				}
				return fmt.Errorf("failed executing DDL statement for table '%s': %w (SQL: %s)", p.StorageName, err, rawStmt)
			}
		}
	}

	return tx.Commit()
}

func (a *PostgresAdapter) resolveTableName(ref model.ModelRef) string {
	tableName := ref.StorageName
	if tableName == "" {
		tableName = ref.Name
	}
	if tableName == "model_configs" || tableName == "data_models" || tableName == "dataset" {
		return "metadata_catalog." + tableName
	}
	return tableName
}

type modelConfigRow struct {
	ID      string
	Schema  string
	Name    string
	Table   string
	RefName string
}

func (a *PostgresAdapter) resolveRelationJoins(ctx context.Context, db *sql.DB, ref model.ModelRef, tableName string, q query.Query) ([]relationJoin, error) {
	requested := relationRequestMap(q)
	if len(requested) == 0 {
		return nil, nil
	}

	sourceCfg, err := a.findModelConfig(ctx, db, ref.ID, ref.Name, tableName)
	if err != nil {
		return nil, err
	}
	if sourceCfg == nil {
		return nil, fmt.Errorf("cannot resolve relations for model '%s': model_config metadata not found", ref.ID)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT column_name, json_field, orbital_reference_model_id, orbital_reference_field_id, orbital_reference_validation, reference, ref_name
		FROM metadata_catalog.data_models
		WHERE model_id = $1
		  AND is_orbital_reference = TRUE
		  AND COALESCE(status, 'active') = 'active'
	`, sourceCfg.ID)
	if err != nil {
		return nil, fmt.Errorf("failed loading orbital references for model '%s': %w", sourceCfg.ID, err)
	}
	defer rows.Close()

	var joins []relationJoin
	resolved := make(map[string]bool)
	aliasCounts := make(map[string]int)
	seenRelNames := make(map[string]int)

	for rows.Next() {
		var columnName, jsonField, targetModelID, targetFieldID, validation, refName sql.NullString
		var refBytes []byte
		if err := rows.Scan(&columnName, &jsonField, &targetModelID, &targetFieldID, &validation, &refBytes, &refName); err != nil {
			return nil, err
		}
		if !targetModelID.Valid || strings.TrimSpace(targetModelID.String) == "" {
			continue
		}

		targetCfg, err := a.findModelConfig(ctx, db, targetModelID.String, targetModelID.String, targetModelID.String)
		if err != nil {
			return nil, err
		}
		if targetCfg == nil {
			continue
		}

		var baseRelName string
		if refName.Valid && strings.TrimSpace(refName.String) != "" {
			baseRelName = strings.TrimSpace(refName.String)
		} else if len(refBytes) > 0 {
			var refObj struct {
				RelationName string `json:"relation_name"`
				Alias        string `json:"alias"`
			}
			if json.Unmarshal(refBytes, &refObj) == nil {
				if refObj.RelationName != "" {
					baseRelName = refObj.RelationName
				} else if refObj.Alias != "" {
					baseRelName = refObj.Alias
				}
			}
		}
		if baseRelName == "" {
			baseRelName = relationNameFromConfig(targetCfg)
		} else {
			baseRelName = toRelationName(baseRelName)
		}

		relName := uniqueRelationName(baseRelName, seenRelNames)

		spec, wanted := requested[strings.ToLower(relName)]
		var nestedPrefixMatches []string
		for k := range requested {
			if strings.HasPrefix(k, strings.ToLower(relName)+".") {
				nestedPrefixMatches = append(nestedPrefixMatches, k)
			}
		}

		if !wanted && len(nestedPrefixMatches) == 0 {
			continue
		}

		sourceColumn := columnName.String
		if sourceColumn == "" {
			sourceColumn = jsonField.String
		}
		targetColumn := "id"
		if targetFieldID.Valid && strings.TrimSpace(targetFieldID.String) != "" {
			targetColumn = targetFieldID.String
		} else {
			var pkCol sql.NullString
			_ = db.QueryRowContext(ctx, `
				SELECT COALESCE(column_name, json_field)
				FROM metadata_catalog.data_models
				WHERE model_id = $1 AND is_primary_key = TRUE
				LIMIT 1
			`, targetCfg.ID).Scan(&pkCol)
			if pkCol.Valid && strings.TrimSpace(pkCol.String) != "" {
				targetColumn = strings.TrimSpace(pkCol.String)
			}
		}

		alias := uniqueSQLAlias(relName, aliasCounts)

		conditions := append([]string{}, spec.Conditions...)
		if validation.Valid && validation.String == "exists_active" {
			conditions = append(conditions, fmt.Sprintf("COALESCE(%s.is_active, TRUE) = TRUE", quoteIdent(alias)))
		}

		parentJoin := relationJoin{
			Name:             relName,
			SourceTable:      tableName,
			SourceColumn:     sourceColumn,
			TargetTable:      storageNameFromConfig(targetCfg),
			TargetColumn:     targetColumn,
			Alias:            alias,
			Conditions:       conditions,
			SelectedFields:   spec.Fields,
			AdditionalOn:     spec.On,
			OrderBy:          spec.Order,
			LoadWithChildren: spec.LoadWithChildren,
		}

		for _, nestedKey := range nestedPrefixMatches {
			parts := strings.Split(nestedKey, ".")
			if len(parts) >= 2 {
				childName := parts[1]
				childJoin, err := a.resolveChildRelation(ctx, db, targetCfg, alias, childName, requested[nestedKey])
				if err != nil {
					return nil, err
				}
				if childJoin != nil {
					parentJoin.NestedJoins = append(parentJoin.NestedJoins, childJoin)
					resolved[nestedKey] = true
				}
			}
		}

		for _, sub := range spec.SubRelations {
			childJoin, err := a.resolveChildRelation(ctx, db, targetCfg, alias, sub.Name, sub)
			if err != nil {
				return nil, err
			}
			if childJoin != nil {
				parentJoin.NestedJoins = append(parentJoin.NestedJoins, childJoin)
			}
		}

		joins = append(joins, parentJoin)
		resolved[strings.ToLower(relName)] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for name := range requested {
		if !resolved[name] {
			return nil, fmt.Errorf("relation '%s' is not available on model '%s': no matching orbital reference found", requested[name].Name, sourceCfg.ID)
		}
	}

	return joins, nil
}

func (a *PostgresAdapter) resolveChildRelation(ctx context.Context, db *sql.DB, parentCfg *modelConfigRow, parentAlias, childName string, spec query.RelationSpec) (*relationJoin, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, json_field, orbital_reference_model_id, orbital_reference_field_id, orbital_reference_validation, reference, ref_name
		FROM metadata_catalog.data_models
		WHERE model_id = $1
		  AND is_orbital_reference = TRUE
		  AND COALESCE(status, 'active') = 'active'
	`, parentCfg.ID)
	if err != nil {
		return nil, fmt.Errorf("failed loading orbital references for parent model '%s': %w", parentCfg.ID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, jsonField, targetModelID, targetFieldID, validation, refName sql.NullString
		var refBytes []byte
		if err := rows.Scan(&columnName, &jsonField, &targetModelID, &targetFieldID, &validation, &refBytes, &refName); err != nil {
			return nil, err
		}
		if !targetModelID.Valid || strings.TrimSpace(targetModelID.String) == "" {
			continue
		}

		targetCfg, err := a.findModelConfig(ctx, db, targetModelID.String, targetModelID.String, targetModelID.String)
		if err != nil {
			return nil, err
		}
		if targetCfg == nil {
			continue
		}

		var baseRelName string
		if refName.Valid && strings.TrimSpace(refName.String) != "" {
			baseRelName = strings.TrimSpace(refName.String)
		} else if len(refBytes) > 0 {
			var refObj struct {
				RelationName string `json:"relation_name"`
				Alias        string `json:"alias"`
			}
			if json.Unmarshal(refBytes, &refObj) == nil {
				if refObj.RelationName != "" {
					baseRelName = refObj.RelationName
				} else if refObj.Alias != "" {
					baseRelName = refObj.Alias
				}
			}
		}
		if baseRelName == "" {
			baseRelName = relationNameFromConfig(targetCfg)
		} else {
			baseRelName = toRelationName(baseRelName)
		}

		if !strings.EqualFold(baseRelName, childName) {
			continue
		}

		sourceColumn := columnName.String
		if sourceColumn == "" {
			sourceColumn = jsonField.String
		}
		targetColumn := "id"
		if targetFieldID.Valid && strings.TrimSpace(targetFieldID.String) != "" {
			targetColumn = targetFieldID.String
		} else {
			var pkCol sql.NullString
			_ = db.QueryRowContext(ctx, `
				SELECT COALESCE(column_name, json_field)
				FROM metadata_catalog.data_models
				WHERE model_id = $1 AND is_primary_key = TRUE
				LIMIT 1
			`, targetCfg.ID).Scan(&pkCol)
			if pkCol.Valid && strings.TrimSpace(pkCol.String) != "" {
				targetColumn = strings.TrimSpace(pkCol.String)
			}
		}

		alias := parentAlias + "__" + strings.ToLower(baseRelName)

		conditions := append([]string{}, spec.Conditions...)
		if validation.Valid && validation.String == "exists_active" {
			conditions = append(conditions, fmt.Sprintf("COALESCE(%s.is_active, TRUE) = TRUE", quoteIdent(alias)))
		}

		return &relationJoin{
			Name:             baseRelName,
			SourceTable:      parentAlias,
			ParentAlias:      parentAlias,
			SourceColumn:     sourceColumn,
			TargetTable:      storageNameFromConfig(targetCfg),
			TargetColumn:     targetColumn,
			Alias:            alias,
			Conditions:       conditions,
			SelectedFields:   spec.Fields,
			AdditionalOn:     spec.On,
			OrderBy:          spec.Order,
			LoadWithChildren: spec.LoadWithChildren,
		}, nil
	}

	return nil, fmt.Errorf("child relation '%s' is not available on model '%s': no matching orbital reference found", childName, parentCfg.ID)
}

func relationRequestMap(q query.Query) map[string]query.RelationSpec {
	requested := make(map[string]query.RelationSpec)
	for _, name := range q.Relations {
		if strings.TrimSpace(name) == "" {
			continue
		}
		requested[strings.ToLower(name)] = query.RelationSpec{Name: name, LoadWithChildren: q.LoadWithChildren}
	}
	for _, spec := range q.RelationSpecs {
		if strings.TrimSpace(spec.Name) == "" {
			continue
		}
		requested[strings.ToLower(spec.Name)] = spec
	}
	return requested
}

func (a *PostgresAdapter) findModelConfig(ctx context.Context, db *sql.DB, idOrName, name, tableName string) (*modelConfigRow, error) {
	schemaName, cleanTable := splitStorageName(tableName)
	candidates := uniqueNonEmpty(idOrName, name, tableName, cleanTable)
	for _, candidate := range candidates {
		row := db.QueryRowContext(ctx, `
			SELECT id, COALESCE(schema, ''), COALESCE(name, ''), COALESCE("table", ''), COALESCE(ref_name, '')
			FROM metadata_catalog.model_configs
			WHERE id = $1
			   OR name = $1
			   OR "table" = $1
			   OR ref_name = $1
			   OR ($2 <> '' AND schema = $2 AND "table" = $3)
			LIMIT 1
		`, candidate, schemaName, cleanTable)

		var cfg modelConfigRow
		if err := row.Scan(&cfg.ID, &cfg.Schema, &cfg.Name, &cfg.Table, &cfg.RefName); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		return &cfg, nil
	}
	return nil, nil
}

func splitStorageName(storage string) (string, string) {
	storage = strings.Trim(storage, `"`)
	if strings.Contains(storage, ".") {
		parts := strings.SplitN(storage, ".", 2)
		return strings.Trim(parts[0], `"`), strings.Trim(parts[1], `"`)
	}
	return "", storage
}

func storageNameFromConfig(cfg *modelConfigRow) string {
	if cfg == nil {
		return ""
	}
	table := cfg.Table
	if table == "" {
		table = cfg.RefName
	}
	if table == "" {
		table = cfg.Name
	}
	if cfg.Schema != "" && cfg.Schema != "public" && !strings.Contains(table, ".") {
		return cfg.Schema + "." + table
	}
	return table
}

func relationNameFromConfig(cfg *modelConfigRow) string {
	if cfg == nil {
		return "Relation"
	}
	for _, candidate := range []string{cfg.Name, cfg.RefName, cfg.Table, cfg.ID} {
		if strings.TrimSpace(candidate) != "" {
			return toRelationName(candidate)
		}
	}
	return "Relation"
}

func toRelationName(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "Relation"
	}
	if idx := strings.LastIndex(raw, "."); idx >= 0 {
		raw = raw[idx+1:]
	}
	raw = strings.TrimSuffix(raw, "_id")
	raw = strings.TrimSuffix(raw, "Id")
	if strings.HasSuffix(strings.ToLower(raw), "ies") && len(raw) > 3 {
		raw = raw[:len(raw)-3] + "y"
	} else if strings.HasSuffix(strings.ToLower(raw), "s") && len(raw) > 1 {
		raw = raw[:len(raw)-1]
	}

	words := splitIdentifierWords(raw)
	if len(words) == 0 {
		return "Relation"
	}

	var b strings.Builder
	for _, w := range words {
		if w == "" {
			continue
		}
		runes := []rune(w)
		runes[0] = unicode.ToUpper(runes[0])
		b.WriteString(string(runes))
	}
	if b.Len() == 0 {
		return "Relation"
	}
	return b.String()
}

func splitIdentifierWords(s string) []string {
	var words []string
	var current []rune
	for i, r := range s {
		if r == '_' || r == '-' || r == ' ' {
			if len(current) > 0 {
				words = append(words, string(current))
				current = nil
			}
			continue
		}
		if unicode.IsUpper(r) && len(current) > 0 {
			prev := current[len(current)-1]
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (i+1 < len(s) && unicode.IsLower(rune(s[i+1]))) {
				words = append(words, string(current))
				current = nil
			}
		}
		current = append(current, r)
	}
	if len(current) > 0 {
		words = append(words, string(current))
	}
	return words
}

func uniqueRelationName(base string, seen map[string]int) string {
	if base == "" {
		base = "Relation"
	}
	count := seen[base]
	seen[base] = count + 1
	if count == 0 {
		return base
	}
	return fmt.Sprintf("%s%d", base, count+1)
}

func uniqueSQLAlias(base string, seen map[string]int) string {
	if base == "" {
		base = "relation"
	}
	count := seen[base]
	seen[base] = count + 1
	if count == 0 {
		return base
	}
	return fmt.Sprintf("%s%d", base, count+1)
}

func uniqueNonEmpty(values ...string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

// Create inserts a row using parameterized SQL.
func (a *PostgresAdapter) Create(ctx context.Context, ref model.ModelRef, data map[string]any) (map[string]any, error) {
	tableName := a.resolveTableName(ref)
	res := make(map[string]any)
	for k, v := range data {
		res[k] = v
	}

	db, _ := a.getDB(ctx)
	if db == nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.mockStore[tableName] = append(a.mockStore[tableName], res)
		return res, nil
	}

	sqlStr, args := a.queryBuilder.BuildInsert(tableName, data)
	if _, err := db.ExecContext(ctx, sqlStr, args...); err != nil {
		return nil, fmt.Errorf("failed executing insert into PostgreSQL table '%s': %w", tableName, err)
	}
	return res, nil
}

// Find queries PostgreSQL.
func (a *PostgresAdapter) Find(ctx context.Context, ref model.ModelRef, q query.Query) ([]map[string]any, int64, error) {
	tableName := a.resolveTableName(ref)
	db, _ := a.getDB(ctx)
	if db == nil {
		if len(q.Relations) > 0 || len(q.RelationSpecs) > 0 {
			return nil, 0, fmt.Errorf("relations require live PostgreSQL metadata; no database connection is available")
		}
		a.mu.RLock()
		defer a.mu.RUnlock()
		rows := a.mockStore[tableName]
		var results []map[string]any
		for _, r := range rows {
			cp := make(map[string]any)
			for k, v := range r {
				cp[k] = v
			}
			results = append(results, cp)
		}
		return results, int64(len(results)), nil
	}

	relationJoins, err := a.resolveRelationJoins(ctx, db, ref, tableName, q)
	if err != nil {
		return nil, 0, err
	}

	sqlStr, args := a.queryBuilder.BuildSelectWithRelations(tableName, q, relationJoins)
	rows, err := db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}

	var results []map[string]any
	for rows.Next() {
		columns := make([]any, len(cols))
		columnPointers := make([]any, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, 0, err
		}
		rowMap := make(map[string]any)
		for i, colName := range cols {
			val := columnPointers[i].(*any)
			rowMap[colName] = normalizePostgresValue(*val)
		}
		results = append(results, rowMap)
	}

	return results, int64(len(results)), nil
}

func normalizePostgresValue(val any) any {
	switch v := val.(type) {
	case []byte:
		if len(v) == 0 {
			return ""
		}
		var obj map[string]any
		if json.Unmarshal(v, &obj) == nil {
			return obj
		}
		var arr []any
		if json.Unmarshal(v, &arr) == nil {
			return arr
		}
		return string(v)
	default:
		return val
	}
}

// FindOne finds a record by ID.
func (a *PostgresAdapter) FindOne(ctx context.Context, ref model.ModelRef, id any) (map[string]any, error) {
	tableName := a.resolveTableName(ref)
	idStr := fmt.Sprintf("%v", id)

	db, _ := a.getDB(ctx)
	if db == nil {
		a.mu.RLock()
		defer a.mu.RUnlock()
		for _, r := range a.mockStore[tableName] {
			if fmt.Sprintf("%v", r["id"]) == idStr {
				cp := make(map[string]any)
				for k, v := range r {
					cp[k] = v
				}
				return cp, nil
			}
		}
		return nil, fmt.Errorf("record '%v' not found", id)
	}

	q := query.NewQuery().Where("id", query.OpEq, id).LimitOffset(1, 0)
	results, _, err := a.Find(ctx, ref, q)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("record '%v' not found", id)
	}
	return results[0], nil
}

// Update updates a record by ID.
func (a *PostgresAdapter) Update(ctx context.Context, ref model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	tableName := a.resolveTableName(ref)
	idStr := fmt.Sprintf("%v", id)

	db, _ := a.getDB(ctx)
	if db == nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, r := range a.mockStore[tableName] {
			if fmt.Sprintf("%v", r["id"]) == idStr {
				data["id"] = r["id"]
				a.mockStore[tableName][i] = data
				return data, nil
			}
		}
		return nil, fmt.Errorf("record '%v' not found", id)
	}

	sqlStr, args := a.queryBuilder.BuildUpdate(tableName, id, data)
	if _, err := db.ExecContext(ctx, sqlStr, args...); err != nil {
		return nil, err
	}
	return a.FindOne(ctx, ref, id)
}

// Patch updates specific fields by ID.
func (a *PostgresAdapter) Patch(ctx context.Context, ref model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	tableName := a.resolveTableName(ref)
	idStr := fmt.Sprintf("%v", id)

	db, _ := a.getDB(ctx)
	if db == nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, r := range a.mockStore[tableName] {
			if fmt.Sprintf("%v", r["id"]) == idStr {
				for k, v := range data {
					a.mockStore[tableName][i][k] = v
				}
				return a.mockStore[tableName][i], nil
			}
		}
		return nil, fmt.Errorf("record '%v' not found", id)
	}

	return a.Update(ctx, ref, id, data)
}

// Delete removes a record by ID.
func (a *PostgresAdapter) Delete(ctx context.Context, ref model.ModelRef, id any) error {
	tableName := a.resolveTableName(ref)
	idStr := fmt.Sprintf("%v", id)

	db, _ := a.getDB(ctx)
	if db == nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, r := range a.mockStore[tableName] {
			if fmt.Sprintf("%v", r["id"]) == idStr {
				a.mockStore[tableName] = append(a.mockStore[tableName][:i], a.mockStore[tableName][i+1:]...)
				return nil
			}
		}
		return nil
	}

	sqlStr, args := a.queryBuilder.BuildDelete(tableName, id)
	_, err := db.ExecContext(ctx, sqlStr, args...)
	return err
}

// Execute executes a generic operation (function, procedure, command) in PostgreSQL.
func (a *PostgresAdapter) Execute(ctx context.Context, req execution.ExecutionRequest) (*execution.ExecutionResult, error) {
	switch req.Operation {
	case operation.OpFunction:
		// SELECT <target>($1, $2, ...)
		var paramPlaceholders []string
		var args []any
		idx := 1
		for _, v := range req.Arguments {
			paramPlaceholders = append(paramPlaceholders, fmt.Sprintf("$%d", idx))
			args = append(args, v)
			idx++
		}
		query := fmt.Sprintf("SELECT %s(%s);", req.Target, strings.Join(paramPlaceholders, ", "))
		if a.db != nil {
			row := a.db.QueryRowContext(ctx, query, args...)
			var result any
			_ = row.Scan(&result)
			return &execution.ExecutionResult{
				Data:   result,
				Status: "SUCCESS",
				Metadata: map[string]any{
					"query": query,
				},
			}, nil
		}
		return &execution.ExecutionResult{
			Data:   map[string]any{"target": req.Target, "args": req.Arguments},
			Status: "SUCCESS",
			Metadata: map[string]any{
				"sql": query,
			},
		}, nil

	case operation.OpProcedure:
		// CALL <target>($1, $2, ...)
		var paramPlaceholders []string
		var args []any
		idx := 1
		for _, v := range req.Arguments {
			paramPlaceholders = append(paramPlaceholders, fmt.Sprintf("$%d", idx))
			args = append(args, v)
			idx++
		}
		stmt := fmt.Sprintf("CALL %s(%s);", req.Target, strings.Join(paramPlaceholders, ", "))
		if a.db != nil {
			_, err := a.db.ExecContext(ctx, stmt, args...)
			if err != nil {
				return nil, err
			}
		}
		return &execution.ExecutionResult{
			Status: "SUCCESS",
			Metadata: map[string]any{
				"sql": stmt,
			},
		}, nil

	case operation.OpQuery:
		if a.db != nil {
			queryStr := strings.TrimSpace(req.Target)
			rows, err := a.db.QueryContext(ctx, queryStr)
			if err != nil {
				return nil, fmt.Errorf("failed to execute preview query: %w", err)
			}
			defer rows.Close()

			cols, err := rows.Columns()
			if err != nil {
				return nil, err
			}

			var resultRows []map[string]any
			for rows.Next() {
				scanArgs := make([]any, len(cols))
				values := make([]any, len(cols))
				for i := range scanArgs {
					scanArgs[i] = &values[i]
				}

				if err := rows.Scan(scanArgs...); err != nil {
					return nil, err
				}

				rowMap := make(map[string]any)
				for i, col := range cols {
					val := values[i]
					if b, ok := val.([]byte); ok {
						rowMap[col] = string(b)
					} else {
						rowMap[col] = val
					}
				}
				resultRows = append(resultRows, rowMap)
			}

			return &execution.ExecutionResult{
				Data:   resultRows,
				Status: "SUCCESS",
				Metadata: map[string]any{
					"count": len(resultRows),
				},
			}, nil
		}
		return &execution.ExecutionResult{
			Data:   []map[string]any{},
			Status: "SUCCESS",
		}, nil

	case operation.OpDDL, operation.OpCommand, operation.OpCustom:
		if a.db != nil {
			res, err := a.db.ExecContext(ctx, req.Target)
			if err != nil {
				return nil, err
			}
			affected, _ := res.RowsAffected()
			return &execution.ExecutionResult{
				RowsAffected: affected,
				Status:       "SUCCESS",
			}, nil
		}
		return &execution.ExecutionResult{
			Status: "SUCCESS",
			Metadata: map[string]any{
				"target": req.Target,
			},
		}, nil

	default:
		return nil, adapter.ErrOperationNotSupported
	}
}

// Begin starts a new PostgreSQL transaction.
func (a *PostgresAdapter) Begin(ctx context.Context) (adapter.Transaction, error) {
	if a.db == nil {
		return &PostgresTransaction{adapter: a}, nil
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin postgres transaction: %w", err)
	}
	return &PostgresTransaction{
		adapter: a,
		tx:      tx,
	}, nil
}

// PostgresTransaction implements adapter.Transaction for PostgreSQL.
type PostgresTransaction struct {
	adapter *PostgresAdapter
	tx      *sql.Tx
}

func (t *PostgresTransaction) Create(ctx context.Context, model model.ModelRef, data map[string]any) (map[string]any, error) {
	return t.adapter.Create(ctx, model, data)
}

func (t *PostgresTransaction) Find(ctx context.Context, model model.ModelRef, q query.Query) ([]map[string]any, int64, error) {
	return t.adapter.Find(ctx, model, q)
}

func (t *PostgresTransaction) FindOne(ctx context.Context, model model.ModelRef, id any) (map[string]any, error) {
	return t.adapter.FindOne(ctx, model, id)
}

func (t *PostgresTransaction) Update(ctx context.Context, model model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	return t.adapter.Update(ctx, model, id, data)
}

func (t *PostgresTransaction) Patch(ctx context.Context, model model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	return t.adapter.Patch(ctx, model, id, data)
}

func (t *PostgresTransaction) Delete(ctx context.Context, model model.ModelRef, id any) error {
	return t.adapter.Delete(ctx, model, id)
}

func (t *PostgresTransaction) Execute(ctx context.Context, req execution.ExecutionRequest) (*execution.ExecutionResult, error) {
	return t.adapter.Execute(ctx, req)
}

func (t *PostgresTransaction) Commit(ctx context.Context) error {
	if t.tx != nil {
		return t.tx.Commit()
	}
	return nil
}

func (t *PostgresTransaction) Rollback(ctx context.Context) error {
	if t.tx != nil {
		return t.tx.Rollback()
	}
	return nil
}
