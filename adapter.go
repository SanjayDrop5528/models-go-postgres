// Package postgres provides the PostgreSQL database adapter implementing the engine's Adapter interface.
//
// Usage:
// This package manages live PostgreSQL database connectivity, schema introspection, metadata cataloging,
// DDL migration execution, relational and raw query execution, transactional operations, and dataset routine compilation.
//
// Standalone Usage:
//
//	adp := postgres.NewPostgresAdapter("postgres://user:pass@localhost:5432/mydb?sslmode=disable")
//	defer adp.Disconnect(ctx)
//
// Combined Usage with Validation Engine & Engine Services:
//
//	valEngine := validation.NewValidationEngine(validation.WithAdapter(adp))
//	datasetService := service.NewDataSetService(nil, adp)
//	datasetService.RegisterCompiler("postgres", adp.DataSetCompiler())
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"regexp"
	"sort"
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

const (
	ansiColorReset      = "\033[0m"
	ansiColorYellowBold = "\033[1;33m"
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
	ownsDB       bool
}

// NewPostgresAdapter creates a new PostgreSQL adapter instance.
//
// Purpose:
// Initializes a PostgreSQL adapter with connection details, DDL generators, query builders, and mock fallback stores.
//
// Where it is used:
// In application bootstrap, CLI tools, server setup (e.g. server.go), and test suites.
//
// When can it be used:
// Can be used whenever an application needs to interact with a PostgreSQL database or mock in-memory fallback.
func NewPostgresAdapter(dsn string) *PostgresAdapter {
	return &PostgresAdapter{
		dsn:          dsn,
		ddlGen:       NewDDLGenerator(),
		queryBuilder: &QueryBuilder{},
		mockStore:    make(map[string][]map[string]any),
		ownsDB:       true,
	}
}

// NewPostgresAdapterFromDB wraps an existing database/sql pool. The caller
// retains ownership of the pool, so Close detaches the adapter without closing
// the shared connection. This is intended for tenant routers and applications
// that already manage their own pools.
func NewPostgresAdapterFromDB(db *sql.DB) *PostgresAdapter {
	a := NewPostgresAdapter("")
	a.db = db
	a.ownsDB = false
	a.introspector = NewIntrospector(db)
	return a
}

// WithSchemas configures specific PostgreSQL database schemas for introspection and operations.
//
// Purpose:
// Restricts or configures the target PostgreSQL schemas (e.g. "public", "tenant_a") for introspection and table discovery.
//
// Where it is used:
// Called during adapter initialization before running introspection or migration commands.
//
// When can it be used:
// Can be used when working with multi-schema PostgreSQL databases or non-default schemas.
func (a *PostgresAdapter) WithSchemas(schemas ...string) *PostgresAdapter {
	a.schemas = schemas
	return a
}

// Name returns the identifier of the adapter driver ("postgres").
//
// Purpose:
// Identifies the adapter type to the engine for driver selection and dispatch.
//
// Where it is used:
// In engine registry, logging, and capability negotiation.
//
// When can it be used:
// Can be used whenever an adapter driver name is inspected.
func (a *PostgresAdapter) Name() string {
	return "postgres"
}

// Capabilities returns PostgreSQL's supported feature matrix.
//
// Purpose:
// Advertises supported capabilities (transactions, DDL migration, stored procedures, functions, JSON validation).
//
// Where it is used:
// In validation engine, query planner, and dataset service to verify database feature support.
//
// When can it be used:
// Can be used before initiating advanced database features like stored procedures or JSON schema checks.
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
	a.mu.RLock()
	if a.db != nil {
		db := a.db
		a.mu.RUnlock()
		return db, nil
	}
	a.mu.RUnlock()

	if strings.TrimSpace(a.dsn) == "" {
		return nil, nil // Offline mock fallback mode
	}

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
		is_encrypted BOOLEAN DEFAULT FALSE,
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
		load_with_children BOOLEAN DEFAULT FALSE,
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
	if _, err := db.ExecContext(ctx, `ALTER TABLE metadata_catalog.data_models ADD COLUMN IF NOT EXISTS is_encrypted BOOLEAN DEFAULT FALSE`); err != nil {
		return fmt.Errorf("failed to add data_models.is_encrypted: %w", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE metadata_catalog.data_models ADD COLUMN IF NOT EXISTS load_with_children BOOLEAN DEFAULT FALSE`); err != nil {
		return fmt.Errorf("failed to add data_models.load_with_children: %w", err)
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

		modelName := fmt.Sprintf("%s.%s", schemaName, tableName)
		if schemaName == "" {
			modelName = fmt.Sprintf("public.%s", tableName)
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
		var err error
		if a.ownsDB {
			err = a.db.Close()
		}
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
	for _, spec := range requested {
		if err := validateRelationSpecPredicates(spec); err != nil {
			return nil, fmt.Errorf("invalid relation %q: %w", spec.Name, err)
		}
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
		ORDER BY id, column_name
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
		targetColumn, err := a.resolveMetadataColumn(ctx, db, targetCfg.ID, targetFieldID.String)
		if err != nil {
			return nil, err
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
				childSpec := relationSpecForPath(parts[1:], requested[nestedKey])
				childJoin, err := a.resolveChildRelation(ctx, db, targetCfg, alias, childName, childSpec)
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
	if err := rows.Close(); err != nil {
		return nil, err
	}

	// A child table can point back to the source model, for example
	// order_products.order_id -> orders.id. Resolve that inverse edge as a
	// collection so Relation("OrderProducts") returns [] rather than an object.
	reverseRows, err := db.QueryContext(ctx, `
		SELECT dm.column_name, dm.json_field, dm.orbital_reference_field_id,
		       mc.id, COALESCE(mc.schema, ''), COALESCE(mc.name, ''),
		       COALESCE(mc."table", ''), COALESCE(mc.ref_name, '')
		FROM metadata_catalog.data_models dm
		JOIN metadata_catalog.model_configs mc ON mc.id = dm.model_id
		WHERE dm.is_orbital_reference = TRUE
		  AND COALESCE(dm.status, 'active') = 'active'
		  AND (dm.orbital_reference_model_id = $1
		       OR dm.orbital_reference_model_id = $2
		       OR dm.orbital_reference_model_id = $3
		       OR dm.orbital_reference_model_id = $4)
		ORDER BY dm.id, dm.column_name
	`, sourceCfg.ID, sourceCfg.Name, sourceCfg.Table, sourceCfg.RefName)
	if err != nil {
		return nil, fmt.Errorf("failed loading reverse orbital references for model '%s': %w", sourceCfg.ID, err)
	}
	defer reverseRows.Close()

	for reverseRows.Next() {
		var sourceColumn, jsonField, targetFieldID sql.NullString
		childCfg := &modelConfigRow{}
		if err := reverseRows.Scan(&sourceColumn, &jsonField, &targetFieldID,
			&childCfg.ID, &childCfg.Schema, &childCfg.Name, &childCfg.Table, &childCfg.RefName); err != nil {
			return nil, err
		}

		baseName := pluralRelationName(relationNameFromConfig(childCfg))
		singularName := relationNameFromConfig(childCfg)
		requestedName, spec, wanted := findRequestedRelation(requested, baseName, singularName)
		var nestedKeys []string
		for key := range requested {
			if strings.HasPrefix(key, strings.ToLower(baseName)+".") || strings.HasPrefix(key, strings.ToLower(singularName)+".") {
				nestedKeys = append(nestedKeys, key)
				if requestedName == "" {
					requestedName = strings.Split(requested[key].Name, ".")[0]
				}
			}
		}
		sort.Strings(nestedKeys)
		if !wanted && len(nestedKeys) == 0 {
			continue
		}

		childColumn := strings.TrimSpace(sourceColumn.String)
		if childColumn == "" {
			childColumn = strings.TrimSpace(jsonField.String)
		}
		if childColumn == "" {
			continue
		}
		parentColumn, err := a.resolveMetadataColumn(ctx, db, sourceCfg.ID, targetFieldID.String)
		if err != nil {
			return nil, err
		}

		alias := uniqueSQLAlias(requestedName, aliasCounts)
		manyJoin := relationJoin{
			Name:           requestedName,
			SourceTable:    tableName,
			SourceColumn:   childColumn,
			TargetTable:    storageNameFromConfig(childCfg),
			TargetColumn:   parentColumn,
			Alias:          alias,
			Conditions:     append([]string{}, spec.Conditions...),
			SelectedFields: spec.Fields,
			AdditionalOn:   spec.On,
			OrderBy:        spec.Order,
			Many:           true,
		}
		for _, sub := range spec.SubRelations {
			childJoin, err := a.resolveChildRelation(ctx, db, childCfg, alias, sub.Name, sub)
			if err != nil {
				return nil, err
			}
			if childJoin != nil {
				manyJoin.NestedJoins = append(manyJoin.NestedJoins, childJoin)
			}
		}
		for _, nestedKey := range nestedKeys {
			parts := strings.Split(requested[nestedKey].Name, ".")
			if len(parts) < 2 {
				continue
			}
			childSpec := relationSpecForPath(parts[1:], requested[nestedKey])
			childJoin, err := a.resolveChildRelation(ctx, db, childCfg, alias, parts[1], childSpec)
			if err != nil {
				return nil, err
			}
			if childJoin != nil {
				manyJoin.NestedJoins = append(manyJoin.NestedJoins, childJoin)
				resolved[nestedKey] = true
			}
		}
		joins = append(joins, manyJoin)
		if wanted {
			resolved[strings.ToLower(spec.Name)] = true
		}
	}
	if err := reverseRows.Err(); err != nil {
		return nil, err
	}

	for name := range requested {
		if !resolved[name] {
			return nil, fmt.Errorf("relation '%s' is not available on model '%s': no matching orbital reference found", requested[name].Name, sourceCfg.ID)
		}
	}

	return joins, nil
}

var safeRelationPredicate = regexp.MustCompile(`(?i)^\s*(?:"?[a-z_][a-z0-9_]*"?\.)?"?[a-z_][a-z0-9_]*"?\s*(?:=|<>|!=|<=|>=|<|>|LIKE|ILIKE|IS\s+NULL|IS\s+NOT\s+NULL)\s*(?:TRUE|FALSE|NULL|-?[0-9]+(?:\.[0-9]+)?|'[^']*'|(?:"?[a-z_][a-z0-9_]*"?\.)?"?[a-z_][a-z0-9_]*"?)?\s*$`)

func validateRelationSpecPredicates(spec query.RelationSpec) error {
	for _, predicate := range append(append([]string{}, spec.Conditions...), spec.On...) {
		if !safeRelationPredicate.MatchString(predicate) {
			return fmt.Errorf("unsafe relation predicate %q; use one simple identifier/operator/literal predicate per entry", predicate)
		}
	}
	for _, child := range spec.SubRelations {
		if err := validateRelationSpecPredicates(child); err != nil {
			return err
		}
	}
	return nil
}

func (a *PostgresAdapter) resolveMetadataColumn(ctx context.Context, db *sql.DB, modelID, fieldID string) (string, error) {
	fieldID = strings.TrimSpace(fieldID)
	if fieldID != "" {
		var column sql.NullString
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(NULLIF(column_name, ''), json_field)
			FROM metadata_catalog.data_models
			WHERE model_id = $1
			  AND (id = $2 OR column_name = $2 OR json_field = $2 OR ref_name = $2)
			ORDER BY CASE WHEN id = $2 THEN 0 ELSE 1 END
			LIMIT 1
		`, modelID, fieldID).Scan(&column)
		if err == nil && column.Valid && strings.TrimSpace(column.String) != "" {
			return strings.TrimSpace(column.String), nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		// Backward compatibility: catalogs commonly store the physical column
		// directly even if there is no separate DataModel row for it.
		return fieldID, nil
	}

	var primaryKey sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(column_name, ''), json_field)
		FROM metadata_catalog.data_models
		WHERE model_id = $1 AND is_primary_key = TRUE
		ORDER BY id
		LIMIT 1
	`, modelID).Scan(&primaryKey)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if primaryKey.Valid && strings.TrimSpace(primaryKey.String) != "" {
		return strings.TrimSpace(primaryKey.String), nil
	}
	return "id", nil
}

func findRequestedRelation(requested map[string]query.RelationSpec, names ...string) (string, query.RelationSpec, bool) {
	for _, name := range names {
		if spec, ok := requested[strings.ToLower(name)]; ok {
			return spec.Name, spec, true
		}
	}
	return "", query.RelationSpec{}, false
}

func pluralRelationName(name string) string {
	if name == "" {
		return "Relations"
	}
	lower := strings.ToLower(name)
	if strings.HasSuffix(lower, "y") && len(name) > 1 {
		prev := lower[len(lower)-2]
		if !strings.ContainsRune("aeiou", rune(prev)) {
			return name[:len(name)-1] + "ies"
		}
	}
	if strings.HasSuffix(lower, "s") || strings.HasSuffix(lower, "x") || strings.HasSuffix(lower, "ch") || strings.HasSuffix(lower, "sh") {
		return name + "es"
	}
	return name + "s"
}

func (a *PostgresAdapter) resolveChildRelation(ctx context.Context, db *sql.DB, parentCfg *modelConfigRow, parentAlias, childName string, spec query.RelationSpec) (*relationJoin, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, json_field, orbital_reference_model_id, orbital_reference_field_id, orbital_reference_validation, reference, ref_name
		FROM metadata_catalog.data_models
		WHERE model_id = $1
		  AND is_orbital_reference = TRUE
		  AND COALESCE(status, 'active') = 'active'
		ORDER BY id, column_name
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
		targetColumn, err := a.resolveMetadataColumn(ctx, db, targetCfg.ID, targetFieldID.String)
		if err != nil {
			return nil, err
		}

		alias := parentAlias + "__" + strings.ToLower(baseRelName)

		conditions := append([]string{}, spec.Conditions...)
		if validation.Valid && validation.String == "exists_active" {
			conditions = append(conditions, fmt.Sprintf("COALESCE(%s.is_active, TRUE) = TRUE", quoteIdent(alias)))
		}

		join := &relationJoin{
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
		}
		for _, sub := range spec.SubRelations {
			childJoin, err := a.resolveChildRelation(ctx, db, targetCfg, alias, sub.Name, sub)
			if err != nil {
				return nil, err
			}
			if childJoin != nil {
				join.NestedJoins = append(join.NestedJoins, childJoin)
			}
		}
		return join, nil
	}

	return nil, fmt.Errorf("child relation '%s' is not available on model '%s': no matching orbital reference found", childName, parentCfg.ID)
}

func relationSpecForPath(parts []string, leaf query.RelationSpec) query.RelationSpec {
	if len(parts) == 0 {
		return leaf
	}
	root := query.RelationSpec{Name: parts[0], LoadWithChildren: true}
	if len(parts) == 1 {
		leaf.Name = parts[0]
		return leaf
	}
	root.SubRelations = []query.RelationSpec{relationSpecForPath(parts[1:], leaf)}
	return root
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
	rows, err := db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed executing insert into PostgreSQL table '%s': %w", tableName, err)
	}
	created, err := scanSingleMap(rows)
	if err != nil {
		return nil, fmt.Errorf("failed reading inserted PostgreSQL row from '%s': %w", tableName, err)
	}
	return created, nil
}

// Find queries PostgreSQL.
func (a *PostgresAdapter) Find(ctx context.Context, ref model.ModelRef, q query.Query) ([]map[string]any, int64, error) {
	q = q.EnsureDebugTrace()
	started := time.Now()
	tableName := a.resolveTableName(ref)
	db, _ := a.getDB(ctx)
	if db == nil {
		if q.Debug {
			log.Printf("[Query Debug][%s][PostgreSQL] phase=backend table=%s backend=offline-mock", q.DebugTraceID, tableName)
		}
		if len(q.Relations) > 0 || len(q.RelationSpecs) > 0 {
			if q.Debug {
				log.Printf("[Query Debug][%s][PostgreSQL] phase=error duration=%s error=%q", q.DebugTraceID, time.Since(started), "relations require live PostgreSQL metadata")
			}
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
		elapsed := time.Since(started)
		if q.Debug {
			log.Printf("[Query Debug][%s][PostgreSQL] phase=complete backend=offline-mock duration=%s rows=%d", q.DebugTraceID, elapsed, len(results))
		}
		if q.IsSlow(elapsed) {
			traceID := q.DebugTraceID
			if traceID == "" {
				traceID = "slow"
			}
			log.Printf("%s[Query Debug][%s][PostgreSQL] phase=slow-query duration=%s threshold_ms=%d%s", ansiColorYellowBold, traceID, elapsed, q.SlowQueryThresholdMS, ansiColorReset)
		}
		return results, int64(len(results)), nil
	}

	relationJoins, err := a.resolveRelationJoins(ctx, db, ref, tableName, q)
	if err != nil {
		if q.Debug {
			log.Printf("[Query Debug][%s][PostgreSQL] phase=relation-resolution-error duration=%s error=%q", q.DebugTraceID, time.Since(started), err)
		}
		return nil, 0, err
	}
	if q.Debug {
		debugPostgresRelationJoins(q, "", relationJoins)
	}

	sqlStr, args := a.queryBuilder.BuildSelectWithRelations(tableName, q, relationJoins)
	if q.Debug {
		relationModes := make([]string, 0, len(relationJoins))
		for _, relation := range relationJoins {
			mode := "object"
			if relation.Many {
				mode = "array"
			}
			relationModes = append(relationModes, fmt.Sprintf("%s:%s", relation.Name, mode))
		}
		log.Printf("[Query Debug][%s][PostgreSQL] phase=compiled table=%s relations=%v sql=%s args=%v", q.DebugTraceID, tableName, relationModes, sqlStr, q.DebugArguments(args))
	}
	rows, err := db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		if q.Debug {
			log.Printf("[Query Debug][%s][PostgreSQL] phase=execution-error duration=%s error=%q", q.DebugTraceID, time.Since(started), err)
		}
		return nil, 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		if q.Debug {
			log.Printf("[Query Debug][%s][PostgreSQL] phase=column-error duration=%s error=%q", q.DebugTraceID, time.Since(started), err)
		}
		return nil, 0, err
	}
	if q.Debug {
		log.Printf("[Query Debug][%s][PostgreSQL] phase=scanning columns=%v", q.DebugTraceID, cols)
	}

	var results []map[string]any
	for rows.Next() {
		columns := make([]any, len(cols))
		columnPointers := make([]any, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			if q.Debug {
				log.Printf("[Query Debug][%s][PostgreSQL] phase=scan-error row=%d duration=%s error=%q", q.DebugTraceID, len(results)+1, time.Since(started), err)
			}
			return nil, 0, err
		}
		rowMap := make(map[string]any)
		for i, colName := range cols {
			val := columnPointers[i].(*any)
			rowMap[colName] = normalizePostgresValue(*val)
		}
		results = append(results, rowMap)
	}
	if err := rows.Err(); err != nil {
		if q.Debug {
			log.Printf("[Query Debug][%s][PostgreSQL] phase=cursor-error rows=%d duration=%s error=%q", q.DebugTraceID, len(results), time.Since(started), err)
		}
		return nil, 0, err
	}
	total := int64(len(results))
	if q.CountTotal {
		countSQL, countArgs := a.queryBuilder.BuildCountWithRelations(tableName, q, relationJoins)
		if q.Debug {
			log.Printf("[Query Debug][%s][PostgreSQL] phase=count-compiled sql=%s args=%v", q.DebugTraceID, countSQL, q.DebugArguments(countArgs))
		}
		if err := db.QueryRowContext(ctx, countSQL, countArgs...).Scan(&total); err != nil {
			return nil, 0, fmt.Errorf("postgres count failed: %w", err)
		}
	}
	elapsed := time.Since(started)
	if q.Debug {
		log.Printf("[Query Debug][%s][PostgreSQL] phase=complete duration=%s rows=%d total=%d", q.DebugTraceID, elapsed, len(results), total)
	}
	if q.IsSlow(elapsed) {
		traceID := q.DebugTraceID
		if traceID == "" {
			traceID = "slow"
		}
		log.Printf("%s[Query Debug][%s][PostgreSQL] phase=slow-query duration=%s threshold_ms=%d%s", ansiColorYellowBold, traceID, elapsed, q.SlowQueryThresholdMS, ansiColorReset)
	}

	return results, total, nil
}

func debugPostgresRelationJoins(q query.Query, parentPath string, joins []relationJoin) {
	for _, relation := range joins {
		path := relation.Name
		if parentPath != "" {
			path = parentPath + "." + relation.Name
		}
		cardinality := "object"
		if relation.Many {
			cardinality = "array"
		}
		log.Printf("[Query Debug][%s][PostgreSQL] phase=relation-resolved path=%s cardinality=%s source=%s.%s target=%s.%s alias=%s selected_fields=%v conditions=%v additional_on=%v order=%v nested=%d",
			q.DebugTraceID, path, cardinality, relation.SourceTable, relation.SourceColumn, relation.TargetTable, relation.TargetColumn, relation.Alias,
			relation.SelectedFields, q.DebugValue(relation.Conditions, len(relation.Conditions)), q.DebugValue(relation.AdditionalOn, len(relation.AdditionalOn)), relation.OrderBy, len(relation.NestedJoins))
		if len(relation.NestedJoins) > 0 {
			children := make([]relationJoin, 0, len(relation.NestedJoins))
			for _, child := range relation.NestedJoins {
				if child != nil {
					children = append(children, *child)
				}
			}
			debugPostgresRelationJoins(q, path, children)
		}
	}
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

func scanSingleMap(rows *sql.Rows) (map[string]any, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	if err := rows.Scan(pointers...); err != nil {
		return nil, err
	}
	row := make(map[string]any, len(columns))
	for i, column := range columns {
		row[column] = normalizePostgresValue(values[i])
	}
	return row, rows.Err()
}

func scanMapRows(rows *sql.Rows) ([]map[string]any, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = normalizePostgresValue(values[i])
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// FindOne finds a record by ID.
func (a *PostgresAdapter) FindOne(ctx context.Context, ref model.ModelRef, id any) (map[string]any, error) {
	return a.FindOneWithQuery(ctx, ref, id, query.NewQuery())
}

// FindOneWithQuery retrieves one row while preserving relation requests. This
// is the single-record counterpart of Find and is what allows callers to load
// orbital objects and reverse-reference arrays for a primary-key lookup.
func (a *PostgresAdapter) FindOneWithQuery(ctx context.Context, ref model.ModelRef, id any, q query.Query) (map[string]any, error) {
	tableName := a.resolveTableName(ref)
	idStr := fmt.Sprintf("%v", id)
	primaryKey := ref.PrimaryKey
	if strings.TrimSpace(primaryKey) == "" {
		primaryKey = "id"
	}

	db, _ := a.getDB(ctx)
	if db == nil {
		if len(q.Relations) > 0 || len(q.RelationSpecs) > 0 {
			return nil, fmt.Errorf("relations require live PostgreSQL metadata; no database connection is available")
		}
		a.mu.RLock()
		defer a.mu.RUnlock()
		for _, r := range a.mockStore[tableName] {
			if fmt.Sprintf("%v", r[primaryKey]) == idStr {
				cp := make(map[string]any)
				for k, v := range r {
					cp[k] = v
				}
				return cp, nil
			}
		}
		return nil, fmt.Errorf("record '%v' not found", id)
	}

	q = q.Where(primaryKey, query.OpEq, id).LimitOffset(1, 0)
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
	primaryKey := ref.PrimaryKey
	if primaryKey == "" {
		primaryKey = "id"
	}
	if !hasMutableFields(data, primaryKey) {
		return a.FindOne(ctx, ref, id)
	}

	db, _ := a.getDB(ctx)
	if db == nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, r := range a.mockStore[tableName] {
			if fmt.Sprintf("%v", r[primaryKey]) == idStr {
				data[primaryKey] = r[primaryKey]
				a.mockStore[tableName][i] = data
				return data, nil
			}
		}
		return nil, fmt.Errorf("record '%v' not found", id)
	}

	sqlStr, args := a.queryBuilder.BuildUpdateByKey(tableName, primaryKey, id, data)
	rows, err := db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	return scanSingleMap(rows)
}

func hasMutableFields(data map[string]any, primaryKey string) bool {
	for key := range data {
		if !strings.EqualFold(key, primaryKey) {
			return true
		}
	}
	return false
}

// Patch updates specific fields by ID.
func (a *PostgresAdapter) Patch(ctx context.Context, ref model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	tableName := a.resolveTableName(ref)
	idStr := fmt.Sprintf("%v", id)
	primaryKey := ref.PrimaryKey
	if primaryKey == "" {
		primaryKey = "id"
	}

	db, _ := a.getDB(ctx)
	if db == nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, r := range a.mockStore[tableName] {
			if fmt.Sprintf("%v", r[primaryKey]) == idStr {
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
	primaryKey := ref.PrimaryKey
	if primaryKey == "" {
		primaryKey = "id"
	}

	db, _ := a.getDB(ctx)
	if db == nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, r := range a.mockStore[tableName] {
			if fmt.Sprintf("%v", r[primaryKey]) == idStr {
				a.mockStore[tableName] = append(a.mockStore[tableName][:i], a.mockStore[tableName][i+1:]...)
				return nil
			}
		}
		return nil
	}

	sqlStr, args := a.queryBuilder.BuildDeleteByKey(tableName, primaryKey, id)
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
		orderedArgs, err := orderedExecutionArgs(req)
		if err != nil {
			return nil, err
		}
		for _, v := range orderedArgs {
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
		orderedArgs, err := orderedExecutionArgs(req)
		if err != nil {
			return nil, err
		}
		for _, v := range orderedArgs {
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
			args, err := orderedExecutionArgs(req)
			if err != nil {
				return nil, err
			}
			rows, err := a.db.QueryContext(ctx, queryStr, args...)
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
			args, err := orderedExecutionArgs(req)
			if err != nil {
				return nil, err
			}
			res, err := a.db.ExecContext(ctx, req.Target, args...)
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

func orderedExecutionArgs(req execution.ExecutionRequest) ([]any, error) {
	if req.Options != nil {
		if raw, ok := req.Options["args"]; ok {
			switch values := raw.(type) {
			case []any:
				return values, nil
			default:
				value := reflect.ValueOf(raw)
				if value.IsValid() && (value.Kind() == reflect.Slice || value.Kind() == reflect.Array) {
					args := make([]any, value.Len())
					for i := 0; i < value.Len(); i++ {
						args[i] = value.Index(i).Interface()
					}
					return args, nil
				}
				return nil, fmt.Errorf("execution option 'args' must be an array")
			}
		}
	}
	if len(req.Arguments) == 0 {
		return nil, nil
	}
	var names []string
	if req.Options != nil {
		if rawOrder, ok := req.Options["parameter_order"]; ok {
			switch values := rawOrder.(type) {
			case []string:
				names = append(names, values...)
			case []any:
				for _, value := range values {
					names = append(names, fmt.Sprint(value))
				}
			}
		}
	}
	if len(names) == 0 {
		for name := range req.Arguments {
			names = append(names, name)
		}
		sort.Strings(names)
	}
	args := make([]any, 0, len(names))
	for _, name := range names {
		value, ok := req.Arguments[name]
		if !ok {
			return nil, fmt.Errorf("execution parameter %q is missing", name)
		}
		args = append(args, value)
	}
	return args, nil
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
	if t.tx == nil {
		return t.adapter.Create(ctx, model, data)
	}
	tableName := t.adapter.resolveTableName(model)
	sqlText, args := t.adapter.queryBuilder.BuildInsert(tableName, data)
	rows, err := t.tx.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	return scanSingleMap(rows)
}

func (t *PostgresTransaction) Find(ctx context.Context, model model.ModelRef, q query.Query) ([]map[string]any, int64, error) {
	if t.tx == nil {
		return t.adapter.Find(ctx, model, q)
	}
	if len(q.Relations) > 0 || len(q.RelationSpecs) > 0 {
		return nil, 0, fmt.Errorf("native relation metadata queries are not supported inside PostgresTransaction; use declared engine hydration")
	}
	tableName := t.adapter.resolveTableName(model)
	sqlText, args := t.adapter.queryBuilder.BuildSelect(tableName, q)
	rows, err := t.tx.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, 0, err
	}
	results, err := scanMapRows(rows)
	if err != nil {
		return nil, 0, err
	}
	total := int64(len(results))
	if q.CountTotal {
		countSQL, countArgs := t.adapter.queryBuilder.BuildCountWithRelations(tableName, q, nil)
		if err := t.tx.QueryRowContext(ctx, countSQL, countArgs...).Scan(&total); err != nil {
			return nil, 0, err
		}
	}
	return results, total, nil
}

func (t *PostgresTransaction) FindOne(ctx context.Context, model model.ModelRef, id any) (map[string]any, error) {
	return t.FindOneWithQuery(ctx, model, id, query.NewQuery())
}

func (t *PostgresTransaction) FindOneWithQuery(ctx context.Context, model model.ModelRef, id any, q query.Query) (map[string]any, error) {
	if t.tx == nil {
		return t.adapter.FindOneWithQuery(ctx, model, id, q)
	}
	primaryKey := model.PrimaryKey
	if primaryKey == "" {
		primaryKey = "id"
	}
	q = q.Where(primaryKey, query.OpEq, id).LimitOffset(1, 0)
	rows, _, err := t.Find(ctx, model, q)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("record '%v' not found", id)
	}
	return rows[0], nil
}

func (t *PostgresTransaction) Update(ctx context.Context, model model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	if t.tx == nil {
		return t.adapter.Update(ctx, model, id, data)
	}
	primaryKey := model.PrimaryKey
	if primaryKey == "" {
		primaryKey = "id"
	}
	if !hasMutableFields(data, primaryKey) {
		return t.FindOne(ctx, model, id)
	}
	sqlText, args := t.adapter.queryBuilder.BuildUpdateByKey(t.adapter.resolveTableName(model), primaryKey, id, data)
	rows, err := t.tx.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	return scanSingleMap(rows)
}

func (t *PostgresTransaction) Patch(ctx context.Context, model model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	return t.Update(ctx, model, id, data)
}

func (t *PostgresTransaction) Delete(ctx context.Context, model model.ModelRef, id any) error {
	if t.tx == nil {
		return t.adapter.Delete(ctx, model, id)
	}
	primaryKey := model.PrimaryKey
	if primaryKey == "" {
		primaryKey = "id"
	}
	sqlText, args := t.adapter.queryBuilder.BuildDeleteByKey(t.adapter.resolveTableName(model), primaryKey, id)
	_, err := t.tx.ExecContext(ctx, sqlText, args...)
	return err
}

func (t *PostgresTransaction) Execute(ctx context.Context, req execution.ExecutionRequest) (*execution.ExecutionResult, error) {
	if t.tx == nil {
		return t.adapter.Execute(ctx, req)
	}
	args, err := orderedExecutionArgs(req)
	if err != nil {
		return nil, err
	}
	switch req.Operation {
	case operation.OpQuery:
		rows, err := t.tx.QueryContext(ctx, strings.TrimSpace(req.Target), args...)
		if err != nil {
			return nil, err
		}
		data, err := scanMapRows(rows)
		if err != nil {
			return nil, err
		}
		return &execution.ExecutionResult{Data: data, Status: "SUCCESS", Metadata: map[string]any{"count": len(data)}}, nil
	case operation.OpFunction:
		placeholders := make([]string, len(args))
		for i := range args {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		statement := fmt.Sprintf("SELECT %s(%s);", req.Target, strings.Join(placeholders, ", "))
		var result any
		if err := t.tx.QueryRowContext(ctx, statement, args...).Scan(&result); err != nil {
			return nil, err
		}
		return &execution.ExecutionResult{Data: normalizePostgresValue(result), Status: "SUCCESS"}, nil
	case operation.OpProcedure:
		placeholders := make([]string, len(args))
		for i := range args {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		statement := fmt.Sprintf("CALL %s(%s);", req.Target, strings.Join(placeholders, ", "))
		if _, err := t.tx.ExecContext(ctx, statement, args...); err != nil {
			return nil, err
		}
		return &execution.ExecutionResult{Status: "SUCCESS"}, nil
	case operation.OpDDL, operation.OpCommand, operation.OpCustom:
		result, err := t.tx.ExecContext(ctx, req.Target, args...)
		if err != nil {
			return nil, err
		}
		affected, _ := result.RowsAffected()
		return &execution.ExecutionResult{RowsAffected: affected, Status: "SUCCESS"}, nil
	default:
		return nil, adapter.ErrOperationNotSupported
	}
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

func (t *PostgresTransaction) Tx() *sql.Tx {
	return t.tx
}
