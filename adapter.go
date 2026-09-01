package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"github.com/SanjayDrop5528/models-go-engine/adapter"
	"github.com/SanjayDrop5528/models-go-engine/execution"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/operation"
	"github.com/SanjayDrop5528/models-go-engine/plan"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"net/url"
	"strings"
	"sync"
	"time"

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
	createCfgTable := `
	CREATE TABLE IF NOT EXISTS model_configs (
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
	CREATE TABLE IF NOT EXISTS data_models (
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

	if _, err := db.ExecContext(ctx, createCfgTable); err != nil {
		return fmt.Errorf("failed to create 'model_configs' table: %w", err)
	}
	if _, err := db.ExecContext(ctx, createDMTable); err != nil {
		return fmt.Errorf("failed to create 'data_models' table: %w", err)
	}

	log.Printf("[PostgreSQL] ✔ System metadata tables ('model_configs' and 'data_models') verified & active.")
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

		if tableName == "model_configs" || tableName == "data_models" || tableName == "schema_migrations" || tableName == "alembic_version" || tableName == "flyway_schema_history" {
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
		if strings.Contains(tableName, "_") {
			parts := strings.Split(tableName, "_")
			for i, p := range parts {
				parts[i] = strings.Title(p)
			}
			modelName = strings.Join(parts, "")
		} else {
			modelName = strings.Title(tableName)
		}
		if schemaName != "public" {
			modelName = strings.Title(schemaName) + modelName
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

// Create inserts a row using parameterized SQL.
func (a *PostgresAdapter) Create(ctx context.Context, ref model.ModelRef, data map[string]any) (map[string]any, error) {
	tableName := ref.StorageName
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
	tableName := ref.StorageName
	db, _ := a.getDB(ctx)
	if db == nil {
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

	sqlStr, args := a.queryBuilder.BuildSelect(tableName, q)
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
			rowMap[colName] = *val
		}
		results = append(results, rowMap)
	}

	return results, int64(len(results)), nil
}

// FindOne finds a record by ID.
func (a *PostgresAdapter) FindOne(ctx context.Context, ref model.ModelRef, id any) (map[string]any, error) {
	tableName := ref.StorageName
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
	tableName := ref.StorageName
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
	tableName := ref.StorageName
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
	tableName := ref.StorageName
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

	case operation.OpCommand, operation.OpCustom:
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
