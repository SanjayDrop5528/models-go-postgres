# models-go-postgres

> **PostgreSQL Database Adapter, DDL Migrator & DataSet SQL Compiler**

`models-go-postgres` provides the PostgreSQL database adapter for `models-go-engine`. It supports schema introspection, migration DDL generation (`ALTER TABLE`, `ADD FOREIGN KEY`, `ALTER COLUMN TYPE`), parameterized SQL execution, and Stored Procedure / Stored Function DDL compilation (`sp_...`, `fn_...`).

---

## 🛠️ Key Exported Functions & Methods Reference

### 1. `PostgresAdapter` ([`adapter.go`](./adapter.go))

Implements the `adapter.Adapter` interface for PostgreSQL.

| Function / Method | Signature | Description |
| :--- | :--- | :--- |
| `NewPostgresAdapter` | `(dsn string) *PostgresAdapter` | Creates a new PostgreSQL adapter instance using pgx/stdlib driver. |
| `WithSchemas` | `(schemas ...string) *PostgresAdapter` | Restricts schema operations and introspection to specific schemas (e.g. `public`, `spares`). |
| `Connect` | `(ctx context.Context) error` | Establishes database connection pool and pings database. |
| `ApplySchemaChange` | `(ctx context.Context, p *plan.SchemaPlan) error` | Executes schema migration DDL statements inside a transaction. |
| `Execute` | `(ctx context.Context, req execution.ExecutionRequest) (*execution.ExecutionResult, error)` | Executes queries, DDL, or stored procedures against PostgreSQL. |
| `CompileDataSet` | `(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error)` | Compiles a `QueryAST` into PostgreSQL SQL and Procedure/Function DDL. |

---

### 2. `PostgresDataSetCompiler` ([`dataset_compiler.go`](./dataset_compiler.go))

Compiles Query AST into dialect-specific PostgreSQL queries and Stored Procedures / Functions.

| Function / Method | Signature | Description |
| :--- | :--- | :--- |
| `NewPostgresDataSetCompiler` | `() *PostgresDataSetCompiler` | Instantiates a new PostgreSQL dataset compiler. |
| `Compile` | `(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error)` | Compiles `ast` into `ExecutableQuery`, `ReferencePipeline`, and DDL statements (`sp_` or `fn_`). |

---

### 3. `DDLGenerator` ([`ddl.go`](./ddl.go))

Compiles schema operations into PostgreSQL DDL statements.

| Function / Method | Signature | Description |
| :--- | :--- | :--- |
| `NewDDLGenerator` | `() *DDLGenerator` | Instantiates PostgreSQL DDL statement generator. |
| `GenerateStatements` | `(ops []diff.SchemaOperation) ([]string, error)` | Converts schema operations to PostgreSQL SQL queries (`CREATE TABLE`, `ALTER TABLE ... ALTER COLUMN TYPE`, etc.). |

---

## 🚀 Usage Example

### PostgreSQL Stored Procedure Compilation & Execution

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-postgres"
)

func main() {
	ctx := context.Background()

	adapter := postgres.NewPostgresAdapter("postgres://postgres:postgres@localhost:5432/spares_db?sslmode=disable")
	if err := adapter.Connect(ctx); err != nil {
		log.Fatalf("Connect error: %v", err)
	}

	ds := &domain.DataSet{
		Name:          "Spares & Categories",
		ReferenceName: "spares_categories",
		SaveMode:      domain.SaveModeProcedure,
		BaseCollection: domain.BaseCollection{
			Schema:     "spares",
			Collection: "spares",
		},
		JoinCollections: []domain.JoinCollection{
			{
				FromCollection:      "spares",
				FromCollectionField: "category_id",
				ToCollection:        "spare_categories",
				ToCollectionField:   "id",
				NamedAs:             "spare_categories_alias",
				JoinType:            domain.JoinInner,
				ConvertToString:     true,
			},
		},
		SelectedList: []domain.SelectedField{
			{Field: "spares.name", HeaderName: "name", DataType: "string"},
			{Field: "spare_categories_alias.description", HeaderName: "spare_categories_alias_description", DataType: "string"},
		},
	}

	compiler := postgres.NewPostgresDataSetCompiler()
	p, pErr := compiler.Compile(ctx, nil, ds)
	if pErr != nil {
		log.Fatalf("Compilation error: %v", pErr)
	}

	fmt.Println("Generated Executable Pipeline:")
	fmt.Println(p.ExecutableQuery)
	fmt.Println("Generated DDL Statement:")
	fmt.Println(p.DDLStatement)
}
```
