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

### Orbital relation loading

Orbital values are lazy by default. Set `load_with_children: true` on the
orbital `DataModel` field (or `LoadWithChildren: true` on an explicit
`model.Relation`) to make the CRUD engine include that relation automatically
for both `Find` and `FindOne`. When it is false, a normal read returns the
stored foreign-key value only; an explicit `Relation(...)` request still loads
it on demand.

```go
// Object references declared on orders, such as customer_id or two separate
// address fields, are returned as independent JSON objects.
q := query.New().
    Relation("Customer").
    Relation("BillingAddress").
    Relation("ShippingAddress").
    Relation("OrderProducts") // reverse order_products.order_id reference => array

order, err := adapter.FindOneWithQuery(ctx, orderRef, orderID, q)
```

For the HTTP API, use either
`GET /api/data/orders/{id}?relations=Customer,OrderProducts` or include
`"relations": ["Customer", "OrderProducts"]` in the POST filter/query body.
Forward references return an object (or `null`); reverse references return an
array (or `[]`). Relation names can be made explicit and stable with
`reference.relation_name`/`reference.alias`, which is recommended when two
fields reference the same target table.

The query is assembled in this order: the CRUD engine adds model relations
whose `load_with_children` flag is true, merges explicit request relations,
removes duplicates, and sends the resulting `query.Query` to the adapter. The
PostgreSQL adapter resolves relation metadata and generates object joins or
correlated JSON-array subqueries before executing the final SQL.

Query debugging is opt-in and scoped to one request:

```http
GET /api/data/orders/123?debug=true
GET /api/data/orders/123?relations=OrderProducts.Product&debug=true
```

```json
{
  "start": 0,
  "end": 20,
  "relations": ["Customer", "OrderProducts"],
  "debug": true
}
```

With `debug: true`, every query receives a correlation ID such as
`query-000123`. Runtime logs include:

- engine dispatch and automatic/explicit/lazy relation decisions;
- relation path and cardinality (`object` or `array`);
- source and target tables, columns, SQL aliases, conditions, selected fields,
  ordering, and nested-relation count;
- generated SQL or database filter shape; argument values are redacted by
  default and appear only when `debug_include_args: true` is explicitly set;
- returned columns, row counts, total counts, and execution duration;
- relation-resolution, execution, scan, decode, and cursor errors.

When false or omitted, no `[Query Debug]` messages are emitted. Set
`slow_query_threshold_ms` to emit a correlated `slow-query` event when a query
crosses the configured duration. Because unredacted bound arguments can contain
application data, use `debug_include_args` only in an appropriate environment.

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
