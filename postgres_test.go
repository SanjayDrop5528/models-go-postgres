package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/dataset/planner"
	"github.com/SanjayDrop5528/models-go-engine/dataset/resolver"
	"github.com/SanjayDrop5528/models-go-engine/diff"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"github.com/SanjayDrop5528/models-go-postgres"
)

func TestPostgres_DDL_AddColumn(t *testing.T) {
	gen := postgres.NewDDLGenerator()

	op := diff.SchemaOperation{
		Type:        diff.OpAddColumn,
		TargetTable: "employees",
		ObjectName:  "salary",
		After: schema.SchemaAttribute{
			Name:      "salary",
			Type:      model.TypeDecimal,
			Precision: 10,
			Scale:     2,
			Nullable:  true,
		},
	}

	stmt, err := gen.GenerateStatement(op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `ALTER TABLE "employees" ADD COLUMN "salary" NUMERIC(10, 2);`
	if stmt != expected {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, stmt)
	}
}

func TestPostgres_DDL_RenameColumn(t *testing.T) {
	gen := postgres.NewDDLGenerator()

	op := diff.SchemaOperation{
		Type:        diff.OpRenameColumn,
		TargetTable: "employees",
		OldName:     "employee_name",
		ObjectName:  "name",
	}

	stmt, err := gen.GenerateStatement(op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `ALTER TABLE "employees" RENAME COLUMN "employee_name" TO "name";`
	if stmt != expected {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, stmt)
	}
}

func TestPostgres_DDL_RemoveColumn(t *testing.T) {
	gen := postgres.NewDDLGenerator()

	op := diff.SchemaOperation{
		Type:        diff.OpRemoveColumn,
		TargetTable: "employees",
		ObjectName:  "salary",
	}

	stmt, err := gen.GenerateStatement(op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `ALTER TABLE "employees" DROP COLUMN "salary";`
	if stmt != expected {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, stmt)
	}
}

func TestPostgres_DDL_CreateTable(t *testing.T) {
	gen := postgres.NewDDLGenerator()

	des := &schema.Schema{
		Name: "employees",
		Attributes: []schema.SchemaAttribute{
			{Name: "id", Type: model.TypeLong, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: model.TypeString, Length: 100, Nullable: false},
			{Name: "email", Type: model.TypeString, Nullable: false, Unique: true},
		},
		PrimaryKey: &schema.SchemaKey{Columns: []string{"id"}},
	}

	op := diff.SchemaOperation{
		Type:        diff.OpCreateTable,
		TargetTable: "employees",
		ObjectName:  "employees",
		After:       des,
	}

	stmt, err := gen.GenerateStatement(op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(stmt, `CREATE TABLE IF NOT EXISTS "employees"`) {
		t.Fatalf("expected CREATE TABLE statement, got: %s", stmt)
	}
	if !strings.Contains(stmt, `"id" BIGSERIAL`) {
		t.Fatalf("expected BIGSERIAL for id, got: %s", stmt)
	}
	if !strings.Contains(stmt, `PRIMARY KEY ("id")`) {
		t.Fatalf("expected PRIMARY KEY constraint, got: %s", stmt)
	}
}

func TestPostgres_WithSchemas(t *testing.T) {
	adapter := postgres.NewPostgresAdapter("postgres://postgres:postgres@localhost:5432/testdb").WithSchemas("tenant_a", "sales")
	if adapter.Name() != "postgres" {
		t.Fatalf("expected adapter name postgres, got: %s", adapter.Name())
	}
}

func TestPostgres_DataSetCompiler_RelativeDateMacro(t *testing.T) {
	c := postgres.NewPostgresDataSetCompiler()
	ds := &domain.DataSet{
		BaseCollection: domain.BaseCollection{
			Collection: "attendance_logs",
			Filter: map[string]any{
				"created_on": map[string]any{
					"$gte": "C[-7d]",
				},
				"action": "check_in",
			},
		},
	}
	astPlanner := planner.NewPlanner(nil)
	ast, err := astPlanner.BuildAST(context.Background(), ds)
	if err != nil {
		t.Fatalf("unexpected plan error: %v", err)
	}
	res, err := c.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	if !strings.Contains(res.ExecutableQuery, `"attendance_logs"."created_on" >= (CURRENT_DATE - INTERVAL '7 days')`) {
		t.Fatalf("expected compiled query to contain INTERVAL '7 days', got:\n%s", res.ExecutableQuery)
	}
}

func TestPostgresDataSetCompiler_AggregatesAndJoinedGroupByUseAliases(t *testing.T) {
	c := postgres.NewPostgresDataSetCompiler()
	ds := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "employees"},
		JoinCollections: []domain.JoinCollection{
			{
				FromCollection:      "employees",
				FromCollectionField: "department_id",
				ToCollection:        "departments",
				ToCollectionField:   "id",
				NamedAs:             "d",
				JoinType:            domain.JoinLeft,
			},
		},
		GroupByFields: []domain.GroupByField{
			{TableName: "departments", FieldName: "name"},
		},
		SelectedList: []domain.SelectedField{
			{Field: "departments.name", HeaderName: "department_name"},
		},
		Filter: map[string]any{
			"departments.is_active": true,
		},
		CustomColumns: []domain.CustomColumn{
			{
				CustomColumnName:      "total_salary",
				CustomAggregateFnName: "SUM",
				Fields: []domain.DataSetCustomField{
					{TableName: "employees", FieldName: "salary"},
				},
			},
			{
				CustomColumnName:      "employee_count",
				CustomAggregateFnName: "COUNT_ALL",
			},
		},
	}

	astPlanner := planner.NewPlanner(resolver.NewFunctionRegistry())
	ast, err := astPlanner.BuildAST(context.Background(), ds)
	if err != nil {
		t.Fatalf("unexpected plan error: %v", err)
	}
	res, err := c.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	for _, want := range []string{
		`"d"."name" AS "department_name"`,
		`SUM("employees"."salary") AS "total_salary"`,
		`COUNT(*) AS "employee_count"`,
		`"d"."is_active" = 'true'`,
		`GROUP BY "d"."name"`,
	} {
		if !strings.Contains(res.ExecutableQuery, want) {
			t.Fatalf("expected query to contain %s, got:\n%s", want, res.ExecutableQuery)
		}
	}
	if strings.Contains(res.ExecutableQuery, `"departments"."name"`) {
		t.Fatalf("expected joined group by field to use alias d, got:\n%s", res.ExecutableQuery)
	}
}

func TestPostgresQueryBuilder_BuildSelectWithRelations(t *testing.T) {
	b := &postgres.QueryBuilder{}

	// Scenario 1: Single relation query with default projection
	q1 := query.New().
		Column("id", "first_name", "department_id").
		WhereFilter("is_active", query.OpEq, true)

	joins1 := []postgres.RelationJoin{
		{
			Name:         "Department",
			SourceTable:  "employees",
			SourceColumn: "department_id",
			TargetTable:  "departments",
			TargetColumn: "id",
			Alias:        "dept",
			Conditions:   []string{`"dept"."is_active" = TRUE`},
		},
	}

	sql1, args1 := b.BuildSelectWithRelations("employees", q1, joins1)
	if !strings.Contains(sql1, `CASE WHEN "dept"."id" IS NULL THEN NULL ELSE to_jsonb("dept".*) END AS "Department"`) {
		t.Fatalf("unexpected select clause: %s", sql1)
	}
	if !strings.Contains(sql1, `LEFT JOIN "departments" AS "dept" ON "employees"."department_id" = "dept"."id" AND "dept"."is_active" = TRUE`) {
		t.Fatalf("unexpected join clause: %s", sql1)
	}
	if !strings.Contains(sql1, `WHERE "employees"."is_active" = $1`) || len(args1) != 1 {
		t.Fatalf("unexpected where clause: %s args=%v", sql1, args1)
	}

	// Scenario 2: Relation with selected columns (jsonb_build_object), extra ON conditions, and relation-specific ordering
	q2 := query.New().Column("id", "first_name")
	joins2 := []postgres.RelationJoin{
		{
			Name:           "Department",
			SourceTable:    "employees",
			SourceColumn:   "department_id",
			TargetTable:    "departments",
			TargetColumn:   "id",
			Alias:          "dept",
			SelectedFields: []string{"id", "name"},
			AdditionalOn:   []string{`"dept"."is_active" = TRUE`},
			OrderBy:        []query.Sort{{Field: "name", Order: query.SortAsc}},
		},
	}

	sql2, _ := b.BuildSelectWithRelations("employees", q2, joins2)
	if !strings.Contains(sql2, `CASE WHEN "dept"."id" IS NULL THEN NULL ELSE jsonb_build_object('id', "dept"."id", 'name', "dept"."name") END AS "Department"`) {
		t.Fatalf("expected jsonb_build_object for selected fields, got: %s", sql2)
	}
	if !strings.Contains(sql2, `LEFT JOIN "departments" AS "dept" ON "employees"."department_id" = "dept"."id" AND "dept"."is_active" = TRUE`) {
		t.Fatalf("expected additional ON condition, got: %s", sql2)
	}
	if !strings.Contains(sql2, `ORDER BY "dept"."name" ASC`) {
		t.Fatalf("expected relation order by, got: %s", sql2)
	}

	// Scenario 3: Nested relations (Employee -> Department -> Organization)
	q3 := query.New()
	joins3 := []postgres.RelationJoin{
		{
			Name:         "Department",
			SourceTable:  "employees",
			SourceColumn: "department_id",
			TargetTable:  "departments",
			TargetColumn: "id",
			Alias:        "dept",
			NestedJoins: []*postgres.RelationJoin{
				{
					Name:         "Organization",
					SourceTable:  "dept",
					ParentAlias:  "dept",
					SourceColumn: "org_id",
					TargetTable:  "organizations",
					TargetColumn: "id",
					Alias:        "dept__org",
				},
			},
		},
	}

	sql3, _ := b.BuildSelectWithRelations("employees", q3, joins3)
	if !strings.Contains(sql3, `(to_jsonb("dept".*) || jsonb_build_object('Organization', CASE WHEN "dept__org"."id" IS NULL THEN NULL ELSE to_jsonb("dept__org".*) END))`) {
		t.Fatalf("expected nested jsonb build object, got: %s", sql3)
	}
	if !strings.Contains(sql3, `LEFT JOIN "departments" AS "dept" ON "employees"."department_id" = "dept"."id"`) {
		t.Fatalf("expected parent join, got: %s", sql3)
	}
	if !strings.Contains(sql3, `LEFT JOIN "organizations" AS "dept__org" ON "dept"."org_id" = "dept__org"."id"`) {
		t.Fatalf("expected nested child join, got: %s", sql3)
	}

	// Scenario 4: Multiple relations in one query
	q4 := query.New()
	joins4 := []postgres.RelationJoin{
		{
			Name:         "Employee",
			SourceTable:  "project_assignments",
			SourceColumn: "employee_id",
			TargetTable:  "employees",
			TargetColumn: "id",
			Alias:        "emp",
		},
		{
			Name:         "Department",
			SourceTable:  "project_assignments",
			SourceColumn: "department_id",
			TargetTable:  "departments",
			TargetColumn: "id",
			Alias:        "dept",
		},
	}

	sql4, _ := b.BuildSelectWithRelations("project_assignments", q4, joins4)
	if !strings.Contains(sql4, `CASE WHEN "emp"."id" IS NULL THEN NULL ELSE to_jsonb("emp".*) END AS "Employee"`) || !strings.Contains(sql4, `CASE WHEN "dept"."id" IS NULL THEN NULL ELSE to_jsonb("dept".*) END AS "Department"`) {
		t.Fatalf("expected both relations projected, got: %s", sql4)
	}
	if !strings.Contains(sql4, `LEFT JOIN "employees" AS "emp"`) || !strings.Contains(sql4, `LEFT JOIN "departments" AS "dept"`) {
		t.Fatalf("expected both joins emitted, got: %s", sql4)
	}
}

func TestPostgresDataSetCompiler_AllCustomAndAggregateFunctions(t *testing.T) {
	c := postgres.NewPostgresDataSetCompiler()

	// 1. Test Math, String, Date, and Conditional Row Calculations
	dsCalc := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "orders"},
		CustomColumns: []domain.CustomColumn{
			{CustomColumnName: "col_add", CustomAggregateFnName: "ADD", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "subtotal"}, {TableName: "_LITERAL_", FieldName: "10", IsLiteral: true}}},
			{CustomColumnName: "col_sub", CustomAggregateFnName: "SUBTRACT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "total"}, {TableName: "orders", FieldName: "tax"}}},
			{CustomColumnName: "col_mul", CustomAggregateFnName: "MULTIPLY", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "price"}, {TableName: "orders", FieldName: "qty"}}},
			{CustomColumnName: "col_div", CustomAggregateFnName: "DIVIDE", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "total"}, {TableName: "orders", FieldName: "items_count"}}},
			{CustomColumnName: "col_mod", CustomAggregateFnName: "MODULO", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "id"}, {TableName: "_LITERAL_", FieldName: "10", IsLiteral: true}}},
			{CustomColumnName: "col_pow", CustomAggregateFnName: "POWER", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "rating"}, {TableName: "_LITERAL_", FieldName: "2", IsLiteral: true}}},
			{CustomColumnName: "col_round", CustomAggregateFnName: "ROUND", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}, {TableName: "_LITERAL_", FieldName: "2", IsLiteral: true}}},
			{CustomColumnName: "col_ceil", CustomAggregateFnName: "CEIL", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "shipping_fee"}}},
			{CustomColumnName: "col_floor", CustomAggregateFnName: "FLOOR", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "shipping_fee"}}},
			{CustomColumnName: "col_abs", CustomAggregateFnName: "ABS", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "variance"}}},
			{CustomColumnName: "col_sqrt", CustomAggregateFnName: "SQRT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "area"}}},

			{CustomColumnName: "col_concat", CustomAggregateFnName: "CONCAT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "prefix"}, {TableName: "_LITERAL_", FieldName: "-", IsLiteral: true}, {TableName: "orders", FieldName: "order_num"}}},
			{CustomColumnName: "col_concat_ws", CustomAggregateFnName: "CONCAT_WS", Fields: []domain.DataSetCustomField{{TableName: "_LITERAL_", FieldName: ",", IsLiteral: true}, {TableName: "orders", FieldName: "city"}, {TableName: "orders", FieldName: "country"}}},
			{CustomColumnName: "col_upper", CustomAggregateFnName: "UPPER", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "code"}}},
			{CustomColumnName: "col_lower", CustomAggregateFnName: "LOWER", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "email"}}},
			{CustomColumnName: "col_trim", CustomAggregateFnName: "TRIM", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "notes"}}},
			{CustomColumnName: "col_len", CustomAggregateFnName: "LENGTH", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "code"}}},
			{CustomColumnName: "col_substr", CustomAggregateFnName: "SUBSTRING", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "code"}, {TableName: "_LITERAL_", FieldName: "1", IsLiteral: true}, {TableName: "_LITERAL_", FieldName: "4", IsLiteral: true}}},
			{CustomColumnName: "col_replace", CustomAggregateFnName: "REPLACE", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "title"}, {TableName: "_LITERAL_", FieldName: "old", IsLiteral: true}, {TableName: "_LITERAL_", FieldName: "new", IsLiteral: true}}},

			{CustomColumnName: "col_year", CustomAggregateFnName: "YEAR", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "order_date"}}},
			{CustomColumnName: "col_month", CustomAggregateFnName: "MONTH", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "order_date"}}},
			{CustomColumnName: "col_day", CustomAggregateFnName: "DAY", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "order_date"}}},
			{CustomColumnName: "col_now", CustomAggregateFnName: "NOW"},
			{CustomColumnName: "col_today", CustomAggregateFnName: "CURRENT_DATE"},
			{CustomColumnName: "col_date_diff", CustomAggregateFnName: "DATE_DIFF", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "delivered_at"}, {TableName: "orders", FieldName: "shipped_at"}}},

			{CustomColumnName: "col_pct", CustomAggregateFnName: "PERCENTAGE", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "margin"}, {TableName: "orders", FieldName: "revenue"}}},
			{CustomColumnName: "col_disc", CustomAggregateFnName: "DISCOUNT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "price"}, {TableName: "_LITERAL_", FieldName: "15", IsLiteral: true}}},
			{CustomColumnName: "col_coalesce", CustomAggregateFnName: "COALESCE", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "discount"}, {TableName: "_LITERAL_", FieldName: "0", IsLiteral: true}}},
			{CustomColumnName: "col_if", CustomAggregateFnName: "IF", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "is_gift"}, {TableName: "_LITERAL_", FieldName: "5", IsLiteral: true}, {TableName: "_LITERAL_", FieldName: "0", IsLiteral: true}}},
			{CustomColumnName: "col_to_str", CustomAggregateFnName: "TO_STRING", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "id"}}},
			{CustomColumnName: "col_to_int", CustomAggregateFnName: "TO_INTEGER", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "col_to_dec", CustomAggregateFnName: "TO_DECIMAL", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "rate"}}},
		},
	}

	astPlanner := planner.NewPlanner(resolver.NewFunctionRegistry())
	astCalc, err := astPlanner.BuildAST(context.Background(), dsCalc)
	if err != nil {
		t.Fatalf("failed building calculation AST: %v", err)
	}

	resCalc, err := c.Compile(context.Background(), astCalc, dsCalc)
	if err != nil {
		t.Fatalf("failed compiling calculation query: %v", err)
	}

	expectedSnippets := []string{
		`("orders"."subtotal" + 10) AS "col_add"`,
		`("orders"."total" - "orders"."tax") AS "col_sub"`,
		`("orders"."price" * "orders"."qty") AS "col_mul"`,
		`("orders"."total" / NULLIF("orders"."items_count", 0)) AS "col_div"`,
		`MOD("orders"."id", 10) AS "col_mod"`,
		`POWER("orders"."rating", 2) AS "col_pow"`,
		`ROUND("orders"."amount", 2) AS "col_round"`,
		`CEIL("orders"."shipping_fee") AS "col_ceil"`,
		`FLOOR("orders"."shipping_fee") AS "col_floor"`,
		`ABS("orders"."variance") AS "col_abs"`,
		`SQRT("orders"."area") AS "col_sqrt"`,
		`CONCAT("orders"."prefix", '-', "orders"."order_num") AS "col_concat"`,
		`CONCAT_WS(',', "orders"."city", "orders"."country") AS "col_concat_ws"`,
		`UPPER("orders"."code") AS "col_upper"`,
		`LOWER("orders"."email") AS "col_lower"`,
		`TRIM("orders"."notes") AS "col_trim"`,
		`LENGTH("orders"."code") AS "col_len"`,
		`SUBSTRING("orders"."code", 1, 4) AS "col_substr"`,
		`REPLACE("orders"."title", 'old', 'new') AS "col_replace"`,
		`EXTRACT(YEAR FROM "orders"."order_date") AS "col_year"`,
		`EXTRACT(MONTH FROM "orders"."order_date") AS "col_month"`,
		`EXTRACT(DAY FROM "orders"."order_date") AS "col_day"`,
		`NOW() AS "col_now"`,
		`CURRENT_DATE AS "col_today"`,
		`("orders"."delivered_at"::date - "orders"."shipped_at"::date) AS "col_date_diff"`,
		`(("orders"."margin" / NULLIF("orders"."revenue", 0)) * 100.0) AS "col_pct"`,
		`("orders"."price" - ("orders"."price" * (15 / 100.0))) AS "col_disc"`,
		`COALESCE("orders"."discount", 0) AS "col_coalesce"`,
		`CASE WHEN "orders"."is_gift" THEN 5 ELSE 0 END AS "col_if"`,
		`CAST("orders"."id" AS TEXT) AS "col_to_str"`,
		`CAST("orders"."amount" AS INTEGER) AS "col_to_int"`,
		`CAST("orders"."rate" AS NUMERIC) AS "col_to_dec"`,
	}

	for _, snippet := range expectedSnippets {
		if !strings.Contains(resCalc.ExecutableQuery, snippet) {
			t.Errorf("missing expected snippet:\n  %s\nin compiled query:\n  %s", snippet, resCalc.ExecutableQuery)
		}
	}

	// 2. Test Aggregates (SUM, AVG, MIN, MAX, COUNT, COUNT_ALL, COUNT_DISTINCT, COUNT_IF, SUM_IF)
	dsAgg := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "orders"},
		GroupByFields: []domain.GroupByField{
			{TableName: "orders", FieldName: "status"},
		},
		CustomColumns: []domain.CustomColumn{
			{CustomColumnName: "total_amount", CustomAggregateFnName: "SUM", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "avg_amount", CustomAggregateFnName: "AVG", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "min_amount", CustomAggregateFnName: "MIN", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "max_amount", CustomAggregateFnName: "MAX", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "order_count", CustomAggregateFnName: "COUNT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "id"}}},
			{CustomColumnName: "row_count", CustomAggregateFnName: "COUNT_ALL"},
			{CustomColumnName: "distinct_customers", CustomAggregateFnName: "COUNT_DISTINCT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "customer_id"}}},
			{CustomColumnName: "delivered_count", CustomAggregateFnName: "COUNT_IF", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "is_delivered"}}},
			{CustomColumnName: "active_total", CustomAggregateFnName: "SUM_IF", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "is_active"}, {TableName: "orders", FieldName: "amount"}}},
		},
	}

	astAgg, err := astPlanner.BuildAST(context.Background(), dsAgg)
	if err != nil {
		t.Fatalf("failed building aggregate AST: %v", err)
	}

	resAgg, err := c.Compile(context.Background(), astAgg, dsAgg)
	if err != nil {
		t.Fatalf("failed compiling aggregate query: %v", err)
	}

	expectedAggSnippets := []string{
		`SUM("orders"."amount") AS "total_amount"`,
		`AVG("orders"."amount") AS "avg_amount"`,
		`MIN("orders"."amount") AS "min_amount"`,
		`MAX("orders"."amount") AS "max_amount"`,
		`COUNT("orders"."id") AS "order_count"`,
		`COUNT(*) AS "row_count"`,
		`COUNT(DISTINCT "orders"."customer_id") AS "distinct_customers"`,
		`COUNT(CASE WHEN "orders"."is_delivered" THEN 1 END) AS "delivered_count"`,
		`SUM(CASE WHEN "orders"."is_active" THEN "orders"."amount" ELSE 0 END) AS "active_total"`,
		`GROUP BY "orders"."status"`,
	}

	for _, snippet := range expectedAggSnippets {
		if !strings.Contains(resAgg.ExecutableQuery, snippet) {
			t.Errorf("missing expected aggregate snippet:\n  %s\nin compiled query:\n  %s", snippet, resAgg.ExecutableQuery)
		}
	}
}


