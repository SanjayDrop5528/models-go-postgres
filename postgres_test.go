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

func TestBuildCountWithRelationsRemovesPagination(t *testing.T) {
	builder := &postgres.QueryBuilder{}
	q := query.New().Where("status", query.OpEq, "active").OrderBy("created_at", query.SortDesc).LimitOffset(10, 20)
	sqlText, args := builder.BuildCountWithRelations("orders", q, []postgres.RelationJoin{{
		Name: "Customer", SourceTable: "orders", SourceColumn: "customer_id", TargetTable: "customers", TargetColumn: "id", Alias: "customer",
	}})
	if strings.Contains(sqlText, " LIMIT ") || strings.Contains(sqlText, " OFFSET ") || strings.Contains(sqlText, " ORDER BY ") {
		t.Fatalf("count query must not contain pagination or sorting: %s", sqlText)
	}
	if !strings.HasPrefix(sqlText, "SELECT COUNT(*) FROM (") || len(args) != 1 || args[0] != "active" {
		t.Fatalf("unexpected count query: %s args=%v", sqlText, args)
	}
}

func TestBuildUpdateAndDeleteUseRuntimePrimaryKey(t *testing.T) {
	builder := &postgres.QueryBuilder{}
	updateSQL, updateArgs := builder.BuildUpdateByKey("shared.settings", "code", "fleet", map[string]any{
		"code": "must-not-be-updated", "value": "enabled", "description": "Fleet feature",
	})
	wantUpdate := `UPDATE "shared"."settings" SET "description" = $1, "value" = $2 WHERE "code" = $3 RETURNING *;`
	if updateSQL != wantUpdate {
		t.Fatalf("unexpected update SQL:\n%s\nwant:\n%s", updateSQL, wantUpdate)
	}
	if len(updateArgs) != 3 || updateArgs[0] != "Fleet feature" || updateArgs[1] != "enabled" || updateArgs[2] != "fleet" {
		t.Fatalf("unexpected update arguments: %#v", updateArgs)
	}

	deleteSQL, deleteArgs := builder.BuildDeleteByKey("shared.settings", "code", "fleet")
	wantDelete := `DELETE FROM "shared"."settings" WHERE "code" = $1;`
	if deleteSQL != wantDelete || len(deleteArgs) != 1 || deleteArgs[0] != "fleet" {
		t.Fatalf("unexpected delete: %s %#v", deleteSQL, deleteArgs)
	}
}

func TestBuildInsertSupportsDatabaseDefaults(t *testing.T) {
	builder := &postgres.QueryBuilder{}
	sqlText, args := builder.BuildInsert("shared.settings", nil)
	want := `INSERT INTO "shared"."settings" DEFAULT VALUES RETURNING *;`
	if sqlText != want || len(args) != 0 {
		t.Fatalf("unexpected default insert: %s %#v", sqlText, args)
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

	// Scenario 5: reverse orbital reference is an array and must not join or
	// duplicate the parent order row.
	q5 := query.New().Where("id", query.OpEq, "order-1")
	joins5 := []postgres.RelationJoin{
		{
			Name:           "OrderProducts",
			SourceTable:    "orders",
			SourceColumn:   "order_id",
			TargetTable:    "order_products",
			TargetColumn:   "id",
			Alias:          "order_products",
			SelectedFields: []string{"id", "product_id", "quantity"},
			OrderBy:        []query.Sort{{Field: "id", Order: query.SortAsc}},
			Many:           true,
		},
	}

	sql5, _ := b.BuildSelectWithRelations("orders", q5, joins5)
	if !strings.Contains(sql5, `COALESCE((SELECT jsonb_agg(_relation_item)`) ||
		!strings.Contains(sql5, `FROM "order_products" AS "order_products" WHERE "order_products"."order_id" = "orders"."id" ORDER BY "order_products"."id" ASC`) ||
		!strings.Contains(sql5, `'[]'::jsonb) AS "OrderProducts"`) {
		t.Fatalf("expected reverse relation JSON array projection, got: %s", sql5)
	}
	if strings.Contains(sql5, `LEFT JOIN "order_products"`) {
		t.Fatalf("reverse relation must not multiply parent rows with a LEFT JOIN: %s", sql5)
	}

	// Scenario 6: two object references to the same table retain independent
	// aliases and values (for example billing and shipping addresses).
	q6 := query.New()
	joins6 := []postgres.RelationJoin{
		{Name: "BillingAddress", SourceTable: "orders", SourceColumn: "billing_address_id", TargetTable: "addresses", TargetColumn: "id", Alias: "billing_address"},
		{Name: "ShippingAddress", SourceTable: "orders", SourceColumn: "shipping_address_id", TargetTable: "addresses", TargetColumn: "id", Alias: "shipping_address"},
	}
	sql6, _ := b.BuildSelectWithRelations("orders", q6, joins6)
	for _, expected := range []string{
		`LEFT JOIN "addresses" AS "billing_address" ON "orders"."billing_address_id" = "billing_address"."id"`,
		`LEFT JOIN "addresses" AS "shipping_address" ON "orders"."shipping_address_id" = "shipping_address"."id"`,
		`AS "BillingAddress"`,
		`AS "ShippingAddress"`,
	} {
		if !strings.Contains(sql6, expected) {
			t.Fatalf("expected independent same-table object references (%s), got: %s", expected, sql6)
		}
	}

	// Scenario 7: every nested level is both joined and projected.
	q7 := query.New()
	joins7 := []postgres.RelationJoin{
		{
			Name: "Department", SourceTable: "employees", SourceColumn: "department_id", TargetTable: "departments", TargetColumn: "id", Alias: "department",
			NestedJoins: []*postgres.RelationJoin{
				{
					Name: "Organisation", SourceTable: "department", ParentAlias: "department", SourceColumn: "org_id", TargetTable: "organisations", TargetColumn: "id", Alias: "department__organisation",
					NestedJoins: []*postgres.RelationJoin{
						{Name: "Country", SourceTable: "department__organisation", ParentAlias: "department__organisation", SourceColumn: "country_id", TargetTable: "countries", TargetColumn: "id", Alias: "department__organisation__country"},
					},
				},
			},
		},
	}
	sql7, _ := b.BuildSelectWithRelations("employees", q7, joins7)
	if !strings.Contains(sql7, `LEFT JOIN "countries" AS "department__organisation__country"`) ||
		!strings.Contains(sql7, `jsonb_build_object('Country', CASE WHEN "department__organisation__country"."id" IS NULL`) {
		t.Fatalf("expected all three relation levels to be loaded, got: %s", sql7)
	}

	// Scenario 8: an array relation can itself contain an orbital object.
	q8 := query.New()
	joins8 := []postgres.RelationJoin{
		{
			Name: "OrderProducts", SourceTable: "orders", SourceColumn: "order_id", TargetTable: "order_products", TargetColumn: "id", Alias: "order_products", Many: true,
			NestedJoins: []*postgres.RelationJoin{
				{Name: "Product", SourceTable: "order_products", ParentAlias: "order_products", SourceColumn: "product_id", TargetTable: "products", TargetColumn: "id", Alias: "order_products__product"},
			},
		},
	}
	sql8, _ := b.BuildSelectWithRelations("orders", q8, joins8)
	if !strings.Contains(sql8, `LEFT JOIN "products" AS "order_products__product" ON "order_products"."product_id" = "order_products__product"."id"`) ||
		!strings.Contains(sql8, `jsonb_build_object('Product', CASE WHEN "order_products__product"."id" IS NULL`) {
		t.Fatalf("expected nested Product object inside OrderProducts array, got: %s", sql8)
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
