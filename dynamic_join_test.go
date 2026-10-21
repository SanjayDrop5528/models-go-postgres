package postgres

import (
	"strings"
	"testing"

	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/model"
)

func dynamicCatalog(a, b string) JoinCatalog {
	result := JoinCatalog{}
	for _, x := range []struct{ id, typ string }{{"s", a}, {"t", b}, {"j", a}} {
		result[x.id] = JoinModel{Config: &model.ModelConfig{ID: x.id, Schema: "test", Table: "table_" + x.id, Status: model.ModelConfigStatusActive}, Fields: []*model.DataModel{
			{ID: "key", ModelID: x.id, ColumnName: "unusual_key", DataType: model.TypeText, PhysicalDataType: x.typ, Status: model.DataModelStatusActive},
			{ID: "extra", ModelID: x.id, ColumnName: "extra", PhysicalDataType: "TEXT", Status: model.DataModelStatusActive},
		}, VerifiedUniqueKeys: [][]string{{"key"}}}
	}
	return result
}

func dynamicRelation(op string) domain.Relationship {
	return domain.Relationship{SourceModelID: "s", SourceFieldID: "key", TargetModelID: "t", TargetFieldID: "key", Operator: op, TargetFields: []string{"key"}}
}

func TestDynamicJoinPredicateMatrix(t *testing.T) {
	tests := []struct{ a, b, op, want string }{
		{"UUID", "UUID", "EQ", `"source"."unusual_key" = "target"."unusual_key"`},
		{"TEXT", "TEXT", "EQ", `"source"."unusual_key" = "target"."unusual_key"`},
		{"VARCHAR(50)", "TEXT", "EQ", `"source"."unusual_key" = "target"."unusual_key"`},
		{"INTEGER", "INTEGER", "EQ", `"source"."unusual_key" = "target"."unusual_key"`},
		{"INTEGER", "BIGINT", "EQ", `"source"."unusual_key" = "target"."unusual_key"`},
		{"UUID", "TEXT", "EQ_CAST", `"source"."unusual_key"::text = "target"."unusual_key"`},
		{"TEXT", "UUID", "EQ_CAST", `"source"."unusual_key" = "target"."unusual_key"::text`},
		{"INTEGER", "TEXT", "EQ_CAST", `"source"."unusual_key"::text = "target"."unusual_key"`},
		{"TEXT", "INTEGER", "EQ_CAST", `"source"."unusual_key" = "target"."unusual_key"::text`},
		{"DATE", "DATE", "EQ", `"source"."unusual_key" = "target"."unusual_key"`},
		{"TIMESTAMP", "TIMESTAMP", "EQ", `"source"."unusual_key" = "target"."unusual_key"`},
		{"BOOLEAN", "BOOLEAN", "EQ", `"source"."unusual_key" = "target"."unusual_key"`},
		{"UUID[]", "UUID", "MEMBER_OF", `"target"."unusual_key" = ANY("source"."unusual_key")`},
		{"TEXT[]", "TEXT", "MEMBER_OF", `"target"."unusual_key" = ANY("source"."unusual_key")`},
		{"INTEGER[]", "INTEGER", "MEMBER_OF", `"target"."unusual_key" = ANY("source"."unusual_key")`},
		{"UUID[]", "TEXT", "MEMBER_OF_CAST", `"target"."unusual_key" = ANY("source"."unusual_key"::text[])`},
		{"TEXT[]", "UUID", "MEMBER_OF_CAST", `"target"."unusual_key"::text = ANY("source"."unusual_key")`},
		{"INTEGER[]", "TEXT", "MEMBER_OF_CAST", `"target"."unusual_key" = ANY("source"."unusual_key"::text[])`},
		{"TEXT[]", "INTEGER", "MEMBER_OF_CAST", `"target"."unusual_key"::text = ANY("source"."unusual_key")`},
		{"UUID", "UUID[]", "MEMBER_OF", `"source"."unusual_key" = ANY("target"."unusual_key")`},
		{"TEXT", "TEXT[]", "MEMBER_OF", `"source"."unusual_key" = ANY("target"."unusual_key")`},
		{"UUID[]", "UUID[]", "OVERLAP", `"source"."unusual_key" && "target"."unusual_key"`},
		{"TEXT[]", "TEXT[]", "OVERLAP", `"source"."unusual_key" && "target"."unusual_key"`},
		{"INTEGER[]", "INTEGER[]", "OVERLAP", `"source"."unusual_key" && "target"."unusual_key"`},
		{"UUID[]", "UUID[]", "CONTAINS", `"source"."unusual_key" @> "target"."unusual_key"`},
		{"UUID[]", "UUID[]", "CONTAINED_BY", `"source"."unusual_key" <@ "target"."unusual_key"`},
		{"UUID[]", "UUID[]", "EQUAL", `"source"."unusual_key" = "target"."unusual_key"`},
		{"JSONB", "UUID", "JSON_PATH_EQ", `->> E'custom_key') = "target"."unusual_key"::text`},
		{"JSONB", "TEXT", "JSON_PATH_EQ", `->> E'custom_key') = "target"."unusual_key"`},
		{"JSONB", "UUID", "JSON_MEMBER", `) ? "target"."unusual_key"::text`},
		{"JSONB", "TEXT", "JSON_MEMBER", `) ? "target"."unusual_key"`},
		{"JSONB", "UUID", "JSON_ARRAY_MATCH", `"element"."value" ->> E'custom_key') = "target"."unusual_key"::text`},
		{"JSONB", "TEXT", "JSON_ARRAY_MATCH", `"element"."value" ->> E'custom_key') = "target"."unusual_key"`},
		{"JSONB", "JSONB", "JSON_CONTAINS", `"source"."unusual_key" @> "target"."unusual_key"`},
		{"JSONB", "JSONB", "JSON_EQUAL", `"source"."unusual_key" = "target"."unusual_key"`},
		{"JSONB", "JSONB", "JSON_OVERLAP", `AS y(value) ON x.value = y.value)`},
		{"TEXT", "TEXT", "CASE_INSENSITIVE_EQ", `lower("source"."unusual_key") = lower("target"."unusual_key")`},
	}
	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.op+"_"+tt.b, func(t *testing.T) {
			r := dynamicRelation(tt.op)
			if strings.HasSuffix(tt.op, "_CAST") {
				r.ConversionPolicy = "TEXT"
			}
			if tt.op == "JSON_PATH_EQ" || tt.op == "JSON_ARRAY_MATCH" {
				r.SourceJSONPath = []string{"custom_key"}
			}
			got, err := CompileDynamicJoin(dynamicCatalog(tt.a, tt.b), r)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got.SQL, tt.want) {
				t.Fatalf("missing %s in %s", tt.want, got.SQL)
			}
			if strings.Contains(tt.op, "JSON") && strings.Contains(got.SQL, "array_elements") && !strings.Contains(got.SQL, "CASE WHEN jsonb_typeof") {
				t.Fatal("missing shape guard")
			}
		})
	}
}

func TestDynamicJoinRejectsInvalidCombinations(t *testing.T) {
	for _, tt := range []struct{ a, b, op string }{
		{"UUID", "TEXT", "AUTO"}, {"UUID", "TEXT", "EQ"}, {"UUID", "TEXT", "EQ_CAST"},
		{"UUID[]", "TEXT", "MEMBER_OF_CAST"}, {"UUID[]", "UUID[]", "AUTO"}, {"JSONB", "UUID", "AUTO"},
		{"JSONB", "UUID", "MEMBER_OF"}, {"UUID", "UUID", "MEMBER_OF"}, {"JSONB[]", "UUID", "JSON_MEMBER"},
		{"JSONB", "UUID", "JSON_PATH_EQ"}, {"JSONB", "UUID", "JSON_ARRAY_MATCH"}, {"UUID", "UUID", "BOGUS"},
		{"ARRAY", "UUID", "MEMBER_OF"}, {"", "UUID", "EQ"}, {"UUID[]", "TEXT[]", "OVERLAP"},
		{"JSON", "JSON", "EQ"}, {"JSON[]", "JSON[]", "EQUAL"}, {"JSONB", "TEXT", "JSON_EQUAL"},
	} {
		t.Run(tt.a+tt.op+tt.b, func(t *testing.T) {
			if _, err := CompileDynamicJoin(dynamicCatalog(tt.a, tt.b), dynamicRelation(tt.op)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestDynamicJoinStrategies(t *testing.T) {
	for _, tt := range []struct{ jt, card, out, want string }{
		{"INNER", "ONE", "FLAT", " INNER JOIN "}, {"LEFT", "ZERO_OR_ONE", "OBJECT", "LEFT JOIN LATERAL"},
		{"LEFT", "MANY", "FLAT", " LEFT JOIN "}, {"RIGHT", "MANY", "FLAT", " RIGHT JOIN "}, {"FULL", "MANY", "FLAT", " FULL OUTER JOIN "},
		{"LEFT", "MANY", "JSON_ARRAY", "jsonb_agg(DISTINCT jsonb_build_object("},
		{"LEFT", "MANY", "SCALAR_ARRAY", "array_agg(DISTINCT "}, {"LEFT", "MANY", "COUNT", "SELECT COUNT(*)"},
		{"LEFT", "MANY", "EXISTS", "EXISTS (SELECT 1"}, {"LATERAL", "MANY", "FIRST", `ORDER BY "target"."unusual_key" ASC LIMIT 1`},
		{"SEMI", "MANY", "FLAT", " WHERE EXISTS ("}, {"ANTI", "MANY", "FLAT", " WHERE NOT EXISTS ("}, {"CROSS", "MANY", "FLAT", " CROSS JOIN "},
		{"LEFT", "MANY_TO_MANY", "JSON_ARRAY", `AS "junction" INNER JOIN "test"."table_t"`},
	} {
		t.Run(tt.jt+tt.card+tt.out, func(t *testing.T) {
			r := dynamicRelation("EQ")
			r.JoinType, r.Cardinality, r.OutputFormat = tt.jt, tt.card, tt.out
			if tt.out == "COUNT" || tt.out == "EXISTS" || tt.jt == "SEMI" || tt.jt == "ANTI" {
				r.TargetFields = nil
			}
			if tt.jt == "CROSS" {
				r.SourceFieldID = ""
				r.TargetFieldID = ""
				r.Operator = ""
			}
			if tt.out == "FIRST" {
				r.TargetOrder = []domain.RelationshipOrder{{FieldID: "key"}}
			}
			if tt.card == "MANY_TO_MANY" {
				r.Junction = &domain.RelationshipJunction{ModelID: "j", SourceFieldID: "key", TargetFieldID: "key"}
			}
			got, err := CompileDynamicJoin(dynamicCatalog("UUID", "UUID"), r)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got.SQL, tt.want) {
				t.Fatal(got.SQL)
			}
			if tt.out == "FLAT" && tt.card == "MANY" && strings.Contains(got.SQL, "jsonb_agg") {
				t.Fatal("MANY FLAT must not aggregate")
			}
		})
	}
}

func TestDynamicJoinMetadataAndUniqueness(t *testing.T) {
	for _, mutate := range []func(JoinCatalog, *domain.Relationship){
		func(c JoinCatalog, r *domain.Relationship) { c["s"].Fields[0].ModelID = "wrong" },
		func(c JoinCatalog, r *domain.Relationship) { c["s"].Fields[0].Status = model.DataModelStatusInactive },
		func(c JoinCatalog, r *domain.Relationship) { c["s"].Config.Status = model.ModelConfigStatusInactive },
		func(c JoinCatalog, r *domain.Relationship) { r.TargetFieldID = "missing" },
		func(c JoinCatalog, r *domain.Relationship) {
			r.Cardinality = "ONE"
			m := c["t"]
			m.VerifiedUniqueKeys = nil
			c["t"] = m
		},
		func(c JoinCatalog, r *domain.Relationship) {
			r.Cardinality = "ONE"
			r.NullPolicy = "MATCH"
			c["t"].Fields[0].IsNullable = true
		},
		func(c JoinCatalog, r *domain.Relationship) { r.OutputFormat = "FIRST" },
		func(c JoinCatalog, r *domain.Relationship) {
			r.OutputFormat = "FIRST"
			r.TargetOrder = []domain.RelationshipOrder{{FieldID: "extra"}}
		},
		func(c JoinCatalog, r *domain.Relationship) {
			r.OutputFormat = "FIRST"
			r.TargetOrder = []domain.RelationshipOrder{{FieldID: "key"}}
			c["t"].Fields[0].IsNullable = true
		},
		func(c JoinCatalog, r *domain.Relationship) { r.PreserveSourceOrder = true },
		func(c JoinCatalog, r *domain.Relationship) { r.OutputFormat = "JSON_ARRAY"; r.JoinType = "FULL" },
		func(c JoinCatalog, r *domain.Relationship) { r.Cardinality = "ONE"; r.Operator = "CASE_INSENSITIVE_EQ" },
		func(c JoinCatalog, r *domain.Relationship) { r.NullPolicy = "BOGUS" },
		func(c JoinCatalog, r *domain.Relationship) { r.ConversionPolicy = "BOGUS" },
		func(c JoinCatalog, r *domain.Relationship) { r.DuplicatePolicy = "BOGUS" },
		func(c JoinCatalog, r *domain.Relationship) { r.JoinType = "BOGUS" },
	} {
		c := dynamicCatalog("TEXT", "TEXT")
		r := dynamicRelation("EQ")
		mutate(c, &r)
		if _, err := CompileDynamicJoin(c, r); err == nil {
			t.Fatal("expected metadata/policy validation error")
		}
	}
}

func TestDynamicJoinConditionsFiltersPaths(t *testing.T) {
	c := dynamicCatalog("TEXT", "TEXT")
	r := dynamicRelation("EQ")
	r.NullPolicy = "MATCH"
	r.Conditions = []domain.RelationshipCondition{{Logic: "OR", Conditions: []domain.RelationshipCondition{
		{SourceFieldID: "key", TargetFieldID: "key", Operator: "EQ"}, {SourceFieldID: "extra", TargetFieldID: "extra", Operator: "EQ"},
	}}}
	r.TargetFilters = []domain.RelationshipFilter{{FieldID: "extra", Value: "'; DROP TABLE secret; --"}}
	r.PreserveSourceOrder = true
	r.SourceOrder = []domain.RelationshipOrder{{FieldID: "key"}}
	got, err := CompileDynamicJoin(c, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{" IS NOT DISTINCT FROM ", " AND (", " OR ", `"target"."extra" = $1`, `ORDER BY "source"."unusual_key" ASC`} {
		if !strings.Contains(got.SQL, want) {
			t.Fatal(got.SQL)
		}
	}
	if len(got.Args) != 1 || strings.Contains(got.SQL, "DROP TABLE") || strings.Contains(got.SQL, " WHERE ") {
		t.Fatal("filter should be bound in ON", got)
	}
	c = dynamicCatalog("JSON", "UUID")
	r = dynamicRelation("JSON_PATH_EQ")
	r.SourceJSONPath = []string{"plant's", `a\b`}
	got, err = CompileDynamicJoin(c, r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.SQL, `::jsonb #>> ARRAY[E'plant''s', E'a\\b']::text[]`) {
		t.Fatal(got.SQL)
	}
	if strings.Contains(got.SQL, "::uuid") {
		t.Fatal("must never cast arbitrary JSON text to UUID")
	}
}

func TestDynamicJoinCompositeKeyAndRange(t *testing.T) {
	c := dynamicCatalog("INTEGER", "INTEGER")
	m := c["t"]
	m.VerifiedUniqueKeys = [][]string{{"key", "extra"}}
	c["t"] = m
	r := dynamicRelation("EQ")
	r.Cardinality = "ONE"
	if _, err := CompileDynamicJoin(c, r); err == nil {
		t.Fatal("partial composite key accepted")
	}
	r.Conditions = []domain.RelationshipCondition{{SourceFieldID: "extra", TargetFieldID: "extra", Operator: "EQ"}}
	if _, err := CompileDynamicJoin(c, r); err != nil {
		t.Fatal(err)
	}
	r.Cardinality = "MANY"
	c["t"].Fields[1].PhysicalDataType = "INTEGER"
	r.Conditions = []domain.RelationshipCondition{{SourceFieldID: "key", TargetFieldID: "key", TargetEndFieldID: "extra", Operator: "BETWEEN"}}
	got, err := CompileDynamicJoin(c, r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.SQL, `BETWEEN "target"."unusual_key" AND "target"."extra"`) {
		t.Fatal(got.SQL)
	}
}
