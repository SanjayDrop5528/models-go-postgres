package postgres

import (
	"strings"
	"testing"

	"github.com/SanjayDrop5528/models-go-engine/dataset/planner"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/schema"
)

func TestVerifiedDatasetJoinAvoidsRedundantArrayCasts(t *testing.T) {
	for _, mode := range []string{"", "ANY_FROM", "ANY_TO", "BOTH"} {
		j := planner.ASTJoin{FromField: "arbitrary_values", ToField: "key", FromFieldDataType: "uuid[]", ToFieldDataType: "uuid", ConvertString: true, CastMode: mode}
		got, err := verifiedDatasetJoinPredicate(j, "source", "target")
		if err != nil {
			t.Fatal(err)
		}
		if got != `"target"."key" = ANY("source"."arbitrary_values")` {
			t.Fatal(got)
		}
	}
	j := planner.ASTJoin{FromField: "values", ToField: "key", FromFieldDataType: "text[]", ToFieldDataType: "uuid"}
	if _, err := verifiedDatasetJoinPredicate(j, "s", "t"); err == nil {
		t.Fatal("implicit conversion allowed")
	}
	j.ConvertString = true
	got, err := verifiedDatasetJoinPredicate(j, "s", "t")
	if err != nil {
		t.Fatal(err)
	}
	if got != `"t"."key"::text = ANY("s"."values")` {
		t.Fatal(got)
	}
}

func TestUUIDArrayMetadataProducesNativeDDL(t *testing.T) {
	f := &model.DataModel{ColumnName: "ids", DataType: "uuid", IsArray: true}
	s := schema.FromModel(&model.Model{Attributes: []model.Attribute{f.ToAttribute()}})
	if len(s.Attributes) != 1 {
		t.Fatal(s)
	}
	if got := ToPostgresType(s.Attributes[0]); got != "UUID[]" {
		t.Fatal(got)
	}
	if strings.Contains(ToPostgresType(schema.SchemaAttribute{Type: model.TypeArray}), "UUID") {
		t.Fatal("untyped arrays must not be guessed as UUID")
	}
}

func TestIsQueryValidationNeeded(t *testing.T) {
	keys := []string{"QUERY_VALIDATION_NEED", "QueryValidationNeed", "QueryvalidaitonNeed", "QUERYVALIDATIONNEED", "QUERY_VALIDATION_NEEDED"}
	clearAll := func() {
		for _, k := range keys {
			t.Setenv(k, "")
		}
	}

	clearAll()
	if isQueryValidationNeeded() {
		t.Fatal("expected false by default")
	}

	for _, k := range []string{"QUERY_VALIDATION_NEED", "QueryValidationNeed", "QueryvalidaitonNeed"} {
		clearAll()
		t.Setenv(k, "True")
		if !isQueryValidationNeeded() {
			t.Fatalf("expected true for %s=True", k)
		}
		t.Setenv(k, "false")
		if isQueryValidationNeeded() {
			t.Fatalf("expected false for %s=false", k)
		}
	}
}

