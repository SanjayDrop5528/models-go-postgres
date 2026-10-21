package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/dataset/repository"
	"github.com/SanjayDrop5528/models-go-engine/dataset/resolver"
	"github.com/SanjayDrop5528/models-go-engine/dataset/service"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/registry"
	"github.com/SanjayDrop5528/models-go-engine/validation"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestUUIDArrayCreateThroughDatasetPreview(t *testing.T) {
	dsn := os.Getenv("DYNAMIC_JOIN_TEST_DSN")
	if dsn == "" {
		t.Skip("set DYNAMIC_JOIN_TEST_DSN for PostgreSQL integration")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*config)
	defer db.Close()
	ctx := context.Background()
	namespace := fmt.Sprintf("dataset_array_test_%d", time.Now().UnixNano())
	if _, err = db.ExecContext(ctx, "CREATE SCHEMA "+joinIdent(namespace)); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, "DROP SCHEMA "+joinIdent(namespace)+" CASCADE")
	q := joinIdent(namespace) + "."
	_, err = db.ExecContext(ctx, `CREATE TABLE `+q+`staff (id uuid PRIMARY KEY, name text, primary_refs uuid[], secondary_refs uuid[], location_refs uuid[], optional_refs uuid[]);
CREATE TABLE `+q+`places (id uuid PRIMARY KEY, name text);
CREATE TABLE `+q+`identifications (id uuid PRIMARY KEY, owner_id uuid, name text);
CREATE TABLE `+q+`licenses (id uuid PRIMARY KEY, owner_id uuid, name text);
INSERT INTO `+q+`places VALUES ('00000000-0000-4000-8000-000000000001','Place A'),('00000000-0000-4000-8000-000000000002','Place B');
INSERT INTO `+q+`identifications VALUES ('00000000-0000-4000-8000-000000000011','00000000-0000-4000-8000-000000000099','ID A'),('00000000-0000-4000-8000-000000000012','00000000-0000-4000-8000-000000000099','ID B');
INSERT INTO `+q+`licenses VALUES ('00000000-0000-4000-8000-000000000021','00000000-0000-4000-8000-000000000099','L1'),('00000000-0000-4000-8000-000000000022','00000000-0000-4000-8000-000000000099','L2'),('00000000-0000-4000-8000-000000000023','00000000-0000-4000-8000-000000000099','L3');`)
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.NewModelRegistry()
	build := func(table string, arrays []string, owner bool) *model.Model {
		fields := []*model.DataModel{
			{ID: table + "_id", ModelID: table, ColumnName: "id", DataType: "uuid", IsPrimaryKey: true, Status: model.DataModelStatusActive},
			{ID: table + "_name", ModelID: table, ColumnName: "name", DataType: "text", Status: model.DataModelStatusActive},
		}
		for _, col := range arrays {
			fields = append(fields, &model.DataModel{ID: table + "_" + col, ModelID: table, ColumnName: col, DataType: "uuid", IsArray: true, IsNullable: true, Status: model.DataModelStatusActive})
		}
		if owner {
			fields = append(fields, &model.DataModel{ID: table + "_owner", ModelID: table, ColumnName: "owner_id", DataType: "uuid", Status: model.DataModelStatusActive})
		}
		m := model.BuildModel(&model.ModelConfig{ID: table, Name: table, Schema: namespace, Table: table, Status: model.ModelConfigStatusActive}, fields, "", model.StorageRelational)
		if _, err := reg.SetActive(table, m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	staff := build("staff", []string{"primary_refs", "secondary_refs", "location_refs", "optional_refs"}, false)
	build("places", nil, false)
	build("identifications", nil, true)
	build("licenses", nil, true)
	payload := map[string]any{"id": "00000000-0000-4000-8000-000000000099", "name": "Test", "primary_refs": []any{"00000000-0000-4000-8000-000000000001"}, "secondary_refs": []any{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"}, "location_refs": []any{}, "optional_refs": nil}
	if err := validation.ValidateData(staff, payload); err != nil {
		t.Fatal(err)
	}
	adp := NewPostgresAdapterFromDB(db)
	if _, err := adp.Create(ctx, staff.Ref(), payload); err != nil {
		t.Fatal(err)
	}
	mr := resolver.NewModelResolver(reg)
	svc := service.NewDataSetService(repository.NewDataSetRepository(), mr, mr, resolver.NewFunctionRegistry(), adp)
	ds := &domain.DataSet{Name: "preview", ReferenceName: "preview", Driver: "postgres", BaseCollection: domain.BaseCollection{Schema: namespace, Collection: "staff"}, SelectedList: []domain.SelectedField{{Field: "staff.id", HeaderName: "id"}}}
	for _, edge := range []struct{ col, alias, label string }{{"primary_refs", "primary", "primary_name"}, {"secondary_refs", "secondary", "secondary_name"}, {"location_refs", "locations", "location_name"}} {
		ds.JoinCollections = append(ds.JoinCollections, domain.JoinCollection{Schema: namespace, FromCollection: "staff", FromCollectionField: edge.col, ToCollection: "places", ToCollectionField: "id", NamedAs: edge.alias, Cardinality: "MANY", OutputFormat: "JSON_ARRAY", ConvertToString: true, CastMode: "ANY_FROM", FromFieldDataType: "TEXT", ToFieldDataType: "INTEGER"})
		ds.SelectedList = append(ds.SelectedList, domain.SelectedField{Field: edge.alias + ".name", HeaderName: edge.label})
	}
	for _, target := range []string{"identifications", "licenses"} {
		ds.JoinCollections = append(ds.JoinCollections, domain.JoinCollection{Schema: namespace, FromCollection: "staff", FromCollectionField: "id", ToCollection: target, ToCollectionField: "owner_id", NamedAs: target, Cardinality: "MANY", OutputFormat: "JSON_ARRAY"})
		ds.SelectedList = append(ds.SelectedList, domain.SelectedField{Field: target + ".name", HeaderName: target + "_label"})
	}
	result, err := svc.Preview(ctx, ds)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result.Pipeline, "CAST(") || strings.Contains(result.Pipeline, "::text") {
		t.Fatal("unexpected UUID array casts", result.Pipeline)
	}
	for _, snippet := range []string{`"primary_many"."id" = ANY("staff"."primary_refs")`, `"secondary_many"."id" = ANY("staff"."secondary_refs")`, `"locations_many"."id" = ANY("staff"."location_refs")`} {
		if !strings.Contains(result.Pipeline, snippet) {
			t.Fatal("missing predicate", snippet, result.Pipeline)
		}
	}
	if len(result.Rows) != 1 {
		t.Fatalf("driver joins multiplied employees: %#v", result.Rows)
	}
	for alias, count := range map[string]int{"primary": 1, "secondary": 2, "locations": 0, "identifications": 2, "licenses": 3} {
		items, ok := result.Rows[0][alias].([]any)
		if !ok || len(items) != count {
			t.Fatalf("%s: %#v", alias, result.Rows[0][alias])
		}
		for _, item := range items {
			object, ok := item.(map[string]any)
			if !ok || len(object) == 0 {
				t.Fatalf("projected matching fields away: %#v", items)
			}
		}
	}
	t.Setenv("QUERY_VALIDATION_NEED", "true")
	ds.JoinCollections[3].Cardinality = "ONE"
	ds.JoinCollections[3].OutputFormat = "OBJECT"
	if _, err = svc.Preview(ctx, ds); err == nil || !strings.Contains(err.Error(), "verified unique") {
		t.Fatalf("accepted nonunique singular relation: %v", err)
	}
	ds.JoinCollections[3].Cardinality = "MANY"
	ds.JoinCollections[3].OutputFormat = "JSON_ARRAY"
	ds.SelectedList = append(ds.SelectedList, domain.SelectedField{Field: "staff.missing_column"})
	if _, err = svc.Preview(ctx, ds); err == nil || !strings.Contains(err.Error(), "execution failed") {
		t.Fatalf("SQL failure hidden as empty rows: %v", err)
	}
}
