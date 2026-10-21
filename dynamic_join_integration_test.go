package postgres

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Runs against an explicitly selected scratch database; every fixture is rolled
// back. No schema or data from the caller is dropped or overwritten.
func TestDynamicJoinPostgreSQL(t *testing.T) {
	dsn := os.Getenv("DYNAMIC_JOIN_TEST_DSN")
	if dsn == "" {
		t.Skip("set DYNAMIC_JOIN_TEST_DSN for PostgreSQL integration tests")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Fixtures recreate tables with different types between transactions.
	config.DefaultQueryExecMode = pgx.QueryExecModeExec
	db := stdlib.OpenDB(*config)
	defer db.Close()
	cases := []struct {
		name, a, b, source, target, op, out string
		count                               int
	}{
		{"scalar", "integer", "bigint", "(1,'a'),(2,'b')", "(1,'a'),(1,'dup')", "EQ", "FLAT", 3},
		{"array", "integer[]", "integer", "(ARRAY[1,2],'a'),(ARRAY[]::int[],'b')", "(1,'a'),(2,'b')", "MEMBER_OF", "JSON_ARRAY", 2},
		{"cast", "text", "integer", "('01','a'),('1','b'),('invalid','c')", "(1,'a')", "EQ_CAST", "FLAT", 3},
		{"json_shapes", "jsonb", "text", `('["a"]','a'),('{}','b'),('null','c'),('4','d'),(NULL,'e')`, `('a','a')`, "JSON_MEMBER", "JSON_ARRAY", 5},
		{"json_objects", "jsonb", "text", `('[{"custom_key":"a"}]','a'),('{}','b'),('null','c'),('4','d')`, `('a','a')`, "JSON_ARRAY_MATCH", "JSON_ARRAY", 4},
		{"json_overlap", "jsonb", "jsonb", `('[1,2]','a'),('{}','b'),('null','c')`, `('[2]','a'),('false','b')`, "JSON_OVERLAP", "COUNT", 3},
		{"json_path", "json", "text", `('{"custom_key":"a"}','a'),('{"custom_key":"invalid"}','b')`, `('a','a')`, "JSON_PATH_EQ", "FLAT", 2},
		{"scalar_array", "text", "text", `('a','a'),('b','b')`, `('a','a'),('a','b')`, "EQ", "SCALAR_ARRAY", 2},
		{"exists", "text", "text", `('a','a'),('b','b')`, `('a','a')`, "EQ", "EXISTS", 2},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			setup := `CREATE SCHEMA dynamic_join_test; CREATE TABLE dynamic_join_test.table_s (unusual_key ` + tt.a + `, extra text); CREATE TABLE dynamic_join_test.table_t (unusual_key ` + tt.b + `, extra text); INSERT INTO dynamic_join_test.table_s VALUES ` + tt.source + `; INSERT INTO dynamic_join_test.table_t VALUES ` + tt.target
			if _, err = tx.Exec(setup); err != nil {
				t.Fatal(err)
			}
			catalog := dynamicCatalog(tt.a, tt.b)
			delete(catalog, "j")
			for _, m := range catalog {
				m.Config.Schema = "dynamic_join_test"
			}
			verified, err := VerifyJoinCatalog(context.Background(), tx, catalog)
			if err != nil {
				t.Fatal(err)
			}
			r := dynamicRelation(tt.op)
			r.OutputFormat = tt.out
			r.ConversionPolicy = "TEXT"
			if strings.Contains(tt.op, "PATH") || tt.op == "JSON_ARRAY_MATCH" {
				r.SourceJSONPath = []string{"custom_key"}
			}
			if tt.out == "COUNT" || tt.out == "EXISTS" {
				r.TargetFields = nil
			}
			query, err := CompileDynamicJoin(verified, r)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := tx.Query(query.SQL, query.Args...)
			if err != nil {
				t.Fatalf("%v\n%s", err, query.SQL)
			}
			n := 0
			for rows.Next() {
				n++
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				t.Fatal(err)
			}
			if n != tt.count {
				t.Fatalf("got %d rows, want %d", n, tt.count)
			}
		})
	}

	t.Run("constraints_outputs_and_filters", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, err = tx.Exec(`CREATE SCHEMA dynamic_join_test;
CREATE TABLE dynamic_join_test.table_s (unusual_key integer, extra text);
CREATE TABLE dynamic_join_test.table_t (unusual_key integer PRIMARY KEY, extra text);
CREATE TABLE dynamic_join_test.table_j (unusual_key integer, extra text);
INSERT INTO dynamic_join_test.table_s VALUES (1,'s'),(2,'s');
INSERT INTO dynamic_join_test.table_t VALUES (1,'t');
INSERT INTO dynamic_join_test.table_j VALUES (1,'j'),(1,'j');`)
		if err != nil {
			t.Fatal(err)
		}
		catalog := dynamicCatalog("integer", "integer")
		for _, m := range catalog {
			m.Config.Schema = "dynamic_join_test"
		}
		catalog, err = VerifyJoinCatalog(context.Background(), tx, catalog)
		if err != nil {
			t.Fatal(err)
		}
		if len(catalog["t"].VerifiedUniqueKeys) != 1 || len(catalog["s"].VerifiedUniqueKeys) != 0 {
			t.Fatal("constraint evidence not loaded")
		}
		for _, output := range []string{"OBJECT", "FIRST", "JSON_ARRAY", "SCALAR_ARRAY", "COUNT", "EXISTS"} {
			r := dynamicRelation("EQ")
			r.OutputFormat = output
			if output == "OBJECT" {
				r.Cardinality = "ZERO_OR_ONE"
			}
			if output == "FIRST" {
				r.TargetOrder = []domain.RelationshipOrder{{FieldID: "key"}}
			}
			if output == "COUNT" || output == "EXISTS" {
				r.TargetFields = nil
			}
			r.TargetFilters = []domain.RelationshipFilter{{FieldID: "extra", Value: "no match"}}
			q, e := CompileDynamicJoin(catalog, r)
			if e != nil {
				t.Fatal(e)
			}
			rows, e := tx.Query(q.SQL, q.Args...)
			if e != nil {
				t.Fatalf("%s: %v", output, e)
			}
			n := 0
			for rows.Next() {
				var key int
				var extra string
				var value any
				if e = rows.Scan(&key, &extra, &value); e != nil {
					t.Fatal(e)
				}
				if (output == "OBJECT" || output == "FIRST") && value != nil {
					t.Fatalf("unmatched %s is %v", output, value)
				}
				n++
			}
			e = rows.Err()
			rows.Close()
			if e != nil || n != 2 {
				t.Fatalf("LEFT output lost rows: %d %v", n, e)
			}
		}
		for _, jt := range []string{"INNER", "LEFT", "RIGHT", "FULL", "SEMI", "ANTI", "CROSS"} {
			r := dynamicRelation("EQ")
			r.JoinType = jt
			if jt == "SEMI" || jt == "ANTI" {
				r.TargetFields = nil
			}
			if jt == "CROSS" {
				r.SourceFieldID = ""
				r.TargetFieldID = ""
				r.Operator = ""
			}
			q, e := CompileDynamicJoin(catalog, r)
			if e != nil {
				t.Fatal(e)
			}
			rows, e := tx.Query(q.SQL, q.Args...)
			if e != nil {
				t.Fatal(e)
			}
			for rows.Next() {
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				t.Fatal(e)
			}
		}
		r := dynamicRelation("EQ")
		r.Cardinality = "MANY_TO_MANY"
		r.OutputFormat = "JSON_ARRAY"
		r.Junction = &domain.RelationshipJunction{ModelID: "j", SourceFieldID: "key", TargetFieldID: "key"}
		r.SourceOrder = []domain.RelationshipOrder{{FieldID: "key"}}
		for _, policy := range []string{"ALL", "DISTINCT"} {
			r.DuplicatePolicy = policy
			q, e := CompileDynamicJoin(catalog, r)
			if e != nil {
				t.Fatal(e)
			}
			var key int
			var extra string
			var raw []byte
			e = tx.QueryRow(q.SQL).Scan(&key, &extra, &raw)
			if e != nil {
				t.Fatal(e)
			}
			want := 1
			if policy == "ALL" {
				want = 2
			}
			if strings.Count(string(raw), "unusual_key") != want {
				t.Fatalf("%s: %s", policy, raw)
			}
		}
	})
}
