package postgres_test

import (
	"models-go/adapters/postgres"
	"github.com/SanjayDrop5528/models-go-engine/diff"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"strings"
	"testing"
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
