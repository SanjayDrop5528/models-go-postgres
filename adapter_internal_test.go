package postgres

import (
	"bytes"
	"context"
	"log"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SanjayDrop5528/models-go-engine/execution"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/query"
)

func TestOrderedExecutionArgs(t *testing.T) {
	req := execution.ExecutionRequest{
		Arguments: map[string]any{"second": 2, "first": 1},
		Options:   map[string]any{"parameter_order": []string{"first", "second"}},
	}
	args, err := orderedExecutionArgs(req)
	if err != nil {
		t.Fatalf("ordered arguments: %v", err)
	}
	if len(args) != 2 || args[0] != 1 || args[1] != 2 {
		t.Fatalf("unexpected argument order: %#v", args)
	}

	req.Options = map[string]any{"args": []string{"a", "b"}}
	args, err = orderedExecutionArgs(req)
	if err != nil || len(args) != 2 || args[0] != "a" || args[1] != "b" {
		t.Fatalf("explicit args not preserved: %#v, %v", args, err)
	}
}

func TestWrappedDatabaseIsNotClosedByAdapter(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	adapter := NewPostgresAdapterFromDB(database)
	if err := adapter.Close(context.Background()); err != nil {
		t.Fatalf("adapter close: %v", err)
	}
	mock.ExpectPing()
	if err := database.Ping(); err != nil {
		t.Fatalf("wrapped database was closed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionCreateUsesTransactionAndReturnsDatabaseRow(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "widgets" ("name") VALUES ($1) RETURNING *;`)).
		WithArgs("new").
		WillReturnRows(sqlmock.NewRows([]string{"widget_key", "name"}).AddRow("generated", "new"))
	mock.ExpectCommit()

	adapter := NewPostgresAdapterFromDB(database)
	tx, err := adapter.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	row, err := tx.Create(context.Background(), model.NewModelRef("widget", "Widget", "widgets", "widget_key"), map[string]any{"name": "new"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if row["widget_key"] != "generated" || row["name"] != "new" {
		t.Fatalf("unexpected returned row: %#v", row)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSlowQueryLogsInColor(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT "widgets".* FROM "widgets"`)).
		WillDelayFor(10 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow("1", "widget1"))

	previousWriter := log.Writer()
	previousFlags := log.Flags()
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})

	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)

	adapter := NewPostgresAdapterFromDB(database)
	q := query.New()
	q.SlowQueryThresholdMS = 1 // 1ms threshold

	_, _, err = adapter.Find(context.Background(), model.NewModelRef("widget", "Widget", "widgets", "id"), q)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	logOutput := buf.String()
	if !strings.Contains(logOutput, ansiColorYellowBold) {
		t.Fatalf("expected yellow color code in slow query log, got %q", logOutput)
	}
	if !strings.Contains(logOutput, "phase=slow-query") {
		t.Fatalf("expected phase=slow-query in log, got %q", logOutput)
	}
	if !strings.Contains(logOutput, ansiColorReset) {
		t.Fatalf("expected reset color code in slow query log, got %q", logOutput)
	}
}

func TestExecute_OpQuery_NormalizesJSONAndArray(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM service_types")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "services_id", "repair_service_name_alias"}).
			AddRow(
				"d06a2be8-0676-4acc-804b-261e88bf7649",
				"{9256960b-122e-4555-b507-dde679f71b2c,727100b8-b416-4ded-aac0-2491f6826401}",
				[]byte(`[{"id": "9256960b-122e-4555-b507-dde679f71b2c", "service_name": "dsadsa"}]`),
			))

	adapter := NewPostgresAdapterFromDB(database)
	res, err := adapter.Execute(context.Background(), execution.ExecutionRequest{
		Operation: "QUERY",
		Target:    "SELECT * FROM service_types",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rows, ok := res.Data.([]map[string]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("expected 1 row, got %#v", res.Data)
	}
	aliasVal, ok := rows[0]["repair_service_name_alias"].([]any)
	if !ok {
		t.Fatalf("expected repair_service_name_alias to be []any, got %T: %#v", rows[0]["repair_service_name_alias"], rows[0]["repair_service_name_alias"])
	}
	if len(aliasVal) != 1 {
		t.Fatalf("expected 1 item in alias, got %d", len(aliasVal))
	}
	servicesIDVal, ok := rows[0]["services_id"].([]any)
	if !ok {
		t.Fatalf("expected services_id to be []any, got %T: %#v", rows[0]["services_id"], rows[0]["services_id"])
	}
	if len(servicesIDVal) != 2 || servicesIDVal[0] != "9256960b-122e-4555-b507-dde679f71b2c" {
		t.Fatalf("expected 2 uuids in services_id, got %#v", servicesIDVal)
	}
}

