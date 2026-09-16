// Package postgres provides PostgreSQL query building for relational operations,
// CRUD statements, parameterized filters, and orbital reference JSON projections.
//
// Usage:
// This file transforms high-level engine query definitions (query.Query) into parameterized
// PostgreSQL SQL statements ($1, $2, etc.). It supports:
// 1. SELECT queries with projections, joins, WHERE filters, ORDER BY, LIMIT, and OFFSET.
// 2. Nested orbital reference joins rendered as JSONB objects (jsonb_build_object, to_jsonb).
// 3. Parameterized INSERT, UPDATE, and DELETE statements with RETURNING *.
package postgres

import (
	"fmt"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"strings"
)

// QueryBuilder compiles query.Query into parameterized PostgreSQL SQL queries.
type QueryBuilder struct{}

// RelationJoin specifies how an orbital reference relation is joined and projected.
type RelationJoin struct {
	Name             string
	SourceTable      string
	SourceColumn     string
	TargetTable      string
	TargetColumn     string
	Alias            string
	Conditions       []string
	SelectedFields   []string
	AdditionalOn     []string
	OrderBy          []query.Sort
	ParentAlias      string
	NestedJoins      []*RelationJoin
	LoadWithChildren bool
}

type relationJoin = RelationJoin

// BuildSelect compiles a SELECT query, returning the query string and argument slice.
//
// Purpose:
// Compiles a query.Query into a parameterized PostgreSQL SELECT statement and argument list.
//
// Where it is used:
// In PostgresAdapter.Query for standard table queries.
//
// When can it be used:
// Can be used when querying a table without orbital reference relation expansion.
func (b *QueryBuilder) BuildSelect(table string, q query.Query) (string, []any) {
	return b.BuildSelectWithRelations(table, q, nil)
}

// BuildSelectWithRelations compiles a SELECT with explicit joins and metadata-backed relations.
//
// Purpose:
// Compiles a SELECT statement with left-joined metadata-backed orbital relations projected as JSONB objects.
//
// Where it is used:
// In PostgresAdapter.Query when models declare orbital references with LoadWithParent or relations.
//
// When can it be used:
// Can be used when relational data must be fetched and nested hierarchically in a single SQL query.
func (b *QueryBuilder) BuildSelectWithRelations(table string, q query.Query, relations []relationJoin) (string, []any) {
	var args []any
	argIdx := 1

	hasJoins := len(relations) > 0 || len(q.Joins) > 0
	qualifier := ""
	if hasJoins {
		qualifier = table
	}

	cols := quoteIdent(table) + ".*"
	if len(q.Fields) > 0 {
		quoted := make([]string, len(q.Fields))
		for i, f := range q.Fields {
			quoted[i] = qualifyIdent(qualifier, f)
		}
		cols = strings.Join(quoted, ", ")
	}
	if len(relations) > 0 {
		relCols := make([]string, 0, len(relations))
		for _, rel := range relations {
			relCols = append(relCols, b.buildRelationColumnExpr(rel))
		}
		cols += ", " + strings.Join(relCols, ", ")
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", cols, quoteIdent(table))

	for _, j := range q.Joins {
		joinType := strings.TrimSpace(j.Type)
		if joinType == "" {
			joinType = "JOIN"
		}
		joinTable := strings.TrimSpace(j.Table)
		if joinTable == "" {
			continue
		}
		sql += fmt.Sprintf(" %s %s", joinType, joinTable)
		if j.On != "" {
			onClause := j.On
			for _, arg := range j.Args {
				if strings.Contains(onClause, "?") {
					onClause = strings.Replace(onClause, "?", fmt.Sprintf("$%d", argIdx), 1)
					argIdx++
				}
				args = append(args, arg)
			}
			sql += " ON " + onClause
		}
	}

	for _, rel := range relations {
		sql += b.buildRelationJoinSQL(rel)
	}

	if len(q.Filters) > 0 || len(q.RawWheres) > 0 || len(q.WhereGroups) > 0 {
		whereClause, whereArgs := b.buildWhere(qualifier, q, &argIdx)
		sql += " WHERE " + whereClause
		args = append(args, whereArgs...)
	}

	var sortClauses []string
	for _, s := range q.Sorts {
		order := "ASC"
		if s.Order == query.SortDesc {
			order = "DESC"
		}
		sortClauses = append(sortClauses, fmt.Sprintf("%s %s", qualifyIdent(qualifier, s.Field), order))
	}
	for _, rel := range relations {
		for _, s := range rel.OrderBy {
			order := "ASC"
			if s.Order == query.SortDesc {
				order = "DESC"
			}
			sortClauses = append(sortClauses, fmt.Sprintf("%s.%s %s", quoteIdent(rel.Alias), quoteIdent(s.Field), order))
		}
	}
	if len(sortClauses) > 0 {
		sql += " ORDER BY " + strings.Join(sortClauses, ", ")
	}

	if q.Pagination.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", q.Pagination.Limit)
	}
	if q.Pagination.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", q.Pagination.Offset)
	}

	return sql + ";", args
}

// buildRelationColumnExpr builds a JSONB column projection expression for an orbital relation.
//
// Purpose:
// Generates a jsonb_build_object or to_jsonb expression (with nested child joins) to project related rows as JSON.
//
// Where it is used:
// In BuildSelectWithRelations when projecting related entity columns.
//
// When can it be used:
// Can be used whenever an orbital reference is selected and needs to be nested hierarchically.
func (b *QueryBuilder) buildRelationColumnExpr(rel relationJoin) string {
	targetCol := rel.TargetColumn
	if targetCol == "" {
		targetCol = "id"
	}

	var baseExpr string
	if len(rel.SelectedFields) > 0 {
		pairs := make([]string, 0, len(rel.SelectedFields)*2)
		for _, f := range rel.SelectedFields {
			pairs = append(pairs, fmt.Sprintf("'%s'", f), fmt.Sprintf("%s.%s", quoteIdent(rel.Alias), quoteIdent(f)))
		}
		baseExpr = fmt.Sprintf("jsonb_build_object(%s)", strings.Join(pairs, ", "))
	} else {
		baseExpr = fmt.Sprintf("to_jsonb(%s.*)", quoteIdent(rel.Alias))
	}

	for _, child := range rel.NestedJoins {
		if child == nil {
			continue
		}
		childTargetCol := child.TargetColumn
		if childTargetCol == "" {
			childTargetCol = "id"
		}
		var childExpr string
		if len(child.SelectedFields) > 0 {
			cpairs := make([]string, 0, len(child.SelectedFields)*2)
			for _, cf := range child.SelectedFields {
				cpairs = append(cpairs, fmt.Sprintf("'%s'", cf), fmt.Sprintf("%s.%s", quoteIdent(child.Alias), quoteIdent(cf)))
			}
			childExpr = fmt.Sprintf("jsonb_build_object(%s)", strings.Join(cpairs, ", "))
		} else {
			childExpr = fmt.Sprintf("to_jsonb(%s.*)", quoteIdent(child.Alias))
		}
		childExpr = fmt.Sprintf("CASE WHEN %s.%s IS NULL THEN NULL ELSE %s END", quoteIdent(child.Alias), quoteIdent(childTargetCol), childExpr)
		baseExpr = fmt.Sprintf("(%s || jsonb_build_object('%s', %s))", baseExpr, child.Name, childExpr)
	}

	return fmt.Sprintf("CASE WHEN %s.%s IS NULL THEN NULL ELSE %s END AS %s", quoteIdent(rel.Alias), quoteIdent(targetCol), baseExpr, quoteIdent(rel.Name))
}

// buildRelationJoinSQL generates the LEFT JOIN clause for an orbital relation.
//
// Purpose:
// Formats the LEFT JOIN ON clause matching source and target columns, including any nested relation joins.
//
// Where it is used:
// In BuildSelectWithRelations when assembling the FROM and JOIN clauses.
//
// When can it be used:
// Can be used whenever an orbital relation join must be appended to the SQL statement.
func (b *QueryBuilder) buildRelationJoinSQL(rel relationJoin) string {
	source := rel.SourceTable
	if rel.ParentAlias != "" {
		source = rel.ParentAlias
	}
	onClause := fmt.Sprintf("%s.%s = %s.%s",
		quoteIdent(source),
		quoteIdent(rel.SourceColumn),
		quoteIdent(rel.Alias),
		quoteIdent(rel.TargetColumn),
	)
	if len(rel.Conditions) > 0 {
		onClause += " AND " + strings.Join(rel.Conditions, " AND ")
	}
	if len(rel.AdditionalOn) > 0 {
		onClause += " AND " + strings.Join(rel.AdditionalOn, " AND ")
	}
	sqlStr := fmt.Sprintf(" LEFT JOIN %s AS %s ON %s", quoteIdent(rel.TargetTable), quoteIdent(rel.Alias), onClause)
	for _, child := range rel.NestedJoins {
		if child != nil {
			sqlStr += b.buildRelationJoinSQL(*child)
		}
	}
	return sqlStr
}

// BuildInsert compiles an INSERT statement returning generated rows.
//
// Purpose:
// Generates a parameterized INSERT INTO table (...) VALUES (...) RETURNING * SQL statement.
//
// Where it is used:
// In PostgresAdapter.Execute when executing Create operations.
//
// When can it be used:
// Can be used whenever inserting a new row into a PostgreSQL table.
func (b *QueryBuilder) BuildInsert(table string, data map[string]any) (string, []any) {
	var cols []string
	var placeholders []string
	var args []any

	idx := 1
	for k, v := range data {
		cols = append(cols, quoteIdent(k))
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, v)
		idx++
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING *;",
		quoteIdent(table),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	return sql, args
}

// BuildUpdate compiles an UPDATE statement by ID returning updated row.
//
// Purpose:
// Generates a parameterized UPDATE table SET ... WHERE id = $id RETURNING * SQL statement.
//
// Where it is used:
// In PostgresAdapter.Execute when executing Update operations.
//
// When can it be used:
// Can be used whenever updating an existing row by ID in PostgreSQL.
func (b *QueryBuilder) BuildUpdate(table string, id any, data map[string]any) (string, []any) {
	var setClauses []string
	var args []any

	idx := 1
	for k, v := range data {
		if k == "id" {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", quoteIdent(k), idx))
		args = append(args, v)
		idx++
	}

	args = append(args, id)
	idPlaceholder := fmt.Sprintf("$%d", idx)

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE \"id\" = %s RETURNING *;",
		quoteIdent(table),
		strings.Join(setClauses, ", "),
		idPlaceholder,
	)

	return sql, args
}

// BuildDelete compiles a DELETE statement by ID.
//
// Purpose:
// Generates a parameterized DELETE FROM table WHERE id = $1 SQL statement.
//
// Where it is used:
// In PostgresAdapter.Execute when executing Delete operations.
//
// When can it be used:
// Can be used whenever deleting a row by its primary key ID in PostgreSQL.
func (b *QueryBuilder) BuildDelete(table string, id any) (string, []any) {
	sql := fmt.Sprintf("DELETE FROM %s WHERE \"id\" = $1;", quoteIdent(table))
	return sql, []any{id}
}

// buildWhere compiles filter expressions, raw wheres, and where groups into a SQL WHERE clause.
//
// Purpose:
// Transforms query.Filter conditions (Eq, Neq, Gt, Gte, Lt, Lte, Like, ILike, IsNull, In, Between) into parameterized SQL.
//
// Where it is used:
// In BuildSelectWithRelations and count query builders.
//
// When can it be used:
// Can be used whenever compiling query filters into PostgreSQL WHERE conditions.
func (b *QueryBuilder) buildWhere(table string, q query.Query, argIdx *int) (string, []any) {
	var clauses []string
	var args []any

	opJoin := " AND "
	if q.LogicalOp == query.OpOr {
		opJoin = " OR "
	}

	for _, f := range q.Filters {
		col := qualifyIdent(table, f.Field)

		switch f.Op {
		case query.OpEq:
			clauses = append(clauses, fmt.Sprintf("%s = $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpNeq:
			clauses = append(clauses, fmt.Sprintf("%s != $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpGt:
			clauses = append(clauses, fmt.Sprintf("%s > $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpGte:
			clauses = append(clauses, fmt.Sprintf("%s >= $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpLt:
			clauses = append(clauses, fmt.Sprintf("%s < $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpLte:
			clauses = append(clauses, fmt.Sprintf("%s <= $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpLike:
			clauses = append(clauses, fmt.Sprintf("%s LIKE $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpILike:
			clauses = append(clauses, fmt.Sprintf("%s ILIKE $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpNotLike:
			clauses = append(clauses, fmt.Sprintf("%s NOT ILIKE $%d", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpIsNull:
			clauses = append(clauses, fmt.Sprintf("%s IS NULL", col))
		case query.OpIsNotNull:
			clauses = append(clauses, fmt.Sprintf("%s IS NOT NULL", col))
		case query.OpIn:
			clauses = append(clauses, fmt.Sprintf("%s = ANY($%d)", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpNin:
			clauses = append(clauses, fmt.Sprintf("NOT (%s = ANY($%d))", col, *argIdx))
			args = append(args, f.Value)
			*argIdx++
		case query.OpBetween:
			clauses = append(clauses, fmt.Sprintf("%s BETWEEN $%d AND $%d", col, *argIdx, *argIdx+1))
			args = append(args, f.Value, f.ValueTo)
			*argIdx += 2
		}
	}

	for _, rw := range q.RawWheres {
		clause := rw.Query
		for _, arg := range rw.Args {
			if strings.Contains(clause, "?") {
				clause = strings.Replace(clause, "?", fmt.Sprintf("$%d", *argIdx), 1)
				*argIdx++
			}
			args = append(args, arg)
		}
		clauses = append(clauses, clause)
	}

	for _, wg := range q.WhereGroups {
		subClause, subArgs := b.buildWhere(table, wg.Query, argIdx)
		if subClause != "" {
			clauses = append(clauses, fmt.Sprintf("(%s)", subClause))
			args = append(args, subArgs...)
		}
	}

	return strings.Join(clauses, opJoin), args
}

// qualifyIdent quotes and table-qualifies column names to prevent SQL collisions.
//
// Purpose:
// Generates properly double-quoted table.column identifiers (e.g., "users"."email") or "*" wildcards.
//
// Where it is used:
// In BuildSelectWithRelations, buildWhere, and ORDER BY clause builders.
//
// When can it be used:
// Can be used whenever an identifier must be safely qualified for a PostgreSQL query.
func qualifyIdent(table, field string) string {
	field = strings.TrimSpace(field)
	if field == "*" {
		if table != "" {
			return quoteIdent(table) + ".*"
		}
		return "*"
	}
	if strings.Contains(field, ".") {
		return quoteIdent(field)
	}
	if table != "" {
		return fmt.Sprintf("%s.%s", quoteIdent(table), quoteIdent(field))
	}
	return quoteIdent(field)
}
