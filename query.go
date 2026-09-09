package postgres

import (
	"fmt"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"strings"
)

// QueryBuilder compiles query.Query into parameterized PostgreSQL SQL queries.
type QueryBuilder struct{}

// BuildSelect compiles a SELECT query, returning the query string and argument slice.
func (b *QueryBuilder) BuildSelect(table string, q query.Query) (string, []any) {
	var args []any
	argIdx := 1

	cols := "*"
	if len(q.Fields) > 0 {
		quoted := make([]string, len(q.Fields))
		for i, f := range q.Fields {
			quoted[i] = quoteIdent(f)
		}
		cols = strings.Join(quoted, ", ")
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", cols, quoteIdent(table))

	if len(q.Filters) > 0 || len(q.RawWheres) > 0 || len(q.WhereGroups) > 0 {
		whereClause, whereArgs := b.buildWhere(q, &argIdx)
		sql += " WHERE " + whereClause
		args = append(args, whereArgs...)
	}

	if len(q.Sorts) > 0 {
		var sortClauses []string
		for _, s := range q.Sorts {
			order := "ASC"
			if s.Order == query.SortDesc {
				order = "DESC"
			}
			sortClauses = append(sortClauses, fmt.Sprintf("%s %s", quoteIdent(s.Field), order))
		}
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

// BuildInsert compiles an INSERT statement returning generated rows.
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
func (b *QueryBuilder) BuildDelete(table string, id any) (string, []any) {
	sql := fmt.Sprintf("DELETE FROM %s WHERE \"id\" = $1;", quoteIdent(table))
	return sql, []any{id}
}

func (b *QueryBuilder) buildWhere(q query.Query, argIdx *int) (string, []any) {
	var clauses []string
	var args []any

	opJoin := " AND "
	if q.LogicalOp == query.OpOr {
		opJoin = " OR "
	}

	for _, f := range q.Filters {
		col := quoteIdent(f.Field)

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
		subClause, subArgs := b.buildWhere(wg.Query, argIdx)
		if subClause != "" {
			clauses = append(clauses, fmt.Sprintf("(%s)", subClause))
			args = append(args, subArgs...)
		}
	}

	return strings.Join(clauses, opJoin), args
}
