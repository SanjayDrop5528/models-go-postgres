package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/SanjayDrop5528/models-go-engine/adapter"
	"github.com/SanjayDrop5528/models-go-engine/dataset/compiler"
	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/dataset/planner"
)

// PostgresDataSetCompiler compiles QueryAST into PostgreSQL SQL and Procedures/Functions.
type PostgresDataSetCompiler struct{}

// NewPostgresDataSetCompiler creates a new PostgreSQL dataset compiler instance.
func NewPostgresDataSetCompiler() *PostgresDataSetCompiler {
	return &PostgresDataSetCompiler{}
}

// Compile compiles the QueryAST into PostgreSQL SQL and stored DDL.
func (c *PostgresDataSetCompiler) Compile(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error) {
	if ast == nil {
		return nil, domain.NewError(domain.ErrPipelineCompilationFailed, "cannot compile nil AST")
	}

	saveMode := ds.SaveMode
	if saveMode == "" {
		saveMode = domain.SaveModeQuery
	}

	execQuery := c.buildSelectSQL(ast, false, false)
	refQuery := c.buildSelectSQL(ast, true, false)
	routineQuery := c.buildSelectSQL(ast, false, true)
	baseSchema := ds.BaseCollection.Schema
	if baseSchema == "" {
		baseSchema = "metadata_catalog"
	}
	ddl := c.buildDDL(ds.ReferenceName, baseSchema, routineQuery, ast.Parameters, saveMode)

	return &compiler.CompiledPipeline{
		ExecutableQuery:   execQuery,
		ReferencePipeline: refQuery,
		Parameters:        ast.Parameters,
		DDLStatement:      ddl,
		SaveMode:          saveMode,
		Driver:            "postgres",
	}, nil
}

func (c *PostgresDataSetCompiler) buildSelectSQL(ast *planner.QueryAST, parameterized, isRoutine bool) string {
	var selectCols []string

	// 1. Projections
	for _, p := range ast.Projections {
		colExpr := fmt.Sprintf("\"%s\".\"%s\"", p.SourceTable, p.SourceField)
		if p.Alias != "" && p.Alias != p.SourceField {
			colExpr += fmt.Sprintf(" AS \"%s\"", p.Alias)
		}
		selectCols = append(selectCols, colExpr)
	}

	// 2. Custom Columns
	for _, cc := range ast.CustomColumns {
		expr := cc.Expression
		if cc.Function != nil && cc.Function.PostgresExpression != "" {
			expr = renderPostgresFunctionExpression(cc.Function.PostgresExpression, cc.Operands)
		} else if expr == "" && cc.Function != nil {
			expr = buildPostgresFunctionExpression(cc.Function.Name, cc.Operands)
		} else if expr == "" && cc.IsAggregate {
			expr = buildPostgresFunctionExpression(cc.FunctionName, cc.Operands)
		} else if expr == "" && cc.FunctionName != "" {
			expr = buildPostgresFunctionExpression(cc.FunctionName, cc.Operands)
		}

		if expr != "" {
			alias := cc.Alias
			if alias == "" {
				alias = cc.Label
			}
			selectCols = append(selectCols, fmt.Sprintf("%s AS \"%s\"", expr, alias))
		}
	}

	if len(selectCols) == 0 {
		selectCols = append(selectCols, "*")
	}

	// 3. FROM Base Table
	baseTbl := fmt.Sprintf("\"%s\"", ast.BaseTable.Table)
	if ast.BaseTable.Schema != "" && ast.BaseTable.Schema != "public" {
		baseTbl = fmt.Sprintf("\"%s\".\"%s\"", ast.BaseTable.Schema, ast.BaseTable.Table)
	}
	fromClause := fmt.Sprintf("FROM %s AS \"%s\"", baseTbl, ast.BaseTable.Alias)

	// 4. Joins
	var joinClauses []string
	for _, j := range ast.Joins {
		jType := "LEFT JOIN"
		switch j.Type {
		case domain.JoinInner:
			jType = "INNER JOIN"
		case domain.JoinRight:
			jType = "RIGHT JOIN"
		case domain.JoinFull:
			jType = "FULL JOIN"
		}

		onCondition := fmt.Sprintf("\"%s\".\"%s\" = \"%s\".\"%s\"", j.FromTable, j.FromField, j.Alias, j.ToField)
		if j.ConvertString {
			switch strings.ToUpper(j.CastMode) {
			case "FROM_ONLY":
				onCondition = fmt.Sprintf("CAST(\"%s\".\"%s\" AS TEXT) = \"%s\".\"%s\"", j.FromTable, j.FromField, j.Alias, j.ToField)
			case "TO_ONLY":
				onCondition = fmt.Sprintf("\"%s\".\"%s\" = CAST(\"%s\".\"%s\" AS TEXT)", j.FromTable, j.FromField, j.Alias, j.ToField)
			default:
				onCondition = fmt.Sprintf("CAST(\"%s\".\"%s\" AS TEXT) = CAST(\"%s\".\"%s\" AS TEXT)", j.FromTable, j.FromField, j.Alias, j.ToField)
			}
		}

		// Join filter applied directly to the ON clause
		if len(j.JoinFilter) > 0 {
			var filterParts []string
			for k, v := range j.JoinFilter {
				filterParts = append(filterParts, fmt.Sprintf("\"%s\".\"%s\" = '%v'", j.Alias, k, v))
			}
			onCondition += " AND " + strings.Join(filterParts, " AND ")
		}

		targetTbl := fmt.Sprintf("\"%s\"", j.ToTable)
		if j.Schema != "" && j.Schema != "public" {
			targetTbl = fmt.Sprintf("\"%s\".\"%s\"", j.Schema, j.ToTable)
		} else if strings.Contains(j.ToTable, ".") {
			parts := strings.SplitN(j.ToTable, ".", 2)
			targetTbl = fmt.Sprintf("\"%s\".\"%s\"", parts[0], parts[1])
		}

		joinClauses = append(joinClauses, fmt.Sprintf("%s %s AS \"%s\" ON %s", jType, targetTbl, j.Alias, onCondition))
	}

	// 5. WHERE Clauses
	var whereClauses []string
	if len(ast.BaseTable.Filter) > 0 {
		for k, v := range ast.BaseTable.Filter {
			whereClauses = append(whereClauses, formatFilterCondition(ast.BaseTable.Alias, k, v))
		}
	}

	argIdx := 1
	for _, cond := range ast.WhereFilters {
		if cond.IsParamRef {
			if isRoutine {
				whereClauses = append(whereClauses, fmt.Sprintf("(p_%s IS NULL OR \"%s\".\"%s\" = p_%s)", cond.ParamName, cond.Table, cond.Column, cond.ParamName))
			} else if parameterized {
				whereClauses = append(whereClauses, fmt.Sprintf("($%d IS NULL OR \"%s\".\"%s\" = $%d)", argIdx, cond.Table, cond.Column, argIdx))
				argIdx++
			} else {
				// Replace param with default/literal string value
				foundDefault := false
				for _, p := range ast.Parameters {
					if strings.EqualFold(p.ParamName, cond.ParamName) && p.DefaultValue != nil {
						whereClauses = append(whereClauses, fmt.Sprintf("\"%s\".\"%s\" = '%v'", cond.Table, cond.Column, p.DefaultValue))
						foundDefault = true
						break
					}
				}
				if !foundDefault {
					whereClauses = append(whereClauses, fmt.Sprintf("\"%s\".\"%s\" = '%v'", cond.Table, cond.Column, cond.Value))
				}
			}
		} else if cond.Value != nil {
			whereClauses = append(whereClauses, fmt.Sprintf("\"%s\".\"%s\" = '%v'", cond.Table, cond.Column, cond.Value))
		}
	}

	// 6. GROUP BY
	var groupByCols []string
	for _, g := range ast.GroupBy {
		groupByCols = append(groupByCols, fmt.Sprintf("\"%s\".\"%s\"", g.Table, g.Field))
	}

	// Build full SQL
	sql := fmt.Sprintf("SELECT\n  %s\n%s", strings.Join(selectCols, ",\n  "), fromClause)
	if len(joinClauses) > 0 {
		sql += "\n" + strings.Join(joinClauses, "\n")
	}
	if len(whereClauses) > 0 {
		sql += "\nWHERE " + strings.Join(whereClauses, " AND ")
	}
	if len(groupByCols) > 0 {
		sql += "\nGROUP BY " + strings.Join(groupByCols, ", ")
	}

	return sql + ";"
}

func renderPostgresFunctionExpression(template string, operands []planner.ASTOperand) string {
	expr := template
	var allArgs []string
	for i, op := range operands {
		opSQL := formatPostgresOperand(op)
		expr = strings.ReplaceAll(expr, fmt.Sprintf("{{%d}}", i), opSQL)
		allArgs = append(allArgs, opSQL)
	}
	return strings.ReplaceAll(expr, "{{args}}", strings.Join(allArgs, ", "))
}

func buildPostgresFunctionExpression(fnName string, operands []planner.ASTOperand) string {
	fn := strings.ToUpper(strings.TrimSpace(fnName))
	first := "*"
	if len(operands) > 0 {
		first = formatPostgresOperand(operands[0])
	}
	switch fn {
	case "COUNT_ALL", "COUNT(*)":
		return "COUNT(*)"
	case "COUNT":
		if first == "" {
			first = "*"
		}
		return fmt.Sprintf("COUNT(%s)", first)
	case "COUNT_DISTINCT":
		return fmt.Sprintf("COUNT(DISTINCT %s)", first)
	case "SUM", "AVG", "MIN", "MAX", "ABS", "SQRT":
		return fmt.Sprintf("%s(%s)", fn, first)
	case "ADD":
		return buildPostgresBinaryExpression(operands, "+")
	case "SUBTRACT":
		return buildPostgresBinaryExpression(operands, "-")
	case "MULTIPLY":
		return buildPostgresBinaryExpression(operands, "*")
	case "DIVIDE":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("(%s / NULLIF(%s, 0))", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "CONCAT":
		return fmt.Sprintf("CONCAT(%s)", strings.Join(formatPostgresOperands(operands), ", "))
	case "CONCAT_WS":
		args := formatPostgresOperands(operands)
		if len(args) == 0 {
			return ""
		}
		return fmt.Sprintf("CONCAT_WS(%s)", strings.Join(args, ", "))
	case "UPPER", "LOWER", "TRIM", "LENGTH":
		return fmt.Sprintf("%s(%s)", fn, first)
	case "YEAR":
		return fmt.Sprintf("EXTRACT(YEAR FROM %s)", first)
	case "MONTH":
		return fmt.Sprintf("EXTRACT(MONTH FROM %s)", first)
	case "DAY":
		return fmt.Sprintf("EXTRACT(DAY FROM %s)", first)
	case "NOW":
		return "NOW()"
	case "CURRENT_DATE":
		return "CURRENT_DATE"
	default:
		return ""
	}
}

func buildPostgresBinaryExpression(operands []planner.ASTOperand, op string) string {
	if len(operands) < 2 {
		return ""
	}
	return fmt.Sprintf("(%s %s %s)", formatPostgresOperand(operands[0]), op, formatPostgresOperand(operands[1]))
}

func formatPostgresOperands(operands []planner.ASTOperand) []string {
	args := make([]string, 0, len(operands))
	for _, op := range operands {
		args = append(args, formatPostgresOperand(op))
	}
	return args
}

func formatPostgresOperand(op planner.ASTOperand) string {
	if op.SourceTable == "" || op.SourceTable == "_LITERAL_" {
		if op.IsLiteral {
			return fmt.Sprintf("%v", op.LiteralVal)
		}
		return fmt.Sprintf("\"%s\"", op.SourceField)
	}
	return fmt.Sprintf("\"%s\".\"%s\"", op.SourceTable, op.SourceField)
}

func (c *PostgresDataSetCompiler) buildDDL(procName, baseSchema, querySQL string, params []domain.FilterParam, mode domain.SaveMode) string {
	if mode == domain.SaveModeQuery {
		return ""
	}
	if baseSchema == "" {
		baseSchema = "metadata_catalog"
	}

	cleanName := strings.ReplaceAll(procName, "-", "_")
	var paramDefs []string
	for _, p := range params {
		pgType := "TEXT"
		switch strings.ToLower(p.ParamDataType) {
		case "int", "integer":
			pgType = "INTEGER"
		case "decimal", "numeric", "float":
			pgType = "NUMERIC"
		case "boolean", "bool":
			pgType = "BOOLEAN"
		case "date":
			pgType = "DATE"
		case "timestamp", "datetime":
			pgType = "TIMESTAMP"
		}

		defaultClause := "DEFAULT NULL"
		if p.DefaultValue != nil {
			defaultClause = fmt.Sprintf("DEFAULT '%v'", p.DefaultValue)
		}
		paramDefs = append(paramDefs, fmt.Sprintf("p_%s %s %s", p.ParamName, pgType, defaultClause))
	}

	if mode == domain.SaveModeFunction {
		return fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;
CREATE OR REPLACE FUNCTION %s.fn_%s(%s)
RETURNS TABLE (result_json jsonb)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT to_jsonb(t) FROM (%s) t;
END;
$$;`, baseSchema, baseSchema, cleanName, strings.Join(paramDefs, ", "), strings.TrimSuffix(querySQL, ";"))
	}

	return fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;
CREATE OR REPLACE PROCEDURE %s.sp_%s(%s)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Executable query for dataset '%s'
    %s
END;
$$;`, baseSchema, baseSchema, cleanName, strings.Join(paramDefs, ", "), procName, querySQL)
}

// CompileDataSet compiles QueryAST into PostgreSQL SQL.
func (a *PostgresAdapter) CompileDataSet(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error) {
	return NewPostgresDataSetCompiler().Compile(ctx, ast, ds)
}

// DataSetCompiler returns the adapter.DataSetCompiler instance.
func (a *PostgresAdapter) DataSetCompiler() adapter.DataSetCompiler {
	return &genericCompilerWrapper{c: NewPostgresDataSetCompiler()}
}

type genericCompilerWrapper struct {
	c compiler.DataSetCompiler
}

func (w *genericCompilerWrapper) Compile(ctx context.Context, ast any, ds any) (any, error) {
	qAst, _ := ast.(*planner.QueryAST)
	dSet, _ := ds.(*domain.DataSet)
	return w.c.Compile(ctx, qAst, dSet)
}

func formatFilterCondition(table, col string, val any) string {
	targetTable := table
	targetCol := col
	if idx := strings.Index(col, "."); idx >= 0 {
		targetTable = col[:idx]
		targetCol = col[idx+1:]
	}

	if m, ok := val.(map[string]any); ok {
		var parts []string
		for op, operand := range m {
			switch op {
			case "$gt":
				parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" > %s", targetTable, targetCol, formatSQLVal(operand)))
			case "$gte":
				parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" >= %s", targetTable, targetCol, formatSQLVal(operand)))
			case "$lt":
				parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" < %s", targetTable, targetCol, formatSQLVal(operand)))
			case "$lte":
				parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" <= %s", targetTable, targetCol, formatSQLVal(operand)))
			case "$ne":
				parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" <> %s", targetTable, targetCol, formatSQLVal(operand)))
			case "$regex", "$like":
				parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" ILIKE '%%%v%%'", targetTable, targetCol, operand))
			case "$in":
				parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" IN (%s)", targetTable, targetCol, formatSQLIn(operand)))
			default:
				parts = append(parts, fmt.Sprintf("\"%s\".\"%s\" = %s", targetTable, targetCol, formatSQLVal(operand)))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, " AND ")
		}
	}
	if val == nil {
		return fmt.Sprintf("\"%s\".\"%s\" IS NULL", targetTable, targetCol)
	}
	return fmt.Sprintf("\"%s\".\"%s\" = %s", targetTable, targetCol, formatSQLVal(val))
}

func formatSQLVal(val any) string {
	switch v := val.(type) {
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		s := strings.TrimSpace(fmt.Sprintf("%v", v))
		if isNumericString(s) {
			return s
		}
		// If it's a parameter placeholder (e.g. :employee_id or $1)
		if strings.HasPrefix(s, ":") || strings.HasPrefix(s, "$") {
			return s
		}
		// If it's a dynamic relative date macro, e.g. C[-7d], C[-1d], C[0d], C[-30d], C[-1m]
		if sqlExpr, ok := parseCustomDateMacro(s); ok {
			return sqlExpr
		}
		// If it's a SQL date/time expression (e.g. NOW(), CURRENT_TIMESTAMP, CURRENT_DATE)
		sUpper := strings.ToUpper(s)
		if strings.HasPrefix(sUpper, "NOW()") || strings.HasPrefix(sUpper, "CURRENT_DATE") || strings.HasPrefix(sUpper, "CURRENT_TIMESTAMP") {
			return s
		}
		return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
	}
}

// parseCustomDateMacro translates custom relative date macros like C[-7d] into PostgreSQL interval expressions.
func parseCustomDateMacro(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if (strings.HasPrefix(s, "C[") || strings.HasPrefix(s, "c[")) && strings.HasSuffix(s, "]") {
		inner := strings.TrimSpace(s[2 : len(s)-1])
		if inner == "" || strings.EqualFold(inner, "0d") || strings.EqualFold(inner, "today") {
			return "CURRENT_DATE", true
		}
		if strings.EqualFold(inner, "now") {
			return "NOW()", true
		}

		sign := "-"
		offsetStr := inner
		if strings.HasPrefix(inner, "+") {
			sign = "+"
			offsetStr = inner[1:]
		} else if strings.HasPrefix(inner, "-") {
			sign = "-"
			offsetStr = inner[1:]
		}

		numEnd := 0
		for numEnd < len(offsetStr) && (offsetStr[numEnd] >= '0' && offsetStr[numEnd] <= '9') {
			numEnd++
		}

		if numEnd > 0 {
			num := offsetStr[:numEnd]
			unitRaw := strings.ToLower(strings.TrimSpace(offsetStr[numEnd:]))
			unit := "days"
			switch {
			case unitRaw == "d" || strings.HasPrefix(unitRaw, "day"):
				unit = "days"
			case unitRaw == "m" || strings.HasPrefix(unitRaw, "month"):
				unit = "months"
			case unitRaw == "y" || strings.HasPrefix(unitRaw, "year"):
				unit = "years"
			case unitRaw == "h" || strings.HasPrefix(unitRaw, "hour"):
				unit = "hours"
			case unitRaw == "w" || strings.HasPrefix(unitRaw, "week"):
				unit = "weeks"
			}
			return fmt.Sprintf("(CURRENT_DATE %s INTERVAL '%s %s')", sign, num, unit), true
		}
	}
	return "", false
}

func formatSQLIn(val any) string {
	if slice, ok := val.([]any); ok {
		var formatted []string
		for _, item := range slice {
			formatted = append(formatted, formatSQLVal(item))
		}
		return strings.Join(formatted, ", ")
	}
	return formatSQLVal(val)
}

func isNumericString(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}
