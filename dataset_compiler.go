package postgres

import (
	"context"
	"fmt"
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

	ddl := c.buildDDL(ds.ReferenceName, routineQuery, ast.Parameters, saveMode)

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
			expr = cc.Function.PostgresExpression
			for i, op := range cc.Operands {
				ph := fmt.Sprintf("{{%d}}", i)
				opSql := fmt.Sprintf("\"%s\".\"%s\"", op.SourceTable, op.SourceField)
				expr = strings.ReplaceAll(expr, ph, opSql)
			}
			// Handle {{args}}
			var allArgs []string
			for _, op := range cc.Operands {
				allArgs = append(allArgs, fmt.Sprintf("\"%s\".\"%s\"", op.SourceTable, op.SourceField))
			}
			expr = strings.ReplaceAll(expr, "{{args}}", strings.Join(allArgs, ", "))
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
			onCondition = fmt.Sprintf("CAST(\"%s\".\"%s\" AS TEXT) = CAST(\"%s\".\"%s\" AS TEXT)", j.FromTable, j.FromField, j.Alias, j.ToField)
		}

		// Join filter applied directly to the ON clause
		if len(j.JoinFilter) > 0 {
			var filterParts []string
			for k, v := range j.JoinFilter {
				filterParts = append(filterParts, fmt.Sprintf("\"%s\".\"%s\" = '%v'", j.Alias, k, v))
			}
			onCondition += " AND " + strings.Join(filterParts, " AND ")
		}

		joinClauses = append(joinClauses, fmt.Sprintf("%s \"%s\" AS \"%s\" ON %s", jType, j.ToTable, j.Alias, onCondition))
	}

	// 5. WHERE Clauses
	var whereClauses []string
	if len(ast.BaseTable.Filter) > 0 {
		for k, v := range ast.BaseTable.Filter {
			whereClauses = append(whereClauses, fmt.Sprintf("\"%s\".\"%s\" = '%v'", ast.BaseTable.Alias, k, v))
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

func (c *PostgresDataSetCompiler) buildDDL(procName, querySQL string, params []domain.FilterParam, mode domain.SaveMode) string {
	if mode == domain.SaveModeQuery {
		return ""
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
		return fmt.Sprintf(`CREATE OR REPLACE FUNCTION fn_%s(%s)
RETURNS TABLE (result_json jsonb)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT to_jsonb(t) FROM (%s) t;
END;
$$;`, cleanName, strings.Join(paramDefs, ", "), strings.TrimSuffix(querySQL, ";"))
	}

	return fmt.Sprintf(`CREATE OR REPLACE PROCEDURE sp_%s(%s)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Executable query for dataset '%s'
    %s
END;
$$;`, cleanName, strings.Join(paramDefs, ", "), procName, querySQL)
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
