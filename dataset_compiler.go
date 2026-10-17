// Package postgres provides the dataset compilation logic for PostgreSQL databases.
//
// Usage:
// This file transforms an engine QueryAST and DataSet definition into dialect-specific PostgreSQL
// SELECT queries, stored procedures, or user-defined table functions (SETOF RECORD). It supports:
// 1. Projections, aliases, and table joins (INNER, LEFT, RIGHT, FULL OUTER).
// 2. Custom column calculations (arithmetic, string operations, date intervals, conditionals).
// 3. Aggregations (COUNT, SUM, AVG, MIN, MAX) with GROUP BY and HAVING clauses.
// 4. Dynamic runtime parameter interpolation, relative date macros ("CD|+1|ED"), and stored DDL generation.
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
//
// Purpose:
// Instantiates a PostgresDataSetCompiler capable of translating an abstract QueryAST into PostgreSQL SQL.
//
// Where it is used:
// Used in PostgresAdapter.CompileDataSet, PostgresAdapter.DataSetCompiler, and can be used directly
// in test suites or engine service registrations.
//
// When can it be used:
// Can be used during application initialization or adapter registration to provide PostgreSQL compilation support.
//
// Parameters:
//   - None.
//
// Returns:
//   - result1 (*PostgresDataSetCompiler): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by NewPostgresDataSetCompiler and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - This function does not return an error.
//
// Note:
//   - Callers should preserve the documented ownership and lifecycle of supplied values.
func NewPostgresDataSetCompiler() *PostgresDataSetCompiler {
	return &PostgresDataSetCompiler{}
}

// Compile compiles the QueryAST into PostgreSQL SQL and stored DDL.
//
// Purpose:
// Compiles a database-agnostic QueryAST into an executable PostgreSQL SQL query, a reference parameterized pipeline,
// and a routine DDL statement (procedure or function) depending on the dataset's SaveMode.
//
// Where it is used:
// Invoked by DataSetService.Preview, DataSetService.Save, and PostgresAdapter.CompileDataSet.
//
// When can it be used:
// Can be used whenever a DataSet definition has been validated and planned by the DataSetPlanner and needs to be executed
// or persisted in a PostgreSQL database.
//
// Parameters:
//   - ctx (context.Context): Input value used by the operation.
//   - ast (*planner.QueryAST): Input value used by the operation.
//   - ds (*domain.DataSet): Input value used by the operation.
//
// Returns:
//   - result1 (*compiler.CompiledPipeline): Returned value.
//   - result2 (error): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by Compile and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - Returns validation, configuration, context, or underlying dependency errors.
//
// Note:
//   - The context controls cancellation and request lifetime.
func (c *PostgresDataSetCompiler) Compile(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error) {
	if ast == nil {
		return nil, domain.NewError(domain.ErrPipelineCompilationFailed, "cannot compile nil AST")
	}

	saveMode := ds.SaveMode
	if saveMode == "" {
		saveMode = domain.SaveModeQuery
	}

	enrichASTJoinTypes(ast, ds)

	execQuery := c.buildSelectSQL(ast, false, false)
	refQuery := c.buildSelectSQL(ast, true, false)
	routineQuery := c.buildSelectSQL(ast, false, true)
	if saveMode != domain.SaveModeQuery {
		refQuery = routineQuery
	}
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

// buildSelectSQL constructs the PostgreSQL SELECT statement.
//
// Purpose:
// Builds the complete PostgreSQL SQL query string including SELECT projections, calculations, JOIN clauses,
// ON filters, WHERE filters, GROUP BY groupings, and HAVING clauses.
//
// Where it is used:
// Called internally by Compile to generate executable queries, parameter-templated pipelines, and routine bodies.
//
// When can it be used:
// Can be used during compilation whenever a QueryAST must be serialized into a PostgreSQL SELECT statement.
func (c *PostgresDataSetCompiler) buildSelectSQL(ast *planner.QueryAST, parameterized, isRoutine bool) string {
	var selectCols []string

	// 1. Projections
	for _, p := range ast.Projections {
		isNestedRelation := false
		for _, j := range ast.Joins {
			if strings.EqualFold(j.Alias, p.SourceTable) {
				if strings.EqualFold(j.OutputFormat, "NONE") || strings.EqualFold(j.Cardinality, "NONE") {
					continue
				}
				if strings.EqualFold(j.Cardinality, "MANY") || strings.EqualFold(j.OutputFormat, "JSON_ARRAY") || strings.EqualFold(j.OutputFormat, "OBJECT") {
					isNestedRelation = true
					break
				}
			}
		}
		if isNestedRelation {
			continue
		}
		colExpr := fmt.Sprintf("\"%s\".\"%s\"", p.SourceTable, p.SourceField)
		if p.Alias != "" && p.Alias != p.SourceField {
			colExpr += fmt.Sprintf(" AS \"%s\"", p.Alias)
		}
		selectCols = append(selectCols, colExpr)
	}

	// Project relations according to the requested JSON shape.
	for _, j := range ast.Joins {
		if strings.EqualFold(j.OutputFormat, "NONE") || strings.EqualFold(j.Cardinality, "NONE") {
			continue
		}
		relationFields := relationProjections(ast, j)
		if strings.EqualFold(j.OutputFormat, "OBJECT") && !strings.EqualFold(j.Cardinality, "MANY") {
			objectExpr := postgresJSONObject(j.Alias, relationFields)
			selectCols = append(selectCols, fmt.Sprintf(`CASE WHEN "%s"."%s" IS NULL THEN NULL ELSE %s END AS "%s"`, j.Alias, j.ToField, objectExpr, j.Alias))
			continue
		}
		if !strings.EqualFold(j.Cardinality, "MANY") && !strings.EqualFold(j.OutputFormat, "JSON_ARRAY") {
			continue
		}
		targetTbl := fmt.Sprintf(`"%s"`, j.ToTable)
		if j.Schema != "" && j.Schema != "public" {
			targetTbl = fmt.Sprintf(`"%s"."%s"`, j.Schema, j.ToTable)
		}
		itemAlias := j.Alias + "_many"
		whereCondition := buildPostgresJoinCondition(j, j.FromTable, itemAlias)
		for k, v := range j.JoinFilter {
			whereCondition += " AND " + formatFilterCondition(itemAlias, k, v)
		}
		itemExpr := postgresJSONObject(itemAlias, relationFields)
		selectCols = append(selectCols, fmt.Sprintf(`COALESCE((SELECT jsonb_agg(%s) FROM %s AS "%s" WHERE %s), '[]'::jsonb) AS "%s"`, itemExpr, targetTbl, itemAlias, whereCondition, j.Alias))
	}

	// 2. Custom Columns
	for _, cc := range ast.CustomColumns {
		// When query has GROUP BY, row-level calculations cannot be projected unless grouped
		if len(ast.GroupBy) > 0 && !cc.IsAggregate {
			inGroupBy := false
			for _, g := range ast.GroupBy {
				if strings.EqualFold(g.Field, cc.Alias) || strings.EqualFold(g.Field, cc.Label) {
					inGroupBy = true
					break
				}
			}
			if !inGroupBy {
				continue
			}
		}

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
		if strings.EqualFold(j.OutputFormat, "NONE") || strings.EqualFold(j.Cardinality, "NONE") {
			// keep standard JOIN for flat fields
		} else if strings.EqualFold(j.Cardinality, "MANY") || strings.EqualFold(j.OutputFormat, "JSON_ARRAY") {
			continue
		}
		jType := "LEFT JOIN"
		switch j.Type {
		case domain.JoinInner:
			jType = "INNER JOIN"
		case domain.JoinRight:
			jType = "RIGHT JOIN"
		case domain.JoinFull:
			jType = "FULL JOIN"
		}

		onCondition := buildPostgresJoinCondition(j, j.FromTable, j.Alias)

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
			if name, ok := datasetParamName(v); ok {
				whereClauses = append(whereClauses, formatDatasetParamCondition(ast.BaseTable.Alias, k, name, ast.Parameters, parameterized, isRoutine))
			} else {
				whereClauses = append(whereClauses, formatFilterCondition(ast.BaseTable.Alias, k, v))
			}
		}
	}

	for _, cond := range ast.WhereFilters {
		if cond.IsParamRef {
			whereClauses = append(whereClauses, formatDatasetParamCondition(cond.Table, cond.Column, cond.ParamName, ast.Parameters, parameterized, isRoutine))
		} else if cond.Value != nil {
			whereClauses = append(whereClauses, formatFilterCondition(cond.Table, cond.Column, cond.Value))
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

// relationProjections returns fields selected from a specific joined relation.
func relationProjections(ast *planner.QueryAST, join planner.ASTJoin) []planner.ASTProjection {
	var fields []planner.ASTProjection
	for _, projection := range ast.Projections {
		if strings.EqualFold(projection.SourceTable, join.Alias) {
			fields = append(fields, projection)
		}
	}
	return fields
}

// relationProjectionKey returns the JSON key for a selected relation field.
func relationProjectionKey(join planner.ASTJoin, projection planner.ASTProjection) string {
	if projection.Alias == "" || projection.Alias == projection.SourceField || strings.EqualFold(projection.Alias, join.Alias+"_"+projection.SourceField) {
		return projection.SourceField
	}
	return projection.Alias
}

// postgresJSONObject builds a JSON object containing only selected relation fields.
func postgresJSONObject(tableAlias string, fields []planner.ASTProjection) string {
	if len(fields) == 0 {
		return `'{}'::jsonb`
	}
	args := make([]string, 0, len(fields)*2)
	join := planner.ASTJoin{Alias: strings.TrimSuffix(tableAlias, "_many")}
	for _, field := range fields {
		key := strings.ReplaceAll(relationProjectionKey(join, field), "'", "''")
		args = append(args, fmt.Sprintf("'%s'", key), fmt.Sprintf(`"%s"."%s"`, tableAlias, field.SourceField))
	}
	return "jsonb_build_object(" + strings.Join(args, ", ") + ")"
}

// PGStructureCategory classifies the 5 fundamental PostgreSQL structures.
type PGStructureCategory int

const (
	PGStructureScalar PGStructureCategory = iota
	PGStructureArray
	PGStructureJSONArray
	PGStructureJSONObject
	PGStructureJSONArrayOfObjects
)

// classifyPostgresStructure determines the structure category and base type for a PostgreSQL column.
func classifyPostgresStructure(dataType string) (PGStructureCategory, string) {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	if dt == "" {
		return PGStructureScalar, ""
	}

	// 1. JSON Array of Objects ([{}])
	if strings.Contains(dt, "OBJECT[]") || strings.Contains(dt, "ARRAY_OF_OBJECTS") ||
		strings.Contains(dt, "JSON_ARRAY_OF_OBJECTS") || dt == "[{}]" {
		return PGStructureJSONArrayOfObjects, "JSONB"
	}

	// 2. JSON Array of scalars (JSONB storing ["id1", "id2"])
	if dt == "JSON_ARRAY" || dt == "JSONB_ARRAY" || dt == "JSON[]" || dt == "JSONB[]" || dt == "[]" {
		return PGStructureJSONArray, "JSONB"
	}

	// 3. JSON Object (JSONB storing {"id": "..."})
	if dt == "JSON_OBJECT" || dt == "JSONB_OBJECT" || dt == "{}" || dt == "JSON" || dt == "JSONB" || dt == "CUSTOM_TYPE" {
		return PGStructureJSONObject, "JSONB"
	}

	// 4. Native Array (UUID[], TEXT[], INTEGER[], etc.)
	if strings.HasSuffix(dt, "[]") || strings.HasPrefix(dt, "_") || dt == "ARRAY" {
		base := strings.TrimSuffix(dt, "[]")
		base = strings.TrimPrefix(base, "_")
		if base == "" || base == "ARRAY" {
			base = "TEXT"
		}
		return PGStructureArray, base
	}

	// 5. Scalar
	return PGStructureScalar, dt
}

// isTextType returns true if data type is textual.
func isTextType(t string) bool {
	upper := strings.ToUpper(strings.TrimSpace(t))
	return upper == "TEXT" || upper == "STRING" || upper == "VARCHAR" || upper == "CHAR"
}

// isIntegerType returns true if data type is integer.
func isIntegerType(t string) bool {
	upper := strings.ToUpper(strings.TrimSpace(t))
	return upper == "INT" || upper == "INTEGER" || upper == "BIGINT" || upper == "SMALLINT" || upper == "SERIAL" || upper == "BIGSERIAL"
}

// enrichASTJoinTypes introspects selected list and projections to populate join column data types.
func enrichASTJoinTypes(ast *planner.QueryAST, ds *domain.DataSet) {
	if ast == nil {
		return
	}
	for i := range ast.Joins {
		j := &ast.Joins[i]
		if j.FromFieldDataType == "" {
			j.FromFieldDataType = lookupFieldDataType(j.FromTable, j.FromField, ast, ds)
		}
		if j.ToFieldDataType == "" {
			j.ToFieldDataType = lookupFieldDataType(j.ToTable, j.ToField, ast, ds)
			if j.ToFieldDataType == "" {
				j.ToFieldDataType = lookupFieldDataType(j.Alias, j.ToField, ast, ds)
			}
		}
	}
}

func lookupFieldDataType(table, field string, ast *planner.QueryAST, ds *domain.DataSet) string {
	tblClean := strings.ToLower(table)
	if idx := strings.Index(tblClean, "."); idx >= 0 {
		tblClean = tblClean[idx+1:]
	}
	fldLower := strings.ToLower(field)

	// Check SelectedList
	if ds != nil {
		for _, s := range ds.SelectedList {
			sField := strings.ToLower(s.Field)
			if s.DataType != "" {
				if sField == tblClean+"."+fldLower || sField == fldLower || strings.EqualFold(s.HeaderName, field) {
					return s.DataType
				}
			}
		}
	}

	// Check Projections
	if ast != nil {
		for _, p := range ast.Projections {
			pTable := strings.ToLower(p.SourceTable)
			if idx := strings.Index(pTable, "."); idx >= 0 {
				pTable = pTable[idx+1:]
			}
			if (pTable == tblClean || pTable == strings.ToLower(table)) && strings.EqualFold(p.SourceField, field) {
				if p.DataType != "" {
					return p.DataType
				}
			}
		}
	}

	// Heuristics
	if fldLower == "id" || strings.HasSuffix(fldLower, "_id") {
		return "UUID"
	}
	if strings.Contains(fldLower, "array") || strings.Contains(fldLower, "plant_mine") || strings.Contains(fldLower, "locations") || strings.Contains(fldLower, "days") {
		return "ARRAY"
	}
	if strings.HasSuffix(fldLower, "_at") || strings.HasSuffix(fldLower, "_date") || strings.Contains(fldLower, "date") {
		return "DATE"
	}
	if strings.HasPrefix(fldLower, "is_") || strings.HasPrefix(fldLower, "has_") {
		return "BOOLEAN"
	}

	return ""
}

// buildPostgresJoinCondition generates the type-aware SQL join condition.
func buildPostgresJoinCondition(j planner.ASTJoin, fromAlias, toAlias string) string {
	fromCol := fmt.Sprintf(`"%s"."%s"`, fromAlias, j.FromField)
	toCol := fmt.Sprintf(`"%s"."%s"`, toAlias, j.ToField)

	fromCat, fromBase := classifyPostgresStructure(j.FromFieldDataType)
	toCat, toBase := classifyPostgresStructure(j.ToFieldDataType)

	// If explicit relationship operator is specified, honor it:
	if op := strings.TrimSpace(j.RelationshipOp); op != "" {
		return resolveExplicitOperatorCondition(op, fromCol, toCol, fromCat, toCat, fromBase, toBase)
	}

	castMode := strings.ToUpper(strings.TrimSpace(j.CastMode))

	fromIsArray := fromCat == PGStructureArray || fromCat == PGStructureJSONArray || fromCat == PGStructureJSONArrayOfObjects || castMode == "ANY_FROM" || castMode == "ANY_BOTH"
	toIsArray := toCat == PGStructureArray || toCat == PGStructureJSONArray || toCat == PGStructureJSONArrayOfObjects || castMode == "ANY_TO" || castMode == "ANY_BOTH"

	// Auto-correct inverted ANY_TO when source is array or target is scalar PK "id"
	if castMode == "ANY_TO" && (fromCat == PGStructureArray || strings.EqualFold(j.ToField, "id") || strings.Contains(strings.ToLower(j.FromField), "array") || strings.Contains(strings.ToLower(j.FromField), "plant_mine")) {
		fromIsArray = true
		toIsArray = false
		castMode = "ANY_FROM"
	}

	// 1. Array Overlap (Both are arrays): a && b
	if (fromIsArray && toIsArray) || castMode == "ANY_BOTH" {
		if fromBase != "" && fromBase == toBase && fromBase != "ARRAY" {
			return fmt.Sprintf("%s && %s", fromCol, toCol)
		}
		return fmt.Sprintf("CAST(%s AS TEXT[]) && CAST(%s AS TEXT[])", fromCol, toCol)
	}

	// 2. Source is Array, Target is Scalar: b = ANY(a)
	if fromIsArray && !toIsArray {
		if fromCat == PGStructureJSONArray {
			return fmt.Sprintf("%s ? CAST(%s AS TEXT)", fromCol, toCol)
		}
		if fromCat == PGStructureJSONArrayOfObjects {
			return fmt.Sprintf("EXISTS (SELECT 1 FROM jsonb_array_elements(%s) AS elem WHERE elem->>'id' = CAST(%s AS TEXT))", fromCol, toCol)
		}
		if !j.ConvertString && castMode == "" && fromBase != "" && fromBase == toBase && fromBase != "ARRAY" {
			return fmt.Sprintf("%s = ANY(%s)", toCol, fromCol)
		}
		return fmt.Sprintf("CAST(%s AS TEXT) = ANY(CAST(%s AS TEXT[]))", toCol, fromCol)
	}

	// 3. Target is Array, Source is Scalar: a = ANY(b)
	if toIsArray && !fromIsArray {
		if toCat == PGStructureJSONArray {
			return fmt.Sprintf("%s ? CAST(%s AS TEXT)", toCol, fromCol)
		}
		if toCat == PGStructureJSONArrayOfObjects {
			return fmt.Sprintf("EXISTS (SELECT 1 FROM jsonb_array_elements(%s) AS elem WHERE elem->>'id' = CAST(%s AS TEXT))", toCol, fromCol)
		}
		if !j.ConvertString && castMode == "" && toBase != "" && toBase == fromBase && toBase != "ARRAY" {
			return fmt.Sprintf("%s = ANY(%s)", fromCol, toCol)
		}
		return fmt.Sprintf("CAST(%s AS TEXT) = ANY(CAST(%s AS TEXT[]))", fromCol, toCol)
	}

	// 4. JSON Object relationships: (a->>'id')::UUID = b
	if fromCat == PGStructureJSONObject && toCat == PGStructureScalar {
		if toBase == "UUID" {
			return fmt.Sprintf("(%s->>'id')::UUID = %s", fromCol, toCol)
		}
		return fmt.Sprintf("(%s->>'id') = CAST(%s AS TEXT)", fromCol, toCol)
	}
	if toCat == PGStructureJSONObject && fromCat == PGStructureScalar {
		if fromBase == "UUID" {
			return fmt.Sprintf("(%s->>'id')::UUID = %s", toCol, fromCol)
		}
		return fmt.Sprintf("(%s->>'id') = CAST(%s AS TEXT)", toCol, fromCol)
	}

	// 5. Scalar vs Scalar
	if castMode == "FROM_ONLY" {
		return fmt.Sprintf("CAST(%s AS TEXT) = %s", fromCol, toCol)
	}
	if castMode == "TO_ONLY" {
		return fmt.Sprintf("%s = CAST(%s AS TEXT)", fromCol, toCol)
	}

	// Clean type matching without redundant casts when types are compatible
	if fromBase != "" && toBase != "" {
		if fromBase == toBase {
			return fmt.Sprintf("%s = %s", fromCol, toCol)
		}
		if isIntegerType(fromBase) && isIntegerType(toBase) {
			return fmt.Sprintf("%s = %s", fromCol, toCol)
		}
		if fromBase == "UUID" && isTextType(toBase) {
			return fmt.Sprintf("CAST(%s AS TEXT) = %s", fromCol, toCol)
		}
		if isTextType(fromBase) && toBase == "UUID" {
			return fmt.Sprintf("%s = CAST(%s AS TEXT)", fromCol, toCol)
		}
	}

	if j.ConvertString || castMode == "BOTH" {
		return fmt.Sprintf("CAST(%s AS TEXT) = CAST(%s AS TEXT)", fromCol, toCol)
	}

	return fmt.Sprintf("%s = %s", fromCol, toCol)
}

func resolveExplicitOperatorCondition(op, fromCol, toCol string, fromCat, toCat PGStructureCategory, fromBase, toBase string) string {
	cleanOp := strings.ToUpper(strings.TrimSpace(op))
	switch cleanOp {
	case "=", "EQ", "EQUAL":
		if fromBase != "" && toBase != "" && fromBase != toBase {
			if fromBase == "UUID" && isTextType(toBase) {
				return fmt.Sprintf("CAST(%s AS TEXT) = %s", fromCol, toCol)
			}
			if isTextType(fromBase) && toBase == "UUID" {
				return fmt.Sprintf("%s = CAST(%s AS TEXT)", fromCol, toCol)
			}
			return fmt.Sprintf("CAST(%s AS TEXT) = CAST(%s AS TEXT)", fromCol, toCol)
		}
		return fmt.Sprintf("%s = %s", fromCol, toCol)

	case "<>", "!=", "NEQ", "NOT_EQUAL":
		return fmt.Sprintf("%s <> %s", fromCol, toCol)

	case "ANY", "= ANY":
		if fromCat == PGStructureArray || fromCat == PGStructureJSONArray {
			if fromBase != "" && fromBase == toBase && fromBase != "ARRAY" {
				return fmt.Sprintf("%s = ANY(%s)", toCol, fromCol)
			}
			return fmt.Sprintf("CAST(%s AS TEXT) = ANY(CAST(%s AS TEXT[]))", toCol, fromCol)
		}
		if toBase != "" && toBase == fromBase && toBase != "ARRAY" {
			return fmt.Sprintf("%s = ANY(%s)", fromCol, toCol)
		}
		return fmt.Sprintf("CAST(%s AS TEXT) = ANY(CAST(%s AS TEXT[]))", fromCol, toCol)

	case "ALL", "= ALL":
		if fromCat == PGStructureArray {
			return fmt.Sprintf("%s = ALL(%s)", toCol, fromCol)
		}
		return fmt.Sprintf("%s = ALL(%s)", fromCol, toCol)

	case "&&", "OVERLAP", "OVERLAPS":
		if fromBase != "" && fromBase == toBase && fromBase != "ARRAY" {
			return fmt.Sprintf("%s && %s", fromCol, toCol)
		}
		return fmt.Sprintf("CAST(%s AS TEXT[]) && CAST(%s AS TEXT[])", fromCol, toCol)

	case "@>", "CONTAINS":
		return fmt.Sprintf("%s @> %s", fromCol, toCol)

	case "<@", "CONTAINED_BY":
		return fmt.Sprintf("%s <@ %s", fromCol, toCol)

	case "?":
		if toCat == PGStructureJSONArray || toCat == PGStructureJSONObject {
			return fmt.Sprintf("%s ? CAST(%s AS TEXT)", toCol, fromCol)
		}
		return fmt.Sprintf("%s ? CAST(%s AS TEXT)", fromCol, toCol)

	case "?|":
		return fmt.Sprintf("%s ?| CAST(%s AS TEXT[])", fromCol, toCol)

	case "?&":
		return fmt.Sprintf("%s ?& CAST(%s AS TEXT[])", fromCol, toCol)

	case "EXISTS":
		if fromCat == PGStructureJSONArrayOfObjects {
			return fmt.Sprintf("EXISTS (SELECT 1 FROM jsonb_array_elements(%s) AS elem WHERE elem->>'id' = CAST(%s AS TEXT))", fromCol, toCol)
		}
		return fmt.Sprintf("EXISTS (SELECT 1 FROM jsonb_array_elements(%s) AS elem WHERE elem->>'id' = CAST(%s AS TEXT))", toCol, fromCol)

	case "IS NOT DISTINCT FROM":
		return fmt.Sprintf("%s IS NOT DISTINCT FROM %s", fromCol, toCol)

	default:
		return fmt.Sprintf("%s %s %s", fromCol, op, toCol)
	}
}

func renderPostgresFunctionExpression(template string, operands []planner.ASTOperand) string {
	expr := template
	var allArgs []string
	for i, op := range operands {
		opSQL := formatPostgresOperand(op)
		expr = strings.ReplaceAll(expr, fmt.Sprintf("{{%d}}", i), opSQL)
		allArgs = append(allArgs, opSQL)
	}
	expr = strings.ReplaceAll(expr, "{{args}}", strings.Join(allArgs, ", "))
	if strings.Contains(expr, "{{") {
		expr = strings.ReplaceAll(expr, ", {{1}}", "")
		expr = strings.ReplaceAll(expr, ", {{2}}", "")
	}
	return expr
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
	case "COUNT_IF":
		return fmt.Sprintf("COUNT(CASE WHEN %s THEN 1 END)", first)
	case "SUM_IF":
		if len(operands) >= 2 {
			return fmt.Sprintf("SUM(CASE WHEN %s THEN %s ELSE 0 END)", first, formatPostgresOperand(operands[1]))
		}
		return fmt.Sprintf("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", first)
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
	case "MODULO", "MOD":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("MOD(%s, %s)", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "POWER", "POW":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("POWER(%s, %s)", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "ROUND":
		if len(operands) >= 2 {
			return fmt.Sprintf("ROUND(%s, %s)", first, formatPostgresOperand(operands[1]))
		}
		return fmt.Sprintf("ROUND(%s)", first)
	case "CEIL", "CEILING":
		return fmt.Sprintf("CEIL(%s)", first)
	case "FLOOR":
		return fmt.Sprintf("FLOOR(%s)", first)
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
	case "SUBSTRING":
		args := formatPostgresOperands(operands)
		if len(args) < 2 {
			return ""
		}
		return fmt.Sprintf("SUBSTRING(%s)", strings.Join(args, ", "))
	case "REPLACE":
		args := formatPostgresOperands(operands)
		if len(args) < 3 {
			return ""
		}
		return fmt.Sprintf("REPLACE(%s, %s, %s)", args[0], args[1], args[2])
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
	case "DATE_ADD":
		if len(operands) < 2 {
			return ""
		}
		unit := "days"
		if len(operands) >= 3 {
			unit = strings.Trim(formatPostgresOperand(operands[2]), "'\"")
		}
		return fmt.Sprintf("(%s + (%s || ' %s')::interval)", first, formatPostgresOperand(operands[1]), unit)
	case "DATE_DIFF", "DATEDIFF":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("(%s::date - %s::date)", first, formatPostgresOperand(operands[1]))
	case "EQUAL":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("(%s = %s)", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "NOT_EQUAL":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("(%s != %s)", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "GREATER_THAN":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("(%s > %s)", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "LESS_THAN":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("(%s < %s)", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "COALESCE":
		args := formatPostgresOperands(operands)
		return fmt.Sprintf("COALESCE(%s)", strings.Join(args, ", "))
	case "CASE", "IF":
		if len(operands) >= 3 {
			return fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]), formatPostgresOperand(operands[2]))
		} else if len(operands) == 2 {
			return fmt.Sprintf("CASE WHEN %s THEN %s END", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
		}
		return ""
	case "TO_STRING":
		return fmt.Sprintf("CAST(%s AS TEXT)", first)
	case "TO_INTEGER":
		return fmt.Sprintf("CAST(%s AS INTEGER)", first)
	case "TO_DECIMAL":
		return fmt.Sprintf("CAST(%s AS NUMERIC)", first)
	case "PERCENTAGE":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("((%s / NULLIF(%s, 0)) * 100.0)", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "DISCOUNT":
		if len(operands) < 2 {
			return ""
		}
		return fmt.Sprintf("(%s - (%s * (%s / 100.0)))", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
	case "DISCOUNT_AMOUNT":
		if len(operands) >= 2 {
			return fmt.Sprintf("(%s * (%s / 100.0))", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
		}
		return first
	case "DISCOUNTED_PRICE":
		if len(operands) >= 2 {
			return fmt.Sprintf("(%s - (%s * (%s / 100.0)))", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]))
		}
		return first
	case "MARGIN":
		if len(operands) >= 2 {
			return fmt.Sprintf("(((%s - %s) * 100.0) / NULLIF(%s, 0))", formatPostgresOperand(operands[0]), formatPostgresOperand(operands[1]), formatPostgresOperand(operands[0]))
		}
		return first
	case "GREATEST":
		return fmt.Sprintf("GREATEST(%s)", strings.Join(formatPostgresOperands(operands), ", "))
	case "LEAST":
		return fmt.Sprintf("LEAST(%s)", strings.Join(formatPostgresOperands(operands), ", "))
	case "IS_NULL":
		return fmt.Sprintf("(%s IS NULL)", first)
	case "IS_NOT_NULL":
		return fmt.Sprintf("(%s IS NOT NULL)", first)
	default:
		return ""
	}
}

// buildPostgresBinaryExpression builds a binary arithmetic expression for PostgreSQL.
//
// Purpose:
// Wraps two operands and an operator in parentheses to form a valid SQL arithmetic expression.
//
// Where it is used:
// Used in buildPostgresFunctionExpression for ADD, SUBTRACT, MULTIPLY, and DIVIDE operations.
//
// When can it be used:
// Can be used whenever an infix binary operator must be compiled for two operands.
func buildPostgresBinaryExpression(operands []planner.ASTOperand, op string) string {
	if len(operands) < 2 {
		return ""
	}
	formatted := formatPostgresOperands(operands)
	return fmt.Sprintf("(%s)", strings.Join(formatted, fmt.Sprintf(" %s ", op)))
}

// formatPostgresOperands formats a slice of ASTOperands into SQL operand strings.
//
// Purpose:
// Converts multiple abstract operands into their PostgreSQL string representations.
//
// Where it is used:
// Used in buildPostgresFunctionExpression when formatting argument lists for functions like CONCAT, GREATEST, LEAST, etc.
//
// When can it be used:
// Can be used when a function takes a variadic list of operands.
func formatPostgresOperands(operands []planner.ASTOperand) []string {
	args := make([]string, 0, len(operands))
	for _, op := range operands {
		args = append(args, formatPostgresOperand(op))
	}
	return args
}

// formatPostgresOperand formats a single ASTOperand into an escaped PostgreSQL SQL expression.
//
// Purpose:
// Converts an operand into either a quoted column reference ("schema"."table"."col") or a properly typed SQL literal.
//
// Where it is used:
// Used across all expression builders in PostgreSQL compilation.
//
// When can it be used:
// Can be used whenever an operand (field reference, number, string, boolean) must be emitted into SQL.
func formatPostgresOperand(op planner.ASTOperand) string {
	if op.SourceTable == "" || op.SourceTable == "_LITERAL_" || op.SourceTable == "CALC" {
		valStr := fmt.Sprintf("%v", op.LiteralVal)
		if valStr == "" && op.SourceField != "" {
			valStr = op.SourceField
		}
		if op.IsLiteral || op.SourceTable == "_LITERAL_" {
			if isNumericString(valStr) || strings.HasPrefix(valStr, "'") || strings.EqualFold(valStr, "TRUE") || strings.EqualFold(valStr, "FALSE") || strings.EqualFold(valStr, "NULL") {
				return valStr
			}
			return fmt.Sprintf("'%s'", strings.ReplaceAll(valStr, "'", "''"))
		}
		return fmt.Sprintf("\"%s\"", op.SourceField)
	}
	return fmt.Sprintf("\"%s\".\"%s\"", op.SourceTable, op.SourceField)
}

// buildDDL generates PostgreSQL routine statements for PROCEDURE or FUNCTION save modes.
//
// Purpose:
// Generates CREATE OR REPLACE PROCEDURE or CREATE OR REPLACE FUNCTION DDL statements to persist dataset logic inside PostgreSQL.
//
// Where it is used:
// Used in Compile when SaveMode is SaveModeProcedure or SaveModeFunction.
//
// When can it be used:
// Can be used when saving a dataset definition that should be callable as a native stored routine.
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
			if defaultString, ok := p.DefaultValue.(string); !ok || (defaultString != "CD" && !strings.HasPrefix(defaultString, "CD|") && !strings.HasPrefix(defaultString, "KTON|")) {
				defaultClause = "DEFAULT " + formatDatasetDefault(p.DefaultValue, p.ParamDataType)
			}
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
LANGUAGE sql
AS $$
    %s
$$;`, baseSchema, baseSchema, cleanName, strings.Join(paramDefs, ", "), querySQL)
}

// CompileDataSet compiles QueryAST into PostgreSQL SQL.
//
// Purpose:
// Compiles a QueryAST and DataSet definition into a PostgreSQL-specific CompiledPipeline.
//
// Where it is used:
// Invoked directly on PostgresAdapter or by external callers requiring PostgreSQL SQL compilation.
//
// When can it be used:
// Can be used whenever an application holds a PostgresAdapter instance and needs to compile a dataset.
//
// Parameters:
//   - ctx (context.Context): Input value used by the operation.
//   - ast (*planner.QueryAST): Input value used by the operation.
//   - ds (*domain.DataSet): Input value used by the operation.
//
// Returns:
//   - result1 (*compiler.CompiledPipeline): Returned value.
//   - result2 (error): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by CompileDataSet and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - Returns validation, configuration, context, or underlying dependency errors.
//
// Note:
//   - The context controls cancellation and request lifetime.
func (a *PostgresAdapter) CompileDataSet(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error) {
	return NewPostgresDataSetCompiler().Compile(ctx, ast, ds)
}

// DataSetCompiler returns the adapter.DataSetCompiler instance.
//
// Purpose:
// Returns the generic adapter.DataSetCompiler interface wrapper for registering with DataSetService.
//
// Where it is used:
// In application bootstrap and dependency injection (e.g. service.RegisterCompiler("postgres", pgAdapter.DataSetCompiler())).
//
// When can it be used:
// Can be used when initializing the dataset engine and registering the PostgreSQL adapter compiler.
//
// Parameters:
//   - None.
//
// Returns:
//   - result1 (adapter.DataSetCompiler): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by DataSetCompiler and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - This function does not return an error.
//
// Note:
//   - Callers should preserve the documented ownership and lifecycle of supplied values.
func (a *PostgresAdapter) DataSetCompiler() adapter.DataSetCompiler {
	return &genericCompilerWrapper{c: NewPostgresDataSetCompiler()}
}

type genericCompilerWrapper struct {
	c compiler.DataSetCompiler
}

// Compile adapts generic untyped arguments to typed AST and DataSet compiler calls.
//
// Purpose:
// Unpacks generic `any` interface values into `*planner.QueryAST` and `*domain.DataSet` and delegates to the underlying compiler.
//
// Where it is used:
// Called dynamically by DataSetService.Preview and DataSetService.Save via the adapter.DataSetCompiler interface.
//
// When can it be used:
// Can be used whenever compiling across module boundaries where interface{} decoupling is employed.
//
// Parameters:
//   - ctx (context.Context): Input value used by the operation.
//   - ast (any): Input value used by the operation.
//   - ds (any): Input value used by the operation.
//
// Returns:
//   - result1 (any): Returned value.
//   - result2 (error): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by Compile and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - Returns validation, configuration, context, or underlying dependency errors.
//
// Note:
//   - The context controls cancellation and request lifetime.
func (w *genericCompilerWrapper) Compile(ctx context.Context, ast any, ds any) (any, error) {
	qAst, _ := ast.(*planner.QueryAST)
	dSet, _ := ds.(*domain.DataSet)
	return w.c.Compile(ctx, qAst, dSet)
}

// formatFilterCondition translates structured filter conditions into PostgreSQL WHERE clauses.
//
// Purpose:
// Translates a column condition (supporting operators $gt, $gte, $lt, $lte, $ne, $regex, $like, $in) into PostgreSQL SQL.
//
// Where it is used:
// In buildSelectSQL for base collection filters and table join ON filters.
//
// When can it be used:
// Can be used whenever compiling filter conditions into PostgreSQL SQL statements.
func datasetParamName(value any) (string, bool) {
	if raw, ok := value.(string); ok && strings.HasPrefix(raw, ":") && len(raw) > 1 {
		return raw[1:], true
	}
	if object, ok := value.(map[string]any); ok {
		name, ok := object["paramName"].(string)
		return name, ok && name != ""
	}
	return "", false
}

func formatDatasetParamCondition(table, column, name string, params []domain.FilterParam, reference, routine bool) string {
	var field string
	if table != "" {
		field = fmt.Sprintf("\"%s\".\"%s\"", table, column)
	} else {
		field = fmt.Sprintf("\"%s\"", column)
	}
	if routine {
		return fmt.Sprintf("%s = p_%s", field, name)
	}
	if reference {
		dataType := "string"
		for _, param := range params {
			if strings.EqualFold(param.ParamName, name) {
				dataType = param.ParamDataType
				break
			}
		}
		token := fmt.Sprintf(`{"paramName":"%s","paramDataType":"%s"}`, name, dataType)
		return fmt.Sprintf("%s = %s", field, token)
	}
	for _, param := range params {
		if strings.EqualFold(param.ParamName, name) {
			value := param.Paramvalue
			if value == nil || value == "" {
				value = param.DefaultValue
			}
			if value != nil {
				return fmt.Sprintf("%s = %s", field, formatDatasetDefault(value, param.ParamDataType))
			}
			return "TRUE"
		}
	}
	return "TRUE"
}

func formatDatasetDefault(value any, dataType string) string {
	if text, ok := value.(string); ok {
		if expression, valid := parseCustomDateMacro(text); valid {
			return expression
		}
		switch strings.ToLower(dataType) {
		case "int", "integer", "decimal", "numeric", "float", "bool", "boolean":
			return formatSQLVal(value)
		default:
			return "'" + strings.ReplaceAll(text, "'", "''") + "'"
		}
	}
	return formatSQLVal(value)
}

func formatFilterCondition(table, col string, val any) string {
	targetTable := table
	targetCol := col
	if idx := strings.Index(col, "."); idx >= 0 {
		targetTable = col[:idx]
		targetCol = col[idx+1:]
	}

	var colExpr string
	if targetTable != "" {
		colExpr = fmt.Sprintf("\"%s\".\"%s\"", targetTable, targetCol)
	} else {
		colExpr = fmt.Sprintf("\"%s\"", targetCol)
	}

	if m, ok := val.(map[string]any); ok {
		var parts []string
		for op, operand := range m {
			switch op {
			case "$gt":
				parts = append(parts, fmt.Sprintf("%s > %s", colExpr, formatSQLVal(operand)))
			case "$gte":
				parts = append(parts, fmt.Sprintf("%s >= %s", colExpr, formatSQLVal(operand)))
			case "$lt":
				parts = append(parts, fmt.Sprintf("%s < %s", colExpr, formatSQLVal(operand)))
			case "$lte":
				parts = append(parts, fmt.Sprintf("%s <= %s", colExpr, formatSQLVal(operand)))
			case "$ne":
				parts = append(parts, fmt.Sprintf("%s <> %s", colExpr, formatSQLVal(operand)))
			case "$eq", "$equals":
				parts = append(parts, fmt.Sprintf("%s = %s", colExpr, formatSQLVal(operand)))
			case "$regex", "$like":
				parts = append(parts, fmt.Sprintf("%s ILIKE '%%%v%%'", colExpr, operand))
			case "$in":
				parts = append(parts, fmt.Sprintf("%s IN (%s)", colExpr, formatSQLIn(operand)))
			case "$nin", "$notin", "$not_in":
				parts = append(parts, fmt.Sprintf("%s NOT IN (%s)", colExpr, formatSQLIn(operand)))
			default:
				parts = append(parts, fmt.Sprintf("%s = %s", colExpr, formatSQLVal(operand)))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, " AND ")
		}
	}
	if val == nil {
		return fmt.Sprintf("%s IS NULL", colExpr)
	}
	return fmt.Sprintf("%s = %s", colExpr, formatSQLVal(val))
}

// formatSQLVal formats a Go value into a valid PostgreSQL SQL literal or expression.
//
// Purpose:
// Converts numbers, booleans, strings, parameter placeholders, and date macros into properly formatted SQL tokens.
//
// Where it is used:
// In formatFilterCondition and formatSQLIn.
//
// When can it be used:
// Can be used whenever formatting literal values or macros into SQL text.
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
//
// Purpose:
// Parses dynamic date macros such as C[-7d], C[+1m], C[0d], C[today], and C[now] into native PostgreSQL interval expressions.
//
// Where it is used:
// In formatSQLVal when encountering dynamic date strings in filters.
//
// When can it be used:
// Can be used whenever date offset expressions need to be converted to SQL intervals.
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

// formatSQLIn formats slice elements into a comma-separated SQL list for IN clauses.
//
// Purpose:
// Converts a slice of values into a comma-separated string of formatted SQL values.
//
// Where it is used:
// In formatFilterCondition when processing $in operators.
//
// When can it be used:
// Can be used whenever an array or slice must be rendered inside SQL IN (...).
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

// isNumericString determines whether a string can be parsed as a valid numeric literal.
//
// Purpose:
// Tests whether a string represents a valid integer or floating-point number.
//
// Where it is used:
// In formatSQLVal and formatPostgresOperand to determine whether to emit quotes around the value.
//
// When can it be used:
// Can be used whenever determining if a string value should be treated as a number or string literal in SQL.
func isNumericString(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}
