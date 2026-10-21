package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/SanjayDrop5528/models-go-engine/dataset/planner"
	"github.com/SanjayDrop5528/models-go-engine/model"
)

// verifyDatasetJoins is used by the live adapter's existing preview/save path.
// The AST has already resolved model fields; database metadata supplies physical
// types and enforced uniqueness, overriding client hints and logical ARRAY types.
func verifyDatasetJoins(ctx context.Context, db JoinMetadataReader, ast *planner.QueryAST) error {
	catalog := JoinCatalog{}
	hiddenAliases := map[string]bool{}
	addModel := func(alias, schema, table string) error {
		if _, exists := catalog[alias]; exists {
			return fmt.Errorf("duplicate dataset alias %q", alias)
		}
		if schema == "" {
			schema = "public"
		}
		catalog[alias] = JoinModel{Config: &model.ModelConfig{ID: alias, Schema: schema, Table: table, Status: model.ModelConfigStatusActive}}
		return nil
	}
	addField := func(alias, col string) error {
		m, ok := catalog[alias]
		if !ok {
			return fmt.Errorf("unknown source alias %q", alias)
		}
		for _, f := range m.Fields {
			if f.ID == col {
				return nil
			}
		}
		m.Fields = append(m.Fields, &model.DataModel{ID: col, ModelID: alias, ColumnName: col, Status: model.DataModelStatusActive})
		catalog[alias] = m
		return nil
	}
	if err := addModel(ast.BaseTable.Alias, ast.BaseTable.Schema, ast.BaseTable.Table); err != nil {
		return err
	}
	for _, j := range ast.Joins {
		if hiddenAliases[j.FromTable] {
			return fmt.Errorf("join %q: cannot join from an aggregated relationship alias %q", j.Alias, j.FromTable)
		}
		if err := addModel(j.Alias, j.Schema, j.ToTable); err != nil {
			return err
		}
		if err := addField(j.FromTable, j.FromField); err != nil {
			return err
		}
		if err := addField(j.Alias, j.ToField); err != nil {
			return err
		}
		hiddenAliases[j.Alias] = datasetArrayOutput(j)
	}
	verified, err := VerifyJoinCatalog(ctx, db, catalog)
	if err != nil {
		return fmt.Errorf("dataset JOIN metadata: %w", err)
	}
	b := &joinBuilder{catalog: verified, nullPolicy: "NO_MATCH"}
	for i := range ast.Joins {
		j := &ast.Joins[i]
		a, err := b.field(j.FromTable, j.FromField, j.FromTable)
		if err != nil {
			return err
		}
		z, err := b.field(j.Alias, j.ToField, j.Alias)
		if err != nil {
			return err
		}
		j.FromFieldDataType = a.metadata.PhysicalDataType
		j.ToFieldDataType = z.metadata.PhysicalDataType
		j.VerifiedTypes = true
		if _, err := verifiedDatasetJoinPredicate(*j, j.FromTable, j.Alias); err != nil {
			return fmt.Errorf("join %q: %w", j.Alias, err)
		}
		card, out := strings.ToUpper(j.Cardinality), strings.ToUpper(j.OutputFormat)
		if card != "" && card != "NONE" && card != "ONE" && card != "ZERO_OR_ONE" && card != "MANY" {
			return fmt.Errorf("join %q: unsupported cardinality %q", j.Alias, card)
		}
		if out != "" && out != "NONE" && out != "FLAT" && out != "OBJECT" && out != "JSON_ARRAY" {
			return fmt.Errorf("join %q: unsupported output format %q", j.Alias, out)
		}
		if out == "OBJECT" && card != "ONE" && card != "ZERO_OR_ONE" {
			return fmt.Errorf("join %q: OBJECT requires verified singular cardinality", j.Alias)
		}
		if card == "ONE" || card == "ZERO_OR_ONE" {
			op := strings.ToUpper(strings.TrimSpace(j.RelationshipOp))
			typesUnique := a.typ == z.typ || (a.typ == "TEXT" && (z.typ == "UUID" || isIntegerType(z.typ))) || (z.typ == "TEXT" && (a.typ == "UUID" || isIntegerType(a.typ)))
			if a.array || z.array || a.json || z.json || !typesUnique || (op != "" && op != "AUTO" && op != "EQ" && op != "=" && op != "EQ_CAST") || !b.unique(j.Alias, map[string]bool{j.ToField: true}) {
				return fmt.Errorf("join %q: %s requires a verified unique target field and scalar equality; configure MANY with JSON_ARRAY for multiple matches", j.Alias, card)
			}
		}
		switch string(j.Type) {
		case "", "LEFT", "INNER", "RIGHT", "FULL":
		default:
			return fmt.Errorf("join %q: unsupported join type %q", j.Alias, j.Type)
		}
		if (out == "JSON_ARRAY" || card == "MANY" && out != "FLAT" && out != "NONE") && j.Type != "LEFT" && j.Type != "" {
			return fmt.Errorf("join %q: JSON_ARRAY currently requires LEFT", j.Alias)
		}
	}
	return nil
}

func verifiedDatasetJoinPredicate(j planner.ASTJoin, fromAlias, toAlias string) (string, error) {
	at, aa, aj, err := normalizeJoinType(j.FromFieldDataType)
	if err != nil {
		return "", err
	}
	zt, za, zj, err := normalizeJoinType(j.ToFieldDataType)
	if err != nil {
		return "", err
	}
	a := joinField{expr: joinIdent(fromAlias) + "." + joinIdent(j.FromField), typ: at, array: aa, json: aj}
	z := joinField{expr: joinIdent(toAlias) + "." + joinIdent(j.ToField), typ: zt, array: za, json: zj}
	if aj && at == "JSON" {
		a.expr = "(" + a.expr + ")::jsonb"
	}
	if zj && zt == "JSON" {
		z.expr = "(" + z.expr + ")::jsonb"
	}
	policy := "STRICT"
	cast := strings.ToUpper(j.CastMode)
	if j.ConvertString || cast == "BOTH" || cast == "FROM_ONLY" || cast == "TO_ONLY" {
		policy = "TEXT"
	}
	op := strings.ToUpper(strings.TrimSpace(j.RelationshipOp))
	switch op {
	case "=":
		op = "EQ"
	case "ANY", "= ANY":
		op = "MEMBER_OF"
	case "&&":
		op = "OVERLAP"
	case "@>":
		op = "CONTAINS"
	case "<@":
		op = "CONTAINED_BY"
	case "?":
		op = "JSON_MEMBER"
	}
	if op == "" || op == "AUTO" {
		if policy == "TEXT" && !aj && !zj && at != zt && (at == "TEXT" || zt == "TEXT") {
			if aa != za {
				op = "MEMBER_OF_CAST"
			} else if !aa {
				op = "EQ_CAST"
			}
		}
	}
	return (&joinBuilder{policy: policy, nullPolicy: "NO_MATCH"}).predicate(a, z, op, j.SourceJSONPath, j.TargetJSONPath)
}
