package postgres

import (
	"fmt"
	"sort"
	"strings"

	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/model"
)

// JoinModel is trusted server-side catalog metadata, never request-supplied type
// hints. DataModel.PhysicalDataType must be populated from introspection; it also
// distinguishes native jsonb[] from a jsonb column containing a JSON array.
// VerifiedUniqueKeys contains field IDs from verified database constraints.
// Nullable unique keys do not prove uniqueness for NULL matching.
type JoinModel struct {
	Config             *model.ModelConfig
	Fields             []*model.DataModel
	VerifiedUniqueKeys [][]string
}

// JoinCatalog is keyed by ModelConfig.ID. CompileDynamicJoin checks ownership of
// every field against its ModelConfig before emitting any SQL.
type JoinCatalog map[string]JoinModel

type CompiledJoin struct {
	SQL  string
	Args []any
}

type joinField struct {
	expr, typ   string
	array, json bool
	metadata    *model.DataModel
}
type joinBuilder struct {
	catalog            JoinCatalog
	args               []any
	policy, nullPolicy string
}

func joinIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func joinLiteral(s string) string {
	return "E'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
}
func joinDefault(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return strings.ToUpper(strings.TrimSpace(s))
}

func (b *joinBuilder) model(id string) (JoinModel, error) {
	m, ok := b.catalog[id]
	if !ok || m.Config == nil || m.Config.ID != id || id == "" {
		return m, fmt.Errorf("unknown model ID %q", id)
	}
	if m.Config.Status != model.ModelConfigStatusActive || m.Config.Table == "" {
		return m, fmt.Errorf("model %q must be active with a physical table", id)
	}
	if strings.ContainsRune(m.Config.Table+m.Config.Schema, 0) {
		return m, fmt.Errorf("invalid model identifier")
	}
	return m, nil
}

func (b *joinBuilder) table(id, alias string) (string, error) {
	m, err := b.model(id)
	if err != nil {
		return "", err
	}
	t := joinIdent(m.Config.Table)
	if m.Config.Schema != "" {
		t = joinIdent(m.Config.Schema) + "." + t
	}
	return t + " AS " + joinIdent(alias), nil
}

// normalizeJoinType deliberately has no fallback to TEXT for unknown types.
func normalizeJoinType(raw string) (string, bool, bool, error) {
	t := strings.ToUpper(strings.TrimSpace(raw))
	array := strings.HasSuffix(t, "[]") || strings.HasPrefix(t, "_")
	if array {
		t = strings.TrimPrefix(strings.TrimSuffix(t, "[]"), "_")
	}
	if i := strings.IndexByte(t, '('); i >= 0 && strings.HasSuffix(t, ")") {
		t = strings.TrimSpace(t[:i])
	}
	switch t {
	case "VARCHAR", "CHARACTER VARYING", "TEXT":
		t = "TEXT"
	case "INT", "INT4", "INTEGER", "SERIAL":
		t = "INTEGER"
	case "INT8", "BIGINT", "BIGSERIAL":
		t = "BIGINT"
	case "INT2", "SMALLINT", "SMALLSERIAL":
		t = "SMALLINT"
	case "BOOL", "BOOLEAN":
		t = "BOOLEAN"
	case "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP":
		t = "TIMESTAMP"
	case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ":
		t = "TIMESTAMPTZ"
	case "DECIMAL", "NUMERIC":
		t = "NUMERIC"
	case "REAL", "FLOAT4":
		t = "REAL"
	case "DOUBLE PRECISION", "FLOAT8":
		t = "DOUBLE PRECISION"
	case "UUID", "DATE", "TIME", "BYTEA", "JSON", "JSONB":
	default:
		return "", false, false, fmt.Errorf("unsupported or unresolved physical type %q", raw)
	}
	return t, array, !array && (t == "JSON" || t == "JSONB"), nil
}

func (b *joinBuilder) field(mid, fid, alias string) (joinField, error) {
	m, err := b.model(mid)
	if err != nil {
		return joinField{}, err
	}
	var found *model.DataModel
	for _, f := range m.Fields {
		if f != nil && f.ID == fid {
			if found != nil {
				return joinField{}, fmt.Errorf("duplicate field ID %q", fid)
			}
			found = f
		}
	}
	if found == nil || fid == "" || found.ModelID != mid || found.Status != model.DataModelStatusActive || found.ColumnName == "" || found.IsEncrypted {
		return joinField{}, fmt.Errorf("field %q is not an active, unencrypted physical field of model %q", fid, mid)
	}
	if strings.ContainsRune(found.ColumnName, 0) {
		return joinField{}, fmt.Errorf("invalid field identifier")
	}
	t, a, j, err := normalizeJoinType(found.PhysicalDataType)
	if err != nil {
		return joinField{}, fmt.Errorf("field %q: %w", fid, err)
	}
	e := joinIdent(alias) + "." + joinIdent(found.ColumnName)
	if j && t == "JSON" {
		e = "(" + e + ")::jsonb"
	}
	return joinField{e, t, a, j, found}, nil
}

func numericJoinType(t string) bool {
	switch t {
	case "INTEGER", "BIGINT", "SMALLINT", "NUMERIC", "REAL", "DOUBLE PRECISION":
		return true
	}
	return false
}
func compatibleJoinTypes(a, b joinField) bool {
	return a.typ == b.typ || (!a.array && !b.array && numericJoinType(a.typ) && numericJoinType(b.typ))
}
func textJoinExpr(f joinField) (string, error) {
	if f.array || f.json {
		return "", fmt.Errorf("text comparison requires a scalar")
	}
	if f.typ == "TEXT" {
		return f.expr, nil
	}
	if f.typ == "UUID" || numericJoinType(f.typ) || f.typ == "DATE" || f.typ == "TIMESTAMP" || f.typ == "TIMESTAMPTZ" || f.typ == "BOOLEAN" {
		return f.expr + "::text", nil
	}
	return "", fmt.Errorf("unsupported text conversion of %s", f.typ)
}
func jsonJoinPath(expr string, path []string) (string, error) {
	if len(path) == 0 {
		return "", fmt.Errorf("JSON path must be explicitly configured")
	}
	keys := make([]string, len(path))
	for i, k := range path {
		if strings.ContainsRune(k, 0) {
			return "", fmt.Errorf("JSON path contains NUL")
		}
		keys[i] = joinLiteral(k)
	}
	if len(keys) == 1 {
		return "(" + expr + " ->> " + keys[0] + ")", nil
	}
	return "(" + expr + " #>> ARRAY[" + strings.Join(keys, ", ") + "]::text[])", nil
}
func guardedJSONArray(e string) string {
	return "CASE WHEN jsonb_typeof(" + e + ") = 'array' THEN " + e + " ELSE '[]'::jsonb END"
}

func (b *joinBuilder) predicate(a, c joinField, op string, ap, cp []string) (string, error) {
	op = joinDefault(op, "AUTO")
	if op == "AUTO" {
		if a.json || c.json || a.array && c.array {
			return "", fmt.Errorf("AUTO is ambiguous for JSON or two arrays; specify an operator")
		}
		if a.array != c.array {
			op = "MEMBER_OF"
		} else {
			op = "EQ"
		}
	}
	eq := " = "
	if b.nullPolicy == "MATCH" {
		eq = " IS NOT DISTINCT FROM "
	}
	bad := func() (string, error) {
		return "", fmt.Errorf("unsupported %s relationship between %s (array=%t) and %s (array=%t)", op, a.typ, a.array, c.typ, c.array)
	}
	if op != "JSON_PATH_EQ" && op != "JSON_ARRAY_MATCH" && (len(ap) > 0 || len(cp) > 0) {
		return "", fmt.Errorf("JSON paths require a path operator")
	}
	if b.nullPolicy == "MATCH" && op != "EQ" && op != "EQ_CAST" && op != "EQUAL" && op != "JSON_EQUAL" && op != "JSON_PATH_EQ" && op != "CASE_INSENSITIVE_EQ" {
		return "", fmt.Errorf("MATCH null policy is unsupported for %s", op)
	}
	switch op {
	case "EQ", "EQUAL":
		if a.json || c.json || a.array != c.array || !compatibleJoinTypes(a, c) || a.array && op != "EQUAL" || a.array && (a.typ == "JSON" || a.typ == "JSONB") {
			return bad()
		}
		return a.expr + eq + c.expr, nil
	case "EQ_CAST":
		if b.policy != "TEXT" {
			return "", fmt.Errorf("EQ_CAST requires conversion_policy TEXT")
		}
		if a.array || c.array || a.json || c.json || (a.typ != "TEXT" && c.typ != "TEXT") {
			return bad()
		}
		x, err := textJoinExpr(a)
		if err != nil {
			return "", err
		}
		y, err := textJoinExpr(c)
		if err != nil {
			return "", err
		}
		return x + eq + y, nil
	case "MEMBER_OF", "MEMBER_OF_CAST":
		if a.json || c.json || a.array == c.array {
			return bad()
		}
		arr, scalar := a, c
		if c.array {
			arr, scalar = c, a
		}
		if arr.typ == "JSON" || arr.typ == "JSONB" {
			return bad()
		}
		if op == "MEMBER_OF_CAST" {
			if b.policy != "TEXT" {
				return "", fmt.Errorf("MEMBER_OF_CAST requires conversion_policy TEXT")
			}
			if arr.typ != "TEXT" && scalar.typ != "TEXT" {
				return bad()
			}
			x, err := textJoinExpr(scalar)
			if err != nil {
				return "", err
			}
			scalar.expr = x
			if arr.typ != "TEXT" {
				item := arr
				item.array = false
				if _, err := textJoinExpr(item); err != nil {
					return "", err
				}
				arr.expr += "::text[]"
			}
		} else if arr.typ != scalar.typ {
			return bad()
		}
		return scalar.expr + " = ANY(" + arr.expr + ")", nil
	case "OVERLAP", "CONTAINS", "CONTAINED_BY":
		if !a.array || !c.array || a.typ != c.typ || a.typ == "JSON" || a.typ == "JSONB" {
			return bad()
		}
		symbol := map[string]string{"OVERLAP": " && ", "CONTAINS": " @> ", "CONTAINED_BY": " <@ "}[op]
		return a.expr + symbol + c.expr, nil
	case "JSON_PATH_EQ":
		var err error
		if a.json {
			a.expr, err = jsonJoinPath(a.expr, ap)
			a.json = false
			a.typ = "TEXT"
		} else if len(ap) > 0 {
			return bad()
		}
		if err != nil {
			return "", err
		}
		if c.json {
			c.expr, err = jsonJoinPath(c.expr, cp)
			c.json = false
			c.typ = "TEXT"
		} else if len(cp) > 0 {
			return bad()
		}
		if err != nil {
			return "", err
		}
		if len(ap) == 0 && len(cp) == 0 {
			return bad()
		}
		x, err := textJoinExpr(a)
		if err != nil {
			return "", err
		}
		y, err := textJoinExpr(c)
		if err != nil {
			return "", err
		}
		return x + eq + y, nil
	case "JSON_MEMBER", "JSON_ARRAY_MATCH":
		if !a.json || c.json || c.array {
			return bad()
		}
		y, err := textJoinExpr(c)
		if err != nil {
			return "", err
		}
		if op == "JSON_MEMBER" {
			return "(" + guardedJSONArray(a.expr) + ") ? " + y, nil
		}
		if len(cp) > 0 {
			return bad()
		}
		x, err := jsonJoinPath(`"element"."value"`, ap)
		if err != nil {
			return "", err
		}
		return "EXISTS (SELECT 1 FROM jsonb_array_elements(" + guardedJSONArray(a.expr) + ") AS \"element\"(\"value\") WHERE " + x + " = " + y + ")", nil
	case "JSON_EQUAL", "JSON_CONTAINS", "JSON_OVERLAP":
		if !a.json || !c.json {
			return bad()
		}
		if op == "JSON_EQUAL" {
			return a.expr + eq + c.expr, nil
		}
		if op == "JSON_CONTAINS" {
			return a.expr + " @> " + c.expr, nil
		}
		return "EXISTS (SELECT 1 FROM jsonb_array_elements(" + guardedJSONArray(a.expr) + ") AS x(value) JOIN LATERAL jsonb_array_elements(" + guardedJSONArray(c.expr) + ") AS y(value) ON x.value = y.value)", nil
	case "CASE_INSENSITIVE_EQ":
		if a.array || c.array || a.typ != "TEXT" || c.typ != "TEXT" {
			return bad()
		}
		return "lower(" + a.expr + ")" + eq + "lower(" + c.expr + ")", nil
	}
	return bad()
}

func (b *joinBuilder) condition(sid, tid string, c domain.RelationshipCondition, depth int) (string, error) {
	if depth > 32 {
		return "", fmt.Errorf("condition nesting exceeds 32")
	}
	if len(c.Conditions) > 0 {
		logic := joinDefault(c.Logic, "AND")
		if logic != "AND" && logic != "OR" {
			return "", fmt.Errorf("invalid condition logic %q", logic)
		}
		if c.SourceFieldID != "" || c.TargetFieldID != "" || c.Operator != "" || c.TargetEndFieldID != "" || len(c.SourceJSONPath) > 0 || len(c.TargetJSONPath) > 0 {
			return "", fmt.Errorf("condition groups cannot contain comparison fields")
		}
		parts := []string{}
		for _, child := range c.Conditions {
			p, err := b.condition(sid, tid, child, depth+1)
			if err != nil {
				return "", err
			}
			parts = append(parts, p)
		}
		return "(" + strings.Join(parts, " "+logic+" ") + ")", nil
	}
	if c.Logic != "" {
		return "", fmt.Errorf("condition logic requires children")
	}
	a, err := b.field(sid, c.SourceFieldID, "source")
	if err != nil {
		return "", err
	}
	z, err := b.field(tid, c.TargetFieldID, "target")
	if err != nil {
		return "", err
	}
	if strings.EqualFold(c.Operator, "BETWEEN") {
		end, err := b.field(tid, c.TargetEndFieldID, "target")
		if err != nil {
			return "", err
		}
		if a.array || z.array || end.array || a.json || z.json || end.json || !compatibleJoinTypes(a, z) || !compatibleJoinTypes(a, end) || len(c.SourceJSONPath) > 0 || len(c.TargetJSONPath) > 0 || b.nullPolicy == "MATCH" {
			return "", fmt.Errorf("incompatible BETWEEN fields or policies")
		}
		return a.expr + " BETWEEN " + z.expr + " AND " + end.expr, nil
	}
	if c.TargetEndFieldID != "" {
		return "", fmt.Errorf("end field requires BETWEEN")
	}
	return b.predicate(a, z, c.Operator, c.SourceJSONPath, c.TargetJSONPath)
}

func (b *joinBuilder) filters(mid string, filters []domain.RelationshipFilter) ([]string, error) {
	parts := []string{}
	for _, f := range filters {
		col, err := b.field(mid, f.FieldID, "target")
		if err != nil {
			return nil, err
		}
		op := joinDefault(f.Operator, "EQ")
		if op == "IS_NULL" || op == "IS_NOT_NULL" {
			if f.Value != nil {
				return nil, fmt.Errorf("NULL filters cannot have a value")
			}
			suffix := " IS NULL"
			if op == "IS_NOT_NULL" {
				suffix = " IS NOT NULL"
			}
			parts = append(parts, col.expr+suffix)
			continue
		}
		if op != "EQ" && op != "NE" {
			return nil, fmt.Errorf("unsupported target filter %q", op)
		}
		if f.Value == nil {
			return nil, fmt.Errorf("use IS_NULL or IS_NOT_NULL for null filters")
		}
		if col.array || col.json {
			return nil, fmt.Errorf("target value filters require scalar fields")
		}
		b.args = append(b.args, f.Value)
		symbol := " = "
		if op == "NE" {
			symbol = " <> "
		}
		parts = append(parts, fmt.Sprintf("%s%s$%d", col.expr, symbol, len(b.args)))
	}
	return parts, nil
}

func (b *joinBuilder) order(mid, alias string, order []domain.RelationshipOrder) (string, error) {
	parts := []string{}
	for _, o := range order {
		f, err := b.field(mid, o.FieldID, alias)
		if err != nil {
			return "", err
		}
		if f.array || f.json {
			return "", fmt.Errorf("ordering requires scalar fields")
		}
		d := " ASC"
		if o.Descending {
			d = " DESC"
		}
		parts = append(parts, f.expr+d)
	}
	return strings.Join(parts, ", "), nil
}

// unique verifies a complete, nonnullable database key covered by raw equality.
// Casts, paths, OR, membership, and case folding cannot prove singularity.
func (b *joinBuilder) unique(mid string, ids map[string]bool) bool {
	m, err := b.model(mid)
	if err != nil {
		return false
	}
	for _, key := range m.VerifiedUniqueKeys {
		ok := len(key) > 0
		for _, id := range key {
			f, e := b.field(mid, id, "target")
			if e != nil || !ids[id] || b.nullPolicy == "MATCH" && f.metadata.IsNullable {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func (b *joinBuilder) equalityKeys(sid, tid string, c domain.RelationshipCondition, ids map[string]bool) {
	if len(c.Conditions) > 0 {
		if joinDefault(c.Logic, "AND") == "AND" {
			for _, child := range c.Conditions {
				b.equalityKeys(sid, tid, child, ids)
			}
		}
		return
	}
	op := joinDefault(c.Operator, "AUTO")
	if op != "AUTO" && op != "EQ" {
		return
	}
	if len(c.SourceJSONPath) > 0 || len(c.TargetJSONPath) > 0 {
		return
	}
	a, e := b.field(sid, c.SourceFieldID, "source")
	z, f := b.field(tid, c.TargetFieldID, "target")
	if e == nil && f == nil && !a.array && !z.array && !a.json && !z.json && a.typ == z.typ {
		ids[c.TargetFieldID] = true
	}
}

// CompileDynamicJoin compiles one complete relationship query from a trusted
// metadata catalog. All request field references are IDs; SQL identifiers and
// JSON paths are quoted, and filter values are bound parameters.
func CompileDynamicJoin(catalog JoinCatalog, r domain.Relationship) (*CompiledJoin, error) {
	b := &joinBuilder{catalog: catalog, policy: joinDefault(r.ConversionPolicy, "STRICT"), nullPolicy: joinDefault(r.NullPolicy, "NO_MATCH")}
	if b.policy != "STRICT" && b.policy != "TEXT" {
		return nil, fmt.Errorf("unsupported conversion_policy %q", b.policy)
	}
	if b.nullPolicy != "NO_MATCH" && b.nullPolicy != "MATCH" {
		return nil, fmt.Errorf("unsupported null_policy %q", b.nullPolicy)
	}
	jt, card, out, dup := joinDefault(r.JoinType, "LEFT"), joinDefault(r.Cardinality, "MANY"), joinDefault(r.OutputFormat, "FLAT"), joinDefault(r.DuplicatePolicy, "DISTINCT")
	if dup != "DISTINCT" && dup != "ALL" {
		return nil, fmt.Errorf("unsupported duplicate_policy %q", dup)
	}
	switch jt {
	case "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "LATERAL", "SEMI", "ANTI":
	default:
		return nil, fmt.Errorf("unsupported join_type %q", jt)
	}
	switch card {
	case "ONE", "ZERO_OR_ONE", "MANY", "MANY_TO_MANY":
	default:
		return nil, fmt.Errorf("unsupported cardinality %q", card)
	}
	switch out {
	case "FLAT", "OBJECT", "JSON_ARRAY", "SCALAR_ARRAY", "COUNT", "EXISTS", "FIRST":
	default:
		return nil, fmt.Errorf("unsupported output_format %q", out)
	}
	source, err := b.table(r.SourceModelID, "source")
	if err != nil {
		return nil, err
	}
	target, err := b.table(r.TargetModelID, "target")
	if err != nil {
		return nil, err
	}
	if r.PreserveSourceOrder && len(r.SourceOrder) == 0 {
		return nil, fmt.Errorf("preserve_source_order requires explicit source_order")
	}
	sourceOrder, err := b.order(r.SourceModelID, "source", r.SourceOrder)
	if err != nil {
		return nil, err
	}
	targetOrder, err := b.order(r.TargetModelID, "target", r.TargetOrder)
	if err != nil {
		return nil, err
	}
	if len(r.TargetOrder) > 0 && out != "FIRST" {
		return nil, fmt.Errorf("target_order is currently supported only for FIRST")
	}
	if out == "FIRST" {
		if card != "MANY" || jt != "LEFT" && jt != "LATERAL" || targetOrder == "" {
			return nil, fmt.Errorf("FIRST requires MANY, LEFT or LATERAL, and target_order")
		}
		ids := map[string]bool{}
		for _, o := range r.TargetOrder {
			ids[o.FieldID] = true
		}
		orderProof := *b
		orderProof.nullPolicy = "MATCH" // Nullable unique keys allow tied NULL rows.
		if !orderProof.unique(r.TargetModelID, ids) {
			return nil, fmt.Errorf("FIRST target_order must include a verified unique key for deterministic selection")
		}
	} else if jt == "LATERAL" {
		return nil, fmt.Errorf("LATERAL requires FIRST output")
	}
	if out == "OBJECT" && card != "ONE" && card != "ZERO_OR_ONE" {
		return nil, fmt.Errorf("OBJECT requires verified ONE or ZERO_OR_ONE")
	}
	if out == "OBJECT" && jt != "LEFT" && jt != "INNER" {
		return nil, fmt.Errorf("OBJECT supports only LEFT or INNER")
	}
	if card == "ZERO_OR_ONE" && jt != "LEFT" {
		return nil, fmt.Errorf("ZERO_OR_ONE requires LEFT")
	}
	if (card == "ONE" || card == "ZERO_OR_ONE") && out != "FLAT" && out != "OBJECT" {
		return nil, fmt.Errorf("singular cardinality requires FLAT or OBJECT")
	}
	if (jt == "SEMI" || jt == "ANTI") && (out != "FLAT" || card != "MANY" || len(r.TargetFields) > 0) {
		return nil, fmt.Errorf("SEMI/ANTI require MANY, FLAT and no target projections")
	}
	if jt == "CROSS" && (out != "FLAT" || card != "MANY" || r.SourceFieldID != "" || r.TargetFieldID != "" || len(r.Conditions) > 0 || len(r.TargetFilters) > 0 || len(r.SourceJSONPath) > 0 || len(r.TargetJSONPath) > 0 || joinDefault(r.Operator, "AUTO") != "AUTO") {
		return nil, fmt.Errorf("CROSS requires MANY FLAT without a predicate or target filters")
	}
	if out != "FLAT" && out != "OBJECT" && out != "FIRST" && jt != "LEFT" && jt != "INNER" {
		return nil, fmt.Errorf("aggregate outputs support only LEFT or INNER")
	}
	parts := []string{}
	keys := map[string]bool{}
	primary := domain.RelationshipCondition{SourceFieldID: r.SourceFieldID, TargetFieldID: r.TargetFieldID, Operator: r.Operator, SourceJSONPath: r.SourceJSONPath, TargetJSONPath: r.TargetJSONPath}
	if card == "MANY_TO_MANY" {
		if r.Junction == nil || out != "JSON_ARRAY" || joinDefault(r.Operator, "AUTO") != "AUTO" && joinDefault(r.Operator, "AUTO") != "EQ" || len(r.SourceJSONPath) > 0 || len(r.TargetJSONPath) > 0 {
			return nil, fmt.Errorf("MANY_TO_MANY requires a junction, JSON_ARRAY and scalar EQ edges")
		}
		junction, e := b.table(r.Junction.ModelID, "junction")
		if e != nil {
			return nil, e
		}
		a, e := b.field(r.SourceModelID, r.SourceFieldID, "source")
		if e != nil {
			return nil, e
		}
		js, e := b.field(r.Junction.ModelID, r.Junction.SourceFieldID, "junction")
		if e != nil {
			return nil, e
		}
		jz, e := b.field(r.Junction.ModelID, r.Junction.TargetFieldID, "junction")
		if e != nil {
			return nil, e
		}
		z, e := b.field(r.TargetModelID, r.TargetFieldID, "target")
		if e != nil {
			return nil, e
		}
		p, e := b.predicate(a, js, "EQ", nil, nil)
		if e != nil {
			return nil, e
		}
		edge, e := b.predicate(jz, z, "EQ", nil, nil)
		if e != nil {
			return nil, e
		}
		target = junction + " INNER JOIN " + target + " ON " + edge
		parts = append(parts, p)
	} else {
		if r.Junction != nil {
			return nil, fmt.Errorf("junction requires MANY_TO_MANY")
		}
		if jt != "CROSS" {
			if r.SourceFieldID != "" || r.TargetFieldID != "" {
				p, e := b.condition(r.SourceModelID, r.TargetModelID, primary, 0)
				if e != nil {
					return nil, e
				}
				parts = append(parts, p)
				b.equalityKeys(r.SourceModelID, r.TargetModelID, primary, keys)
			} else if len(r.Conditions) == 0 {
				return nil, fmt.Errorf("relationship requires fields or conditions")
			} else if joinDefault(r.Operator, "AUTO") != "AUTO" || len(r.SourceJSONPath) > 0 || len(r.TargetJSONPath) > 0 {
				return nil, fmt.Errorf("primary operator/path requires primary fields")
			}
		}
	}
	for _, c := range r.Conditions {
		p, e := b.condition(r.SourceModelID, r.TargetModelID, c, 0)
		if e != nil {
			return nil, e
		}
		parts = append(parts, p)
		b.equalityKeys(r.SourceModelID, r.TargetModelID, c, keys)
	}
	if (card == "ONE" || card == "ZERO_OR_ONE") && !b.unique(r.TargetModelID, keys) {
		return nil, fmt.Errorf("%s requires a verified target unique key covered by scalar equality", card)
	}
	filters, err := b.filters(r.TargetModelID, r.TargetFilters)
	if err != nil {
		return nil, err
	}
	parts = append(parts, filters...)
	predicate := strings.Join(parts, " AND ")
	fields := append([]string(nil), r.TargetFields...)
	if len(fields) == 0 && out != "COUNT" && out != "EXISTS" && jt != "SEMI" && jt != "ANTI" {
		m, _ := b.model(r.TargetModelID)
		for _, f := range m.Fields {
			if f != nil && f.Status == model.DataModelStatusActive && !f.IsEncrypted {
				fields = append(fields, f.ID)
			}
		}
		sort.Strings(fields)
	}
	if (out == "COUNT" || out == "EXISTS") && len(fields) > 0 {
		return nil, fmt.Errorf("COUNT/EXISTS do not accept target_fields")
	}
	projections := []string{}
	obj := []string{}
	seen := map[string]bool{}
	for _, id := range fields {
		f, e := b.field(r.TargetModelID, id, "target")
		if e != nil {
			return nil, e
		}
		if seen[f.metadata.ColumnName] {
			return nil, fmt.Errorf("duplicate target column")
		}
		seen[f.metadata.ColumnName] = true
		projections = append(projections, f.expr)
		obj = append(obj, joinLiteral(f.metadata.ColumnName), f.expr)
	}
	if len(projections) == 0 && out != "COUNT" && out != "EXISTS" && jt != "SEMI" && jt != "ANTI" {
		return nil, fmt.Errorf("target projection is empty")
	}
	// PostgreSQL limits function calls to 100 arguments. Split wide objects
	// without changing their keys or requiring a whole-row JSON projection.
	objects := []string{}
	for start := 0; start < len(obj); start += 100 {
		end := start + 100
		if end > len(obj) {
			end = len(obj)
		}
		objects = append(objects, "jsonb_build_object("+strings.Join(obj[start:end], ", ")+")")
	}
	object := strings.Join(objects, " || ")
	if len(objects) > 1 {
		object = "(" + object + ")"
	}
	selectSQL := `SELECT "source".*`
	fromSQL := " FROM " + source
	whereSQL := ""
	exists := "EXISTS (SELECT 1 FROM " + target + " WHERE " + predicate + ")"
	if jt == "SEMI" || jt == "ANTI" {
		whereSQL = " WHERE "
		if jt == "ANTI" {
			whereSQL += "NOT "
		}
		whereSQL += exists
	} else if out == "FLAT" || out == "OBJECT" {
		if out == "FLAT" {
			selectSQL += ", " + strings.Join(projections, ", ")
		} else {
			// A lateral marker distinguishes an unmatched row even when its key is NULL.
			target = "LATERAL (SELECT " + object + " AS \"value\" FROM " + target + " WHERE " + predicate + ") AS \"relation\""
			predicate = "TRUE"
			selectSQL += `, "relation"."value" AS "relationship"`
		}
		keyword := jt
		if jt == "FULL" {
			keyword = "FULL OUTER"
		}
		fromSQL += " " + keyword + " JOIN " + target
		if jt != "CROSS" {
			fromSQL += " ON " + predicate
		}
	} else if out == "FIRST" {
		fromSQL += " LEFT JOIN LATERAL (SELECT " + object + " AS \"value\" FROM " + target + " WHERE " + predicate + " ORDER BY " + targetOrder + " LIMIT 1) AS \"relation\" ON TRUE"
		selectSQL += `, "relation"."value" AS "relationship"`
	} else {
		distinct := ""
		if dup == "DISTINCT" {
			distinct = "DISTINCT "
		}
		var expr string
		switch out {
		case "JSON_ARRAY":
			expr = "COALESCE((SELECT jsonb_agg(" + distinct + object + ") FROM " + target + " WHERE " + predicate + "), '[]'::jsonb)"
		case "SCALAR_ARRAY":
			if len(projections) != 1 {
				return nil, fmt.Errorf("SCALAR_ARRAY requires exactly one target field")
			}
			f, e := b.field(r.TargetModelID, fields[0], "target")
			if e != nil {
				return nil, e
			}
			if f.array || f.json {
				return nil, fmt.Errorf("SCALAR_ARRAY requires a scalar target")
			}
			expr = "COALESCE((SELECT array_agg(" + distinct + projections[0] + ") FROM " + target + " WHERE " + predicate + "), ARRAY[]::" + f.typ + "[])"
		case "COUNT":
			expr = "(SELECT COUNT(*) FROM " + target + " WHERE " + predicate + ")"
		case "EXISTS":
			expr = exists
		}
		selectSQL += ", " + expr + ` AS "relationship"`
		if jt == "INNER" {
			whereSQL = " WHERE " + exists
		}
	}
	orderSQL := ""
	if sourceOrder != "" {
		orderSQL = " ORDER BY " + sourceOrder
	}
	return &CompiledJoin{SQL: selectSQL + fromSQL + whereSQL + orderSQL + ";", Args: b.args}, nil
}
