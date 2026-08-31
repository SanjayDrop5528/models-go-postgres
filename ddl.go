package postgres

import (
	"fmt"
	"github.com/SanjayDrop5528/models-go-engine/diff"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"strings"
)

// DDLGenerator compiles core SchemaOperations into exact, minimal PostgreSQL DDL SQL statements.
type DDLGenerator struct{}

// NewDDLGenerator creates a new PostgreSQL DDL compiler.
func NewDDLGenerator() *DDLGenerator {
	return &DDLGenerator{}
}

// GenerateStatements transforms a slice of SchemaOperations into PostgreSQL SQL queries.
func (g *DDLGenerator) GenerateStatements(ops []diff.SchemaOperation) ([]string, error) {
	statements := make([]string, 0, len(ops))

	for _, op := range ops {
		stmt, err := g.GenerateStatement(op)
		if err != nil {
			return nil, err
		}
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements, nil
}

// GenerateStatement transforms an individual SchemaOperation into a PostgreSQL DDL string.
func (g *DDLGenerator) GenerateStatement(op diff.SchemaOperation) (string, error) {
	table := quoteIdent(op.TargetTable)

	switch op.Type {
	case diff.OpCreateTable:
		des, ok := op.After.(*schema.Schema)
		if !ok || des == nil {
			return "", fmt.Errorf("invalid schema payload for CREATE_TABLE")
		}
		return g.buildCreateTable(des), nil

	case diff.OpDropTable:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", table), nil

	case diff.OpRenameTable:
		return fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", table, quoteIdent(op.ObjectName)), nil

	case diff.OpAddColumn:
		attr, ok := op.After.(schema.SchemaAttribute)
		if !ok {
			return "", fmt.Errorf("invalid attribute payload for ADD_COLUMN")
		}
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, g.buildColumnDef(attr)), nil

	case diff.OpRemoveColumn:
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, quoteIdent(op.ObjectName)), nil

	case diff.OpRenameColumn:
		return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", table, quoteIdent(op.OldName), quoteIdent(op.ObjectName)), nil

	case diff.OpAlterColumnType:
		attr, ok := op.After.(schema.SchemaAttribute)
		if !ok {
			return "", fmt.Errorf("invalid attribute payload for ALTER_COLUMN_TYPE")
		}
		pgType := ToPostgresType(attr)
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;", table, quoteIdent(attr.Name), pgType), nil

	case diff.OpAlterColumnNullable:
		nullVal, ok := op.After.(bool)
		if !ok {
			return "", fmt.Errorf("invalid payload for ALTER_COLUMN_NULLABLE")
		}
		if nullVal {
			return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", table, quoteIdent(op.ObjectName)), nil
		}
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", table, quoteIdent(op.ObjectName)), nil

	case diff.OpAlterColumnDefault:
		if op.After == nil {
			return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", table, quoteIdent(op.ObjectName)), nil
		}
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", table, quoteIdent(op.ObjectName), formatDefaultVal(op.After)), nil

	case diff.OpAddPrimaryKey:
		pk, ok := op.After.(*schema.SchemaKey)
		if !ok || pk == nil || len(pk.Columns) == 0 {
			return "", fmt.Errorf("invalid primary key payload for ADD_PRIMARY_KEY")
		}
		quotedCols := make([]string, len(pk.Columns))
		for i, c := range pk.Columns {
			quotedCols[i] = quoteIdent(c)
		}
		return fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);", table, strings.Join(quotedCols, ", ")), nil

	case diff.OpDropPrimaryKey:
		// Drop pkey constraint named <table_name>_pkey
		return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", table, quoteIdent(fmt.Sprintf("%s_pkey", op.TargetTable))), nil

	case diff.OpAddIndex:
		idx, ok := op.After.(schema.SchemaIndex)
		if !ok {
			return "", fmt.Errorf("invalid index payload for ADD_INDEX")
		}
		quotedCols := make([]string, len(idx.Columns))
		for i, c := range idx.Columns {
			quotedCols[i] = quoteIdent(c)
		}
		uniqueStr := ""
		if idx.Unique {
			uniqueStr = "UNIQUE "
		}
		return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s);", uniqueStr, quoteIdent(idx.Name), table, strings.Join(quotedCols, ", ")), nil

	case diff.OpDropIndex:
		return fmt.Sprintf("DROP INDEX IF EXISTS %s;", quoteIdent(op.ObjectName)), nil

	case diff.OpAddForeignKey:
		rel, ok := op.After.(schema.SchemaRelation)
		if !ok {
			return "", fmt.Errorf("invalid relation payload for ADD_FOREIGN_KEY")
		}
		onDel := ""
		if rel.OnDelete != "" {
			onDel = fmt.Sprintf(" ON DELETE %s", rel.OnDelete)
		}
		onUpd := ""
		if rel.OnUpdate != "" {
			onUpd = fmt.Sprintf(" ON UPDATE %s", rel.OnUpdate)
		}
		return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s%s;",
			table, quoteIdent(rel.Name), quoteIdent(rel.Column), quoteIdent(rel.ForeignTable), quoteIdent(rel.ForeignColumn), onDel, onUpd), nil

	case diff.OpDropForeignKey:
		return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", table, quoteIdent(op.ObjectName)), nil

	default:
		return "", fmt.Errorf("unsupported PostgreSQL operation type: %s", op.Type)
	}
}

func (g *DDLGenerator) buildCreateTable(s *schema.Schema) string {
	lines := make([]string, 0, len(s.Attributes)+1)

	var pkCols []string
	for _, attr := range s.Attributes {
		lines = append(lines, "    "+g.buildColumnDef(attr))
		if attr.PrimaryKey && s.PrimaryKey == nil {
			pkCols = append(pkCols, attr.Name)
		}
	}

	if s.PrimaryKey != nil && len(s.PrimaryKey.Columns) > 0 {
		pkCols = s.PrimaryKey.Columns
	}

	if len(pkCols) > 0 {
		quotedPKs := make([]string, len(pkCols))
		for i, c := range pkCols {
			quotedPKs[i] = quoteIdent(c)
		}
		lines = append(lines, fmt.Sprintf("    PRIMARY KEY (%s)", strings.Join(quotedPKs, ", ")))
	}

	tableStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);", quoteIdent(s.Name), strings.Join(lines, ",\n"))

	fkStmts := g.buildForeignKeyStatements(s)
	if len(fkStmts) > 0 {
		return tableStmt + "\n" + strings.Join(fkStmts, "\n")
	}
	return tableStmt
}

func (g *DDLGenerator) buildForeignKeyStatements(s *schema.Schema) []string {
	var stmts []string
	for _, rel := range s.Relations {
		fkName := rel.Name
		if fkName == "" {
			fkName = fmt.Sprintf("fk_%s_%s", s.Name, rel.Column)
		}
		onDel := ""
		if rel.OnDelete != "" {
			onDel = fmt.Sprintf(" ON DELETE %s", rel.OnDelete)
		}
		onUpd := ""
		if rel.OnUpdate != "" {
			onUpd = fmt.Sprintf(" ON UPDATE %s", rel.OnUpdate)
		}

		dropStmt := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", quoteIdent(s.Name), quoteIdent(fkName))
		addStmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s%s;",
			quoteIdent(s.Name), quoteIdent(fkName), quoteIdent(rel.Column), quoteIdent(rel.ForeignTable), quoteIdent(rel.ForeignColumn), onDel, onUpd)

		stmts = append(stmts, dropStmt, addStmt)
	}
	return stmts
}

func (g *DDLGenerator) buildColumnDef(attr schema.SchemaAttribute) string {
	pgType := ToPostgresType(attr)
	parts := []string{quoteIdent(attr.Name), pgType}

	if !attr.Nullable && !attr.AutoIncrement {
		parts = append(parts, "NOT NULL")
	}

	if attr.Default != nil {
		parts = append(parts, fmt.Sprintf("DEFAULT %s", formatDefaultVal(attr.Default)))
	}

	if attr.Unique && !attr.PrimaryKey {
		parts = append(parts, "UNIQUE")
	}

	return strings.Join(parts, " ")
}

func quoteIdent(name string) string {
	return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
}

func formatDefaultVal(val any) string {
	switch v := val.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", v)
	}
}
