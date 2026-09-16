// Package postgres implements the PostgreSQL storage adapter, query generator,
// DDL schema migrator, introspection engine, and Dataset Studio compiler.
//
// File: types.go
// Usage:
//   This file defines the bidirectional type mapping between core engine generic DataType
//   enums (model.TypeString, model.TypeInt, model.TypeDecimal, model.TypeJSON, etc.) and
//   PostgreSQL-native SQL data types (VARCHAR, SERIAL, NUMERIC, JSONB, TIMESTAMPTZ, etc.).
package postgres

import (
	"fmt"

	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/schema"
)

// ToPostgresType maps core generic DataType to PostgreSQL SQL types.
//
// Purpose:
//   Translates an engine schema attribute into a valid PostgreSQL column type string (e.g. VARCHAR(255), NUMERIC(10,2)).
//
// Where it is used:
//   - Used by DDLGenerator when constructing CREATE TABLE and ADD COLUMN statements.
//
// When can it be used:
//   - When translating core schema definitions into PostgreSQL table columns.
func ToPostgresType(attr schema.SchemaAttribute) string {
	switch attr.Type {
	case model.TypeString:
		if attr.Length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", attr.Length)
		}
		return "VARCHAR(255)"

	case model.TypeText:
		return "TEXT"

	case model.TypeInt:
		if attr.AutoIncrement {
			return "SERIAL"
		}
		return "INTEGER"

	case model.TypeLong:
		if attr.AutoIncrement {
			return "BIGSERIAL"
		}
		return "BIGINT"

	case model.TypeFloat:
		return "DOUBLE PRECISION"

	case model.TypeDecimal:
		if attr.Precision > 0 && attr.Scale > 0 {
			return fmt.Sprintf("NUMERIC(%d, %d)", attr.Precision, attr.Scale)
		}
		return "NUMERIC"

	case model.TypeBoolean:
		return "BOOLEAN"

	case model.TypeDateTime:
		return "TIMESTAMPTZ"

	case model.TypeDate:
		return "DATE"

	case model.TypeTime:
		return "TIME"

	case model.TypeJSON:
		return "JSONB"

	case model.TypeUUID:
		return "UUID"

	case model.TypeBinary:
		return "BYTEA"

	case model.TypeArray:
		return "TEXT[]"

	default:
		return "TEXT"
	}
}

// FromPostgresType maps PostgreSQL information_schema data_type string back to core DataType.
//
// Purpose:
//   Translates raw catalog type strings (e.g. "character varying", "timestamptz", "jsonb") into engine DataType constants.
//
// Where it is used:
//   - Used by Introspector when reverse-engineering tables from information_schema.columns.
//
// When can it be used:
//   - When converting database-discovered column types into generic engine types.
func FromPostgresType(pgType string) model.DataType {
	switch pgType {
	case "character varying", "varchar", "character", "char":
		return model.TypeString
	case "text":
		return model.TypeText
	case "integer", "int", "int4", "smallint", "int2":
		return model.TypeInt
	case "bigint", "int8":
		return model.TypeLong
	case "numeric", "decimal":
		return model.TypeDecimal
	case "double precision", "real", "float8", "float4":
		return model.TypeFloat
	case "boolean", "bool":
		return model.TypeBoolean
	case "timestamp with time zone", "timestamp without time zone", "timestamptz", "timestamp":
		return model.TypeDateTime
	case "date":
		return model.TypeDate
	case "time with time zone", "time without time zone", "time":
		return model.TypeTime
	case "json", "jsonb":
		return model.TypeJSON
	case "uuid":
		return model.TypeUUID
	case "bytea":
		return model.TypeBinary
	case "ARRAY":
		return model.TypeArray
	default:
		return model.TypeString
	}
}
