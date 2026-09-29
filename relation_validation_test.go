package postgres

import (
	"testing"

	"github.com/SanjayDrop5528/models-go-engine/query"
)

func TestValidateRelationSpecPredicates(t *testing.T) {
	safe := query.RelationSpec{Name: "Customer", Conditions: []string{`"customer"."active" = TRUE`, `status = 'active'`}}
	if err := validateRelationSpecPredicates(safe); err != nil {
		t.Fatalf("safe predicates rejected: %v", err)
	}
	unsafe := query.RelationSpec{Name: "Customer", Conditions: []string{"active = TRUE; DROP TABLE users"}}
	if err := validateRelationSpecPredicates(unsafe); err == nil {
		t.Fatal("expected unsafe relation predicate to be rejected")
	}
}
