package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
)

func TestGenerationCalculationVersionSeparatesRetainedArtifacts(t *testing.T) {
	f := generationFixtureNew(t)
	p, canonical, _, err := canonicalGeneration(f.plan, f.raw, fixtureNow)
	if err != nil || p.CalculationVersion != generationVersion {
		t.Fatal("version not bound into canonical plan", err)
	}
	legacy := p
	legacy.CalculationVersion = ""
	legacyRaw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(canonical, legacyRaw) || generationHash(canonical) == generationHash(legacyRaw) {
		t.Fatal("new rules reused old plan/artifact key")
	}
	ctx := context.Background()
	if err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, legacyRaw, f.raw, false)
	}); err != nil {
		t.Fatal(err)
	}
	oldRequest := generationRequest(legacy, legacyRaw)
	if _, err := loadGeneration(ctx, f.receiver, oldRequest, fixtureNow); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("old retained plan accepted under current calculation", err)
	}
	// Current admissions use a distinct durable key; the retained legacy row
	// remains unchanged, rather than getting rewritten under a new rule label.
	if err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, canonical, f.raw, false)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := loadGeneration(ctx, f.receiver, generationRequest(p, canonical), fixtureNow); err != nil {
		t.Fatal("current canonical plan did not load", err)
	}
	var kept []byte
	if err := f.receiver.QueryRowContext(ctx, "SELECT plan FROM portal_monthly_generation_inputs WHERE id=?", generationHash(legacyRaw)).Scan(&kept); err != nil || !bytes.Equal(kept, legacyRaw) {
		t.Fatal("rewrote legacy retained evidence", err)
	}
	for _, version := range []string{"local-monthly-draft-v1", "local-monthly-draft-v3-profile-audit", "local-monthly-draft-v4-resource-audit", "local-monthly-draft-v5-resource-route-audit", "local-monthly-draft-v6-selected-reconstruction"} {
		legacy.CalculationVersion = version
		if _, _, _, err := canonicalGeneration(legacy, f.raw, fixtureNow); !errors.Is(err, ErrGenerationInput) {
			t.Fatal("explicit stale rule version accepted", version, err)
		}
	}
}
