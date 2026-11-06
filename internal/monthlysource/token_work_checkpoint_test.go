package monthlysource

import (
	"context"
	"encoding/json"
	"testing"
)

func TestTokenWorkCheckpointIsLocalOpaqueAndTracksOriginals(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	} // classified timeout/binding, not a capture
	f := m.f
	queries := f.queries()
	changes := adaptiveTokenCount(t, f.store, `SELECT total_changes()`)
	a, err := ReadTokenWorkCheckpoint(t.Context(), f.store, f.job, m.scope)
	if err != nil {
		t.Fatal(err)
	}
	if a.Observed() != 0 || a.Schedule().JobKey != f.job || !validDigest(a.Fingerprint()) || f.queries() != queries || adaptiveTokenCount(t, f.store, `SELECT total_changes()`) != changes {
		t.Fatal("work snapshot queried/wrote or invented a capture")
	}
	copy := a.Schedule()
	copy.Segments[0].ToOrdinal++
	if copy.Segments[0] == a.Schedule().Segments[0] {
		t.Fatal("checkpoint schedule can be mutated by caller")
	}
	if _, err := json.Marshal(a); err == nil {
		t.Fatal("checkpoint serialized as evidence")
	}
	var decoded TokenWorkCheckpoint
	if err := json.Unmarshal([]byte(`{}`), &decoded); err == nil {
		t.Fatal("caller deserialized a work proof")
	}
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	}
	b, err := ReadTokenWorkCheckpoint(t.Context(), f.store, f.job, m.scope)
	if err != nil {
		t.Fatal(err)
	}
	if b.Observed() != 1 || b.Fingerprint() == a.Fingerprint() || b.Limit() != a.Limit() || b.Schedule().PlanSHA256 != a.Schedule().PlanSHA256 {
		t.Fatal("work identity/count drift")
	}
	if f.store.Stats().InUse != 0 {
		t.Fatal("snapshot retained connection")
	}
}

func TestTokenWorkCheckpointRejectsScopeAndCancellation(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	}
	plans := adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM portal_monthly_funding_capture_plans`)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ReadTokenWorkCheckpoint(ctx, m.f.store, m.f.job, m.scope); err == nil {
		t.Fatal("canceled read succeeded")
	}
	other := m.scope
	other.GroupID++
	if _, err := ReadTokenWorkCheckpoint(t.Context(), m.f.store, m.f.job, other); err == nil {
		t.Fatal("foreign scope accepted")
	}
	if _, err := ReadTokenWorkCheckpoint(t.Context(), m.f.store, digest([]byte("foreign job")), m.scope); err == nil {
		t.Fatal("foreign job initialized")
	}
	if adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM portal_monthly_funding_capture_plans`) != plans {
		t.Fatal("snapshot created a new plan")
	}
}
