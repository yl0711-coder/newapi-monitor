package monthlysource

import "testing"

func TestTokenWorkMetadataOnlyRecoveryKeepsOriginalCheckpoint(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	a, err := ReadTokenWorkCheckpoint(t.Context(), m.f.store, m.f.job, m.scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	}
	b, err := ReadTokenWorkCheckpoint(t.Context(), m.f.store, m.f.job, m.scope)
	if err != nil {
		t.Fatal(err)
	}
	if a.Observed() != 0 || b.Observed() != 0 || a.Fingerprint() != b.Fingerprint() || a.Limit() != b.Limit() || a.Schedule().PlanSHA256 != b.Schedule().PlanSHA256 {
		t.Fatal("planning a classified recovery changed the successful-original checkpoint")
	}
	if adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_adaptive_token_roots`) != 1 || adaptiveTokenCount(t, m.f.store, `SELECT count(*) FROM monthly_adaptive_token_splits`) != 1 {
		t.Fatal("fixture did not actually create recovery metadata")
	}
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	}
	c, err := ReadTokenWorkCheckpoint(t.Context(), m.f.store, m.f.job, m.scope)
	if err != nil || c.Observed() != 1 || c.Fingerprint() == b.Fingerprint() {
		t.Fatal("a real child original was not counted", err)
	}
}

func TestTokenWorkMetadataWithoutOriginalsStillRequiresValidRecovery(t *testing.T) {
	m := newRecoveringMonthFixture(t, false)
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	}
	queries := m.f.queries()
	// Corruption injection in this one disposable fixture, not a repair API.
	if _, err := m.f.store.Exec(`DROP TRIGGER adaptive_token_roots_no_UPDATE; UPDATE monthly_adaptive_token_roots SET payload='{}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTokenWorkCheckpoint(t.Context(), m.f.store, m.f.job, m.scope); err == nil || m.f.queries() != queries {
		t.Fatal("ignoring planning in work stamp skipped its integrity audit or queried source", err)
	}
}
