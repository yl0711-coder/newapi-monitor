package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func stoppedResumeMonth(t *testing.T, two bool) *recoveringMonthFixture {
	t.Helper()
	m := newRecoveringMonthFixture(t, false)
	f := m.f
	if err := InitAdaptiveTokenResumptionStore(t.Context(), f.store); err != nil {
		t.Fatal(err)
	}
	if _, err := m.advance(t); err != nil {
		t.Fatal(err)
	}
	base := m.ops.capture
	m.ops.capture = func(ctx context.Context, db *sql.DB, r UsageWindowRequest) (TokenUsageWindowObservation, error) {
		f.driver.queryErr = io.EOF
		f.driver.usage.data = nil
		return captureTokenUsageWindow(ctx, db, r, func() time.Time { return f.now })
	}
	for range 3 {
		if _, err := m.advance(t); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		f.now = f.now.Add(31 * time.Second)
	}
	m.ops.capture = base
	if two {
		r, err := tokenCaptureRequest(m.plan, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := ensureAdaptiveTokenRoot(t.Context(), f.store, f.job, r, f.now); err != nil {
			t.Fatal(err)
		}
		s, err := loadAdaptiveTokenState(t.Context(), f.store, f.job, r, f.now)
		if err != nil {
			t.Fatal(err)
		}
		if err := saveAdaptiveTokenSplit(t.Context(), f.store, s, r, 0, f.now); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.Exec(`INSERT INTO monthly_token_recovery_bindings VALUES(?,?,?,?)`, f.job, 1, s.id, s.created); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			s, err = loadAdaptiveTokenStateMode(t.Context(), f.store, f.job, r, f.now, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := saveAdaptiveTokenFault(t.Context(), f.store, s, "retryable_source", false, f.now); err != nil {
				t.Fatal(err)
			}
			f.now = f.now.Add(31 * time.Second)
		}
	}
	return m
}

func TestRecoveringMonthResumeTwoRootsPartialCommitUsesSamePlan(t *testing.T) {
	m := stoppedResumeMonth(t, true)
	f := m.f
	p, err := previewRecoveringMonthResume(t.Context(), f.store, f.job, m.scope, f.now)
	if err != nil || len(p.Roots) != 2 {
		t.Fatal(p, err)
	}
	queries := f.queries()
	for _, r := range p.Roots {
		f.now = time.UnixMilli(max(f.now.UnixMilli(), r.EarliestMS))
	}
	if _, err := f.store.Exec(fmt.Sprintf(`CREATE TRIGGER synthetic_second_resume BEFORE INSERT ON monthly_adaptive_token_resumptions WHEN NEW.root_id='%s' BEGIN SELECT RAISE(ABORT,'synthetic second-root failure'); END`, p.Roots[1].RootID)); err != nil {
		t.Fatal(err)
	}
	if err := applyRecoveringMonthResume(t.Context(), f.store, p, f.now); err == nil {
		t.Fatal("second failure ignored")
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resumptions`) != 1 || f.queries() != queries {
		t.Fatal("partial append/query mismatch")
	}
	if _, err := previewRecoveringMonthResume(t.Context(), f.store, f.job, m.scope, f.now); !errors.Is(err, ErrAdaptiveTokenResumeDenied) {
		t.Fatal("invented plan from partly resumed source", err)
	}
	if err := checkRecoveringMonthResume(t.Context(), f.store, p, f.now); err != nil {
		t.Fatal("pinned partial plan unusable", err)
	}
	if _, err := f.store.Exec(`DROP TRIGGER synthetic_second_resume`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := applyRecoveringMonthResume(t.Context(), f.store, p, f.now); err != nil {
			t.Fatal(err)
		}
	}
	if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resumptions`) != 2 || adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_faults`) != 6 || queries != f.queries() {
		t.Fatal("replay reset or queried")
	}
	finishRecoveringMonth(t, m)
	pairs, _, err := LoadRecoveredTokenMonth(t.Context(), f.store, f.job, m.scope, f.root.UserID)
	if err != nil {
		t.Fatal(err)
	}
	var quota int64
	for _, pair := range pairs {
		for _, row := range pair.Newer.Rows {
			quota += row.Quota
		}
	}
	if quota != 11604 || m.parentCalls != 1 {
		t.Fatal("money or parent reread changed", quota, m.parentCalls)
	}
}

func TestRecoveringMonthResumeMissingForeignOrForgedRootCannotAdvance(t *testing.T) {
	for _, scenario := range []string{"missing_root", "foreign_request", "duplicate_root", "forged_snapshot", "forged_ordinal", "cooldown"} {
		t.Run(scenario, func(t *testing.T) {
			m := stoppedResumeMonth(t, true)
			f := m.f
			p, err := previewRecoveringMonthResume(t.Context(), f.store, f.job, m.scope, f.now)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range p.Roots {
				f.now = time.UnixMilli(max(f.now.UnixMilli(), r.EarliestMS))
			}
			switch scenario {
			case "missing_root":
				p.Roots = p.Roots[:1]
			case "foreign_request":
				p.Roots[1].Request.UserID++
			case "duplicate_root":
				p.Roots[1] = p.Roots[0]
			case "forged_snapshot":
				p.SnapshotSHA256 = digest([]byte("forged snapshot"))
			case "forged_ordinal":
				p.Roots[1].Ordinal++
			case "cooldown":
				f.now = time.UnixMilli(p.Roots[1].EarliestMS - 1)
			}
			queries := f.queries()
			if err := applyRecoveringMonthResume(t.Context(), f.store, p, f.now); err == nil {
				t.Fatal("invalid plan accepted")
			}
			if adaptiveTokenCount(t, f.store, `SELECT count(*) FROM monthly_adaptive_token_resumptions`) != 0 || f.queries() != queries {
				t.Fatal("invalid plan partly applied")
			}
		})
	}
}
