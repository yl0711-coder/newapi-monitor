package monitor

import (
	"context"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func tokenDetectionRetainedWindowFixture(t *testing.T, corrected ...bool) (*Monitor, int64, string, int64) {
	t.Helper()
	m, now := tokenIsolatedRuntimeFixture(t, "")
	var path string
	if err := m.storeDB.Raw("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 250, 15
	base := detectionFact(t, m, 1, now-86400, 3, 100, 0, 0)
	base.LinkRevision = 1
	from, to := tokenDetectionCarryWindow(now)
	var facts []TokenMeteringFact
	for _, population := range []struct{ first, count, at int64 }{
		{10, 5000, now - 2*3600}, {1_000_000, 1200, from + 1}, {2_000_000, 3000, to + 1},
	} {
		for i := int64(0); i < population.count; i++ {
			f := base
			f.LogID, f.CreatedAt = population.first+i, population.at
			f.LinkState, f.LinkRevision = "unique", 1
			facts = append(facts, f)
		}
	}
	if err := m.tokenStore().CreateInBatches(facts, 128).Error; err != nil {
		t.Fatal(err)
	}
	if len(corrected) > 0 && corrected[0] {
		heads := make([]TokenDetectionCurrent, len(facts))
		for i, f := range facts {
			heads[i] = TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: f.LogID, CreatedAt: f.CreatedAt,
				Revision: 1, FactRevision: f.Revision, LinkRevision: 0, PolicyHash: tokenDetectionPolicyHash(detectionPolicy()), Stage: 3}
		}
		if err := m.tokenStore().CreateInBatches(heads, 128).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	return m, now, path, from
}

func TestTokenDetectionRetainedWindowSurvivesMovingSealsAndRestart(t *testing.T) {
	tokenDetectionCheckRetainedWindow(t, false)
}

func TestTokenDetectionCorrectedWindowSurvivesMovingSealsAndRestart(t *testing.T) {
	tokenDetectionCheckRetainedWindow(t, true)
}

func tokenDetectionCheckRetainedWindow(t *testing.T, corrected bool) {
	m, now, path, from := tokenDetectionRetainedWindowFixture(t, corrected)
	defer func() { m.Close() }()
	for turn := 0; turn < 18; turn++ {
		// Advance through many seal boundaries while the old cohort is still
		// unfinished. Its reservation must not move with the wall clock.
		processed, _, err := m.runTokenDetectionRoundProgress(context.Background(), now+int64(turn)*300)
		if err != nil || processed == 0 {
			t.Fatal("retained worker stopped", turn, processed, err)
		}
		if turn == 0 {
			var state TokenDetectionState
			if err := m.tokenStore().First(&state).Error; err != nil || (!corrected && state.FreshWindowAt != from) || (corrected && state.CorrectionWindowAt != from) || state.LastSuccessAt != now {
				t.Fatal("reservation did not commit with successful decisions", state, err)
			}
			// Reopen the actual persisted store, discarding all learned in-memory
			// batch/scheduling state just as a process restart does.
			cfg := m.cfg
			m.Close()
			db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Discard, PrepareStmt: true})
			if err != nil {
				t.Fatal(err)
			}
			pool, _ := db.DB()
			pool.SetMaxOpenConns(1)
			m = &Monitor{cfg: cfg, storeDB: db}
			if err := m.openTokenStore(); err != nil {
				t.Fatal(err)
			}
		}
	}
	var target, history, newer int64
	for _, p := range []struct {
		where string
		count *int64
	}{{"log_id>=1000000 AND log_id<2000000", &target}, {"log_id<1000000", &history}, {"log_id>=2000000", &newer}} {
		if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where(p.where).Where("evaluated_at>=?", now).Count(p.count).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("retained fixed window=%d/1200 historical=%d newer=%d after 18 moving seals and reopen", target, history, newer)
	if target != 1200 || history == 0 || newer == 0 {
		t.Fatal("moving seal stranded a partially processed window", target, history, newer)
	}
}

func TestTokenDetectionWindowReservationRollsBackWithDecisions(t *testing.T) {
	m, now, _, _ := tokenDetectionRetainedWindowFixture(t)
	defer m.Close()
	if err := m.tokenStore().Exec(`CREATE TEMP TRIGGER reject_window_reservation BEFORE UPDATE ON token_detection_states
		WHEN NEW.fresh_window_at>0 BEGIN SELECT RAISE(ABORT,'reject reservation'); END`).Error; err != nil {
		t.Fatal(err)
	}
	processed, _, err := m.runTokenDetectionRoundProgress(context.Background(), now)
	if err == nil || processed != 0 {
		t.Fatal("rollback fixture failed to reject the decision transaction", processed, err)
	}
	var state TokenDetectionState
	var heads int64
	if err := m.tokenStore().First(&state).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil {
		t.Fatal(err)
	}
	if state.FreshWindowAt != 0 || state.CorrectionWindowAt != 0 || state.LastSuccessAt != 0 || heads != 0 {
		t.Fatal("failed decision transaction published scheduling/success progress", state, heads)
	}
}
