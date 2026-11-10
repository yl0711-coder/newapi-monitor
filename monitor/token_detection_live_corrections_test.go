package monitor

import (
	"context"
	"testing"
)

// The live pipeline produces two kinds of new work: a first decision after
// collection, then a changed decision after delayed association. Both must
// drain while older first decisions and corrections remain queued.
func TestTokenDetectionContinuousFreshAndLinkedCorrectionsDrain(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenProcessingBatchRows = 50
	m.cfg.TokenProcessingPollSeconds = 15
	base := detectionFact(t, m, 1, now-86400, 3, 20000, 0, 0)
	if err := m.tokenStore().Where("source_epoch=?", base.SourceEpoch).Delete(&TokenMeteringFact{}).Error; err != nil {
		t.Fatal(err)
	}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	facts, heads := make([]TokenMeteringFact, 0, 2048), make([]TokenDetectionCurrent, 0, 1024)
	for id := int64(1); id <= 2048; id++ {
		f := base
		f.LogID, f.UserID, f.CreatedAt = id, id, now-86400+id
		facts = append(facts, f)
		if id > 1024 {
			heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: id, CreatedAt: f.CreatedAt,
				Revision: 1, FactRevision: f.Revision, LinkRevision: 0, PolicyHash: hash, Stage: 3,
				FirstEvaluatedAt: now - 100, ExpiresAt: now + 86400, ResultJSON: "{}"})
		}
	}
	if err := m.tokenStore().CreateInBatches(facts, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().CreateInBatches(heads, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	// Recent arrivals use most, but not all, of the 50-job budget. The unused
	// capacity must serve history rather than reserving half of each lane for
	// it while the previous recent window is still waiting.
	const arrivals, turns = int64(16), int64(12)
	for turn := int64(0); turn < turns; turn++ {
		at := now + turn*60
		fresh, delayed, prior := []TokenMeteringFact{}, []TokenMeteringFact{}, []TokenDetectionCurrent{}
		for n := int64(0); n < arrivals; n++ {
			f := base
			f.LogID, f.UserID, f.CreatedAt = 10000+turn*arrivals+n, 10000+turn*arrivals+n, at-600
			fresh = append(fresh, f)
			f.LogID, f.UserID, f.CreatedAt = 20000+turn*arrivals+n, 20000+turn*arrivals+n, at-1800
			delayed = append(delayed, f)
			prior = append(prior, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: f.LogID, CreatedAt: f.CreatedAt,
				Revision: 1, FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: hash, Stage: 3,
				FirstEvaluatedAt: at - 600, ExpiresAt: at + 86400, ResultJSON: "{}"})
		}
		for _, rows := range []any{&fresh, &delayed, &prior} {
			if err := m.tokenStore().Create(rows).Error; err != nil {
				t.Fatal(err)
			}
		}
		for _, f := range delayed {
			updated := f
			// The association correction is already resolved to the same exact
			// request.  A changed unique revision must be eligible for detection;
			// an expired/waiting state is intentionally deferred by the worker.
			updated.LinkState, updated.LinkReason = "unique", "evidence_refreshed"
			updated.LinkAttempts++
			if saved, err := m.saveTokenMeteringLink(context.Background(), f, updated, at); err != nil || !saved {
				t.Fatal("actual association change did not commit", saved, err)
			}
		}
		if n, err := m.runTokenDetectionRound(context.Background(), at); err != nil || n != 50 {
			t.Fatal("bounded detection turn", n, err)
		}
	}
	count := func(where string) int64 {
		t.Helper()
		var n int64
		if err := m.tokenReadStore().Table(tokenReportPendingMembers).Where("source_epoch=? AND "+where, base.SourceEpoch).Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		return n
	}
	freshPending, correctionPending := count("log_id>=10000 AND log_id<20000 AND base_pending=1"), count("log_id>=20000 AND base_pending=1")
	oldFresh, oldCorrections := count("log_id<=1024 AND base_pending=1"), count("log_id>1024 AND log_id<10000 AND base_pending=1")
	if freshPending > arrivals || correctionPending > arrivals {
		t.Fatalf("new work accumulates despite spare per-turn capacity: fresh=%d corrections=%d arrivals_per_turn=%d", freshPending, correctionPending, arrivals)
	}
	if oldFresh >= 1024 || oldCorrections >= 1024 || oldFresh == 0 || oldCorrections == 0 {
		t.Fatal("historical queues must both advance and remain present", oldFresh, oldCorrections)
	}
	// Reopening the derived index must preserve the committed queue state.
	if err := m.tokenStore().Transaction(initTokenDetectionReportIndex); err != nil {
		t.Fatal(err)
	}
	if count("log_id>=10000 AND log_id<20000 AND base_pending=1") != freshPending || count("log_id>=20000 AND base_pending=1") != correctionPending {
		t.Fatal("index reopening changed committed progress")
	}
}

func TestTokenDetectionHistoricalCorrectionsKeepFreshReservation(t *testing.T) {
	// An old correction backlog alone must not reduce the proven first-decision
	// throughput. A genuinely recent correction activates the balanced share.
	const now = int64(1800000000)
	lanes := make([][]TokenMeteringFact, 4)
	for lane := range lanes {
		for n := int64(0); n < 120; n++ {
			lanes[lane] = append(lanes[lane], TokenMeteringFact{LogID: int64(lane)*1000 + n + 1, CreatedAt: now - 86400})
		}
	}
	counts := func() [4]int {
		var n [4]int
		for _, job := range tokenDetectionInterleaveJobs(lanes, now, 120, 15) {
			n[job.LogID/1000]++
			if job.CreatedAt != 0 {
				t.Fatal("scheduler must return identities without a stale fact copy")
			}
		}
		return n
	}
	old := counts()
	if old[0] < 75 || old[1] < 15 || old[2] == 0 || old[3] == 0 {
		t.Fatal("historical corrections displaced fresh work or a lane starved", old)
	}
	lanes[1][0].CreatedAt = now - 1800
	recent := counts()
	if recent[0] < 45 || recent[1] < 45 || recent[2] == 0 || recent[3] == 0 {
		t.Fatal("late association did not receive enough reserved capacity", recent)
	}
}

func TestTokenDetectionRecentWindowSuffixPrecedesNewerArrivals(t *testing.T) {
	for _, corrected := range []bool{false, true} {
		name := "first_decisions"
		if corrected {
			name = "changed_decisions"
		}
		t.Run(name, func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			base := detectionFact(t, m, 1, now-86400, 3, 20000, 0, 0)
			window := now/300*300 - 900
			hash := tokenDetectionPolicyHash(detectionPolicy())
			var heads []TokenDetectionCurrent
			for id := int64(1); id <= 104; id++ {
				f := base
				f.LogID, f.CreatedAt = id, now-86400+id
				if id > 80 {
					f.CreatedAt = window + id - 80
				}
				if id > 92 {
					f.CreatedAt = window + 300 + id - 92
				}
				if id > 1 {
					if err := m.tokenStore().Create(&f).Error; err != nil {
						t.Fatal(err)
					}
				}
				if corrected {
					heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: id, CreatedAt: f.CreatedAt,
						Revision: 1, FactRevision: f.Revision, LinkRevision: 0, PolicyHash: hash, Stage: 3})
				}
			}
			if corrected {
				if err := m.tokenStore().CreateInBatches(heads, 100).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
				t.Fatal(err)
			}
			jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), base.SourceEpoch, hash, now, 16, 15)
			if err != nil || len(jobs) != 16 {
				t.Fatal("bounded recent window selection", len(jobs), err)
			}
			seen := map[int64]bool{}
			for _, job := range jobs {
				if seen[job.LogID] || job.CreatedAt != 0 {
					t.Fatal("nonidentity or duplicate job")
				}
				seen[job.LogID] = true
			}
			var earlier, latest, historical int
			for id := range seen {
				switch {
				case id <= 80:
					historical++
				case id <= 92:
					earlier++
				default:
					latest++
				}
			}
			if earlier < 4 || latest < 4 || historical == 0 {
				t.Fatal("recent ends or historical opportunity lost", earlier, latest, historical)
			}
			m.cfg.TokenProcessingBatchRows = 16
			for turn := int64(0); turn < 4; turn++ {
				if _, err := m.runTokenDetectionRound(context.Background(), now+turn*15); err != nil {
					t.Fatal(err)
				}
			}
			var pending int64
			if err := m.tokenStore().Table(tokenReportPendingMembers).Where("log_id>=81 AND log_id<=104 AND base_pending=1").Count(&pending).Error; err != nil || pending != 0 {
				t.Fatal("recent suffix did not finish across bounded turns", pending, err)
			}
		})
	}
}
