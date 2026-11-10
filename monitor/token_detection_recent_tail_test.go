package monitor

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenDetectionFreshRecentTailNotHiddenByRecentPrefix(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	base := detectionFact(t, m, 1, now-4*3600, 3, 20000, 0, 0)
	if err := m.tokenStore().Where("source_epoch=?", base.SourceEpoch).Delete(&TokenMeteringFact{}).Error; err != nil {
		t.Fatal(err)
	}
	// Keep a substantial historical prefix inside the three-hour probe. This is
	// the shape seen on the retained store: a new sealed window follows thousands
	// of older headless facts with no current detection head.
	rows := make([]TokenMeteringFact, 0, 664)
	for i := int64(0); i < 600; i++ {
		f := base
		f.LogID = 10_000_000 + i
		f.CreatedAt = now - 3*3600 + 30 + i
		f.FirstSeenAt, f.LastSeenAt = f.CreatedAt, f.CreatedAt
		rows = append(rows, f)
	}
	for i := int64(0); i < 64; i++ {
		f := base
		f.LogID = 1_000_000_000 + i
		f.CreatedAt = now - 30 + i
		f.FirstSeenAt, f.LastSeenAt = f.CreatedAt, f.CreatedAt
		rows = append(rows, f)
	}
	if err := m.tokenStore().CreateInBatches(&rows, 128).Error; err != nil {
		t.Fatal(err)
	}
	countTail := func(rows []TokenMeteringFact) int {
		n := 0
		for _, row := range rows {
			if row.LogID >= 1_000_000_000 {
				n++
			}
		}
		return n
	}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	for _, indexed := range []bool{false, true} {
		if indexed {
			if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
				t.Fatal(err)
			}
		}
		jobs, err := tokenDetectionFreshJobs(m.tokenReadStore(), base.SourceEpoch, now, 250, 15, indexed)
		if err != nil {
			t.Fatalf("indexed=%t: %v", indexed, err)
		}
		if got := countTail(jobs); got == 0 {
			t.Fatalf("indexed=%t: recent sealed tail was hidden by prefix: jobs=%d", indexed, len(jobs))
		}
		if got := countTail(jobs[:min(12, len(jobs))]); got == 0 {
			t.Fatalf("indexed=%t: a short committed prefix starves the selected recent tail", indexed)
		}
		all, err := tokenDetectionJobsBounded(m.tokenReadStore(), base.SourceEpoch, hash, now, 250, 15)
		if err != nil {
			t.Fatalf("indexed=%t bounded queue: %v", indexed, err)
		}
		if got := countTail(all); got == 0 {
			t.Fatalf("indexed=%t bounded queue hid recent sealed tail", indexed)
		}
		if got := countTail(all[:min(12, len(all))]); got == 0 {
			t.Fatalf("indexed=%t bounded queue places the tail after the executable prefix", indexed)
		}
	}
}

func TestTokenDetectionRecentCorrectionsCommitBothEndsInShortRound(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	base := detectionFact(t, m, 1, now-4*3600, 3, 20000, 0, 0)
	facts := make([]TokenMeteringFact, 0, 664)
	heads := []TokenDetectionCurrent{{SourceEpoch: base.SourceEpoch, LogID: base.LogID, CreatedAt: base.CreatedAt, Revision: 1, PolicyHash: "old-policy"}}
	for i := int64(0); i < 664; i++ {
		f := base
		f.LogID, f.CreatedAt = 10_000_000+i, now-3*3600+30+i
		if i >= 600 {
			f.LogID, f.CreatedAt = 1_000_000_000+i-600, now-300+i-600
		}
		facts = append(facts, f)
		heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: f.LogID, CreatedAt: f.CreatedAt, Revision: 1, PolicyHash: "old-policy"})
	}
	if err := m.tokenStore().CreateInBatches(facts, 128).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().CreateInBatches(heads, 128).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 250, 15
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("short_correction_round", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_detection_revisions" {
			waitSourceLifecycle(tx.Statement.Context, 25*time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Create().Remove("short_correction_round"); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	processed, remaining, err := m.runTokenDetectionRoundProgress(ctx, now)
	if err != nil || !remaining || processed <= 0 || processed >= 125 {
		t.Fatal("fixture did not commit a bounded scheduling prefix", processed, remaining, err)
	}
	var prefix, tail int64
	for _, item := range []struct {
		filter string
		count  *int64
	}{{"log_id<1000000000", &prefix}, {"log_id>=1000000000", &tail}} {
		if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("policy_hash=?", tokenDetectionPolicyHash(detectionPolicy())).Where(item.filter).Count(item.count).Error; err != nil {
			t.Fatal(err)
		}
	}
	if prefix == 0 || tail == 0 || prefix+tail != int64(processed) {
		t.Fatal("short round starved one selected recent end", prefix, tail, processed)
	}
}

func TestTokenDetectionShortPrefixesKeepUpWithAssociationCorrections(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	base := detectionFact(t, m, 1, now-2*3600, 3, 100, 0, 0)
	// Keep the association exact while starting its revision at zero.  The
	// later association update then produces a real correction without feeding
	// an association-pending identity to the detection worker.
	base.LinkState, base.LinkRevision = "unique", 0
	if err := m.tokenStore().Where("source_epoch=? AND log_id=?", base.SourceEpoch, base.LogID).Delete(&TokenMeteringFact{}).Error; err != nil {
		t.Fatal(err)
	}
	facts := make([]TokenMeteringFact, 0, 1200)
	heads := make([]TokenDetectionCurrent, 0, 600)
	for id := int64(1); id <= 1200; id++ {
		f := base
		f.LogID, f.CreatedAt = id, now-2*3600+id/2
		facts = append(facts, f)
		if id%2 == 0 {
			heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: id, CreatedAt: f.CreatedAt,
				Revision: 1, FactRevision: f.Revision, PolicyHash: "old-policy", ResultJSON: "{}"})
		}
	}
	for _, rows := range []any{facts, heads} {
		if err := m.tokenStore().CreateInBatches(rows, 128).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease := "short-prefix-fixture"
	if err := m.tokenStore().Create(&TokenDetectionState{SourceEpoch: base.SourceEpoch, LeaseToken: lease, LeaseUntil: now + 3600}).Error; err != nil {
		t.Fatal(err)
	}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	const arrivals, turns = int64(10), int64(12)
	for turn := int64(0); turn < turns; turn++ {
		at := now + turn*15
		for row := int64(0); row < arrivals; row++ {
			f := base
			f.LogID, f.CreatedAt = 1_000_000_000+turn*10+row, at-600
			if err := m.tokenStore().Create(&f).Error; err != nil {
				t.Fatal(err)
			}
		}
		jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), base.SourceEpoch, hash, at, 250, 15)
		if err != nil || len(jobs) < 32 {
			t.Fatal("full backlog required for a short executable prefix", len(jobs), err)
		}
		// The retained store can execute only a prefix of its selected 250-row
		// batch. Feed that prefix through the real fenced processor. Continuing
		// arrivals must fit within these 32 opportunities, including their
		// second decision after independent association supplies evidence.
		for _, job := range jobs[:32] {
			if err := m.processTokenDetection(context.Background(), job.SourceEpoch, job.LogID, detectionPolicy(), lease, at); err != nil {
				t.Fatal(err)
			}
		}
		if err := m.tokenStore().Model(&TokenMeteringFact{}).
			Where("source_epoch=? AND log_id>=? AND link_revision=0 AND EXISTS (SELECT 1 FROM token_detection_currents c WHERE c.source_epoch=token_metering_facts.source_epoch AND c.log_id=token_metering_facts.log_id)", base.SourceEpoch, int64(1_000_000_000)).
			Updates(map[string]any{"link_revision": 1, "link_state": "unique", "evidence_level": "exact", "request_length": 1000, "length_state": "valid", "evidence_json": `[{"event_id":"synthetic","status":200,"route":"/v1/messages","request_length":1000,"request_length_state":"valid","request_length_version":"request-length-v1"}]`}).Error; err != nil {
			t.Fatal(err)
		}
	}
	var unfinished, historical int64
	if err := m.tokenReadStore().Table(tokenReportPendingMembers+" AS g").
		Where("g.source_epoch=? AND g.log_id>=? AND g.log_id<?", base.SourceEpoch, int64(1_000_000_000), int64(1_000_000_000)+(turns-1)*arrivals).
		Where("g.base_pending=1").Count(&unfinished).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).
		Where("source_epoch=? AND log_id<? AND policy_hash=?", base.SourceEpoch, int64(1_000_000_000), hash).Count(&historical).Error; err != nil {
		t.Fatal(err)
	}
	if unfinished != 0 || historical == 0 {
		t.Fatal("short rounds failed to finish earlier arrivals or starved history", unfinished, historical)
	}
}
