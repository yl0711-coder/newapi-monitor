package monitor

import (
	"context"
	"fmt"
	"testing"

	"gorm.io/gorm"
)

// A small probe did not reproduce the retained failure: the 250-row worker
// selected both ends of a three-hour backlog and missed the previous sealed
// window's 31-row suffix entirely. Keep an older recent backlog AND a large
// newer window, then execute only short prefixes of the real selected queue.
func TestTokenDetectionSealedWindowSuffixSurvivesLargeNewerWindow(t *testing.T) {
	for _, corrected := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrected_%t", corrected), func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			base := detectionFact(t, m, 1, now-86400, 3, 20000, 0, 0)
			hash, p := tokenDetectionPolicyHash(detectionPolicy()), detectionPolicy()
			from, to := tokenDetectionCarryWindow(now)
			var facts []TokenMeteringFact
			heads := []TokenDetectionCurrent{}
			for _, population := range []struct{ first, count, at int64 }{
				{10, 600, now - 2*3600},
				{1_000_000_000, 64, from + 1},
				{2_000_000_000, 600, to + 1},
			} {
				for offset := int64(0); offset < population.count; offset++ {
					f := base
					f.LogID, f.CreatedAt = population.first+offset, population.at
					// Queue fairness is the subject of this fixture.  Keep every
					// identity uniquely associated so the detection worker can consume
					// each lane; association-pending work is covered separately.
					f.LinkState, f.LinkRevision = "unique", 1
					facts = append(facts, f)
					if corrected {
						heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: f.LogID, CreatedAt: f.CreatedAt,
							Revision: 1, FactRevision: f.Revision, LinkRevision: 0, PolicyHash: hash, Stage: 3})
					}
				}
			}
			if err := m.tokenStore().CreateInBatches(facts, 128).Error; err != nil {
				t.Fatal(err)
			}
			if corrected {
				if err := m.tokenStore().CreateInBatches(heads, 128).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
				t.Fatal(err)
			}
			if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
				t.Fatal(err)
			}
			const lease = "carry-window-fixture"
			if err := m.tokenStore().Create(&TokenDetectionState{SourceEpoch: base.SourceEpoch, LeaseToken: lease, LeaseUntil: now + 3600}).Error; err != nil {
				t.Fatal(err)
			}
			previous, newer, history := 0, 0, 0
			// Latest work now leads. The historical suffix keeps its reservation
			// and drains within twelve bounded prefixes, including restarts.
			for turn := 0; turn < 12; turn++ {
				jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), base.SourceEpoch, hash, now, 250, 15)
				if err != nil || len(jobs) != 250 {
					t.Fatal("fixture needs a full retained queue", len(jobs), err)
				}
				if err := m.tokenLocalWriteTransaction(context.Background(), func(tx *gorm.DB) error {
					for _, job := range jobs[:32] {
						prepared, err := prepareTokenDetection(tx, job.SourceEpoch, job.LogID, p, now)
						if err != nil {
							return err
						}
						if err := m.processTokenDetectionTx(tx, job.SourceEpoch, job.LogID, p, lease, now, prepared); err != nil {
							return err
						}
						switch {
						case job.LogID >= 2_000_000_000:
							newer++
						case job.LogID >= 1_000_000_000:
							previous++
						default:
							history++
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			var pending int64
			if err := m.tokenReadStore().Table(tokenReportPendingMembers+" AS g").
				Where("g.source_epoch=? AND g.log_id>=? AND g.log_id<?", base.SourceEpoch, int64(1_000_000_000), int64(2_000_000_000)).
				Where(tokenReportPendingPredicate("g"), hash).Count(&pending).Error; err != nil {
				t.Fatal(err)
			}
			t.Logf("previous_window=%d pending=%d new_window=%d historical=%d", previous, pending, newer, history)
			if pending != 0 || previous < 64 || newer == 0 || history == 0 {
				t.Fatal("sealed suffix or another lane was starved", previous, pending, newer, history)
			}
		})
	}
}
