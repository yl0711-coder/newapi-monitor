package monitor

import (
	"fmt"
	"testing"
)

func TestTokenDetectionDueQueueRepairsNullableHeadsWithoutStarvingDueWork(t *testing.T) {
	for _, population := range []int64{20, 5000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			db, hash := m.tokenStore(), tokenDetectionPolicyHash(detectionPolicy())
			base := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
			facts := make([]TokenMeteringFact, 0, population-1)
			heads := make([]TokenDetectionCurrent, 0, population)
			for id := int64(1); id <= population; id++ {
				f := base
				f.LogID = id
				if id > 1 {
					facts = append(facts, f)
				}
				heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: id, CreatedAt: f.CreatedAt,
					FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: hash, DetectionState: "not_hit", NextAt: now - 1})
			}
			if err := db.CreateInBatches(&facts, 100).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.CreateInBatches(&heads, 100).Error; err != nil {
				t.Fatal(err)
			}
			for _, sql := range []string{
				"UPDATE token_detection_currents SET policy_hash=NULL WHERE log_id IN (1,3)",
				"UPDATE token_metering_facts SET link_revision=NULL WHERE log_id=2",
			} {
				if err := db.Exec(sql).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := initTokenDetectionReportIndex(db); err != nil {
				t.Fatal(err)
			}
			check := func() {
				t.Helper()
				for rotation := int64(0); rotation < 4; rotation++ {
					for _, limit := range []int{1, 7, 100} {
						at := now + rotation*15
						got, err := tokenDetectionJobsBounded(db, base.SourceEpoch, hash, at, limit, 15)
						if err != nil || len(got) != min(limit, int(population)) {
							t.Fatalf("nullable prefix shortened bounded queue: rotation=%d limit=%d count=%d error=%v", rotation, limit, len(got), err)
						}
						seen, known := map[int64]bool{}, 0
						for _, job := range got {
							if seen[job.LogID] || job.LogID < 1 || job.LogID > population {
								t.Fatal("duplicate or invented work", job.LogID)
							}
							seen[job.LogID] = true
							if job.LogID > 3 {
								known++
							}
						}
						if known < len(got)-3 {
							t.Fatal("nullable repair starved known due work", known, len(got))
						}
						// Unknown policy/operand values are repairable work, never proof
						// of completion. They must not vanish behind SQL NULL predicates.
						if limit >= 7 && (!seen[1] || !seen[2] || !seen[3] || !seen[4]) {
							t.Fatal("missing nullable repair or known due prefix", seen)
						}
					}
				}
			}
			check() // No known stale heads, but three unknown heads precede valid due work.
			if err := db.Model(&TokenDetectionCurrent{}).Where("log_id=9").Update("policy_hash", "previous-policy").Error; err != nil {
				t.Fatal(err)
			}
			check() // Sparse corrected work must not shorten the due lane either.
		})
	}
}
