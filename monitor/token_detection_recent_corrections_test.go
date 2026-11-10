package monitor

import (
	"testing"

	"gorm.io/gorm"
)

func TestTokenDetectionRecentCorrectionsProgressAfterPolicyLinkAndCoverageChanges(t *testing.T) {
	for _, cause := range []string{"policy", "association", "coverage"} {
		t.Run(cause, func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			oldStart := now/300*300 - 86400
			recentStart := now/300*300 - 600
			base := detectionFact(t, m, 1, oldStart+1, 3, 10000, 0, 0)
			hash := tokenDetectionPolicyHash(detectionPolicy())
			heads := make([]TokenDetectionCurrent, 0, 80)
			for id := int64(1); id <= 80; id++ {
				f := base
				f.LogID, f.CreatedAt = id, oldStart+id
				if id > 64 {
					f.CreatedAt = recentStart + id - 64
				}
				if id > 1 {
					if err := m.tokenStore().Create(&f).Error; err != nil {
						t.Fatal(err)
					}
				}
				heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: id, CreatedAt: f.CreatedAt,
					Revision: 1, FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: hash, SourceComplete: false})
			}
			if err := m.tokenStore().CreateInBatches(heads, 80).Error; err != nil {
				t.Fatal(err)
			}
			if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
				t.Fatal(err)
			}
			switch cause {
			case "policy":
				hash = "updated-policy"
			case "association":
				if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("source_epoch=?", base.SourceEpoch).Update("link_revision", 2).Error; err != nil {
					t.Fatal(err)
				}
			case "coverage":
				for _, start := range []int64{oldStart, recentStart} {
					w := TokenMeteringWindow{SourceEpoch: base.SourceEpoch, StartAt: start, EndAt: start + 300, Done: true, SourceComplete: true}
					if err := m.tokenStore().Create(&w).Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			check := func() {
				t.Helper()
				for rotation := int64(0); rotation < 2; rotation++ {
					jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), base.SourceEpoch, hash, now+rotation*15, 16, 15)
					if err != nil || len(jobs) != 16 {
						t.Fatal("bounded corrected batch", len(jobs), err)
					}
					old, recent, seen := 0, 0, map[int64]bool{}
					for _, job := range jobs {
						if seen[job.LogID] || job.CreatedAt != 0 {
							t.Fatal("duplicate or nonidentity work item")
						}
						seen[job.LogID] = true
						if job.LogID > 64 {
							recent++
						} else {
							old++
						}
					}
					if old != 1 || recent != 15 || !seen[1] || !seen[65] {
						t.Fatal("old or recent corrections starved", old, recent)
					}
				}
			}
			check()
			if err := m.tokenStore().Transaction(func(tx *gorm.DB) error { return initTokenDetectionReportIndex(tx) }); err != nil {
				t.Fatal(err)
			}
			check()
		})
	}
}
