package monitor

import (
	"testing"
)

func TestTokenDetectionFreshBacklogMakesProgressAtBothEnds(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	base := detectionFact(t, m, 1, now-86400, 3, 20000, 0, 0)
	for id := int64(2); id <= 80; id++ {
		f := base
		f.LogID, f.CreatedAt = id, now-86400+id
		if id > 70 {
			f.CreatedAt = now - 300 + id
		}
		if err := m.tokenStore().Create(&f).Error; err != nil {
			t.Fatal(err)
		}
	}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	check := func() {
		t.Helper()
		for rotation := int64(0); rotation < 2; rotation++ {
			jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), base.SourceEpoch, hash, now+rotation*15, 16, 15)
			if err != nil || len(jobs) != 16 {
				t.Fatal("bounded fresh batch", len(jobs), err)
			}
			old, recent := 0, 0
			seen := map[int64]bool{}
			for _, job := range jobs {
				if seen[job.LogID] {
					t.Fatal("overlapping queue ends duplicated a request")
				}
				seen[job.LogID] = true
				if job.LogID > 70 {
					recent++
				} else {
					old++
				}
			}
			if old != 6 || recent != 10 || !seen[1] || !seen[71] || !seen[80] {
				t.Fatal("old backlog or recent collection starved", old, recent, jobs)
			}
		}
		var single []int64
		for rotation := int64(0); rotation < 4; rotation++ {
			jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), base.SourceEpoch, hash, now+rotation*15, 1, 15)
			if err != nil || len(jobs) != 1 {
				t.Fatal("single-job budget", err)
			}
			single = append(single, jobs[0].LogID)
		}
		seenOld, seenLatest := false, false
		for _, id := range single {
			seenOld = seenOld || id == 1
			seenLatest = seenLatest || id >= 71
		}
		if !seenOld || !seenLatest {
			t.Fatal("single-job budget starved one end across restarts", single)
		}
	}
	check() // Compatibility stores use the same scheduling policy.
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	check()
	if err := m.tokenStore().Transaction(initTokenDetectionReportIndex); err != nil {
		t.Fatal(err)
	}
	check() // Reopened projections do not reset or hide either end.
}
