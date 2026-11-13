package monitor

import (
	"strings"
	"testing"

	"gorm.io/gorm"
)

// A one-minute page must remain a bounded range lookup as other days and
// source epochs accumulate. The old epoch-only/candidate-only indexes read
// every large result_json before applying the selected time range.
func TestTokenDetectionReportUsesBoundedTimeIndexes(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for _, status := range []string{"", "candidate", "insufficient", "not_hit"} {
		t.Run("status_"+status, func(t *testing.T) {
			q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 120, To: now - 60, Status: status}
			var rows []TokenDetectionCurrent
			tx := tokenDetectionRange(m.tokenStore().Session(&gorm.Session{DryRun: true}), q).
				Order("created_at, log_id").Limit(26).Find(&rows)
			var plan []struct{ Detail string }
			if err := m.tokenStore().Raw("EXPLAIN QUERY PLAN "+tx.Statement.SQL.String(), tx.Statement.Vars...).Scan(&plan).Error; err != nil {
				t.Fatal(err)
			}
			ranged := false
			for _, row := range plan {
				if strings.Contains(row.Detail, "TEMP B-TREE FOR ORDER BY") {
					t.Fatalf("unbounded report sort: %+v", plan)
				}
				if strings.Contains(row.Detail, "SEARCH token_detection_currents") {
					ranged = strings.Contains(row.Detail, "source_epoch=?") && strings.Contains(row.Detail, "created_at>?") && strings.Contains(row.Detail, "created_at<?")
				}
			}
			if !ranged {
				t.Fatalf("report reads results outside the selected window: %+v", plan)
			}
		})
	}
}

func TestTokenDetectionPriorSignalsUseCoveredScope(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	var count int64
	tx := m.tokenStore().Session(&gorm.Session{DryRun: true}).Model(&TokenDetectionCurrent{}).
		Where("source_epoch = ? AND channel_id = ? AND bucket = ? AND created_at >= ? AND created_at < ? AND cache_signal = ?", m.cfg.TokenFactsSourceEpoch, 5, "bucket", now-3600, now, true).Count(&count)
	var plan []struct{ Detail string }
	if err := m.tokenStore().Raw("EXPLAIN QUERY PLAN "+tx.Statement.SQL.String(), tx.Statement.Vars...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	if len(plan) != 1 || !strings.Contains(plan[0].Detail, "USING COVERING INDEX") || !strings.Contains(plan[0].Detail, "cache_signal=?") || !strings.Contains(plan[0].Detail, "created_at>?") {
		t.Fatalf("prior-signal counting reads large result rows: %+v", plan)
	}
}

func TestTokenReferenceLookupUsesUserModelRange(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	if err := initTokenDetectionReferenceIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	detectionSeed(t, m, now)
	f := detectionFact(t, m, 99, now-60, 9, 100000, 0, 0)
	// Unrelated traffic shares the interval and size but must not be read
	// through an epoch-wide payload scan to find this user's controls.
	for id := int64(100); id < 140; id++ {
		other := f
		other.LogID, other.CreatedAt, other.ChannelID = id, now-100, 2
		if id%2 == 0 {
			other.UserID++
		} else {
			other.Model = "other-model"
		}
		if err := m.tokenStore().Create(&other).Error; err != nil {
			t.Fatal(err)
		}
	}
	p := detectionPolicy()
	var rows []TokenMeteringFact
	q := tokenReferenceCandidateQuery(m.tokenStore(), f, p, f.CreatedAt-p.LookbackSeconds, f.CreatedAt).
		Session(&gorm.Session{DryRun: true}).Find(&rows)
	var plan []struct{ Detail string }
	if err := m.tokenStore().Raw("EXPLAIN QUERY PLAN "+q.Statement.SQL.String(), q.Statement.Vars...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	ranged := false
	for _, step := range plan {
		if strings.Contains(step.Detail, "SEARCH f ") {
			ranged = strings.Contains(step.Detail, tokenReferenceCandidateIndex) && strings.Contains(step.Detail, "source_epoch=? AND user_id=? AND model=? AND user_group=? AND created_at>?")
		}
	}
	if !ranged {
		t.Fatalf("reference lookup scans unrelated user/model payloads: %+v", plan)
	}
	in, err := tokenDetectionInputOf(f)
	if err != nil {
		t.Fatal(err)
	}
	indexed, err := tokenReferenceFrom(m.tokenStore(), in, p, 0, now)
	if err != nil || indexed.Status != "statistical" || len(indexed.Samples) != 3 {
		t.Fatal("scoped controls", indexed, err)
	}
	// Legacy stores without the index still return identical frozen operands.
	if err := m.tokenStore().Migrator().DropIndex(&TokenMeteringFact{}, tokenReferenceCandidateIndex); err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Migrator().DropIndex(&TokenMeteringFact{}, "idx_token_reference_input"); err != nil {
		t.Fatal(err)
	}
	legacy, err := tokenReferenceFrom(m.tokenStore(), in, p, 0, now)
	if err != nil || tokenDetectionJSON(legacy) != tokenDetectionJSON(indexed) {
		t.Fatal("reference changed with access path", err)
	}
}
