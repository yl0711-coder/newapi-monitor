package monitor

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestTokenReportSummaryProjectionMatchesOriginalAndTransactions(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	quota, delta, loss := "922337203685477580712345", "123456789123456789", "987654321987654321"
	result := tokenDetectionResult{Candidate: true, SourceComplete: true, ActualQuota: &quota, FormulaDifference: &delta, EstimatedLoss: &loss,
		Rules: []tokenDetectionRule{{Name: "usage_format", Eligible: true, Hit: true, EvidenceLevel: "exact", Reason: strings.Repeat("unused-summary-reason", 500)}},
		Price: tokenPriceResult{Subject: "wallet", Unit: "USD", QuotaPerUnit: "500000", Reason: "known"}}
	for i := int64(1); i <= 4; i++ {
		r := result
		if i == 2 {
			r.Candidate, r.SourceComplete = false, false
		} else if i == 3 {
			r.Candidate = false
		} else if i == 4 {
			r.Price.Subject, r.Price.Unit = "", ""
			r.FormulaDifference, r.EstimatedLoss = nil, nil
			r.EstimateReason = "unavailable"
		}
		h := TokenDetectionCurrent{SourceEpoch: epoch, LogID: i, CreatedAt: now - 10 + i, UserID: i % 2, ChannelID: i % 3, Model: "", UserGroup: "", Candidate: r.Candidate, SourceComplete: r.SourceComplete, DetectionState: "evaluated", ResultJSON: tokenDetectionJSON(r)}
		if err := db.Create(&h).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	queries := []tokenDetectionQuery{
		{}, {Status: "candidate"}, {Status: "insufficient"}, {Status: "not_hit"},
		{UserSet: true, ModelSet: true, GroupSet: true}, {ChannelID: 2},
		{Rule: "usage_format", Evidence: "exact"}, {Method: "historical_formula_v1"},
		{Method: "input_upper_reference_v1", Unavailable: true},
		{Method: "observed_consumption_not_loss", Subject: "wallet", Unit: "USD", QuotaPerUnit: "500000"},
	}
	verify := func(tx *gorm.DB) {
		t.Helper()
		for _, q := range queries {
			q.SourceEpoch, q.From, q.To = epoch, now-20, now+20
			var original, compact []TokenDetectionCurrent
			if err := tokenDetectionRange(tx, q).Order("created_at,log_id").Find(&original).Error; err != nil {
				t.Fatal(err)
			}
			if err := tokenReportSummaryRange(tx, q).Order("created_at,log_id").Find(&compact).Error; err != nil {
				t.Fatal(err)
			}
			if len(original) != len(compact) {
				t.Fatalf("projection changed filter membership: query=%+v original=%d compact=%d", q, len(original), len(compact))
			}
			for i := range original {
				if original[i].LogID != compact[i].LogID || original[i].CreatedAt != compact[i].CreatedAt {
					t.Fatal("projection changed identity ordering")
				}
			}
			oldSummary, oldErr := tokenDetectionSummarize(original)
			newSummary, newErr := tokenDetectionSummarize(compact)
			oldAnalysis, oldAnalysisErr := tokenReportAnalyze(original)
			newAnalysis, newAnalysisErr := tokenReportAnalyze(compact)
			if oldErr != nil || newErr != nil || oldAnalysisErr != nil || newAnalysisErr != nil || !reflect.DeepEqual(oldSummary, newSummary) || !reflect.DeepEqual(oldAnalysis, newAnalysis) {
				t.Fatalf("projection changed summary/analysis for query=%+v errors=%v/%v/%v/%v", q, oldErr, newErr, oldAnalysisErr, newAnalysisErr)
			}
		}
	}
	verify(db)
	rollback := errors.New("rollback fixture")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=1", epoch).
			Updates(map[string]any{"created_at": now + 5, "user_id": 0, "model": "changed", "result_json": `{"candidate":true,"rules":[],"actual_quota":"17","price":{"subject":"wallet","unit":"USD","quota_per_unit":"3/2"}}`}).Error; err != nil {
			return err
		}
		verify(tx)
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	verify(db)
	if err := db.Exec("UPDATE token_detection_currents SET log_id=11, created_at=? WHERE source_epoch=? AND log_id=1", now+2, epoch).Error; err != nil {
		t.Fatal(err)
	}
	verify(db)
	if err := db.Where("source_epoch=? AND log_id=2", epoch).Delete(&TokenDetectionCurrent{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	verify(db)
	for _, body := range []string{"not-json", `{"rules":"invalid-array-type"}`} {
		if err := db.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=3", epoch).
			Updates(map[string]any{"result_json": body, "source_complete": false}).Error; err != nil {
			t.Fatal(err)
		}
		var projected []TokenDetectionCurrent
		if err := tokenReportSummaryRange(db, tokenDetectionQuery{SourceEpoch: epoch, From: now - 20, To: now + 20}).Find(&projected).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := tokenDetectionSummarize(projected); err == nil {
			t.Fatal("projection hid invalid original evidence")
		}
	}
}
