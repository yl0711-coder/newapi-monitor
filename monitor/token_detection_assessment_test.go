package monitor

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/metering"
)

func TestTokenFrozenAssessmentSeparatesLocalFormulaAndUpstreamTruth(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	fact := detectionFact(t, m, 1, now-600, 3, 20000, 0, 17)
	in, err := tokenDetectionInputOf(fact)
	if err != nil {
		t.Fatal(err)
	}
	frozen := tokenDetectionFrozen{Input: in, Policy: detectionPolicy()}
	frozen.Result = tokenDetectionEvaluate(in, frozen.Policy, tokenReference{Reason: "reference_insufficient"}, 0, true)
	a := tokenAssessFrozen(frozen)
	if a.State != "local_metering_difference" || !a.LocalDifferenceConfirmed || a.UpstreamConfirmed || a.FormulaDifference == nil || *a.FormulaDifference != "17" || a.QuotaFrom == nil || *a.QuotaFrom != *a.QuotaTo {
		t.Fatalf("local formula confirmation: %+v", a)
	}
	if a.Input == nil || *a.Input != "20000" || *a.RequestLength != "1000" || a.LengthMeaning != "whole_http_request_not_body_or_token_count" {
		t.Fatal("bytes confused with tokens", a)
	}
	for _, change := range []func(*tokenDetectionFrozen){
		func(f *tokenDetectionFrozen) { f.Input.Fact.RequestLength = nil; f.Input.Fact.LengthState = "missing" },
		func(f *tokenDetectionFrozen) { f.Input.Fact.LinkState = "ambiguous" },
		func(f *tokenDetectionFrozen) { f.Result.SourceComplete = false },
	} {
		f := frozen
		change(&f)
		got := tokenAssessFrozen(f)
		if got.State != "local_metering_difference" || !got.LocalDifferenceConfirmed || got.UpstreamConfirmed || len(got.Missing) == 0 {
			t.Fatal("unrelated gap blocked complete formula operands", got)
		}
	}
	inconsistent := frozen
	inconsistent.Result.Price.Quota = "0"
	if got := tokenAssessFrozen(inconsistent); got.State != "log_formula_difference" || got.LocalDifferenceConfirmed || got.UpstreamConfirmed || len(got.Missing) == 0 {
		t.Fatal("unreproduced saved formula confirmed", got)
	}
	frozen.Input.Usage.Fields["completion_tokens"] = metering.Field{State: "missing"}
	a = tokenAssessFrozen(frozen)
	if a.State != "partially_evaluated" || a.Output != nil || a.QuotaFrom != nil || a.FormulaDifference != nil || a.LocalDifferenceConfirmed || !slices.Contains(a.Missing, "completion_tokens:missing") {
		t.Fatal("missing output became zero", a)
	}
}

func TestTokenFrozenAssessmentPreservesZeroSignedDifferenceAndStatistics(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for _, delta := range []int64{0, -17} {
		fact := detectionFact(t, m, 20-delta, now-600, 3, 20000, 0, delta)
		in, err := tokenDetectionInputOf(fact)
		if err != nil {
			t.Fatal(err)
		}
		f := tokenDetectionFrozen{Input: in, Policy: detectionPolicy()}
		f.Result = tokenDetectionEvaluate(in, f.Policy, tokenReference{Reason: "reference_insufficient"}, 0, true)
		before := tokenDetectionHash(f)
		a := tokenAssessFrozen(f)
		want := "local_formula_consistent"
		if delta < 0 {
			want = "local_metering_difference"
		}
		if a.State != want || a.FormulaDifference == nil || f.Result.FormulaDifference == nil || *a.FormulaDifference != *f.Result.FormulaDifference || a.UpstreamConfirmed || tokenDetectionHash(f) != before {
			t.Fatal("signed formula assessment changed frozen evidence", a)
		}
		// Only an eligible statistical rule makes a statistical candidate. A
		// saved formula hit lacking its operands cannot borrow that label.
		if delta == 0 {
			f.Result = tokenDetectionEvaluate(in, f.Policy, tokenReference{Status: "statistical", SampleCount: 3, Upper: ptrTokenMetering(1000)}, 0, true)
			a = tokenAssessFrozen(f)
			if a.State != "statistical_candidate" || a.LocalDifferenceConfirmed || a.ReferenceDifference == nil || *a.ReferenceDifference != "19000" || a.UpstreamConfirmed {
				t.Fatal("statistical bound became an upstream confirmation", a)
			}
			f.Input.Usage.Fields["quota"] = metering.Field{State: "valid", Value: "0"}
			f.Result = tokenDetectionEvaluate(f.Input, f.Policy, tokenReference{Reason: "reference_insufficient"}, 0, true)
			a = tokenAssessFrozen(f)
			if a.ActualQuota == nil || *a.ActualQuota != "0" || a.State != "local_metering_difference" || !a.LocalDifferenceConfirmed || a.UpstreamConfirmed {
				t.Fatal("observed zero quota became missing", a)
			}
		}
	}
}

func TestTokenDetectionHealthMissingCheckpointAndReadFailureAreUnknown(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	call := func() map[string]any {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/health", nil)
		m.serveTokenDetectionHealth(c)
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != 200 {
			t.Fatal(w.Code, err)
		}
		return body
	}
	h := call()
	if h["available"] != false || h["current_results"] != nil || h["pending_sample_count"] != nil || !strings.Contains(tokenDetectionJSON(h), "no_detection_checkpoint") {
		t.Fatal("missing detector looked healthy", h)
	}
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if _, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if h = call(); h["available"] != true || h["current_results"] != float64(1) {
		t.Fatal("committed health unavailable", h)
	}
	if err := m.storeDB.Migrator().DropTable(&TokenDetectionState{}); err != nil {
		t.Fatal(err)
	}
	if h = call(); h["available"] != false || h["current_results"] != nil {
		t.Fatal("read failure became zero", h)
	}
}

func TestTokenDetectionHealthScopesResultsAndStatesToConfiguredEpoch(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if _, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	// A retained store can contain another source epoch after a source
	// rotation.  It contributes to the shared capacity budget, but must not
	// appear in the selected epoch's result count or state breakdown.
	if err := m.storeDB.Create(&TokenDetectionCurrent{
		SourceEpoch:    "rotated-epoch",
		LogID:          99,
		CreatedAt:      now - 600,
		DetectionState: "confirmed",
	}).Error; err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)
	m.serveTokenDetectionHealth(c)
	if w.Code != 200 {
		t.Fatalf("health status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		CurrentResults  int64            `json:"current_results"`
		DetectionStates map[string]int64 `json:"detection_states"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.CurrentResults != 1 {
		t.Fatalf("cross-epoch current result leaked into health: %+v", body)
	}
	if body.DetectionStates["confirmed"] != 0 {
		t.Fatalf("cross-epoch detection state leaked into health: %+v", body.DetectionStates)
	}
}

func TestTokenDetectionHealthKeepsCheckpointDuringProjectionMigration(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if _, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Exec("UPDATE token_report_state_migration_v1 SET ready=0 WHERE id=1").Error; err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)
	m.serveTokenDetectionHealth(c)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["available"] != true || body["queue_ready"] != false || body["policy_configured"] != true ||
		body["current_results"] != float64(1) || body["state"] == nil || body["round"] == nil ||
		!strings.Contains(w.Body.String(), "detection_queue_rebuilding") {
		t.Fatal("projection repair hid committed progress or looked complete", body)
	}
}

func TestTokenDetectionStateIndexesUpgradeExistingHeadAndCoverHealth(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	// Reproduce the retained nine-column index: adding a GORM tag does not
	// upgrade its already existing definition.
	for _, sql := range []string{
		"DROP INDEX idx_token_detection_work_head_v2",
		"DROP INDEX idx_token_detection_work_head",
		"CREATE INDEX idx_token_detection_work_head ON token_detection_currents(source_epoch,log_id,fact_revision,link_revision,policy_hash,reference_generation,reference_bucket,source_complete,next_at)",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := initTokenDetectionStateIndexes(db); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tokenDetectionHeadJoin(db), "idx_token_detection_work_head_v2") {
		t.Fatal("queue still selects the obsolete noncovering index")
	}
	for _, sql := range []string{
		"SELECT detection_state,COUNT(*) FROM token_detection_currents INDEXED BY idx_token_detection_epoch_state_v1 WHERE source_epoch='epoch' GROUP BY detection_state",
		"SELECT c.detection_state,c.fact_revision,c.link_revision FROM " + tokenDetectionHeadTable(db) + " WHERE c.source_epoch='epoch' AND c.log_id=1",
		"SELECT reason,COUNT(*) FROM " + tokenDetectionRetryTable(db) + " WHERE source_epoch='epoch' GROUP BY reason",
	} {
		var rows []struct{ Detail string }
		if err := db.Raw("EXPLAIN QUERY PLAN " + sql).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		covering := false
		for _, row := range rows {
			covering = covering || strings.Contains(row.Detail, "USING COVERING INDEX")
			if strings.Contains(row.Detail, "TEMP B-TREE") {
				t.Fatal("health grouping still sorts retained heads", rows)
			}
		}
		if !covering {
			t.Fatal("large result payload fetch remains in the health/queue plan", rows)
		}
	}
}

func TestTokenDetectionRetryHealthCountsUseOneEpochAndCoveringIndex(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if _, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	for _, row := range []TokenDetectionRetry{
		{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 2, NextAt: now + 60, Reason: "association_pending"},
		{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 3, NextAt: now + 60, Reason: "local_capacity_exhausted"},
		{SourceEpoch: "other-epoch", LogID: 2, NextAt: now + 60, Reason: "association_pending"},
	} {
		if err := m.tokenStore().Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)
	m.serveTokenDetectionHealth(c)
	var body struct {
		Available bool             `json:"available"`
		Deferred  int64            `json:"deferred_requests"`
		Reasons   map[string]int64 `json:"retry_reasons"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Available || body.Deferred != 2 || body.Reasons["association_pending"] != 1 || body.Reasons["local_capacity_exhausted"] != 1 {
		t.Fatal("health mixed epochs or lost retry groups", body, w.Body.String())
	}
	// Old read-only snapshots still expose their real retry counts.
	if err := m.tokenStore().Exec("DROP INDEX idx_token_detection_epoch_retry_reason_v1").Error; err != nil {
		t.Fatal(err)
	}
	rows, err := tokenDetectionRetryCounts(m.tokenStore(), m.cfg.TokenFactsSourceEpoch)
	if err != nil || len(rows) != 2 {
		t.Fatal("legacy snapshot retry health failed", rows, err)
	}
}
