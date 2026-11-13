package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// The retained full-database workload covers history scale separately. This
// bounded disk fixture starts exactly full, retains a 64-MiB reference working
// set, and exercises the real scheduler across two source seal boundaries.
func TestTokenRuntimeFullCapacityScheduledRecovery(t *testing.T) {
	tokenRuntimeFullCapacityScheduledRecovery(t, false)
}

func TestTokenRuntimeFullCapacityComparableScheduledRecovery(t *testing.T) {
	tokenRuntimeFullCapacityScheduledRecovery(t, true)
}

func TestTokenRuntimeDenseHistoryAndComparableArrivals(t *testing.T) {
	tokenRuntimeFullCapacityScheduledRecovery(t, true, true)
}

func tokenRuntimeFullCapacityScheduledRecovery(t *testing.T, comparable bool, history ...bool) {
	if os.Getenv("TOKEN_TEST_CAPACITY_WORKLOAD") != "1" {
		t.Skip("set TOKEN_TEST_CAPACITY_WORKLOAD=1 for the nine-minute disk workload")
	}
	m, now := tokenIsolatedRuntimeFixture(t, "")
	const capacity, rate = int64(8192), int64(8)
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 250, 15
	m.cfg.TokenDetectionMaxRows = capacity
	denseHistory := len(history) > 0 && history[0]
	if denseHistory {
		// The isolated fixture now contains actual retained facts and stale
		// heads, not just expired placeholders. These remain below the running
		// store's existing supported row/byte limits; production settings do not
		// change. Keep the same CPU, memory, work and source-query budgets.
		m.cfg.TokenDetectionMaxRows, m.cfg.TokenDetectionMaxBytes = 120000, 1<<30
	}
	m.cfg.TokenFactsRetentionDays, m.cfg.TokenDetectionRetentionDays = 4, 4
	m.cfg.TokenDetectionRetentionBasis = "first_evaluated"
	start := (now-tokenMeteringLagSeconds)/300*300 - 900
	if err := m.tokenStore().Create(&TokenMeteringState{SourceEpoch: m.cfg.TokenFactsSourceEpoch, SourceMode: "complete", StartAt: start, ScanThrough: start}).Error; err != nil {
		t.Fatal(err)
	}
	tokenCleanupExpiredHeads(t, m, now, capacity)
	// Start with the same small learned batch as a restarted process.
	m.tokenDetectionCleanupRows.Store(0)
	payload := `{"fixture":"` + strings.Repeat("x", 64<<10) + `"}`
	for batch := 0; batch < 32; batch++ {
		refs := make([]TokenDetectionReference, 0, 32)
		for i := 0; i < 32; i++ {
			refs = append(refs, TokenDetectionReference{ID: fmt.Sprintf("retained-%d", batch*32+i), Bucket: fmt.Sprintf("retained-%d", batch*32+i), Through: start, ExpiresAt: now + 86400, SnapshotJSON: payload, StoredBytes: int64(len(payload))})
		}
		if err := m.tokenStore().Create(&refs).Error; err != nil {
			t.Fatal(err)
		}
	}
	for window := int64(0); window < 6; window++ {
		count := rate * 300
		if window < 3 {
			count = 512
		}
		rows := make([]NginxRequestEvidence, 0, count)
		for offset := int64(1); offset <= count; offset++ {
			id := int64(1_000_000_000) + window*10000 + offset
			rows = append(rows, NginxRequestEvidence{EventID: fmt.Sprintf("capacity-association-%d", id), Node: "capacity-fixture",
				EventMS: (start + window*300 + 1) * 1000, HMACKeyID: m.cfg.NginxEvidenceHMACKeyID,
				OneAPIIDHMAC: nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", fmt.Sprintf("synthetic-%d", id)),
				Status:       200, Route: "/v1/messages", RequestLength: ptrTokenMetering(1000), RequestLengthState: "valid", RequestLengthVersion: "request-length-v1"})
		}
		if err := m.nginxEvidenceDB.CreateInBatches(rows, 100).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := initTokenMeteringHealthCounts(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	source := &tokenWorkloadSource{start: start, windows: map[int64]int64{start: 512, start + 300: 512, start + 600: 512}, payload: `{"claude":true,"cache_tokens":0,"cache_creation_tokens":0}`}
	if comparable {
		// 100 input + 10 output*10 = the source's actual quota of 200.
		// Every row has comparable evidence and therefore exercises reference
		// invalidation; missing-semantic fixtures cannot cover that workload.
		source.payload = tokenDetectionJSON(map[string]any{"claude": true, "usage_semantic": "anthropic", "cache_tokens": 0, "cache_creation_tokens": 0, "cache_creation_tokens_5m": 0, "cache_creation_tokens_1h": 0, "cache_ratio": "0.1", "cache_creation_ratio": "1.25", "cache_creation_ratio_5m": "1.25", "cache_creation_ratio_1h": "2", "model_ratio": "1", "model_price": 0, "completion_ratio": 10, "effective_group_ratio": 1, "group_ratio": 1, "billing_mode": "ratio", "billing_source": "wallet", "currency": "USD", "quota_per_unit": 500000, "rounding_mode": "half_up", "image": false, "is_task": false, "request_rules": []any{}, "request_path": "/v1/messages", "is_stream": false, "is_model_mapped": false})
	}
	if denseHistory {
		tokenRuntimeSeedDenseHistory(t, m, source.payload, start, now)
	}
	m.prodDB = sql.OpenDB(source)
	m.prodDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	wallStart := time.Now()
	sourceNow := func() int64 {
		return start + 900 + tokenMeteringLagSeconds + 280 + int64(time.Since(wallStart)/time.Second)
	}
	ctx, cancel := context.WithCancel(context.Background())
	collectorDone, arrivalsDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(arrivalsDone)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				at := (sourceNow() - tokenMeteringLagSeconds) / 300 * 300
				source.mu.Lock()
				source.windows[at] += rate
				source.mu.Unlock()
			}
		}
	}()
	go func() {
		defer close(collectorDone)
		for ctx.Err() == nil {
			pages, err := m.runTokenMeteringRound(ctx, time.Unix(sourceNow(), 0))
			if !waitSourceLifecycle(ctx, m.tokenMeteringRoundDelay(ctx, pages, err, sourceNow())) {
				return
			}
		}
	}()
	m.startTokenMeteringMaintenanceAt(ctx, func() time.Time { return time.Unix(sourceNow(), 0) })
	defer func() {
		t.Log("capacity workload stopping; subsequent context cancellation is teardown")
		cancel()
		<-collectorDone
		<-arrivalsDone
		time.Sleep(6 * time.Second)
	}()
	requests := 0
	var maxLatency time.Duration
	call := func(handler func(*gin.Context), body string) []byte {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/", strings.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		began := time.Now()
		handler(c)
		requests++
		maxLatency = max(maxLatency, time.Since(began))
		var response struct{ Available bool }
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 || !response.Available {
			t.Fatal("local API unavailable during full-capacity recovery", w.Code, w.Body.String(), err)
		}
		return w.Body.Bytes()
	}
	for sample := 0; sample < 18; sample++ {
		until := wallStart.Add(time.Duration(sample+1) * 30 * time.Second)
		for time.Now().Before(until) {
			// Let the first real round publish its checkpoint before health.
			if time.Since(wallStart) > 5*time.Second {
				call(m.serveTokenMeteringHealth, "")
				call(m.serveTokenDetectionHealth, "")
				call(m.serveTokenDetectionQuery, tokenDetectionJSON(tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: start, To: start + 900, Limit: 25}))
			}
			time.Sleep(2 * time.Second)
		}
		var state TokenMeteringState
		var facts, heads, oldHeads, retries int64
		if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
			for _, err := range []error{
				tx.First(&state).Error,
				tx.Model(&TokenMeteringFact{}).Count(&facts).Error,
				tx.Model(&TokenDetectionCurrent{}).Where("log_id>=1000000000").Count(&heads).Error,
				tx.Model(&TokenDetectionCurrent{}).Where("log_id<1000000000").Count(&oldHeads).Error,
				tx.Model(&TokenDetectionRetry{}).Count(&retries).Error,
			} {
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		t.Logf("capacity sample=%d elapsed=%s facts=%d detected=%d old_heads=%d retries=%d scan=%d target=%d reason=%s", sample, time.Since(wallStart), facts, heads, oldHeads, retries, state.ScanThrough, (sourceNow()-tokenMeteringLagSeconds)/300*300, state.LastReason)
		if comparable && sample%4 == 3 {
			tokenRuntimePendingDiagnostics(t, m, m.cfg.TokenFactsSourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), sourceNow(), state.ScanThrough)
		}
	}
	// Include the second seal reached during this run, after allowing the
	// worker less than one source interval to finish it. Stopping immediately
	// after that seal only proves the preceding window, not ongoing recovery.
	settledTo := start + 1500
	source.mu.Lock()
	var expected int64
	for from, n := range source.windows {
		if from < settledTo {
			expected += n
		}
	}
	source.mu.Unlock()
	var summary struct{ Summary tokenDetectionSummary }
	if err := json.Unmarshal(call(m.serveTokenDetectionQuery, tokenDetectionJSON(tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: start, To: settledTo, Limit: 25})), &summary); err != nil {
		t.Fatal(err)
	}
	var facts, unique, heads, pending, failed int64
	if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
		for _, err := range []error{
			tx.Model(&TokenMeteringFact{}).Where("created_at>=? AND created_at<?", start, settledTo).Count(&facts).Error,
			tx.Model(&TokenMeteringFact{}).Where("created_at>=? AND created_at<? AND link_state='unique'", start, settledTo).Count(&unique).Error,
			tx.Model(&TokenDetectionCurrent{}).Where("created_at>=? AND created_at<?", start, settledTo).Count(&heads).Error,
			tx.Table(tokenReportPendingMembers+" AS g").Where("g.created_at>=? AND g.created_at<?", start, settledTo).Where(tokenReportPendingPredicate("g"), tokenDetectionPolicyHash(detectionPolicy())).Count(&pending).Error,
			tx.Model(&TokenMeteringWindow{}).Where("done=0 AND last_reason<>''").Count(&failed).Error,
		} {
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("fixed_capacity_cohort source=%d facts=%d unique=%d detected=%d pending=%d failed_windows=%d api=%+v requests=%d max_latency=%s", expected, facts, unique, heads, pending, failed, summary.Summary, requests, maxLatency)
	if expected != facts || unique != facts || heads != facts || pending != 0 || failed != 0 || summary.Summary.Observed != expected || summary.Summary.Unprocessed != 0 || !summary.Summary.SourceComplete {
		t.Fatal("full capacity did not recover the fixed source cohort")
	}
	if comparable {
		var scoped int64
		if err := m.tokenReadStore().Model(&TokenDetectionBucket{}).Where("parent_bucket<>''").Count(&scoped).Error; err != nil || scoped == 0 {
			t.Fatalf("comparable workload did not exercise scoped reference generations: %d %v", scoped, err)
		}
		if summary.Summary.Candidates != 0 {
			t.Fatal("valid synthetic formula unexpectedly quarantined the comparable population")
		}
		t.Logf("comparable reference scopes=%d", scoped)
	}
}
