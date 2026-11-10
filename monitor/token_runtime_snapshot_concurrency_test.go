package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Retain the real payloads, old results and index population while both actual
// workers operate. Only the appended source rows and the passage of source time
// are synthetic; this is a disconnected copy, not production throughput proof.
func TestTokenRuntimeRetainedSnapshotConcurrentProgress(t *testing.T) {
	snapshot := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to a stopped-writer snapshot")
	}
	m, _ := tokenIsolatedRuntimeFixture(t, snapshot)
	var initial TokenMeteringState
	var revision TokenDetectionRevision
	if err := m.tokenStore().First(&initial).Error; err != nil {
		t.Fatal(err)
	}
	if initial.FactCount < 200000 {
		t.Fatal("representative retained snapshot needs at least 200000 facts")
	}
	if err := m.tokenStore().First(&revision).Error; err != nil {
		t.Fatal(err)
	}
	var frozen tokenDetectionFrozen
	if err := json.Unmarshal([]byte(revision.FrozenJSON), &frozen); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenFactsSourceEpoch = initial.SourceEpoch
	m.cfg.TokenFactsSourceMode = initial.SourceMode
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(frozen.Policy)
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 250, 15
	m.cfg.TokenFactsMaxRows, m.cfg.TokenDetectionMaxRows = 1200000, 1200000
	m.cfg.TokenFactsMaxBytes, m.cfg.TokenDetectionMaxBytes = 6<<30, 24<<30
	m.cfg.NginxEvidenceHMACKeyID = "synthetic-concurrency-key"
	// The frozen writer may have held leases at the stop point. Expire them
	// on this disposable copy; defer old replay so the synthetic source cannot
	// claim to validate the snapshot's historical source windows.
	for _, table := range []string{"token_metering_states", "token_detection_states"} {
		if err := m.tokenStore().Table(table).Where("source_epoch=?", initial.SourceEpoch).
			Updates(map[string]any{"lease_until": 0, "lease_token": ""}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.tokenStore().Model(&TokenMeteringWindow{}).Where("source_epoch=? AND start_at<?", initial.SourceEpoch, initial.ScanThrough).
		Update("next_at", time.Now().Add(24*time.Hour).Unix()).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenMeteringWindow{}).Where("source_epoch=? AND start_at>=?", initial.SourceEpoch, initial.ScanThrough).
		Update("next_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	var initialHeads int64
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Where("source_epoch=?", initial.SourceEpoch).Count(&initialHeads).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var syntheticNow atomic.Int64
	syntheticNow.Store(initial.ScanThrough + tokenMeteringLagSeconds + 900)
	workers := make(chan error, 2)
	collectorRounds, detectorRounds, detected, budgetYields := atomic.Int64{}, atomic.Int64{}, atomic.Int64{}, atomic.Int64{}
	startWorker := func(collect bool) {
		go func() {
			for ctx.Err() == nil {
				now := syntheticNow.Load()
				if collect {
					_, err := m.runTokenMeteringRound(ctx, time.Unix(now, 0))
					collectorRounds.Add(1)
					if err != nil && ctx.Err() == nil {
						workers <- fmt.Errorf("retained collector: %w", err)
						return
					}
				} else {
					roundCtx, stop := context.WithTimeout(ctx, 5*time.Second)
					n, err := m.runTokenDetectionRound(roundCtx, time.Now().Unix())
					expired := errors.Is(roundCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
					stop()
					detectorRounds.Add(1)
					detected.Add(int64(n))
					if err != nil && expired && n > 0 && errors.Is(err, context.DeadlineExceeded) {
						// Require the context identity used by the production
						// scheduler, plus the actual deadline and durable release.
						var state TokenDetectionState
						if readErr := m.tokenReadStore().First(&state).Error; readErr != nil || state.LeaseUntil != 0 || state.LeaseToken != "" || state.LastReason != "local_detection_incomplete" {
							workers <- fmt.Errorf("partial detection release failed: %v", readErr)
							return
						}
						budgetYields.Add(1)
					} else if err != nil && ctx.Err() == nil {
						workers <- fmt.Errorf("retained detector processed=%d: %w", n, err)
						return
					}
				}
				select {
				case <-ctx.Done():
				case <-time.After(2 * time.Second):
				}
			}
			workers <- nil
		}()
	}
	startWorker(true)
	startWorker(false)
	defer func() {
		cancel()
		for i := 0; i < 2; i++ {
			if err := <-workers; err != nil {
				t.Error(err)
			}
		}
	}()
	maxRequest := time.Duration(0)
	requests := 0
	call := func(handler func(*gin.Context), method, payload string) []byte {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(method, "/", strings.NewReader(payload))
		c.Request.Header.Set("Content-Type", "application/json")
		start := time.Now()
		handler(c)
		maxRequest = max(maxRequest, time.Since(start))
		requests++
		if w.Code != 200 {
			t.Fatalf("retained concurrent handler status=%d elapsed=%s body=%s", w.Code, time.Since(start), w.Body.String())
		}
		return w.Body.Bytes()
	}
	cycles := 0
	const batches, arrivals = 6, int64(40)
	for batch := 0; batch < batches; batch++ {
		// One new sealed source window every ten wall-clock seconds gives
		// continuous arrivals at four rows/second without modifying real facts.
		start := initial.ScanThrough + int64(batch+3)*300
		tx, err := m.prodDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		for n := int64(0); n < arrivals; n++ {
			id := int64(1_000_000_000) + int64(batch)*arrivals + n
			if _, err := tx.ExecContext(ctx, "INSERT INTO logs(id,created_at,type,user_id,channel_id,model_name,`group`,prompt_tokens,completion_tokens,quota,is_stream,use_time,other,request_id) VALUES(?,?,2,2,3,'synthetic-model','concurrent-fixture',100,10,200,1,1,'{}','')", id, start+1); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		syntheticNow.Store(start + tokenMeteringLagSeconds + 300)
		until := time.Now().Add(10 * time.Second)
		for time.Now().Before(until) {
			call(m.serveTokenMeteringHealth, "GET", "")
			call(m.serveTokenDetectionHealth, "GET", "")
			q := tokenDetectionQuery{SourceEpoch: initial.SourceEpoch, From: 1793086500, To: 1793086560, Limit: 25}
			seen := map[int64]bool{}
			for page := 0; page < 2; page++ {
				var body struct {
					Items []struct{ Request TokenDetectionCurrent }
				}
				if err := json.Unmarshal(call(m.serveTokenDetectionQuery, "POST", tokenDetectionJSON(q)), &body); err != nil || len(body.Items) != 25 {
					t.Fatal("retained pagination", err, len(body.Items))
				}
				for _, item := range body.Items {
					if seen[item.Request.LogID] {
						t.Fatal("duplicate identity across retained pages")
					}
					seen[item.Request.LogID] = true
				}
				last := body.Items[len(body.Items)-1].Request
				q.AfterAt, q.AfterID = last.CreatedAt, last.LogID
				var detail struct {
					DecisionHash string `json:"decision_hash"`
					Evidence     tokenDetectionFrozen
				}
				if err := json.Unmarshal(call(m.serveTokenDetectionDetail, "POST", tokenDetectionJSON(map[string]any{"source_epoch": initial.SourceEpoch, "log_id": last.LogID, "revision": last.Revision})), &detail); err != nil || detail.DecisionHash != last.DecisionHash || tokenDetectionHash(detail.Evidence) != last.DecisionHash {
					t.Fatal("retained frozen detail changed while workers run", err)
				}
			}
			cycles++
			time.Sleep(time.Second)
		}
		var state TokenMeteringState
		var heads int64
		if err := m.tokenReadStore().First(&state).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("source_epoch=?", initial.SourceEpoch).Count(&heads).Error; err != nil {
			t.Fatal(err)
		}
		t.Logf("retained concurrent batch=%d facts=%d heads=%d without_results=%d scan_through=%d target=%d", batch, state.FactCount, heads, state.FactCount-heads, state.ScanThrough, start+300)
		if batch == batches-1 {
			if state.FactCount != initial.FactCount+batches*arrivals || state.ScanThrough != start+300 || heads <= initialHeads+batches*arrivals {
				t.Fatalf("retained workers failed to catch up with appended source: initial facts/heads=%d/%d final=%d/%d scan=%d target=%d", initial.FactCount, initialHeads, state.FactCount, heads, state.ScanThrough, start+300)
			}
		}
	}
	t.Logf("retained real facts=%d appended=%d collector_rounds=%d detector_rounds=%d processed=%d resumed_rounds=%d API_cycles=%d requests=%d max_request=%s", initial.FactCount, batches*arrivals, collectorRounds.Load(), detectorRounds.Load(), detected.Load(), budgetYields.Load(), cycles, requests, maxRequest)
}
