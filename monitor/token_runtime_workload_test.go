package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Only explicitly registered synthetic windows have a complete source in this
// fixture. Unknown history fails; an empty fake EOF must never certify the real
// snapshot's missing historical source records.
type tokenWorkloadSource struct {
	mu             sync.Mutex
	start          int64
	windows        map[int64]int64
	partial        bool
	partialDone    bool
	queries        int
	historyFail    int
	historyTimeout time.Duration
	payload        string
}

func (s *tokenWorkloadSource) Connect(context.Context) (driver.Conn, error) {
	return &tokenWorkloadConn{s}, nil
}
func (s *tokenWorkloadSource) Driver() driver.Driver { return s }
func (s *tokenWorkloadSource) Open(string) (driver.Conn, error) {
	return &tokenWorkloadConn{s}, nil
}

type tokenWorkloadConn struct{ source *tokenWorkloadSource }

func (*tokenWorkloadConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*tokenWorkloadConn) Close() error                        { return nil }
func (*tokenWorkloadConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *tokenWorkloadConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 7 || !strings.Contains(query, "MAX_EXECUTION_TIME("+strconv.Itoa(tokenMeteringSourceExecutionMS)+")") {
		return nil, errors.New("fixture received an unbudgeted source query")
	}
	from, to, cursorAt, cursor, limit := args[1].Value.(int64), args[2].Value.(int64), args[3].Value.(int64), args[5].Value.(int64), args[6].Value.(int64)
	deadline, ok := ctx.Deadline()
	if to-from != 300 || limit < 8 || limit > 256 || !ok || time.Until(deadline) > 4*time.Second+100*time.Millisecond {
		return nil, errors.New("fixture received changed source limits")
	}
	s := c.source
	s.mu.Lock()
	s.queries++
	count, known := s.windows[from]
	partial := known && from == s.start && s.partial && !s.partialDone
	if partial {
		s.partialDone = true
	}
	if !known {
		s.historyFail++
	}
	s.mu.Unlock()
	if !known {
		if s.historyTimeout > 0 {
			// Focused scheduling tests inject a shorter source timeout so race
			// instrumentation cannot turn it into exhaustion of the whole round.
			// The retained-scale workload leaves this unset and uses the actual
			// four-second transport deadline supplied by the collector.
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.historyTimeout)
			defer cancel()
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	base := int64(1_000_000_000) + (from-s.start)/300*10000
	next := max(base+1, cursor+1)
	if cursorAt > from+1 {
		next = base + count + 1
	}
	return &tokenWorkloadRows{ctx: ctx, source: s, at: from + 1, next: next, end: min(base+count, next+limit-1), partial: partial}, nil
}

type tokenWorkloadRows struct {
	ctx      context.Context
	source   *tokenWorkloadSource
	at, next int64
	end      int64
	emitted  int
	partial  bool
}

func (*tokenWorkloadRows) Columns() []string { return (&tokenTransportRows{}).Columns() }
func (*tokenWorkloadRows) Close() error      { return nil }
func (r *tokenWorkloadRows) Next(dest []driver.Value) error {
	if r.partial && r.emitted == 8 {
		<-r.ctx.Done()
		return r.ctx.Err()
	}
	if r.next > r.end {
		return io.EOF
	}
	copy(dest, []driver.Value{r.next, r.at, int64(2), int64(3), "synthetic-model", "scheduler-fixture", "100", "10", "200", "1", "1", r.source.payload, int64(len(r.source.payload)), fmt.Sprintf("synthetic-%d", r.next)})
	r.next++
	r.emitted++
	return nil
}

func TestTokenMeteringReplayFailureDoesNotIdleDurableForwardProgress(t *testing.T) {
	for _, replayFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay_first_%t", replayFirst), func(t *testing.T) {
			// Exercise the injected replay timeout independently of host disk
			// speed, which may legitimately make a forward page yield early.
			synctest.Test(t, func(t *testing.T) {
				tokenMeteringCheckReplayFailureProgress(t, replayFirst)
			})
		})
	}
}

func tokenMeteringCheckReplayFailureProgress(t *testing.T, replayFirst bool) {
	t.Helper()
	m, now, _ := tokenMeteringHistoryFixture(t)
	state := tokenMeteringStateRead(t, m)
	if replayFirst {
		m.tokenMeteringRounds.Store(3)
	}
	source := &tokenWorkloadSource{start: state.StartAt, windows: map[int64]int64{state.StartAt: 1024}, payload: `{}`, historyTimeout: 100 * time.Millisecond}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	if err := m.tokenStore().Create(&TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: state.StartAt - 300, EndAt: state.StartAt, CursorAt: state.StartAt - 300, Replay: true, LastReason: "source_read_failed"}).Error; err != nil {
		t.Fatal(err)
	}
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	state = tokenMeteringStateRead(t, m)
	if pages == 0 || state.FactCount == 0 || !errors.Is(err, context.DeadlineExceeded) || state.ScanThrough != state.StartAt {
		t.Fatalf("missing durable forward prefix and failed replay: pages=%d facts=%d err=%v", pages, state.FactCount, err)
	}
	func() {
		// A successful failure checkpoint already owns a committed frontier.
		// Maintenance taking the writer next must not add another global wait.
		m.tokenLocalWriteMu.Lock()
		defer m.tokenLocalWriteMu.Unlock()
		if delay := m.tokenMeteringRoundDelay(context.Background(), pages, err, now); delay != 2*time.Second {
			t.Fatalf("one replay timeout idles unrelated durable forward progress: facts=%d delay=%s", state.FactCount, delay)
		}
	}()
	// Even a round whose replay spends all remaining budget before the next
	// forward commit must consult persisted eligibility, not sleep globally.
	if delay := m.tokenMeteringRoundDelay(context.Background(), 0, err, now); delay != 2*time.Second {
		t.Fatalf("replay exhaustion idles a runnable forward window: %s", delay)
	}
	var failed TokenMeteringWindow
	if err := m.tokenStore().First(&failed, "source_epoch=? AND start_at=?", state.SourceEpoch, state.StartAt-300).Error; err != nil {
		t.Fatal(err)
	}
	if failed.Attempts != 1 || failed.NextAt < now+30 || failed.NextAt > now+37 || failed.Done || failed.SourceComplete || !failed.Replay {
		t.Fatalf("failed replay lost its independent backoff or coverage: %+v", failed)
	}
	var forward TokenMeteringWindow
	if err := m.tokenStore().First(&forward, "source_epoch=? AND start_at=?", state.SourceEpoch, state.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	if failed.ReadPageRows != 128 || forward.ReadPageRows != 256 || state.ReadPageRows != 256 {
		t.Fatalf("replay throttled healthy forward pages: replay=%d forward=%d preferred=%d", failed.ReadPageRows, forward.ReadPageRows, state.ReadPageRows)
	}
	if source.historyFail != 1 {
		t.Fatal("same failed window was retried before its next_at")
	}
}

func TestTokenMeteringMixedFailureDelayKeepsLocalFailureAndWindowBackoff(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	readFailure := &tokenMeteringReadError{err: context.DeadlineExceeded, queryStarted: true}
	for _, tc := range []struct {
		name  string
		pages int
		err   error
	}{
		{"local_failure_after_source_failure", 1, errors.Join(readFailure, errors.New("local commit failed"))},
		{"gate_timeout", 1, &tokenMeteringReadError{err: context.DeadlineExceeded}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := m.tokenMeteringRoundDelay(context.Background(), tc.pages, tc.err, now); got != time.Minute {
				t.Fatalf("unexpected accelerated failure retry: %s", got)
			}
		})
	}
	state := tokenMeteringStateRead(t, m)
	if err := m.tokenStore().Create(&TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: state.ScanThrough, EndAt: state.ScanThrough + 300, NextAt: now + 30, LastReason: "source_read_failed"}).Error; err != nil {
		t.Fatal(err)
	}
	if got := m.tokenMeteringRoundDelay(context.Background(), 1, readFailure, now); got > 10*time.Second || got <= 0 {
		t.Fatalf("a committed timeout blocked independent latest work: %s", got)
	}
	if got := m.tokenMeteringRoundDelay(context.Background(), 0, readFailure, now); got > 10*time.Second || got <= 0 {
		t.Fatalf("a no-progress failure blocked independent latest work: %s", got)
	}
	var failed TokenMeteringWindow
	if err := m.tokenStore().First(&failed, "source_epoch=? AND start_at=?", state.SourceEpoch, state.ScanThrough).Error; err != nil || failed.NextAt != now+30 {
		t.Fatal("individual failure backoff changed", failed, err)
	}
}

func TestTokenMeteringLegacyReplayPageWidthDoesNotThrottleForward(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	state := tokenMeteringStateRead(t, m)
	if err := m.tokenStore().Model(&state).Update("last_reason", "source_read_failed").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Create(&TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: state.StartAt - 300, EndAt: state.StartAt, CursorAt: state.StartAt - 299, CursorID: 123, Replay: true, Attempts: 10, LastReason: "source_read_failed"}).Error; err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := m.claimTokenMetering(context.Background(), now)
	if err != nil || !ok {
		t.Fatal("claim", ok, err)
	}
	forward, ok, err := m.nextTokenMeteringWindow(context.Background(), state.SourceEpoch, claimed.LeaseToken, now, false)
	if err != nil || !ok || forward.ReadPageRows != 256 || claimed.ReadPageRows != 256 {
		t.Fatal("legacy replay throttled the forward lane", forward.ReadPageRows, claimed.ReadPageRows, err)
	}
	replay, ok, err := m.nextTokenMeteringWindow(context.Background(), state.SourceEpoch, claimed.LeaseToken, now, true)
	if err != nil || !ok || replay.ReadPageRows != 8 || replay.CursorID != 123 || replay.Attempts != 10 {
		t.Fatal("legacy replay lost its own failure/cursor checkpoint", replay, err)
	}
}

// This supplements the small unit fixtures: retained real payloads and backlog,
// multi-page synthetic windows, arrivals at four rows/second, real source and
// worker budgets, real scheduling delays, and the actual maintenance goroutines.
// It proves only this disconnected workload, never upstream network throughput.
func TestTokenRuntimeRetainedScheduledWorkload(t *testing.T) {
	tokenRuntimeRetainedWorkload(t, false)
}

func TestTokenRuntimeRetainedExpiryWorkload(t *testing.T) {
	tokenRuntimeRetainedWorkload(t, true)
}

func tokenRuntimeRetainedWorkload(t *testing.T, expire bool) {
	snapshot := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to a verified stopped-writer snapshot")
	}
	m, _ := tokenIsolatedRuntimeFixture(t, snapshot)
	m.tokenReadStore().Config.Logger = tokenReportTimingLogger{Interface: logger.Default.LogMode(logger.Silent), t: t}
	m.tokenStore().Config.Logger = tokenReportTimingLogger{Interface: logger.Default.LogMode(logger.Silent), t: t}
	var initial TokenMeteringState
	var revision TokenDetectionRevision
	if err := m.tokenStore().First(&initial).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().First(&revision).Error; err != nil {
		t.Fatal(err)
	}
	var frozen tokenDetectionFrozen
	if err := json.Unmarshal([]byte(revision.FrozenJSON), &frozen); err != nil {
		t.Fatal(err)
	}
	// Compare the new projection against original result JSON before starting
	// workers, using the retained real 5,000-row summary population.
	q := tokenDetectionQuery{SourceEpoch: initial.SourceEpoch, From: initial.StartAt, To: initial.ScanThrough}
	var original, projected []TokenDetectionCurrent
	if err := tokenDetectionRange(m.tokenReadStore(), q).Order("created_at,log_id").Limit(5000).Find(&original).Error; err != nil {
		t.Fatal(err)
	}
	if err := tokenReportSummaryRange(m.tokenReadStore(), q).Order("created_at,log_id").Limit(5000).Find(&projected).Error; err != nil {
		t.Fatal(err)
	}
	originalSummary, e1 := tokenDetectionSummarize(original)
	projectedSummary, e2 := tokenDetectionSummarize(projected)
	originalAnalysis, e3 := tokenReportAnalyze(original)
	projectedAnalysis, e4 := tokenReportAnalyze(projected)
	if len(original) != 5000 || len(projected) != len(original) || e1 != nil || e2 != nil || e3 != nil || e4 != nil || !reflect.DeepEqual(originalSummary, projectedSummary) || !reflect.DeepEqual(originalAnalysis, projectedAnalysis) {
		t.Fatal("real retained summary projection differs from original JSON", e1, e2, e3, e4)
	}
	var originalBytes, projectedBytes int
	for i := range original {
		originalBytes += len(original[i].ResultJSON)
		projectedBytes += len(projected[i].ResultJSON)
	}
	t.Logf("real summary population matched=%d original_bytes=%d projected_bytes=%d", len(original), originalBytes, projectedBytes)
	original, projected = nil, nil
	m.cfg.TokenFactsSourceEpoch, m.cfg.TokenFactsSourceMode = initial.SourceEpoch, initial.SourceMode
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(frozen.Policy)
	m.cfg.TokenFactsUsageAdapter = "unknown"
	m.cfg.TokenFactsSemanticBindings = `{"anthropic":"anthropic_native"}`
	tokenRuntimeReconcileArchivedSource(t, m)
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 250, 15
	m.cfg.TokenFactsMaxRows, m.cfg.TokenDetectionMaxRows = 1200000, 1200000
	m.cfg.TokenFactsMaxBytes, m.cfg.TokenDetectionMaxBytes = 6<<30, 24<<30
	m.cfg.TokenFactsRetentionDays, m.cfg.TokenDetectionRetentionDays = 4, 4
	m.cfg.TokenDetectionRetentionBasis = "first_evaluated"
	m.cfg.NginxEvidenceHMACKeyID = "synthetic-scheduler-key"
	// Attach a labelled synthetic tail after the saved unfinished window.
	// This fixture-only jump proves no real historical coverage: retain that
	// incomplete window, all replay eligibility, and the old complete frontier.
	start := initial.ScanThrough + 300
	if expire {
		var oldest int64
		if err := m.tokenReadStore().Model(&TokenMeteringFact{}).Select("created_at").Order("created_at").Limit(1).Scan(&oldest).Error; err != nil {
			t.Fatal(err)
		}
		// Exercise actual expiry concurrently with collection and detection on
		// the same retained payloads. Keep the four-day policy unchanged and
		// advance the replay clock so at least an hour of facts has expired.
		start = max(start, (oldest+m.cfg.tokenMeteringRetentionSeconds()+3600-tokenMeteringLagSeconds-1180+299)/300*300)
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", initial.SourceEpoch).
		Updates(map[string]any{"scan_through": start, "lease_until": 0, "lease_token": ""}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenDetectionState{}).Where("source_epoch=?", initial.SourceEpoch).
		Updates(map[string]any{"lease_until": 0, "lease_token": ""}).Error; err != nil {
		t.Fatal(err)
	}
	source := &tokenWorkloadSource{start: start, windows: map[int64]int64{}, partial: true,
		payload: `{"claude":true,"cache_tokens":0,"cache_creation_tokens":0,"fixture_padding":"` + strings.Repeat("x", 4096) + `"}`}
	for n := int64(0); n < 3; n++ {
		source.windows[start+n*300] = 512
	}
	arrivalRate := int64(4)
	if configured := os.Getenv("TOKEN_TEST_ARRIVAL_PER_SECOND"); configured != "" {
		var err error
		arrivalRate, err = strconv.ParseInt(configured, 10, 64)
		if err != nil || arrivalRate < 1 || arrivalRate > 16 {
			t.Fatal("invalid explicit offline arrival rate")
		}
	}
	withAssociations := tokenRuntimeAssociationEvidence(t, m, start, arrivalRate)
	m.prodDB = sql.OpenDB(source)
	m.prodDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	// Keep source event time offset from wall time, but advancing at exactly
	// one second per second. The first close boundary is twenty seconds away;
	// the next is five minutes later. Window retry deadlines are never skipped.
	wallStart := time.Now()
	sourceNow := func() int64 {
		return start + 900 + tokenMeteringLagSeconds + 280 + int64(time.Since(wallStart)/time.Second)
	}
	// Select a populated pagination interval safely inside retention. The
	// fixed snapshot's earliest minute can expire during the expiry workload.
	var pageTimes []int64
	pageFloor := max(initial.StartAt, sourceNow()-m.cfg.tokenMeteringRetentionSeconds()+3600)
	if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND created_at>=?", initial.SourceEpoch, pageFloor).
		Select("created_at").Order("created_at").Limit(50).Scan(&pageTimes).Error; err != nil || len(pageTimes) != 50 {
		t.Fatal("retained pagination population", len(pageTimes), err)
	}
	pageFrom, pageTo := pageTimes[0]/60*60, pageTimes[49]/60*60+60
	// A newer retained snapshot can contain a legitimate expiry backlog. Its
	// removal must not be counted as detection throughput. Measure a fixed
	// cohort that remains inside retention for the whole 25-minute test bound,
	// while leaving cleanup and the actual retention policy running normally.
	cohortFrom := (pageFloor + tokenMeteringWindowSeconds - 1) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	var initialCohortFacts int64
	if err := m.tokenReadStore().Model(&TokenMeteringFact{}).
		Where("source_epoch=? AND created_at>=?", initial.SourceEpoch, cohortFrom).Count(&initialCohortFacts).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("stable_retention_cohort_from=%d initial_facts=%d expiry_backlog_excluded_from_throughput=true", cohortFrom, initialCohortFacts)
	pendingWithoutHead := func() int64 {
		t.Helper()
		var pending int64
		if err := m.tokenReadStore().Table(tokenReportPendingGroups).Where("source_epoch=? AND head_present=0 AND start_at>=?", initial.SourceEpoch, cohortFrom).
			Select("COALESCE(SUM(requests),0)").Scan(&pending).Error; err != nil {
			t.Fatal(err)
		}
		return pending
	}
	initialWithout := pendingWithoutHead()
	var rounds, slowDelays atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	// Optional production-shaped lane contention: a slow low-priority
	// consumer and a short high-priority consumer share the real scheduler.
	// All reads remain synthetic/offline; no upstream throughput is inferred.
	var sharedTurns atomic.Int64
	var sharedWorkers sync.WaitGroup
	if os.Getenv("TOKEN_TEST_SHARED_SOURCE_CONTENTION") == "1" {
		m.cfg.BackgroundSourceMinStartIntervalMS = 2000
		for _, priority := range []backgroundSourcePriority{backgroundSourceLow, backgroundSourceHigh} {
			priority := priority
			sharedWorkers.Add(1)
			go func() {
				defer sharedWorkers.Done()
				for ctx.Err() == nil {
					release, err := m.acquireBackgroundSourcePriority(ctx, priority)
					if err != nil {
						return
					}
					hold := 250 * time.Millisecond
					if priority == backgroundSourceLow {
						hold = 6 * time.Second
					}
					alive := waitSourceLifecycle(ctx, hold)
					release()
					if !alive {
						return
					}
					sharedTurns.Add(1)
					if !waitSourceLifecycle(ctx, 7*time.Second) {
						return
					}
				}
			}()
		}
	}
	done := make(chan struct{})
	arrivalsDone := make(chan struct{})
	t.Logf("offline continued arrival rate=%d per second", arrivalRate)
	go func() {
		defer close(arrivalsDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				at := (sourceNow() - tokenMeteringLagSeconds) / 300 * 300
				source.mu.Lock()
				source.windows[at] += arrivalRate
				source.mu.Unlock()
			}
		}
	}()
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			now := sourceNow()
			pages, err := m.runTokenMeteringRound(ctx, time.Unix(now, 0))
			delay := m.tokenMeteringRoundDelay(ctx, pages, err, sourceNow())
			round := rounds.Add(1)
			t.Logf("scheduled collector round=%d pages=%d error=%t error_type=%T deadline=%t transaction_done=%t next_delay=%s", round, pages, err != nil, err, errors.Is(err, context.DeadlineExceeded), errors.Is(err, sql.ErrTxDone), delay)
			if delay == time.Minute {
				slowDelays.Add(1)
			}
			if !waitSourceLifecycle(ctx, delay) {
				return
			}
		}
	}()
	m.startTokenMeteringMaintenanceAt(ctx, func() time.Time { return time.Unix(sourceNow(), 0) })
	defer func() {
		t.Log("scheduled workload stopping workers; subsequent cancellation errors are teardown")
		cancel()
		sharedWorkers.Wait()
		<-done
		<-arrivalsDone
		// The actual maintenance workers have five-second contexts and a
		// separate one-second lease release; allow both to observe cancellation.
		time.Sleep(6 * time.Second)
	}()
	requests, maxLatency := 0, time.Duration(0)
	call := func(endpoint string, handler func(*gin.Context), method, payload string) []byte {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(method, endpoint, strings.NewReader(payload))
		c.Request.Header.Set("Content-Type", "application/json")
		began := time.Now()
		handler(c)
		elapsed := time.Since(began)
		maxLatency = max(maxLatency, elapsed)
		requests++
		if w.Code != 200 {
			t.Fatalf("scheduled workload API status=%d elapsed=%s payload=%s response=%s", w.Code, elapsed, payload, w.Body.String())
		}
		var availability struct {
			Available   *bool    `json:"available"`
			GapReasons  []string `json:"gap_reasons"`
			CountsState string   `json:"health_counts_state"`
			Summary     struct {
				Gaps []string `json:"gaps"`
			} `json:"summary"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &availability); err != nil || availability.Available != nil && !*availability.Available {
			t.Fatalf("scheduled workload returned unavailable data behind HTTP 200: endpoint=%s request=%s elapsed=%s json_error=%v gaps=%v summary_gaps=%v counts=%s", endpoint, payload, elapsed, err, availability.GapReasons, availability.Summary.Gaps, availability.CountsState)
		}
		return w.Body.Bytes()
	}
	checkPages := func(from, to int64) {
		t.Helper()
		q := tokenDetectionQuery{SourceEpoch: initial.SourceEpoch, From: from, To: to, Limit: 25}
		seen := map[int64]bool{}
		for p := 0; p < 2; p++ {
			var response struct {
				Items []struct{ Request TokenDetectionCurrent }
			}
			if err := json.Unmarshal(call("/anomalies/token/detection/query", m.serveTokenDetectionQuery, "POST", tokenDetectionJSON(q)), &response); err != nil || len(response.Items) != 25 {
				t.Fatal("scheduled pagination", err, len(response.Items))
			}
			for _, item := range response.Items {
				if seen[item.Request.LogID] {
					t.Fatal("duplicate across scheduled pages")
				}
				seen[item.Request.LogID] = true
			}
			last := response.Items[24].Request
			q.AfterAt, q.AfterID = last.CreatedAt, last.LogID
			var detail struct {
				DecisionHash string `json:"decision_hash"`
				Evidence     tokenDetectionFrozen
			}
			if err := json.Unmarshal(call("/anomalies/token/detection/detail", m.serveTokenDetectionDetail, "POST", tokenDetectionJSON(map[string]any{"source_epoch": initial.SourceEpoch, "log_id": last.LogID, "revision": last.Revision})), &detail); err != nil || detail.DecisionHash != last.DecisionHash || tokenDetectionHash(detail.Evidence) != last.DecisionHash {
				t.Fatal("scheduled frozen evidence mismatch", err)
			}
		}
	}
	batches := 12
	if configured := os.Getenv("TOKEN_TEST_OBSERVATION_BATCHES"); configured != "" {
		var err error
		batches, err = strconv.Atoi(configured)
		if err != nil || batches < 12 || batches > 24 {
			t.Fatal("invalid explicit offline observation length")
		}
	}
	t.Logf("offline observation batches=%d interval=30s", batches)
	var previousTotal int64
	var previousIncludingOpen int64
	for batch := 0; batch < batches; batch++ {
		until := time.Now().Add(30 * time.Second)
		for time.Now().Before(until) {
			call("/anomalies/token/facts/health", m.serveTokenMeteringHealth, "GET", "")
			call("/anomalies/token/detection/health", m.serveTokenDetectionHealth, "GET", "")
			checkPages(pageFrom, pageTo)
			// Large-range availability must be measured during these writes.
			checkPages(initial.StartAt, initial.ScanThrough)
			time.Sleep(2 * time.Second)
		}
		var state TokenMeteringState
		if err := m.tokenReadStore().First(&state).Error; err != nil {
			t.Fatal(err)
		}
		var collected, detected int64
		if err := m.tokenReadStore().Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id>=?", initial.SourceEpoch, 1_000_000_000).Count(&collected).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id>=?", initial.SourceEpoch, 1_000_000_000).Count(&detected).Error; err != nil {
			t.Fatal(err)
		}
		target := (sourceNow() - tokenMeteringLagSeconds) / 300 * 300
		source.mu.Lock()
		var available, openArrivals int64
		for from, count := range source.windows {
			if from < target {
				available += count
			} else {
				openArrivals += count
			}
		}
		queries, failures := source.queries, source.historyFail
		source.mu.Unlock()
		// Fact expiry and retained result evidence have independent lifetimes.
		// Neither subtraction of total heads nor net growth of facts measures
		// these queues correctly when cleanup is running.
		sourcePending := available - collected
		localPending := pendingWithoutHead()
		removed := initial.FactCount + collected - state.FactCount
		total := sourcePending + localPending
		// The synthetic generator also knows arrivals in the not-yet-sealed
		// minute. They are not collectable coverage gaps, but still count toward
		// sustained throughput: excluding them can make a slower-than-arrival
		// detector appear to catch up just before the next window closes.
		includingOpen := total + openArrivals
		t.Logf("scheduled batch=%d source_rows=%d unsealed_arrivals=%d source_pending=%d local_without_result=%d combined=%d initial_combined=%d scan=%d target=%d page_width=%d rounds=%d minute_delays=%d source_queries=%d history_failures=%d requests=%d max_request=%s", batch, available, openArrivals, sourcePending, localPending, total, initialWithout+3*512, state.ScanThrough, target, state.ReadPageRows, rounds.Load(), slowDelays.Load(), queries, failures, requests, maxLatency)
		t.Logf("sustained workload including_unsealed=%d initial=%d preceding=%d", includingOpen, initialWithout+3*512, previousIncludingOpen)
		t.Logf("retention expiry=%t removed_facts=%d synthetic_collected=%d synthetic_detected=%d expired_gap_windows=%d", expire, removed, collected, detected, state.ExpiredGapWindows)
		if os.Getenv("TOKEN_TEST_DEBUG_QUEUE") == "1" {
			var qdiag struct{ Fresh, Headless, Retry int64 }
			for _, item := range []struct {
				name, sql string
				out       *int64
			}{
				{"fresh", "SELECT COUNT(*) FROM token_report_pending_members_v1 WHERE source_epoch=? AND log_id>=? AND retry_present=0 AND head_present=0", &qdiag.Fresh},
				{"headless", "SELECT COUNT(*) FROM token_report_pending_members_v1 WHERE source_epoch=? AND log_id>=? AND head_present=0", &qdiag.Headless},
				{"retry", "SELECT COUNT(*) FROM token_detection_retries WHERE source_epoch=? AND log_id>=?", &qdiag.Retry},
			} {
				if err := m.tokenReadStore().Raw(item.sql, initial.SourceEpoch, int64(1_000_000_000)).Scan(item.out).Error; err != nil {
					t.Fatal(err)
				}
			}
			var bounds struct{ Min, Max int64 }
			if err := m.tokenReadStore().Raw("SELECT MIN(created_at) AS min,MAX(created_at) AS max FROM token_report_pending_members_v1 WHERE source_epoch=? AND log_id>=?", initial.SourceEpoch, int64(1_000_000_000)).Scan(&bounds).Error; err != nil {
				t.Fatal(err)
			}
			jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), initial.SourceEpoch, tokenDetectionPolicyHash(frozen.Policy), sourceNow(), 250, m.cfg.tokenProcessingPollSeconds())
			if err != nil {
				t.Fatal(err)
			}
			queued := 0
			for _, job := range jobs {
				if job.LogID >= 1_000_000_000 {
					queued++
				}
			}
			var sample []struct {
				LogID     int64 `gorm:"column:log_id"`
				CreatedAt int64 `gorm:"column:created_at"`
			}
			if err := m.tokenReadStore().Raw("SELECT log_id,created_at FROM token_report_pending_members_v1 WHERE source_epoch=? AND log_id>=? ORDER BY log_id LIMIT 5", initial.SourceEpoch, int64(1_000_000_000)).Scan(&sample).Error; err != nil {
				t.Fatal(err)
			}
			t.Logf("DEBUG queue now=%d target=%d bounds=%+v qdiag=%+v sample=%+v jobs=%d synthetic_jobs=%d", sourceNow(), target, bounds, qdiag, sample, len(jobs), queued)
		}
		if state.CompleteThrough > max(initial.CompleteThrough, state.StartAt) {
			t.Fatal("unavailable real history was incorrectly certified by the fixture")
		}
		if batch == batches-1 {
			// Collection and maintenance keep committing during observation. Read
			// one WAL snapshot for conservation and the finished cohort: comparing
			// counts several seconds apart otherwise reports valid new commits as
			// lost/extra facts. The latest sealed window can have closed during
			// this very batch; require the preceding fixed cohort to be complete
			// within one 300-second source interval while newer arrivals continue.
			// The throughput checks below still count ALL arrivals, including open
			// and newly sealed windows; no unfinished suffix is excluded from them.
			settledTo := target - tokenMeteringWindowSeconds
			var expectedSettled int64
			source.mu.Lock()
			for from, count := range source.windows {
				if from < settledTo {
					expectedSettled += count
				}
			}
			source.mu.Unlock()
			var links struct{ Total, Unchecked, Unique int64 }
			var recentPending, cohortFacts, snapshotCollected, settledDetected int64
			var snapshotState TokenMeteringState
			err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
				if err := tx.First(&snapshotState).Error; err != nil {
					return err
				}
				if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id>=?", initial.SourceEpoch, int64(1_000_000_000)).Count(&snapshotCollected).Error; err != nil {
					return err
				}
				if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch=? AND created_at>=?", initial.SourceEpoch, cohortFrom).Count(&cohortFacts).Error; err != nil {
					return err
				}
				if err := tx.Model(&TokenMeteringFact{}).
					Where("source_epoch=? AND log_id>=? AND created_at<?", initial.SourceEpoch, int64(1_000_000_000), settledTo).
					Select("COUNT(*) AS total, COALESCE(SUM(link_checked_at=0),0) AS unchecked, COALESCE(SUM(link_state='unique'),0) AS `unique`").Scan(&links).Error; err != nil {
					return err
				}
				if err := tx.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id>=? AND created_at<?", initial.SourceEpoch, int64(1_000_000_000), settledTo).Count(&settledDetected).Error; err != nil {
					return err
				}
				return tx.Table(tokenReportPendingMembers+" AS g").
					Where("g.source_epoch=? AND g.log_id>=? AND g.created_at<?", initial.SourceEpoch, int64(1_000_000_000), settledTo).
					Where(tokenReportPendingPredicate("g"), tokenDetectionPolicyHash(frozen.Policy)).Count(&recentPending).Error
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("fixed_cohort_to=%d source=%d facts=%d detected=%d pending_or_stale=%d unchecked_links=%d unique_links=%d collection_scan=%d", settledTo, expectedSettled, links.Total, settledDetected, recentPending, links.Unchecked, links.Unique, snapshotState.ScanThrough)
			tokenRuntimePendingDiagnostics(t, m, initial.SourceEpoch, tokenDetectionPolicyHash(frozen.Policy), sourceNow(), target)
			if links.Total != expectedSettled || settledDetected != expectedSettled || recentPending != 0 || snapshotState.ScanThrough < settledTo {
				t.Fatal("fixed sealed cohort did not finish within one source interval while arrivals continued")
			}
			if withAssociations {
				if links.Unchecked != 0 || links.Unique != links.Total {
					t.Fatal("new source windows lack their available association evidence", links)
				}
			}
			t.Logf("stable_retention_cohort_facts=%d expected=%d expired_facts_outside_cohort=%d", cohortFacts, initialCohortFacts+snapshotCollected, removed)
			if cohortFacts != initialCohortFacts+snapshotCollected {
				t.Fatal("throughput observation lost facts inside retention", cohortFacts, initialCohortFacts, snapshotCollected)
			}
			if os.Getenv("TOKEN_TEST_SHARED_SOURCE_CONTENTION") == "1" && sharedTurns.Load() < 10 {
				t.Fatal("shared source consumers did not continue", sharedTurns.Load())
			}
			t.Logf("shared_source_completed_turns=%d min_start_spacing_ms=%d", sharedTurns.Load(), m.cfg.BackgroundSourceMinStartIntervalMS)
			if detected == 0 || expire && removed == 0 {
				t.Fatal("required concurrent detection/expiry made no committed progress", detected, removed)
			}
			if state.ScanThrough < target-tokenMeteringWindowSeconds || total >= initialWithout+3*512 {
				t.Fatalf("scheduled workload is not catching up: source_pending=%d combined=%d initial=%d preceding=%d", sourcePending, total, initialWithout+3*512, previousTotal)
			}
			if includingOpen >= initialWithout+3*512 || includingOpen >= previousIncludingOpen {
				t.Fatalf("processing did not keep up with continued arrivals: including_unsealed=%d initial=%d preceding=%d", includingOpen, initialWithout+3*512, previousIncludingOpen)
			}
		}
		previousTotal = total
		previousIncludingOpen = includingOpen
	}
}

// Keep a bounded, payload-free explanation before the disposable store is
// removed. A head count alone cannot show whether coverage or an association
// invalidated those decisions while the scheduler was serving other work.
func tokenRuntimePendingDiagnostics(t *testing.T, m *Monitor, epoch, hash string, now, target int64) {
	t.Helper()
	var windows []struct {
		StartAt, Total, Pending, Headless, FactChanged, LinkChanged, PolicyChanged, ReferenceChanged, CoverageChanged, Retry int64
	}
	err := m.tokenReadStore().Table(tokenReportPendingMembers+" AS g").
		Joins("LEFT JOIN token_detection_currents c ON c.source_epoch=g.source_epoch AND c.log_id=g.log_id").
		Joins("LEFT JOIN token_detection_buckets b ON b.id=g.reference_bucket").
		Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=g.source_epoch AND w.start_at=g.start_at").
		Where("g.source_epoch=? AND g.log_id>=? AND g.created_at<?", epoch, int64(1_000_000_000), target).
		Select(`g.start_at,COUNT(*) AS total,
			COALESCE(SUM(`+tokenReportPendingPredicate("g")+`),0) AS pending,
			SUM(g.head_present=0) AS headless,
			COALESCE(SUM(c.fact_revision!=g.fact_revision),0) AS fact_changed,
			COALESCE(SUM(c.link_revision!=g.link_revision),0) AS link_changed,
			COALESCE(SUM(g.policy_hash!=?),0) AS policy_changed,
			COALESCE(SUM(g.reference_generation!=COALESCE(b.generation,0)),0) AS reference_changed,
			COALESCE(SUM(g.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END),0) AS coverage_changed,
			SUM(g.retry_present) AS retry`, hash, hash).
		Group("g.start_at").Order("g.start_at").Scan(&windows).Error
	if err != nil {
		t.Fatal("pending diagnostics", err)
	}
	for _, window := range windows {
		t.Logf("recent_pending_reasons=%+v", window)
	}
	jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), epoch, hash, now, m.cfg.tokenProcessingBatchRows(), m.cfg.tokenProcessingPollSeconds())
	if err != nil {
		t.Fatal("pending scheduling diagnostics", err)
	}
	var synthetic, recent int
	for index, job := range jobs[:min(32, len(jobs))] {
		var item struct{ CreatedAt, HeadPresent, BasePending int64 }
		if err := m.tokenReadStore().Table(tokenReportPendingMembers).
			Select("created_at,head_present,base_pending").Where("source_epoch=? AND log_id=?", epoch, job.LogID).Scan(&item).Error; err != nil {
			t.Fatal(err)
		}
		if job.LogID >= 1_000_000_000 {
			synthetic++
		}
		if item.CreatedAt >= now-3600 {
			recent++
		}
		t.Logf("pending_job_prefix index=%d synthetic=%t created_at=%d head=%d base_pending=%d", index, job.LogID >= 1_000_000_000, item.CreatedAt, item.HeadPresent, item.BasePending)
	}
	t.Logf("pending_job_prefix total=%d synthetic=%d recent_hour=%d now=%d target=%d", min(32, len(jobs)), synthetic, recent, now, target)
}
