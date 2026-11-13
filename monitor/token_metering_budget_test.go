package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gorm.io/gorm"
)

type tokenMeteringSlowHealthySource struct {
	workload  tokenWorkloadSource
	queries   atomic.Int64
	timeouts  atomic.Int64
	latencies []time.Duration
}

func (s *tokenMeteringSlowHealthySource) Connect(context.Context) (driver.Conn, error) {
	return &tokenMeteringSlowHealthyConn{tokenWorkloadConn{&s.workload}, s}, nil
}
func (s *tokenMeteringSlowHealthySource) Driver() driver.Driver { return s }
func (s *tokenMeteringSlowHealthySource) Open(string) (driver.Conn, error) {
	return s.Connect(context.Background())
}

type tokenMeteringSlowHealthyConn struct {
	tokenWorkloadConn
	source *tokenMeteringSlowHealthySource
}

func (c *tokenMeteringSlowHealthyConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryNumber := c.source.queries.Add(1)
	delay := 2800 * time.Millisecond
	if int(queryNumber) <= len(c.source.latencies) {
		delay = c.source.latencies[queryNumber-1]
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		c.source.timeouts.Add(1)
		return nil, ctx.Err()
	case <-timer.C:
		return c.tokenWorkloadConn.QueryContext(ctx, query, args)
	}
}

func TestTokenMeteringSlowHealthyReadYieldsBeforeInsufficientNextBudget(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	state := tokenMeteringStateRead(t, m)
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", state.SourceEpoch).Update("read_page_rows", 16).Error; err != nil {
		t.Fatal(err)
	}
	source := &tokenMeteringSlowHealthySource{workload: tokenWorkloadSource{start: state.StartAt, windows: map[int64]int64{state.StartAt: 20}, payload: `{}`}}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 1 || source.queries.Load() != 1 || source.timeouts.Load() != 0 {
		t.Fatalf("healthy page was followed by an avoidable shortened-deadline source read: pages=%d queries=%d timeouts=%d err=%v", pages, source.queries.Load(), source.timeouts.Load(), err)
	}
	partial := tokenMeteringStateRead(t, m)
	if partial.FactCount != 16 || partial.ScanThrough != state.StartAt || partial.CompleteThrough > state.StartAt {
		t.Fatal("budget yield lost its durable prefix or invented completeness", partial)
	}
	if pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+5, 0)); err != nil || pages != 1 {
		t.Fatal("next bounded round did not resume", pages, err)
	}
	complete := tokenMeteringStateRead(t, m)
	if complete.FactCount != 20 || complete.ScanThrough != state.StartAt+300 || complete.CompleteThrough != state.StartAt+300 || source.timeouts.Load() != 0 {
		t.Fatal("healthy slow pages did not complete across independent round budgets", complete)
	}
}

func TestTokenMeteringSlowHealthyReadLatencyIncreaseKeepsFullBudget(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	state := tokenMeteringStateRead(t, m)
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", state.SourceEpoch).Update("read_page_rows", 64).Error; err != nil {
		t.Fatal(err)
	}
	// Both reads fit the existing four-second query budget. A faster first
	// page must not cause the slower second page to inherit a shorter timeout.
	source := &tokenMeteringSlowHealthySource{
		workload:  tokenWorkloadSource{start: state.StartAt, windows: map[int64]int64{state.StartAt: 68}, payload: `{}`},
		latencies: []time.Duration{1500 * time.Millisecond, 3500 * time.Millisecond},
	}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 1 || source.queries.Load() != 1 || source.timeouts.Load() != 0 {
		t.Fatalf("variable healthy latency was truncated by the previous page estimate: pages=%d queries=%d timeouts=%d err=%v", pages, source.queries.Load(), source.timeouts.Load(), err)
	}
	if delay := m.tokenMeteringRoundDelay(context.Background(), pages, err, now); delay != 100*time.Millisecond {
		t.Fatal("safe budget yield added idle time before resuming shared source scheduling", delay)
	}
	partial := tokenMeteringStateRead(t, m)
	if partial.FactCount != 64 || partial.ScanThrough != state.StartAt || partial.CompleteThrough > state.StartAt {
		t.Fatal("budget yield lost its durable prefix or invented completeness", partial)
	}
	if pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+5, 0)); err != nil || pages != 1 {
		t.Fatal("next bounded round did not resume the slower page", pages, err)
	}
	complete := tokenMeteringStateRead(t, m)
	if complete.FactCount != 68 || complete.ScanThrough != state.StartAt+300 || complete.CompleteThrough != state.StartAt+300 || source.timeouts.Load() != 0 {
		t.Fatal("variable healthy pages did not complete without cancellation", complete)
	}
}

func TestTokenMeteringSlowHealthyReadKeepsFullBudgetAfterSlowWindowClaim(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenMeteringSlowHealthySource{
		workload:  tokenWorkloadSource{start: start, windows: map[int64]int64{start: 1}, payload: `{}`},
		latencies: []time.Duration{3500 * time.Millisecond},
	}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("slow_source_window_claim", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_windows" {
			time.Sleep(2200 * time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	_ = m.tokenStore().Callback().Create().Remove("slow_source_window_claim")
	if err != nil || pages != 1 || source.queries.Load() != 1 || source.timeouts.Load() != 0 {
		t.Fatal("local claim wait shortened or prevented a healthy source read", pages, source.queries.Load(), err)
	}
	state := tokenMeteringStateRead(t, m)
	var window TokenMeteringWindow
	if err := m.tokenStore().First(&window).Error; err != nil {
		t.Fatal(err)
	}
	if state.FactCount != 1 || state.ScanThrough != start+300 || state.CompleteThrough != start+300 || state.LeaseUntil != 0 || !window.Done || !window.SourceComplete || window.Attempts != 0 {
		t.Fatal("healthy read failed to publish its exact completed window", state, window)
	}
}

func TestTokenMeteringSlowHealthyWindowKeepsLearnedPageWidth(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	state := tokenMeteringStateRead(t, m)
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", state.SourceEpoch).Update("read_page_rows", 16).Error; err != nil {
		t.Fatal(err)
	}
	source := &tokenMeteringSlowHealthySource{
		workload:  tokenWorkloadSource{start: state.StartAt, windows: map[int64]int64{state.StartAt: 8}, payload: `{}`},
		latencies: []time.Duration{3500 * time.Millisecond},
	}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 1 || source.timeouts.Load() != 0 {
		t.Fatal("healthy slow source window did not complete", pages, err)
	}
	complete := tokenMeteringStateRead(t, m)
	if complete.FactCount != 8 || complete.ScanThrough != state.StartAt+300 || complete.CompleteThrough != state.StartAt+300 {
		t.Fatal("slow window lost facts or coverage", complete)
	}
	if complete.ReadPageRows != 16 {
		t.Fatal("slow EOF widened the next window without a throughput measurement", complete.ReadPageRows)
	}
}

func TestTokenMeteringLocalCommitsDoNotSpendSourceReadBudget(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	state := tokenMeteringStateRead(t, m)
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", state.SourceEpoch).Update("read_page_rows", 16).Error; err != nil {
		t.Fatal(err)
	}
	source := &tokenWorkloadSource{start: state.StartAt, windows: map[int64]int64{state.StartAt: 32}, payload: `{}`}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("source_budget_local_commit", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" {
			return
		}
		if deadline, ok := tx.Statement.Context.Deadline(); !ok || time.Until(deadline) > 2*time.Second {
			_ = tx.AddError(errors.New("local commit lost its two-second budget"))
			return
		}
		time.Sleep(600 * time.Millisecond)
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Create().Remove("source_budget_local_commit"); err != nil {
			t.Error(err)
		}
	}()
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages < 2 || pages > 3 || source.queries != pages {
		t.Fatal("local commits consumed the next source read's allowance", pages, source.queries, err)
	}
	complete := tokenMeteringStateRead(t, m)
	if complete.FactCount != 32 || complete.ScanThrough != state.StartAt+300 || complete.CompleteThrough != state.StartAt+300 || complete.LastReason != "" {
		t.Fatal("separate local/source budgets lost facts or coverage", complete)
	}
}

func TestTokenMeteringQueuedPageUsesWriteCostAndKeepsFairness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tokenMeteringQueuedPageCheck(t, 128, 300*time.Millisecond)
	})
}

func TestTokenMeteringTransientQueueUsesRemainingCommitBudget(t *testing.T) {
	// One 1.1s maintenance turn drains before the remaining cheap chunks.
	// Including that wait in the next-write prediction stops after 24 rows,
	// although almost 900ms remain. Measure the injected queue delay with a
	// controlled clock so race instrumentation cannot consume that headroom.
	synctest.Test(t, func(t *testing.T) {
		tokenMeteringQueuedPageCheck(t, 120, 1100*time.Millisecond, 1)
	})
}

func TestTokenMeteringRoundFlushesReadSuffixWithoutRepeatingSource(t *testing.T) {
	t.Run("pre_transaction_yield", func(t *testing.T) {
		tokenMeteringRoundReadSuffixCheck(t, 220*time.Millisecond)
	})
	t.Run("queue_deadline_without_transaction", func(t *testing.T) {
		tokenMeteringRoundReadSuffixCheck(t, -150*time.Millisecond)
	})
}

func TestTokenMeteringCachedForwardSuffixPrecedesReservedReplay(t *testing.T) {
	tokenMeteringRoundReadSuffixCheck(t, 220*time.Millisecond, true)
}

func tokenMeteringRoundReadSuffixCheck(t *testing.T, queueHeadroom time.Duration, replay ...bool) {
	t.Helper()
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenWorkloadSource{start: start, windows: map[int64]int64{start: 120}, payload: `{}`}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	var handedOff atomic.Int64
	defer func() { cancel(); workers.Wait() }()
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("read_suffix_queue", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" {
			return
		}
		deadline, ok := tx.Statement.Context.Deadline()
		if !ok || time.Until(deadline) > 2*time.Second {
			_ = tx.AddError(errors.New("local turn lost its two-second deadline"))
			return
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := m.lockTokenLocalWriter(ctx); err != nil {
				return
			}
			defer m.tokenLocalWriteMu.Unlock()
			if handedOff.Add(1) == 1 {
				// Drain a transient queue either at the first turn's safety
				// margin or just after its queue deadline. No SQL can time out
				// during this wait. Later maintenance turns are free.
				waitSourceLifecycle(ctx, time.Until(deadline)-queueHeadroom)
			}
		}()
		awaitTokenWriterQueue(t, &m.tokenLocalWriteMu, 1)
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Create().Remove("read_suffix_queue"); err != nil {
			t.Error(err)
		}
	}()
	pages, err := m.runTokenMeteringRound(ctx, time.Unix(now, 0))
	state := tokenMeteringStateRead(t, m)
	if err != nil || pages != 1 || state.FactCount <= 0 || state.FactCount >= 120 || m.tokenMeteringReadCache == nil {
		t.Fatal("bounded turn did not retain its uncommitted suffix", pages, state.FactCount, err)
	}
	if state.ScanThrough != start || state.CompleteThrough > start {
		t.Fatal("partial cached page prematurely completed coverage", state)
	}
	reservedReplay := len(replay) > 0 && replay[0]
	if reservedReplay {
		m.tokenMeteringRounds.Store(3)
		source.historyTimeout = time.Second
		m.tokenMeteringReadCache.ExpiresAt = time.Now().Add(500 * time.Millisecond)
		if err := m.tokenStore().Create(&TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: start - 300,
			EndAt: start, CursorAt: start - 300, Replay: true, LastReason: "source_read_failed"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	pages, err = m.runTokenMeteringRound(ctx, time.Unix(now+2, 0))
	var sourceErr *tokenMeteringReadError
	if (!reservedReplay && err != nil) || (reservedReplay && !errors.As(err, &sourceErr)) || pages != 1 || handedOff.Load() < 3 {
		t.Fatal("next turn did not continue fairly", pages, handedOff.Load(), err)
	}
	if state := tokenMeteringStateRead(t, m); state.FactCount != 120 || state.ScanThrough != start+300 || state.CompleteThrough != start+300 {
		t.Fatal("next turn lost rows or coverage", state)
	}
	if reservedReplay {
		if source.queries != 2 || source.historyFail != 1 || m.tokenMeteringRounds.Load() != 4 {
			t.Fatal("cached forward suffix repeated the source read or lost reserved replay", source.queries, source.historyFail, m.tokenMeteringRounds.Load())
		}
	} else if source.queries != 1 || m.tokenMeteringRounds.Load() != 1 {
		t.Fatal("the same read page was fetched again", source.queries)
	}
	if m.tokenMeteringReadCache != nil {
		t.Fatal("completed page retained a stale suffix")
	}
	var fresh int64
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("last_seen_at <> ?", now).Count(&fresh).Error; err != nil || fresh != 0 {
		t.Fatal("cached rows acquired an invented source observation time", fresh, err)
	}
}

func TestTokenMeteringCachedReadHonorsDurableCheckpoint(t *testing.T) {
	now := time.Now().Unix()
	w := TokenMeteringWindow{SourceEpoch: "epoch-a", StartAt: now - 600, EndAt: now - 300, CursorAt: now - 599, CursorID: 8, ReadPageRows: 256}
	for _, tc := range []struct {
		name   string
		change func(*TokenMeteringWindow, *tokenMeteringCachedRead)
		retain bool
	}{
		{"expired", func(_ *TokenMeteringWindow, p *tokenMeteringCachedRead) { p.ExpiresAt = time.Now().Add(-time.Second) }, false},
		{"clock_rollback", func(_ *TokenMeteringWindow, p *tokenMeteringCachedRead) { p.ObservedAt = now + 1 }, false},
		{"cursor_id_changed", func(w *TokenMeteringWindow, _ *tokenMeteringCachedRead) { w.CursorID++ }, false},
		{"cursor_time_changed", func(w *TokenMeteringWindow, _ *tokenMeteringCachedRead) { w.CursorAt++ }, false},
		{"replay_restarted", func(w *TokenMeteringWindow, _ *tokenMeteringCachedRead) { w.Replay = true }, false},
		{"source_page_shrunk", func(w *TokenMeteringWindow, _ *tokenMeteringCachedRead) { w.ReadPageRows = 128 }, false},
		{"window_completed", func(w *TokenMeteringWindow, _ *tokenMeteringCachedRead) { w.Done = true }, false},
		{"another_replay_window", func(w *TokenMeteringWindow, _ *tokenMeteringCachedRead) {
			w.StartAt -= 300
			w.EndAt -= 300
			w.Replay = true
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &tokenMeteringCachedRead{Window: w, Rows: []tokenMeteringRaw{{ID: 9}}, ObservedAt: now, ExpiresAt: time.Now().Add(30 * time.Second)}
			candidate := w
			tc.change(&candidate, p)
			m := &Monitor{tokenMeteringReadCache: p}
			if m.takeTokenMeteringCachedRead(candidate, now) != nil {
				t.Fatal("stale or unrelated source page was consumed")
			}
			if tc.retain {
				if m.takeTokenMeteringCachedRead(w, now) != p || m.tokenMeteringReadCache != nil {
					t.Fatal("historical repair discarded the pending forward page")
				}
			} else if m.tokenMeteringReadCache != nil {
				t.Fatal("invalidated page retained")
			}
		})
	}
}

func tokenMeteringQueuedPageCheck(t *testing.T, count int64, hold time.Duration, slowTurns ...int64) {
	t.Helper()
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	state, claimed, err := m.claimTokenMetering(context.Background(), now)
	if err != nil || !claimed {
		t.Fatal("claim", claimed, err)
	}
	w, found, err := m.nextTokenMeteringWindow(context.Background(), state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal("window", found, err)
	}
	var raw []tokenMeteringRaw
	var facts []TokenMeteringFact
	var refs []TokenMeteringRef
	for id := int64(1); id <= count; id++ {
		row := tokenMeteringRawFixture(`{}`)
		row.ID, row.CreatedAt = id, w.StartAt+1
		row.RequestID = fmt.Sprintf("queued-page-%d", id)
		fact, identities := parseTokenMetering(m.cfg, row, now)
		raw, facts, refs = append(raw, row), append(facts, fact), append(refs, identities...)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	var handedOff atomic.Int64
	defer func() { cancel(); workers.Wait() }()
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("contended_page", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" {
			return
		}
		// Each cheap chunk leaves a maintenance writer in front of the next
		// chunk. This must consume the two-second budget without being confused
		// with the collector's own transaction cost or bypassing FIFO order.
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := m.lockTokenLocalWriter(ctx); err != nil {
				return
			}
			defer m.tokenLocalWriteMu.Unlock()
			turn := handedOff.Add(1)
			if len(slowTurns) == 0 || turn <= slowTurns[0] {
				waitSourceLifecycle(ctx, hold)
			}
		}()
		awaitTokenWriterQueue(t, &m.tokenLocalWriteMu, 1)
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Create().Remove("contended_page"); err != nil {
			t.Error(err)
		}
	}()
	written, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw, facts, refs, true, now)
	if err != nil || written != len(raw) {
		t.Fatalf("queue wait prevented a cheap complete page: written=%d error=%v", written, err)
	}
	if handedOff.Load() < 3 {
		t.Fatal("collector bypassed queued maintenance")
	}
	state = tokenMeteringStateRead(t, m)
	if state.FactCount != int64(len(raw)) || state.ScanThrough != w.EndAt || state.CompleteThrough != w.EndAt {
		t.Fatal("complete page lost counts or coverage", state)
	}
}

func TestTokenMeteringResumedPageReusesSafeWriteSizeWithoutPublishingFailedRows(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	ctx := context.Background()
	state, claimed, err := m.claimTokenMetering(ctx, now)
	if err != nil || !claimed {
		t.Fatal(claimed, err)
	}
	w, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	var raw []tokenMeteringRaw
	var facts []TokenMeteringFact
	var firstRefs, nextRefs []TokenMeteringRef
	for id := int64(1); id <= 128; id++ {
		row := tokenMeteringRawFixture(`{}`)
		row.ID, row.CreatedAt, row.RequestID = id, w.StartAt+1, fmt.Sprintf("resumed-page-%d", id)
		fact, refs := parseTokenMetering(m.cfg, row, now)
		raw, facts = append(raw, row), append(facts, fact)
		if id <= 64 {
			firstRefs = append(firstRefs, refs...)
		} else {
			nextRefs = append(nextRefs, refs...)
		}
	}
	if n, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw[:64], facts[:64], firstRefs, false, now); err != nil || n != 64 {
		t.Fatal("initial prefix", n, err)
	}
	if err := m.tokenStore().First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, w.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	var attemptedRows int
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("resumed_page_failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" {
			attemptedRows = tx.Statement.ReflectValue.Len()
			_ = tx.AddError(errors.New("injected resumed transaction failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	n, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw[64:], facts[64:], nextRefs, true, now)
	_ = m.tokenStore().Callback().Create().Remove("resumed_page_failure")
	if n != 0 || err == nil || attemptedRows <= tokenMeteringCommitChunk {
		t.Fatal("resumed page lost measured write size or hid failure", n, attemptedRows, err)
	}
	partial := tokenMeteringStateRead(t, m)
	if partial.FactCount != 64 || partial.ScanThrough != w.StartAt || partial.CompleteThrough != w.StartAt {
		t.Fatal("failed larger transaction published rows or coverage", partial)
	}
	if n, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw[64:], facts[64:], nextRefs, true, now); err != nil || n != 64 {
		t.Fatal("retry", n, err)
	}
	complete := tokenMeteringStateRead(t, m)
	if complete.FactCount != 128 || complete.ScanThrough != w.EndAt || complete.CompleteThrough != w.EndAt {
		t.Fatal("retry lost identities or final coverage", complete)
	}
}

func TestTokenMeteringLearnedWriteSizeShrinksAfterTransactionTimeout(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	ctx := context.Background()
	state, claimed, err := m.claimTokenMetering(ctx, now)
	if err != nil || !claimed {
		t.Fatal(claimed, err)
	}
	w, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	var raw []tokenMeteringRaw
	var facts []TokenMeteringFact
	var refs []TokenMeteringRef
	for id := int64(1); id <= 32; id++ {
		row := tokenMeteringRawFixture(`{}`)
		row.ID, row.CreatedAt = id, w.StartAt+1
		fact, identities := parseTokenMetering(m.cfg, row, now)
		raw, facts, refs = append(raw, row), append(facts, fact), append(refs, identities...)
	}
	m.tokenMeteringCommitRows.Store(32) // Size learned earlier with cheaper rows.
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("dense_page_timeout", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" && tx.Statement.ReflectValue.Len() > 16 {
			<-tx.Statement.Context.Done()
			_ = tx.AddError(tx.Statement.Context.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Create().Remove("dense_page_timeout"); err != nil {
			t.Error(err)
		}
	}()
	if n, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw, facts, refs, true, now); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected atomic rollback of expensive learned batch", n, err)
	}
	if got := tokenMeteringStateRead(t, m); got.FactCount != 0 || got.ScanThrough != w.StartAt || got.CompleteThrough != w.StartAt {
		t.Fatal("timed-out write certified unread data", got)
	}
	if n, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw, facts, refs, true, now); n != 32 || err != nil {
		t.Fatal("learned batch repeated the same timeout instead of shrinking", n, err)
	}
	if got := tokenMeteringStateRead(t, m); got.FactCount != 32 || got.ScanThrough != w.EndAt || got.CompleteThrough != w.EndAt {
		t.Fatal("smaller retry lost data or coverage", got)
	}
}

func TestTokenMeteringFailureCheckpointSurvivesNearlySpentRound(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	state, claimed, err := m.claimTokenMetering(context.Background(), now)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	w, found, err := m.nextTokenMeteringWindow(context.Background(), state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal(err)
	}
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("failure_checkpoint_local_work", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_windows" {
			time.Sleep(150 * time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.storeDB.Callback().Update().Remove("failure_checkpoint_local_work"); err != nil {
			t.Error(err)
		}
	}()
	ctx, stop := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer stop()
	if err := m.failTokenMeteringWindow(ctx, state.LeaseToken, w, now, "source_read_failed", 128); err != nil {
		t.Fatal("nearly expired round lost required failure persistence", err)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("fixture did not cross the old round deadline")
	}
	if err := m.storeDB.First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, w.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	if w.Done || w.SourceComplete || w.CursorID != 0 || w.Attempts != 1 || w.NextAt < now+30 || w.NextAt > now+37 || w.ReadPageRows != 128 {
		t.Fatal("failure checkpoint changed coverage or lost its backoff", w)
	}
}

func TestTokenMeteringCommitProgressWithCostlyRetainedWrites(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	const rows = 64
	for id := int64(1); id <= rows; id++ {
		tokenMeteringSourceRow(t, m, id, start+1, "")
	}
	// Model index/trigger work proportional to rows. The former 64-row
	// transaction could never publish a cursor within the existing two seconds.
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("retained_write_cost", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" {
			return
		}
		if deadline, ok := tx.Statement.Context.Deadline(); !ok || time.Until(deadline) > 2*time.Second {
			_ = tx.AddError(errors.New("costly write lost its two-second budget"))
			return
		}
		timer := time.NewTimer(time.Duration(tx.Statement.ReflectValue.Len()) * 40 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-tx.Statement.Context.Done():
			_ = tx.AddError(tx.Statement.Context.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	_ = m.storeDB.Callback().Create().Remove("retained_write_cost")
	state := tokenMeteringStateRead(t, m)
	if err != nil || pages != 1 || state.FactCount <= 0 || state.FactCount >= rows || state.ScanThrough != start || state.LastReason != "budget_exhausted" || state.LastFailureAt != 0 || state.LastSuccessAt != now {
		t.Fatalf("bounded writes made no durable incomplete prefix: pages=%d error=%v state=%+v", pages, err, state)
	}
	if delay := m.tokenMeteringRoundDelay(context.Background(), pages, err, now); delay != 100*time.Millisecond {
		t.Fatalf("durable prefix entered global failure backoff: %s", delay)
	}
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+2, 0)); err != nil {
		t.Fatal(err)
	}
	state = tokenMeteringStateRead(t, m)
	var count, unique int64
	err = m.storeDB.Raw("SELECT COUNT(*),COUNT(DISTINCT log_id) FROM token_metering_facts").Row().Scan(&count, &unique)
	if err != nil || count != rows || unique != rows || state.FactCount != rows || state.ScanThrough != start+300 || state.CompleteThrough != start+300 {
		t.Fatal("resuming the durable prefix lost rows or coverage", count, unique, state, err)
	}
}

func TestTokenMeteringAmortizesBoundedTransactionLatency(t *testing.T) {
	// The injected transaction latency exercises batching within the unchanged
	// two-second budget; host speed is not part of this scheduling assertion.
	synctest.Test(t, tokenMeteringAmortizedCommitCheck)
}

func tokenMeteringAmortizedCommitCheck(t *testing.T) {
	t.Helper()
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	ctx := context.Background()
	state, claimed, err := m.claimTokenMetering(ctx, now)
	if err != nil || !claimed {
		t.Fatal(claimed, err)
	}
	w, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	var raw []tokenMeteringRaw
	var facts []TokenMeteringFact
	var refs []TokenMeteringRef
	for id := int64(1); id <= 128; id++ {
		row := tokenMeteringRawFixture(`{}`)
		row.ID, row.CreatedAt = id, w.StartAt+1
		fact, identities := parseTokenMetering(m.cfg, row, now)
		raw, facts, refs = append(raw, row), append(facts, fact), append(refs, identities...)
	}
	transactions := 0
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("fixed_collection_transaction_latency", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" {
			return
		}
		transactions++
		if tx.Statement.ReflectValue.Len() > 64 {
			_ = tx.AddError(errors.New("source commit exceeded its existing 64-row cap"))
		}
		if deadline, ok := tx.Statement.Context.Deadline(); !ok || time.Until(deadline) > 2*time.Second {
			_ = tx.AddError(errors.New("source commit lost its existing two-second budget"))
		}
		time.Sleep(300 * time.Millisecond)
	}); err != nil {
		t.Fatal(err)
	}
	n, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw, facts, refs, true, now)
	_ = m.tokenStore().Callback().Create().Remove("fixed_collection_transaction_latency")
	if err != nil || n < 96 {
		t.Fatalf("fixed transaction overhead prevented bounded collection throughput: written=%d transactions=%d error=%v", n, transactions, err)
	}
	if err := m.tokenStore().First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, w.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	checkpoint := tokenMeteringStateRead(t, m)
	if w.CursorID != raw[n-1].ID || checkpoint.FactCount != int64(n) || w.Done != (n == len(raw)) || w.SourceComplete != w.Done {
		t.Fatal("amortized commit published an incorrect prefix or completeness", n, w, checkpoint)
	}
	if n < len(raw) {
		if checkpoint.ScanThrough != w.StartAt || checkpoint.CompleteThrough != w.StartAt {
			t.Fatal("partial page advanced coverage", checkpoint)
		}
		if written, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw[n:], facts[n:], refs, true, now); err != nil || n+written != len(raw) {
			t.Fatal("amortized page did not resume", written, err)
		}
	}
	complete := tokenMeteringStateRead(t, m)
	if complete.FactCount != int64(len(raw)) || complete.ScanThrough != w.EndAt || complete.CompleteThrough != w.EndAt {
		t.Fatal("amortized page lost facts or final coverage", complete)
	}
}

func TestTokenMeteringSQLiteBudgetInterruptKeepsDurableProgress(t *testing.T) {
	for _, duringCommit := range []bool{false, true} {
		name := "next_window"
		if duringCommit {
			name = "second_commit_chunk"
		}
		t.Run(name, func(t *testing.T) {
			m, now, _ := tokenMeteringHistoryFixture(t)
			defer m.Close()
			start := tokenMeteringStateRead(t, m).StartAt
			for id := int64(1); id <= 128; id++ {
				tokenMeteringSourceRow(t, m, id, start+1, "")
			}
			var interrupted error
			calls := 0
			callback := func(tx *gorm.DB) {
				matches := duringCommit && tx.Statement.Table == "token_metering_facts" || !duringCommit && tx.Statement.Table == "token_metering_states"
				if !matches {
					return
				}
				calls++
				inject := duringCommit && calls == 2
				if !duringCommit {
					var persisted int64
					if err := tx.Session(&gorm.Session{NewDB: true}).Raw("SELECT fact_count FROM token_metering_states WHERE source_epoch=?", m.cfg.TokenFactsSourceEpoch).Scan(&persisted).Error; err != nil {
						_ = tx.AddError(err)
						return
					}
					inject = persisted == 128
				}
				if inject {
					interrupted = tx.Session(&gorm.Session{NewDB: true}).Exec("WITH RECURSIVE busy(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM busy) SELECT max(n) FROM busy").Error
					_ = tx.AddError(interrupted)
				}
			}
			if duringCommit {
				if err := m.storeDB.Callback().Create().Before("gorm:create").Register("metering_sqlite_budget", callback); err != nil {
					t.Fatal(err)
				}
			} else if err := m.storeDB.Callback().Query().Before("gorm:query").Register("metering_sqlite_budget", callback); err != nil {
				t.Fatal(err)
			}
			pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
			_ = m.storeDB.Callback().Create().Remove("metering_sqlite_budget")
			_ = m.storeDB.Callback().Query().Remove("metering_sqlite_budget")
			if interrupted == nil || pages != 1 || err != nil {
				t.Fatalf("SQL deadline became a failure or lost progress: calls=%d pages=%d error=%v interrupt=%v", calls, pages, err, interrupted)
			}
			state := tokenMeteringStateRead(t, m)
			want := int64(128)
			if duringCommit {
				want = tokenMeteringCommitChunk
			}
			if state.FactCount != want || state.LastFailureAt != 0 || state.LeaseUntil != 0 || (state.LastReason != "" && state.LastReason != "budget_exhausted") {
				t.Fatalf("interruption lost committed prefix or release: %+v", state)
			}
			if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+2, 0)); err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			state = tokenMeteringStateRead(t, m)
			if state.FactCount != 128 || state.ScanThrough != start+300 || state.CompleteThrough != start+300 {
				t.Fatal("failed to resume the exact persisted prefix", state)
			}
		})
	}
}

type tokenRoundBudgetSource struct {
	created        int64
	queries        atomic.Int64
	firstReadDelay time.Duration
}

func (s *tokenRoundBudgetSource) Connect(context.Context) (driver.Conn, error) {
	return &tokenRoundBudgetConn{s}, nil
}
func (s *tokenRoundBudgetSource) Driver() driver.Driver { return s }
func (s *tokenRoundBudgetSource) Open(string) (driver.Conn, error) {
	return &tokenRoundBudgetConn{s}, nil
}

type tokenRoundBudgetConn struct{ source *tokenRoundBudgetSource }

func (*tokenRoundBudgetConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*tokenRoundBudgetConn) Close() error                        { return nil }
func (*tokenRoundBudgetConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *tokenRoundBudgetConn) QueryContext(ctx context.Context, _ string, args []driver.NamedValue) (driver.Rows, error) {
	remaining := int64(0)
	if len(args) == 7 && c.source.created >= args[1].Value.(int64) && c.source.created < args[2].Value.(int64) && args[3].Value.(int64) <= c.source.created {
		remaining = tokenMeteringPageRows
		if args[3].Value.(int64) == c.source.created {
			remaining -= args[5].Value.(int64)
		}
	}
	switch c.source.queries.Add(1) {
	case 1:
		delay := c.source.firstReadDelay
		if delay == 0 {
			delay = 1500 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return &tokenRoundBudgetRows{created: c.source.created, remaining: remaining}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case 2:
		<-ctx.Done()
		return nil, ctx.Err()
	default:
		return &tokenRoundBudgetRows{created: c.source.created, remaining: remaining}, nil
	}
}

type tokenRoundBudgetRows struct{ created, remaining int64 }

func (*tokenRoundBudgetRows) Columns() []string {
	return (&tokenTransportRows{}).Columns()
}
func (*tokenRoundBudgetRows) Close() error { return nil }
func (r *tokenRoundBudgetRows) Next(dest []driver.Value) error {
	if r.remaining == 0 {
		return io.EOF
	}
	id := int64(tokenMeteringPageRows) - r.remaining + 1
	r.remaining--
	copy(dest, []driver.Value{id, r.created, int64(2), int64(3), "synthetic-model", "group", "100", "10", "200", "1", "1", "{}", int64(2), ""})
	return nil
}

func TestTokenMeteringRoundBudgetKeepsProgressWithoutFailureBackoff(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenRoundBudgetSource{created: start + 1}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 1 || source.queries.Load() != 1 {
		t.Fatalf("round budget should yield after persisted progress: pages=%d err=%v", pages, err)
	}
	state := tokenMeteringStateRead(t, m)
	var window TokenMeteringWindow
	if err := m.storeDB.First(&window, "source_epoch=? AND start_at=?", state.SourceEpoch, start).Error; err != nil {
		t.Fatal(err)
	}
	if state.FactCount != tokenMeteringPageRows || state.ScanThrough != start || state.CompleteThrough != start || state.LeaseUntil != 0 ||
		window.CursorID != tokenMeteringPageRows || window.Attempts != 0 || window.NextAt != 0 || window.LastReason != "" || window.Done {
		t.Fatalf("budget exhaustion lost progress or fabricated a failure: state=%+v window=%+v", state, window)
	}
	// The blocked second call must never consume a short tail allowance.
	// Make subsequent source reads available for the next independent round.
	source.queries.Store(2)
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+60, 0)); err != nil {
		t.Fatal(err)
	}
	if state := tokenMeteringStateRead(t, m); state.CompleteThrough < start+300 || state.FactCount != tokenMeteringPageRows {
		t.Fatalf("next poll failed to resume exact cursor: %+v", state)
	}
}

func TestTokenMeteringCompletedReadSurvivesExpiringRoundBudget(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenRoundBudgetSource{created: start + 1, firstReadDelay: 3600 * time.Millisecond}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	// A successful source page fits the unchanged full four-second allowance.
	// Then inject LOCAL work across the active round deadline, still within
	// the separate two-second commit budget. No source slot is held here.
	slept := false
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("test_slow_local_commit", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" && !slept {
			slept = true
			time.Sleep(1500 * time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.storeDB.Callback().Create().Remove("test_slow_local_commit"); err != nil {
			t.Error(err)
		}
	}()
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 1 {
		t.Fatalf("completed read discarded: pages=%d err=%v", pages, err)
	}
	state := tokenMeteringStateRead(t, m)
	if state.FactCount != tokenMeteringCommitChunk || state.ScanThrough != start || state.LastReason != "budget_exhausted" || state.LastFailureAt != 0 {
		t.Fatalf("invalid partial-page commit: %+v", state)
	}
	var checkpoint TokenMeteringWindow
	if err := m.storeDB.First(&checkpoint).Error; err != nil || checkpoint.CursorID != tokenMeteringCommitChunk || checkpoint.Done {
		t.Fatal("durable prefix missing", err)
	}
	source.queries.Store(2) // Resume the remaining source rows after the durable cursor.
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+10, 0)); err != nil {
		t.Fatal(err)
	}
	if state := tokenMeteringStateRead(t, m); state.CompleteThrough < start+300 || state.FactCount != 256 {
		t.Fatal("completed page did not resume")
	}
}

func TestTokenMeteringFailedCommitAfterRoundDeadlineDoesNotPublishProgress(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenRoundBudgetSource{created: start + 1, firstReadDelay: 3600 * time.Millisecond}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("test_commit_timeout", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" {
			<-tx.Statement.Context.Done()
			_ = tx.AddError(tx.Statement.Context.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 0 {
		t.Fatalf("local budget must defer without publishing progress: pages=%d err=%v", pages, err)
	}
	if err := m.storeDB.Callback().Create().Remove("test_commit_timeout"); err != nil {
		t.Fatal(err)
	}
	state := tokenMeteringStateRead(t, m)
	var window TokenMeteringWindow
	if err := m.storeDB.First(&window).Error; err != nil {
		t.Fatal(err)
	}
	if state.FactCount != 0 || state.ScanThrough != start || state.LastFailureAt != 0 || window.CursorID != 0 || window.Done || window.Attempts != 0 || state.LastReason != "local_budget_exhausted" {
		t.Fatalf("failed commit advanced coverage or lost failure: state=%+v window=%+v", state, window)
	}
	// A restarted process loses the acceleration cache but resumes the exact
	// durable cursor. It must recover the rolled-back page without skipping it.
	m.tokenMeteringReadCache = nil
	source.queries.Store(2)
	tokenMeteringRun(t, m, now+60)
	if state := tokenMeteringStateRead(t, m); state.FactCount != 256 || state.CompleteThrough < start+300 {
		t.Fatal("failed local commit did not resume")
	}
}

func TestTokenMeteringReplayGetsFirstPageAndResumesAfterBudget(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	old := start - 300
	if err := m.storeDB.Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Update("start_at", old).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: old, EndAt: start, CursorAt: old, Done: true, SourceComplete: true}).Error; err != nil {
		t.Fatal(err)
	}
	// Independent forward work is in its own durable backoff. This turn must
	// still let replay commit a bounded prefix, then resume after restart.
	if err := m.storeDB.Create(&TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: start, EndAt: start + 300, CursorAt: start, NextAt: now + 30}).Error; err != nil {
		t.Fatal(err)
	}
	source := &tokenRoundBudgetSource{created: old + 1}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	m.tokenMeteringRounds.Store(3) // A repair opportunity after three forward-first rounds.
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 1 {
		t.Fatalf("repair page: %d %v", pages, err)
	}
	var window TokenMeteringWindow
	if err := m.storeDB.First(&window, "source_epoch=? AND start_at=?", m.cfg.TokenFactsSourceEpoch, old).Error; err != nil {
		t.Fatal(err)
	}
	if !window.Replay || window.Done || window.CursorID != 256 || window.Attempts != 0 {
		t.Fatalf("repair cursor lost at budget: %+v", window)
	}
	if state := tokenMeteringStateRead(t, m); state.CompleteThrough != old || state.ScanThrough != start {
		t.Fatal("partial replay falsely completed coverage")
	}
	if source.queries.Load() != 1 {
		t.Fatal("forward backoff bypassed or repair exceeded its reserved turn")
	}
	source.queries.Store(2)
	tokenMeteringRun(t, m, now+60)
	if state := tokenMeteringStateRead(t, m); state.CompleteThrough < start || state.FactCount != 256 {
		t.Fatalf("repair failed to resume durable page: %+v", state)
	}
}
