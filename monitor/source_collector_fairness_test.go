package monitor

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestSourceReadonlyHealthWaitsForStreamingCollector(t *testing.T) {
	var calls atomic.Int32
	m := &Monitor{cfg: Settings{LogChainOnlySource: true, sourceCheckInterval: 10 * time.Millisecond,
		sourceProbe: func(context.Context, *sql.DB) error { calls.Add(1); return nil }}, prodDB: &sql.DB{}}
	m.setSourceState(sourceStateReady)
	release, err := m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool, 1)
	go func() { _, disconnected, lost := m.monitorSourceEpoch(ctx, nil); done <- disconnected || lost }()
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("health check opened a competing connection while source rows were streaming")
	}
	release()
	lifecycleEventually(t, time.Second, func() bool { return calls.Load() > 0 }, "health check never resumed")
	cancel()
	if <-done {
		t.Fatal("queue wait was treated as source loss")
	}
}

func TestBackgroundSourceLowFIFOAndCanceledHead(t *testing.T) {
	m := &Monitor{}
	release, err := m.acquireBackgroundSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	order := make(chan int, 4)
	head, cancelHead := context.WithCancel(ctx)
	defer cancelHead()
	for i := 0; i < 4; i++ {
		i := i
		waitCtx := ctx
		if i == 0 {
			waitCtx = head
		}
		go func() {
			leave, err := m.acquireBackgroundSourceLow(waitCtx)
			if err != nil {
				order <- -1
				return
			}
			order <- i
			leave()
		}()
		lifecycleEventually(t, time.Second, func() bool { _, n := m.backgroundSourceWaiterCounts(); return n == i+1 }, "waiter did not enter queue")
	}
	cancelHead()
	if got := <-order; got != -1 {
		t.Fatal("canceled head ran", got)
	}
	release()
	for want := 1; want < 4; want++ {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("collector starvation/order: got %d want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("queue stuck after canceled head")
		}
	}
}

func TestTokenMeteringWaitDoesNotConsumeReadRound(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).ScanThrough
	tokenMeteringSourceRow(t, m, 1, start+1, "waited")
	// A real shared-lane wait longer than the old entire round. No SQL or
	// local facts can be committed while the source lane is held.
	release, err := m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	timer := time.AfterFunc(5500*time.Millisecond, release)
	defer timer.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pages, err := m.runTokenMeteringRound(ctx, time.Unix(now, 0))
	if err != nil || pages == 0 {
		t.Fatalf("wait exhausted active work budget: pages=%d err=%v", pages, err)
	}
	var facts int64
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Count(&facts).Error; err != nil || facts != 1 {
		t.Fatal("source row not committed", facts, err)
	}
	if s := tokenMeteringStateRead(t, m); s.ScanThrough <= start || s.LastReason != "" {
		t.Fatal("healthy wait poisoned coverage", s)
	}
}

func TestTokenMeteringCanceledQueueDoesNotFailWindow(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	release, err := m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err = m.runTokenMeteringRound(ctx, time.Unix(now, 0))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	var failed int64
	if err := m.tokenStore().Model(&TokenMeteringWindow{}).Where("attempts > 0 OR next_at > 0 OR last_reason <> ''").Count(&failed).Error; err != nil || failed != 0 {
		t.Fatal("queue cancellation became a source failure", failed, err)
	}
}

func TestTokenMeteringQueueBudgetYieldsAndResumesWithoutSourceFailure(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).ScanThrough
	source := &tokenWorkloadSource{start: start, windows: map[int64]int64{start: 1}, payload: `{}`}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	release, err := m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// Let the real twenty-second queue allowance expire while another
	// collector owns the source. The caller remains live; no SQL may run.
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 0 || source.queries != 0 {
		t.Fatal("queue budget became a source failure or ran SQL", pages, source.queries, err)
	}
	state := tokenMeteringStateRead(t, m)
	if state.FactCount != 0 || state.ScanThrough != start || state.LastReason != "source_slot_wait" || state.LastFailureAt != 0 {
		t.Fatal("waiting published facts or changed the source frontier", state)
	}
	var window TokenMeteringWindow
	if err := m.tokenStore().First(&window, "source_epoch=? AND start_at=?", state.SourceEpoch, start).Error; err != nil ||
		window.Attempts != 0 || window.NextAt != 0 || window.Done || window.SourceComplete || window.CursorID != 0 {
		t.Fatal("queue budget poisoned the durable window", window, err)
	}
	if delay := m.tokenMeteringRoundDelay(context.Background(), pages, err, now); delay != 2*time.Second {
		t.Fatal("runnable forward work entered a global failure backoff", delay)
	}
	release()
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+21, 0)); err != nil {
		t.Fatal(err)
	}
	state = tokenMeteringStateRead(t, m)
	if state.FactCount != 1 || state.ScanThrough != start+300 || state.CompleteThrough != start+300 || source.queries != 1 {
		t.Fatal("released source did not resume the original window", state, source.queries)
	}
}

func TestCustomerHealthTodayProgressesWhileYesterdayFails(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	now := chTestNow()
	day, _ := customerHealthSourceRange(now)
	previous := day - 86400
	initial := previous + 3600
	if err := m.saveCustomerHealthSourceCursor(previous, initial); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	historyCalls, todayCalls := 0, 0
	m.runCustomerHealthSource(ctx, func() time.Time { return now }, func(_ context.Context, from, to int64) (int, error) {
		if from < day {
			historyCalls++
			if from != initial-customerHealthSourceReplaySeconds || to > day {
				t.Fatal("lost historical checkpoint", from, to)
			}
			return 0, errors.New("historical source window unavailable")
		}
		todayCalls++
		if todayCalls == 5 {
			cancel()
			return 0, ctx.Err()
		}
		return 1, nil
	}, func(context.Context, time.Duration) bool { return ctx.Err() == nil })
	var current CustomerHealthSourceCursor
	if err := m.storeDB.First(&current, 1).Error; err != nil || current.DayTs != day || current.ThroughTs != day+4*3600 {
		t.Fatal("yesterday blocked today's prefix", current, err)
	}
	var previousProof CustomerHealthDayCoverage
	if err := m.storeDB.First(&previousProof, "day_ts=?", previous).Error; err != nil || previousProof.ThroughTs != initial || previousProof.TTFTCoverageThroughTs != initial || historyCalls != 1 {
		t.Fatal("historical gap was lost or starved", previousProof, historyCalls, err)
	}
	// A new process resumes the outgoing tail, including late-row replay, and
	// never signs the unread suffix by borrowing today's newer watermark.
	restarted := &Monitor{storeDB: m.storeDB, cfg: m.cfg}
	worked, err := restarted.runCustomerHealthHistoryTurnWith(context.Background(), now, func(_ context.Context, from, to int64) (int, error) {
		if from != initial-customerHealthSourceReplaySeconds {
			t.Fatal("restart discarded pending history", from)
		}
		return 1, nil
	})
	if err != nil || !worked {
		t.Fatal(worked, err)
	}
	proof, err := restarted.customerHealthHistoricalCoverage(context.Background(), previous, day+3600)
	if err != nil || proof.RequestsComplete || proof.FRTComplete || proof.RequestThroughTs != initial-customerHealthSourceReplaySeconds+3600 {
		t.Fatal("today bridged a historical gap", proof, err)
	}
}

func TestCustomerHealthSamplerReleasesSourceBeforeSQLite(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.prodDB = frtRecomputeProdDB(t)
	from, _ := customerHealthSourceRange(chTestNow())
	frtRecomputeInsertLog(t, m.prodDB, 1, from+1, 1, 2, 1, 1, "g", "model", `{"frt":100}`, 1)
	checked := false
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("source_release_before_write", func(tx *gorm.DB) {
		m.backgroundSourceScheduleMu.Lock()
		active := m.backgroundSourceActive
		m.backgroundSourceScheduleMu.Unlock()
		checked = true
		if active {
			_ = tx.AddError(errors.New("SQLite write still holds production source slot"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.storeDB.Callback().Create().Remove("source_release_before_write"); err != nil {
			t.Error(err)
		}
	}()
	_, err := m.sampleCustomerHealthRangeLow(context.Background(), from, from+60)
	if err != nil || !checked {
		t.Fatal("sampler did not release before persistence", checked, err)
	}
}
