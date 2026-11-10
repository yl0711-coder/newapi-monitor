package monitor

import (
	"container/list"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// A writer that just committed must queue behind callers already waiting.
// Polling sync.Mutex.TryLock lets the association/migration loops immediately
// reacquire it for every row, starving collection until its budget expires.
// The queue remains local to this store and cancellation removes its own slot.
type tokenLocalWriter struct {
	mu                 sync.Mutex
	held               bool
	waiters            list.List
	collectionBurst    int
	maxCollectionBurst int
}

type tokenLocalWriterWaiter struct {
	ready   chan struct{}
	element *list.Element
	kind    string
}

// collectionQueued reports whether the source-advancing lane is waiting for
// the shared SQLite writer.  A maintenance transaction that is already the
// owner cannot be preempted safely, but its bounded callback can stop after
// a short atomic prefix and hand the writer back. This keeps a long
// association/detection prefix from consuming the collection lane's entire
// two-second budget.
func (w *tokenLocalWriter) collectionQueued() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for e := w.waiters.Front(); e != nil; e = e.Next() {
		if e.Value.(*tokenLocalWriterWaiter).kind == "collection" {
			return true
		}
	}
	return false
}

// An already granted maintenance turn may share one commit across cheap rows.
// Yielding after its first row whenever collection is active turns fixed WAL
// commit latency into per-row latency. Stop after at most 50 ms of SQL work
// when collection is queued; a single expensive atomic row still yields at its
// first safe boundary. The fair writer then gives collection the next turn.
func (w *tokenLocalWriter) yieldBatchToCollection(started time.Time) bool {
	return time.Since(started) >= 50*time.Millisecond && w.collectionQueued()
}

// tokenLocalWaitError means the caller spent its bounded budget before any
// transaction work started. It is deliberately unwrap-able as the original
// context error, while collection and maintenance can distinguish a fair
// scheduling yield from a real local write failure.
type tokenLocalWaitError struct {
	err    error
	timing tokenWriteTiming
}

func (e *tokenLocalWaitError) Error() string { return e.err.Error() }
func (e *tokenLocalWaitError) Unwrap() error { return e.err }

func (w *tokenLocalWriter) TryLock() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.held {
		return false
	}
	w.held = true
	return true
}

// Low-priority maintenance yields after a committed prefix if another stage
// already owns or is waiting for the writer. It must not spend its remaining
// deadline queueing behind that stage and misreport completed work as failed.
func (w *tokenLocalWriter) busy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.held || w.waiters.Len() > 0
}

func (w *tokenLocalWriter) Lock() {
	_ = w.lock(context.Background())
}

func (w *tokenLocalWriter) unlockLocked() {
	if !w.held {
		panic("unlock of unlocked token writer")
	}
	if next := w.nextWaiterLocked(); next != nil {
		waiter := next.Value.(*tokenLocalWriterWaiter)
		w.waiters.Remove(next)
		waiter.element = nil
		close(waiter.ready)
		return
	}
	w.held = false
}

// Collection is the only writer lane that advances the source cursor. Give a
// queued collection turn priority over maintenance/detection, but after a
// short burst hand one turn back so a continuously arriving source cannot
// starve durable association, migration or detection work.
func (w *tokenLocalWriter) nextWaiterLocked() *list.Element {
	front := w.waiters.Front()
	if front == nil {
		return nil
	}
	var collection *list.Element
	var other *list.Element
	for e := front; e != nil; e = e.Next() {
		waiter := e.Value.(*tokenLocalWriterWaiter)
		if waiter.kind == "collection" {
			if collection == nil {
				collection = e
			}
		} else if other == nil {
			other = e
		}
	}
	if collection == nil {
		w.collectionBurst = 0
		return front
	}
	maxBurst := w.maxCollectionBurst
	if maxBurst <= 0 {
		maxBurst = 3
	}
	if other == nil || w.collectionBurst < maxBurst {
		w.collectionBurst++
		return collection
	}
	w.collectionBurst = 0
	return other
}

func (w *tokenLocalWriter) Unlock() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.unlockLocked()
}

func (w *tokenLocalWriter) lock(ctx context.Context, kinds ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	if !w.held {
		w.held = true
		w.mu.Unlock()
		if err := ctx.Err(); err != nil {
			w.Unlock()
			return err
		}
		return nil
	}
	kind := ""
	if len(kinds) > 0 {
		kind = kinds[0]
	}
	waiter := &tokenLocalWriterWaiter{ready: make(chan struct{}), kind: kind}
	waiter.element = w.waiters.PushBack(waiter)
	w.mu.Unlock()
	select {
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			w.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		w.mu.Lock()
		if waiter.element != nil {
			w.waiters.Remove(waiter.element)
			waiter.element = nil
		} else {
			// Cancellation raced with the handoff; pass the acquired slot on.
			w.unlockLocked()
		}
		w.mu.Unlock()
		return ctx.Err()
	}
}

// Reserve the shared SQLite writer only for a local transaction, never while
// waiting for a source query or reading association evidence. Waiting consumes
// the caller's existing budget, including cancellation before any SQL begins.
func (m *Monitor) tokenLocalWriteTransaction(ctx context.Context, fn func(*gorm.DB) error, holdTime ...*time.Duration) (err error) {
	started := time.Now()
	timing := &tokenWriteTiming{}
	if observed, ok := ctx.Value(tokenWriteTimingKey{}).(*tokenWriteTiming); ok {
		timing = observed
	}
	defer func() {
		if time.Since(started) >= 500*time.Millisecond || err != nil {
			outcome := "completed"
			if err != nil {
				outcome = "failed"
			}
			var waiting *tokenLocalWaitError
			if errors.As(err, &waiting) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || tokenSQLiteBusy(err) {
				outcome = "deferred"
			}
			slog.Warn("token local transaction", "operation", ctx.Value(tokenWriteOperationKey{}), "stage", timing.Stage, "wait_ms", timing.Wait.Milliseconds(),
				"connection_ms", timing.Connection.Milliseconds(), "lock_ms", timing.Lock.Milliseconds(),
				"work_ms", timing.Work.Milliseconds(), "commit_ms", timing.Commit.Milliseconds(),
				"sql_count", timing.Statements, "slow_operation", timing.SlowOperation, "slow_ms", timing.Slow.Milliseconds(), "outcome", outcome, "failed", outcome == "failed")
		}
	}()
	timing.Stage = "writer_wait"
	if lockErr := m.lockTokenLocalWriter(ctx); lockErr != nil {
		timing.Wait = time.Since(started)
		if errors.Is(lockErr, context.DeadlineExceeded) || errors.Is(lockErr, context.Canceled) {
			return &tokenLocalWaitError{err: lockErr, timing: *timing}
		}
		return lockErr
	}
	timing.Wait = time.Since(started)
	defer m.tokenLocalWriteMu.Unlock()
	err = m.tokenLocalTransactionHeld(ctx, fn, timing)
	if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) && timing.Work == 0 && timing.Commit == 0 &&
		(timing.Stage == "writer_wait" || timing.Stage == "connection_wait" || timing.Stage == "database_setup" || timing.Stage == "database_lock") {
		// A queue/pool/SQLite lock budget is a resumable scheduling yield. The
		// transaction callback never ran, so it must not poison the source
		// window or make the caller report local_unavailable.
		err = &tokenLocalWaitError{err: err, timing: *timing}
	}
	if len(holdTime) > 0 && holdTime[0] != nil {
		// Adaptive batch sizing measures execution, not pool or lock waits.
		*holdTime[0] = timing.Work + timing.Commit
	}
	return err
}

type tokenWriteTimingKey struct{}
type tokenWriteOperationKey struct{}
type tokenWriteTiming struct {
	Wait, Connection, Lock, Work, Commit, Slow time.Duration
	Stage, SlowOperation                       string
	Statements                                 int
}

// A committed prefix can yield before spending the tail of its deadline in
// the writer queue again. Include the preceding wait and commit, not only SQL
// time: on a disk-backed store the next short SQL batch can still need a full
// writer turn. This only shortens a round; it never extends its resource cap.
func tokenLocalWriteBudgetAvailable(ctx context.Context, previous time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline) >= max(250*time.Millisecond, previous+previous/2)
	}
	return true
}

// Bind the connection before BEGIN so pool wait, SQLite's BEGIN IMMEDIATE
// writer lock, SQL execution and COMMIT have separate measurements. The same
// prepared statement cache is retained; no fact payload/SQL is logged.
func (m *Monitor) tokenLocalTransactionHeld(ctx context.Context, fn func(*gorm.DB) error, timing *tokenWriteTiming) error {
	db := m.tokenStore().WithContext(ctx)
	start := time.Now()
	timing.Stage = "connection_wait"
	entered := false
	err := db.Connection(func(bound *gorm.DB) (err error) {
		entered = true
		timing.Connection = time.Since(start)
		// SQLite's busy handler can sleep for the full configured five seconds
		// despite a cancelled Go context. Fence the writer at BEGIN IMMEDIATE,
		// then retry short lock waits within the caller's original budget.
		// Scope the busy handler to this bound connection and restore it before
		// returning it; shared main-store callers retain their own settings.
		if conn, ok := bound.Statement.ConnPool.(*sql.Conn); ok && db.Dialector.Name() == "sqlite" {
			timing.Stage = "database_setup"
			var previous int
			if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&previous); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=25"); err != nil {
				return err
			}
			defer func() {
				restore, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				if _, err := conn.ExecContext(restore, fmt.Sprintf("PRAGMA busy_timeout=%d", previous)); err != nil {
					_ = conn.Raw(func(any) error { return driver.ErrBadConn })
				}
			}()
		}
		if prepared, ok := db.ConnPool.(*gorm.PreparedStmtDB); ok {
			bound.Statement.ConnPool = &gorm.PreparedStmtDB{ConnPool: bound.Statement.ConnPool, Mux: prepared.Mux, Stmts: prepared.Stmts}
		}
		bound = bound.Session(&gorm.Session{Logger: &tokenWriteTrace{Interface: logger.Discard, timing: timing}})
		timing.Stage = "database_lock"
		start = time.Now()
		var tx *gorm.DB
		lockDeadline := start.Add(5 * time.Second)
		if deadline, ok := ctx.Deadline(); ok && deadline.Before(lockDeadline) {
			lockDeadline = deadline
		}
		for {
			tx = bound.Begin()
			if !tokenSQLiteBusy(tx.Error) || ctx.Err() != nil || !time.Now().Before(lockDeadline) {
				break
			}
			if !waitSourceLifecycle(ctx, 5*time.Millisecond) {
				break
			}
		}
		timing.Lock = time.Since(start)
		if tx.Error != nil {
			return errors.Join(tx.Error, ctx.Err())
		}
		defer tx.Rollback()
		timing.Stage = "transaction_execute"
		start = time.Now()
		err = fn(tx)
		timing.Work = time.Since(start)
		if err != nil {
			return errors.Join(err, ctx.Err())
		}
		timing.Stage = "transaction_commit"
		start = time.Now()
		err = tx.Commit().Error
		timing.Commit = time.Since(start)
		if err == nil {
			timing.Stage = "complete"
			return nil
		}
		return errors.Join(err, ctx.Err())
	})
	if !entered {
		timing.Connection = time.Since(start)
	}
	return err
}

func tokenSQLiteBusy(err error) bool {
	var coded interface{ Code() int }
	return errors.As(err, &coded) && (coded.Code()&255 == 5 || coded.Code()&255 == 6)
}

type tokenWriteTrace struct {
	logger.Interface
	timing *tokenWriteTiming
}

func (l *tokenWriteTrace) Trace(_ context.Context, begin time.Time, sql func() (string, int64), _ error) {
	l.timing.Statements++
	elapsed := time.Since(begin)
	if elapsed <= l.timing.Slow {
		return
	}
	l.timing.Slow = elapsed
	// Only retain an allowlisted verb/table pair, never the rendered operands.
	query, _ := sql()
	verb := strings.ToLower(strings.Fields(query + " unknown")[0])
	if verb != "select" && verb != "insert" && verb != "update" && verb != "delete" {
		verb = "statement"
	}
	table := "other"
	for _, known := range []string{"token_metering_facts", "token_metering_refs", "token_metering_windows", "token_metering_states", "token_detection_currents", "token_detection_revisions", "token_detection_references", "token_detection_buckets", "token_detection_states"} {
		if strings.Contains(query, "`"+known+"`") || strings.Contains(query, " "+known+" ") {
			table = known
			break
		}
	}
	l.timing.SlowOperation = verb + "." + table
}

func (m *Monitor) lockTokenLocalWriter(ctx context.Context) error {
	kind, _ := ctx.Value(tokenWriteOperationKey{}).(string)
	return m.tokenLocalWriteMu.lock(ctx, kind)
}
