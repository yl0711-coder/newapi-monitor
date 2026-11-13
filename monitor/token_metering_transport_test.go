package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Delivers a valid source row after the former one-second client deadline.
// Query execution and row delivery are separate in the real MySQL protocol.
type tokenTransportConnector struct {
	createdAt int64
	closed    atomic.Int64
	entered   chan struct{}
	release   <-chan struct{}
	paused    atomic.Bool
}

func (d *tokenTransportConnector) Connect(context.Context) (driver.Conn, error) {
	return &tokenTransportConn{source: d}, nil
}
func (d *tokenTransportConnector) Driver() driver.Driver { return d }
func (d *tokenTransportConnector) Open(string) (driver.Conn, error) {
	return &tokenTransportConn{source: d}, nil
}

type tokenTransportConn struct{ source *tokenTransportConnector }

func (*tokenTransportConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*tokenTransportConn) Close() error                        { return nil }
func (*tokenTransportConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *tokenTransportConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "MAX_EXECUTION_TIME("+strconv.Itoa(tokenMeteringSourceExecutionMS)+")") || len(args) != 7 || args[6].Value != int64(256) {
		return nil, errors.New("source SQL execution or page bound changed")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 4*time.Second+100*time.Millisecond {
		return nil, errors.New("client read budget unbounded")
	}
	if c.source.entered != nil && c.source.paused.CompareAndSwap(false, true) {
		close(c.source.entered)
		select {
		case <-c.source.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &tokenTransportRows{ctx: ctx, source: c.source}, nil
}

func TestTokenMeteringSourceWaitDoesNotBlockDetectionOrAssociation(t *testing.T) {
	m, now, _ := detectionFixture(t)
	tokenMeteringDeferRecentFixture(t, m, now)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	detectionFact(t, m, 9001, start+2, 3, 20000, 0, 0)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=9001").Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	source := &tokenTransportConnector{createdAt: start + 1, entered: entered, release: release}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.runTokenMeteringRound(ctx, time.Unix(now, 0)); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("collector never entered source read")
	}
	processed, detectErr := m.runTokenDetectionRound(ctx, now)
	linked, linkErr := m.linkTokenMeteringBatch(ctx, now)
	close(release)
	collectErr := <-done
	if processed != 1 || linked != 1 || detectErr != nil || linkErr != nil || collectErr != nil {
		t.Fatalf("source wait blocked local work: processed=%d linked=%d detection=%v association=%v collection=%v", processed, linked, detectErr, linkErr, collectErr)
	}
	if f := tokenMeteringFactRead(t, m, 1); f.CreatedAt != start+1 {
		t.Fatal("source did not resume at its durable window")
	}
}

type tokenTransportRows struct {
	ctx    context.Context
	source *tokenTransportConnector
	sent   bool
}

func (*tokenTransportRows) Columns() []string {
	return []string{"id", "created_at", "user_id", "channel_id", "model", "group", "input", "output", "quota", "stream", "use_time", "other", "other_bytes", "request_id"}
}
func (r *tokenTransportRows) Close() error { r.source.closed.Add(1); return nil }
func (r *tokenTransportRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	timer := time.NewTimer(1200 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case <-timer.C:
	}
	r.sent = true
	copy(dest, []driver.Value{int64(1), r.source.createdAt, int64(2), int64(3), "synthetic-model", "group", "100", "10", "200", "1", "1", "{}", int64(2), "synthetic-request"})
	return nil
}

func TestTokenMeteringTransportFitsRoundWithoutExtendingSQLBudget(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenTransportConnector{createdAt: start + 1}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	begin := time.Now()
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages != 1 {
		t.Fatalf("valid delayed row was not persisted: pages=%d err=%v", pages, err)
	}
	state := tokenMeteringStateRead(t, m)
	if state.FactCount != 1 || state.CompleteThrough != start+300 || time.Since(begin) > 5*time.Second {
		t.Fatalf("round/coverage mismatch: %+v elapsed=%v", state, time.Since(begin))
	}
	if source.closed.Load() == 0 {
		t.Fatal("source rows left open")
	}
}

func TestTokenMeteringTransportCancellationKeepsWindowAndReleasesSource(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenTransportConnector{createdAt: start + 1}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := m.runTokenMeteringRound(ctx, time.Unix(now, 0))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("parent deadline ignored: %v", err)
	}
	state := tokenMeteringStateRead(t, m)
	if state.FactCount != 0 || state.CompleteThrough != start || state.ScanThrough != start || state.LeaseUntil != 0 || state.LastReason != "source_read_failed" {
		t.Fatalf("cancelled window changed or failure missing: %+v", state)
	}
	probe, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	release, err := m.acquireBackgroundSource(probe)
	if err != nil {
		t.Fatalf("source slot leaked: %v", err)
	}
	release()
	if source.closed.Load() == 0 {
		t.Fatal("cancelled source rows left open")
	}
}
