package monitor

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func newSourceReadLivenessMonitor(t *testing.T) *Monitor {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	for _, query := range sourcePreflightQueries[:2] {
		selection, rest, _ := strings.Cut(strings.TrimPrefix(query, "SELECT "), " FROM ")
		table, _, _ := strings.Cut(rest, " ")
		columns := strings.Split(selection, ",")
		for i := range columns {
			columns[i] += " TEXT"
		}
		if _, err := db.Exec("CREATE TABLE " + table + " (" + strings.Join(columns, ",") + ")"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("INSERT INTO logs (id,request_id) VALUES ('1','request-liveness')"); err != nil {
		t.Fatal(err)
	}
	return &Monitor{
		prodDB: db,
		cfg: Settings{
			LogChainOnlySource:      true,
			sourceCheckInterval:     50 * time.Millisecond,
			sourcePreflightInterval: time.Hour,
		},
		sourceFailureNotify: make(chan struct{}, 1),
	}
}

func readSourceLivenessRow(ctx context.Context, m *Monitor) error {
	_, err := m.withBackgroundSourceLowRead(ctx, time.Second, time.Second, func(readCtx context.Context) error {
		rows, err := m.prodDB.QueryContext(readCtx, "SELECT request_id FROM logs WHERE id = '1'")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			if id != "request-liveness" {
				return errors.New("wrong source row")
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	})
	return err
}

type sourceReadEpochResult struct {
	class        sourceErrorClass
	disconnected bool
	leaseLost    bool
}

func startSourceReadTestEpoch(t *testing.T, m *Monitor, lease sourceLeaseHandle) <-chan sourceReadEpochResult {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan sourceReadEpochResult, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		class, disconnected, lost := m.monitorSourceEpoch(ctx, lease)
		done <- sourceReadEpochResult{class, disconnected, lost}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("source monitor did not stop")
		}
	})
	return done
}

func TestLogChainSourceRecentReadsKeepEpochAliveUntilEvidenceExpires(t *testing.T) {
	m := newSourceReadLivenessMonitor(t)
	var probes atomic.Int32
	m.cfg.sourceProbe = func(context.Context, *sql.DB) error {
		probes.Add(1)
		return context.DeadlineExceeded
	}
	if err := readSourceLivenessRow(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	done := startSourceReadTestEpoch(t, m, nil)
	// Actual row reads keep proving liveness across several check intervals.
	until := time.NewTimer(4 * m.sourceCheckEvery())
	defer until.Stop()
	reads := time.NewTicker(5 * time.Millisecond)
	defer reads.Stop()
active:
	for {
		select {
		case result := <-done:
			t.Fatalf("a redundant health probe stopped successful reads: %+v", result)
		case <-reads.C:
			if err := readSourceLivenessRow(context.Background(), m); err != nil {
				t.Fatal(err)
			}
		case <-until.C:
			break active
		}
	}
	if probes.Load() != 0 || m.sourceLastSuccessAt.Load() == 0 {
		t.Fatalf("active reads were not used as liveness evidence: probes=%d", probes.Load())
	}
	// No further reads: within a bounded interval the failing Ping must be seen.
	select {
	case result := <-done:
		if !result.disconnected || result.class != sourceErrorTransient || probes.Load() != 1 || m.sourceFailureStreak.Load() != 1 {
			t.Fatalf("stale success hid a real failure: %+v probes=%d", result, probes.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("source remained healthy on expired workload evidence")
	}
}

func TestSourceReadLivenessRequiresCompletedUncancelledRead(t *testing.T) {
	for _, kind := range []string{"success", "query_failure", "warmup", "late_success", "cancelled", "gate_wait", "probe"} {
		t.Run(kind, func(t *testing.T) {
			m := newSourceReadLivenessMonitor(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "gate_wait" {
				release, err := m.acquireBackgroundSource(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
			}
			read := func(readCtx context.Context) error {
				if m.sourceReadLastSuccess.Load() != nil {
					t.Error("unfinished read counted as success")
				}
				switch kind {
				case "query_failure":
					return errors.New("source query failed")
				case "warmup":
					return errTokenMeteringConnectionWarm
				case "cancelled":
					cancel()
					return nil
				case "late_success":
					<-readCtx.Done()
					return nil
				case "gate_wait":
					t.Error("read bypassed occupied source gate")
				}
				var id string
				return m.prodDB.QueryRowContext(readCtx, "SELECT request_id FROM logs WHERE id='1'").Scan(&id)
			}
			if kind == "probe" {
				_, err := m.withBackgroundSourceLowOperation(ctx, time.Second, time.Second, false, func(probeCtx context.Context) error { return m.pingSource(probeCtx) })
				if err != nil {
					t.Fatal(err)
				}
			} else {
				_, err := m.withBackgroundSourceLowRead(ctx, 10*time.Millisecond, 10*time.Millisecond, read)
				if (err == nil) != (kind == "success") {
					t.Fatalf("unexpected read result: %v", err)
				}
			}
			if got := m.sourceReadLastSuccess.Load() != nil; got != (kind == "success") {
				t.Fatalf("workload evidence=%t for %s", got, kind)
			}
		})
	}
}

func TestSourceReadLivenessDoesNotRefreshItself(t *testing.T) {
	m := newSourceReadLivenessMonitor(t)
	var probes atomic.Int32
	m.cfg.sourceProbe = func(context.Context, *sql.DB) error {
		if probes.Add(1) >= 3 {
			return context.DeadlineExceeded
		}
		return nil
	}
	done := startSourceReadTestEpoch(t, m, nil)
	select {
	case result := <-done:
		if !result.disconnected || probes.Load() != 3 || m.sourceReadLastSuccess.Load() != nil {
			t.Fatalf("health probe created workload-read evidence: %+v probes=%d", result, probes.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("successful Ping suppressed later health checks")
	}
}

func TestSourceReadLivenessRechecksAfterWaitingForWorkload(t *testing.T) {
	m := newSourceReadLivenessMonitor(t)
	m.cfg.sourceCheckInterval = 200 * time.Millisecond
	var probes atomic.Int32
	m.cfg.sourceProbe = func(context.Context, *sql.DB) error {
		probes.Add(1)
		return context.DeadlineExceeded
	}
	started, finish, readDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	go func() {
		_, err := m.withBackgroundSourceLowRead(readCtx, time.Second, time.Second, func(ctx context.Context) error {
			close(started)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-finish:
			}
			var id string
			return m.prodDB.QueryRowContext(ctx, "SELECT request_id FROM logs WHERE id='1'").Scan(&id)
		})
		readDone <- err
	}()
	<-started
	done := startSourceReadTestEpoch(t, m, nil)
	lifecycleEventually(t, time.Second, func() bool {
		_, low := m.backgroundSourceWaiterCounts()
		return low == 1
	}, "health check never queued behind the active read")
	close(finish)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	lifecycleEventually(t, time.Second, func() bool { return m.sourceLastSuccessAt.Load() > 0 }, "queued check did not observe the completed read")
	if probes.Load() != 0 {
		t.Fatal("queued health check ignored the successful read and opened another connection")
	}
	select {
	case result := <-done:
		t.Fatalf("queued check stopped the healthy epoch: %+v", result)
	default:
	}
}

func TestSourceReadLivenessNeverBypassesPreflightLeaseOrReportedFailure(t *testing.T) {
	for _, kind := range []string{"preflight", "lease_lost", "full_worker", "reported_permission", "reported_connection"} {
		t.Run(kind, func(t *testing.T) {
			m := newSourceReadLivenessMonitor(t)
			if err := readSourceLivenessRow(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			// Keep the successful read valid throughout this test.
			m.cfg.sourceCheckInterval = 200 * time.Millisecond
			var lease sourceLeaseHandle
			wantClass, wantDisconnected, wantLeaseLost := sourceErrorTransient, true, false
			switch kind {
			case "preflight":
				m.cfg.sourcePreflightInterval = time.Millisecond
				if _, err := m.prodDB.Exec("ALTER TABLE logs RENAME COLUMN other TO hidden_other"); err != nil {
					t.Fatal(err)
				}
				// Use the actual zero-row schema query, not a replacement probe.
				if err := m.probeSource(context.Background()); err == nil {
					t.Fatal("test schema still passes preflight")
				}
			case "lease_lost":
				lease = &lifecycleTestLease{released: make(chan struct{})}
				wantDisconnected, wantLeaseLost = false, true
			case "full_worker":
				m.cfg.LogChainOnlySource = false
				m.cfg.sourceProbe = func(context.Context, *sql.DB) error { return context.DeadlineExceeded }
			case "reported_permission":
				wantClass = sourceErrorPermanent
				m.reportSourceQueryError(&mysql.MySQLError{Number: 1142})
			case "reported_connection":
				m.reportSourceQueryError(mysql.ErrInvalidConn)
			}
			// Supply a recent success while the ticker approaches; preflight
			// and lease checks still have to run even with live workload traffic.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			readerDone := make(chan struct{})
			go func() {
				defer close(readerDone)
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
						_ = readSourceLivenessRow(ctx, m)
					}
				}
			}()
			t.Cleanup(func() { cancel(); <-readerDone })
			done := startSourceReadTestEpoch(t, m, lease)
			select {
			case result := <-done:
				if result.class != wantClass || result.disconnected != wantDisconnected || result.leaseLost != wantLeaseLost {
					t.Fatalf("required source check bypassed: %+v", result)
				}
			case <-time.After(time.Second):
				t.Fatal("recent reads masked a required health check")
			}
		})
	}
}
