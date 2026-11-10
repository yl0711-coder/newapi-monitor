package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type tokenPhaseSource struct {
	tokenStreamSource
	connectDelay, executeDelay time.Duration
	connects, queries          atomic.Int32
	failPing                   atomic.Bool
}

func (s *tokenPhaseSource) Connect(ctx context.Context) (driver.Conn, error) {
	n := s.connects.Add(1)
	if n == 1 && s.connectDelay > 0 {
		timer := time.NewTimer(s.connectDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return &tokenPhaseConn{tokenStreamConn: &tokenStreamConn{&s.tokenStreamSource}, owner: s}, nil
}
func (s *tokenPhaseSource) Driver() driver.Driver { return s }

type tokenPhaseConn struct {
	*tokenStreamConn
	owner *tokenPhaseSource
}

func (c *tokenPhaseConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	c.owner.queries.Add(1)
	if c.owner.executeDelay > 0 {
		timer := time.NewTimer(c.owner.executeDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return c.tokenStreamConn.QueryContext(ctx, q, args)
}
func (c *tokenPhaseConn) Ping(context.Context) error {
	if c.owner.failPing.Swap(false) {
		return syscall.ECONNRESET
	}
	return nil
}
func TestTokenSourceWarmConnectionSurvivesBudgetYield(t *testing.T) {
	s := &tokenPhaseSource{tokenStreamSource: tokenStreamSource{created: 101, rows: 1, mode: "normal"}, connectDelay: 140 * time.Millisecond}
	db := sql.OpenDB(s)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	m := &Monitor{prodDB: db}
	w := TokenMeteringWindow{StartAt: 100, EndAt: 400, CursorAt: 100}
	var timing backgroundSourceReadTiming
	rows, done, err := m.readTokenMeteringPageWithBudgets(context.Background(), w, 64, time.Second, 240*time.Millisecond, &timing)
	if !errors.Is(err, errTokenMeteringConnectionWarm) || done || len(rows) != 0 || timing.Stage != "connection_warmup" || s.queries.Load() != 0 {
		t.Fatal(len(rows), done, err, timing)
	}
	rows, done, err = m.readTokenMeteringPageWithBudgets(context.Background(), w, 64, time.Second, 240*time.Millisecond, &timing)
	if err != nil || !done || len(rows) != 1 || s.connects.Load() != 1 || s.queries.Load() != 1 {
		t.Fatal("warm connection was discarded", len(rows), done, err, s.connects.Load())
	}
}
func TestTokenSourceReadStagesAndOrderedPrefix(t *testing.T) {
	for _, phase := range []string{"connection_acquire", "query_execute", "result_receive"} {
		t.Run(phase, func(t *testing.T) {
			s := &tokenPhaseSource{tokenStreamSource: tokenStreamSource{created: 101, rows: 64, mode: "normal"}}
			switch phase {
			case "connection_acquire":
				s.connectDelay = time.Second
			case "query_execute":
				s.executeDelay = time.Second
			case "result_receive":
				s.mode = "timeout"
			}
			db := sql.OpenDB(s)
			defer db.Close()
			m := &Monitor{prodDB: db}
			var timing backgroundSourceReadTiming
			rows, done, err := m.readTokenMeteringPageWithBudgets(context.Background(), TokenMeteringWindow{StartAt: 100, EndAt: 400, CursorAt: 100}, 64, time.Second, 120*time.Millisecond, &timing)
			if !errors.Is(err, context.DeadlineExceeded) || done || timing.Stage != phase {
				t.Fatal(len(rows), done, err, timing)
			}
			if phase == "result_receive" {
				if len(rows) != tokenStreamPrefix {
					t.Fatal(len(rows))
				}
				for i, r := range rows {
					if r.ID != int64(i+1) {
						t.Fatal("unordered prefix")
					}
				}
			} else if len(rows) != 0 {
				t.Fatal(rows)
			}
		})
	}
}
func TestSourceProbeDiscardsFailedConnectionAndRecovers(t *testing.T) {
	s := &tokenPhaseSource{}
	s.failPing.Store(true)
	db := sql.OpenDB(s)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	m := &Monitor{prodDB: db}
	err := m.pingSource(context.Background())
	if !errors.Is(err, syscall.ECONNRESET) || sourceOperationStage(err) != "ping" {
		t.Fatal(err)
	}
	if err := m.pingSource(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.connects.Load() != 2 {
		t.Fatal("failed probe returned stale connection to pool", s.connects.Load())
	}
}
