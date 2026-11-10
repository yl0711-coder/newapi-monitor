package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"testing"
	"time"
)

type tokenQueueResumeSource struct {
	tokenWorkloadSource
	m    *Monitor
	once sync.Once
	done chan struct{}
}

func (s *tokenQueueResumeSource) Connect(context.Context) (driver.Conn, error) {
	return &tokenQueueResumeConn{tokenWorkloadConn: tokenWorkloadConn{&s.tokenWorkloadSource}, source: s}, nil
}

type tokenQueueResumeConn struct {
	tokenWorkloadConn
	source *tokenQueueResumeSource
}

func (c *tokenQueueResumeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.tokenWorkloadConn.QueryContext(ctx, query, args)
	if err == nil {
		c.source.once.Do(func() {
			c.source.m.tokenLocalWriteMu.Lock()
			go func() {
				time.Sleep(2200 * time.Millisecond)
				c.source.m.tokenLocalWriteMu.Unlock()
				close(c.source.done)
			}()
		})
	}
	return rows, err
}

func TestTokenMeteringZeroProgressLocalQueueRetainsReadAndRetry(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenQueueResumeSource{tokenWorkloadSource: tokenWorkloadSource{start: start, windows: map[int64]int64{start: 12}, payload: `{}`}, m: m, done: make(chan struct{})}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	<-source.done
	state := tokenMeteringStateRead(t, m)
	if err != nil || pages != 0 || state.FactCount != 0 || state.ScanThrough != start || state.LastReason != "writer_wait" || state.LastFailureAt != 0 || m.tokenMeteringReadCache == nil {
		t.Fatal("local queue timeout became failed source work", pages, state, err)
	}
	if delay := m.tokenMeteringRoundDelay(context.Background(), pages, err, now); delay > 2*time.Second {
		t.Fatal("ready local suffix entered source backoff", delay)
	}
	// A cached page already owns an unfinished window. Reserving it again
	// would write the identical cursor and consume another FIFO writer turn.
	if err := m.tokenStore().Exec(`CREATE TRIGGER reject_redundant_window_reservation
		BEFORE UPDATE ON token_metering_windows
		WHEN NEW.cursor_at=OLD.cursor_at AND NEW.cursor_id=OLD.cursor_id
		AND NEW.done=OLD.done AND NEW.source_complete=OLD.source_complete AND NEW.replay=OLD.replay
		AND NEW.next_at=OLD.next_at AND NEW.read_page_rows=OLD.read_page_rows
		AND NEW.rows_read=OLD.rows_read AND NEW.updated_at=OLD.updated_at
		BEGIN SELECT RAISE(ABORT,'unchanged window reserved twice'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+3, 0)); err != nil || pages != 1 {
		t.Fatal("queued local read did not resume", pages, err)
	}
	state = tokenMeteringStateRead(t, m)
	if state.FactCount != 12 || state.ScanThrough != start+300 || state.CompleteThrough != start+300 || source.queries != 1 || m.tokenMeteringReadCache != nil {
		t.Fatal("local resume repeated the source read or lost EOF", state, source.queries)
	}
}
