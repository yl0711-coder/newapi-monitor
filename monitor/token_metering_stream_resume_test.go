package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type tokenStreamSource struct {
	created int64
	rows    int64
	mode    string
	mu      sync.Mutex
	cursors []int64
	limits  []int64
}

const tokenStreamPrefix = 32

func (s *tokenStreamSource) Connect(context.Context) (driver.Conn, error) {
	return &tokenStreamConn{s}, nil
}
func (s *tokenStreamSource) Driver() driver.Driver { return s }
func (s *tokenStreamSource) Open(string) (driver.Conn, error) {
	return &tokenStreamConn{s}, nil
}

type tokenStreamConn struct{ source *tokenStreamSource }

func (*tokenStreamConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*tokenStreamConn) Close() error                        { return nil }
func (*tokenStreamConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *tokenStreamConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 7 || !strings.Contains(query, "MAX_EXECUTION_TIME("+strconv.Itoa(tokenMeteringSourceExecutionMS)+")") {
		return nil, errors.New("source SQL budget changed")
	}
	limit := args[6].Value.(int64)
	deadline, ok := ctx.Deadline()
	if limit < 8 || limit > 256 || !ok || time.Until(deadline) > 4*time.Second+100*time.Millisecond {
		return nil, errors.New("source transport/page budget changed")
	}
	c.source.mu.Lock()
	first := len(c.source.limits) == 0
	cursor := args[5].Value.(int64)
	c.source.limits = append(c.source.limits, limit)
	c.source.cursors = append(c.source.cursors, cursor)
	c.source.mu.Unlock()
	total := c.source.rows
	if total == 0 {
		total = 256
	}
	r := &tokenStreamRows{ctx: ctx, source: c.source, next: cursor + 1, through: min(total, cursor+limit), first: first}
	if c.source.mode == "slow_small_page" {
		r.through = min(int64(20), r.through)
	}
	if c.source.created < args[1].Value.(int64) || c.source.created >= args[2].Value.(int64) || args[3].Value.(int64) > c.source.created {
		r.through = 0
	}
	return r, nil
}

type tokenStreamRows struct {
	ctx           context.Context
	source        *tokenStreamSource
	next, through int64
	emitted       int
	first         bool
}

func (*tokenStreamRows) Columns() []string { return (&tokenTransportRows{}).Columns() }
func (*tokenStreamRows) Close() error      { return nil }
func (r *tokenStreamRows) Next(dest []driver.Value) error {
	prefix := tokenStreamPrefix
	if r.source.mode == "empty_timeout" {
		prefix = 0
	}
	if r.source.mode == "slow_small_page" || r.source.mode == "steady_slow" {
		prefix = 12
		if r.emitted == 0 && r.next <= r.through {
			timer := time.NewTimer(2200 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-r.ctx.Done():
				return r.ctx.Err()
			case <-timer.C:
			}
		}
	}
	if r.first && r.source.mode != "normal" && r.source.mode != "steady_slow" && r.emitted == prefix {
		if r.source.mode == "corrupt" {
			return io.ErrUnexpectedEOF
		}
		<-r.ctx.Done()
		return r.ctx.Err()
	}
	if r.next > r.through {
		return io.EOF
	}
	copy(dest, []driver.Value{r.next, r.source.created, int64(2), int64(3), "synthetic-model", "group", "100", "10", "200", "1", "1", "{}", int64(2), ""})
	r.next++
	r.emitted++
	return nil
}

func tokenStreamFixture(t *testing.T, mode string) (*Monitor, int64, string, *tokenStreamSource) {
	t.Helper()
	m, now, file := tokenMeteringHistoryFixture(t)
	source := &tokenStreamSource{created: tokenMeteringStateRead(t, m).StartAt + 1, mode: mode}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	return m, now, file, source
}

func TestTokenMeteringReducedPageRecoversInsideDenseWindow(t *testing.T) {
	m, now, _, source := tokenStreamFixture(t, "normal")
	state := tokenMeteringStateRead(t, m)
	if err := m.storeDB.Model(&state).Update("read_page_rows", 16).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err != nil {
		t.Fatal(err)
	}
	var w TokenMeteringWindow
	if err := m.storeDB.First(&w).Error; err != nil {
		t.Fatal(err)
	}
	if len(source.limits) != 3 || source.limits[0] != 16 || source.limits[1] != 16 || source.limits[2] != 16 || w.ReadPageRows != 24 || w.CursorID != 48 || w.Done || tokenMeteringStateRead(t, m).ScanThrough != state.StartAt {
		t.Fatal("successful pages failed to recover width before window completion", source.limits, w)
	}
	// A new worker resumes from the durable page width and cursor, not an
	// in-memory estimate. All 256 source identities must be persisted once.
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	for round := 0; round < 4 && tokenMeteringStateRead(t, m).ScanThrough == state.StartAt; round++ {
		if _, err := restarted.runTokenMeteringRound(context.Background(), time.Unix(now+int64(round+1)*2, 0)); err != nil {
			t.Fatal(err)
		}
	}
	got := tokenMeteringStateRead(t, m)
	if source.limits[3] != 24 || source.cursors[3] != 48 || got.FactCount != 256 || got.ScanThrough != state.StartAt+300 || got.CompleteThrough != got.ScanThrough {
		t.Fatal("recovered pages lost their restart checkpoint or source coverage", source.limits, source.cursors, got)
	}
}

func TestTokenMeteringFastStreamRecoversWithinBoundedSourceTurns(t *testing.T) {
	m, now, _, source := tokenStreamFixture(t, "normal")
	source.rows = 1024
	state := tokenMeteringStateRead(t, m)
	if err := m.storeDB.Model(&state).Update("read_page_rows", 128).Error; err != nil {
		t.Fatal(err)
	}
	// Require three committed samples before each bounded increase, including
	// for fast fixtures. A single unusually fast page is not a throughput trend.
	for turn := int64(0); turn < 3 && tokenMeteringStateRead(t, m).ScanThrough == state.StartAt; turn++ {
		if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+turn*2, 0)); err != nil {
			t.Fatal(err)
		}
	}
	complete := tokenMeteringStateRead(t, m)
	var count, unique int64
	if err := m.storeDB.Raw("SELECT COUNT(*),COUNT(DISTINCT log_id) FROM token_metering_facts").Row().Scan(&count, &unique); err != nil {
		t.Fatal(err)
	}
	if count != 1024 || unique != 1024 || complete.FactCount != 1024 || complete.ScanThrough != state.StartAt+300 || complete.CompleteThrough != complete.ScanThrough {
		t.Fatal("fast recovery lost facts, identities or complete coverage", count, unique, complete)
	}
	if len(source.limits) > 7 {
		t.Fatal("recovered source still spent excess shared turns on undersized pages", source.limits)
	}
}

func TestTokenMeteringHealthySlowPagesGrowAfterDurableSamples(t *testing.T) {
	m, now, _, source := tokenStreamFixture(t, "steady_slow")
	state := tokenMeteringStateRead(t, m)
	if err := m.storeDB.Model(&state).Update("read_page_rows", 8).Error; err != nil {
		t.Fatal(err)
	}
	for round := int64(0); round < 2; round++ {
		if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+round*10, 0)); err != nil {
			t.Fatal(err)
		}
	}
	var before TokenMeteringWindow
	if err := m.storeDB.First(&before, "source_epoch=? AND start_at=?", state.SourceEpoch, state.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	if before.ReadPageRows != 8 || before.ReadSuccessPages != 2 || before.ReadWorstMS < 2200 || before.CursorID != 16 {
		t.Fatal("one fast sample widened the page or durable slow samples disappeared", before)
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	if _, err := restarted.runTokenMeteringRound(context.Background(), time.Unix(now+20, 0)); err != nil {
		t.Fatal(err)
	}
	var after TokenMeteringWindow
	if err := m.storeDB.First(&after, "source_epoch=? AND start_at=?", state.SourceEpoch, state.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	if after.ReadPageRows <= 8 || after.ReadPageRows > 12 || after.ReadSuccessPages != 0 || after.CursorID != 24 || after.Done || after.SourceComplete || len(source.limits) != 3 {
		t.Fatal("healthy two-second transport remained permanently throttled, or lost its prefix", source.limits, after)
	}
	if tokenMeteringStateRead(t, m).FactCount != 24 || tokenMeteringStateRead(t, m).ScanThrough != state.StartAt {
		t.Fatal("learning fabricated source EOF or duplicated facts")
	}
}

func TestTokenMeteringPageWidthNeedsLastChunkCommit(t *testing.T) {
	m, now, _, _ := tokenStreamFixture(t, "normal")
	state, ok, err := m.claimTokenMetering(context.Background(), now)
	if err != nil || !ok {
		t.Fatal(err)
	}
	w, ok, err := m.nextTokenMeteringWindow(context.Background(), state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !ok {
		t.Fatal(err)
	}
	w.ReadPageRows = 128
	if err := m.storeDB.Save(&w).Error; err != nil {
		t.Fatal(err)
	}
	raw, done, err := m.readTokenMeteringPage(context.Background(), w, 128)
	if err != nil || done || len(raw) != 128 {
		t.Fatal(len(raw), done, err)
	}
	facts, refs := []TokenMeteringFact{}, []TokenMeteringRef{}
	for _, row := range raw {
		fact, rs := parseTokenMetering(m.cfg, row, now)
		facts, refs = append(facts, fact), append(refs, rs...)
	}
	var wantPrefix int64
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("last_chunk_failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" {
			rows := tx.Statement.ReflectValue
			if rows.Index(rows.Len()-1).Interface().(TokenMeteringFact).LogID == raw[len(raw)-1].ID {
				wantPrefix = rows.Index(0).Interface().(TokenMeteringFact).LogID - 1
				_ = tx.AddError(errors.New("injected last chunk failure"))
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.storeDB.Callback().Create().Remove("last_chunk_failure"); err != nil {
			t.Error(err)
		}
	}()
	n, err := m.commitTokenMeteringReadPage(context.Background(), state.LeaseToken, w, raw, facts, refs, false, now, 144, 2, 2200)
	if err == nil || wantPrefix <= 0 || wantPrefix >= int64(len(raw)) || int64(n) != wantPrefix {
		t.Fatal("expected a durable prefix and failed last chunk", n, err)
	}
	if err := m.storeDB.First(&w).Error; err != nil {
		t.Fatal(err)
	}
	got := tokenMeteringStateRead(t, m)
	if w.CursorID != wantPrefix || w.ReadPageRows != 128 || w.ReadSuccessPages != 0 || w.ReadWorstMS != 0 || w.Done || got.FactCount != wantPrefix || got.ScanThrough != state.StartAt {
		t.Fatal("uncommitted last chunk published width or coverage", w, got)
	}
}

func TestTokenMeteringTimedOutStreamCommitsPrefixAndResumesAfterRestart(t *testing.T) {
	m, now, file, source := tokenStreamFixture(t, "timeout")
	start := tokenMeteringStateRead(t, m).StartAt
	prior := TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: start, EndAt: start + 300, CursorAt: start,
		ReadPageRows: 256, ReadSuccessPages: 2, ReadWorstMS: 1800}
	if err := m.storeDB.Create(&prior).Error; err != nil {
		t.Fatal(err)
	}
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if pages != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read failure must remain visible after prefix commit: pages=%d err=%v", pages, err)
	}
	state := tokenMeteringStateRead(t, m)
	var w TokenMeteringWindow
	if err := m.storeDB.First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, start).Error; err != nil {
		t.Fatal(err)
	}
	if state.FactCount != tokenStreamPrefix || state.ScanThrough != start || state.CompleteThrough != start || state.ReadPageRows != 128 ||
		w.CursorID != tokenStreamPrefix || w.Done || w.SourceComplete || w.LastReason != "source_read_failed" || w.Attempts != 1 || w.ReadPageRows != 128 || w.ReadSuccessPages != 0 || w.ReadWorstMS != 0 {
		t.Fatalf("timed out prefix advanced or lost its checkpoint: state=%+v window=%+v", state, w)
	}
	// Reopen the file and build a new worker: both the page cursor and reduced
	// payload width must survive a restart without an in-memory hint.
	oldPool, _ := m.storeDB.DB()
	if err := oldPool.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(sqlite.Open(file), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = pool.Close() })
	restarted := &Monitor{cfg: m.cfg, storeDB: db, prodDB: m.prodDB}
	if _, err := restarted.runTokenMeteringRound(context.Background(), time.Unix(w.NextAt+1, 0)); err != nil {
		t.Fatal(err)
	}
	state = tokenMeteringStateRead(t, restarted)
	if state.FactCount != 256 || state.CompleteThrough != start+300 || state.ScanThrough != start+300 || state.ReadPageRows != 128 {
		t.Fatalf("resume did not finish the exact source population: %+v", state)
	}
	if source.cursors[1] != tokenStreamPrefix || source.limits[1] != 128 {
		t.Fatalf("restart ignored stored cursor/page width: cursors=%v limits=%v", source.cursors, source.limits)
	}
	var count, unique int64
	if err := db.Raw("SELECT COUNT(*),COUNT(DISTINCT log_id) FROM token_metering_facts").Row().Scan(&count, &unique); err != nil || count != 256 || unique != 256 {
		t.Fatal("resume duplicated or lost rows", count, unique, err)
	}
}

func TestTokenMeteringTimedOutStreamNeverPublishesUncommittedPrefix(t *testing.T) {
	m, now, _, _ := tokenStreamFixture(t, "timeout")
	start := tokenMeteringStateRead(t, m).StartAt
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("stream_commit_failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" {
			_ = tx.AddError(errors.New("injected prefix commit failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.storeDB.Callback().Create().Remove("stream_commit_failure"); err != nil {
			t.Error(err)
		}
	}()
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err == nil {
		t.Fatal("failed prefix commit became success")
	}
	state := tokenMeteringStateRead(t, m)
	var w TokenMeteringWindow
	if err := m.storeDB.First(&w).Error; err != nil {
		t.Fatal(err)
	}
	if state.FactCount != 0 || state.ScanThrough != start || state.CompleteThrough != start || w.CursorID != 0 || w.Done || w.SourceComplete {
		t.Fatalf("failed persistence published progress: state=%+v window=%+v", state, w)
	}
}

func TestTokenMeteringStreamTimeoutSurvivesLaterRoundDeadline(t *testing.T) {
	m, now, _, _ := tokenStreamFixture(t, "timeout")
	start := tokenMeteringStateRead(t, m).StartAt
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("slow_stream_prefix", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" {
			time.Sleep(1100 * time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.storeDB.Callback().Create().Remove("slow_stream_prefix"); err != nil {
			t.Error(err)
		}
	}()
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if pages != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transport failure hidden by later local work: pages=%d err=%v", pages, err)
	}
	state := tokenMeteringStateRead(t, m)
	var window TokenMeteringWindow
	if err := m.storeDB.First(&window).Error; err != nil {
		t.Fatal(err)
	}
	if state.FactCount <= 0 || state.FactCount > tokenStreamPrefix || window.CursorID != state.FactCount ||
		state.ScanThrough != start || state.CompleteThrough != start || state.ReadPageRows != 128 ||
		window.Done || window.SourceComplete || window.Attempts != 1 || window.LastReason != "source_read_failed" || window.NextAt <= now {
		t.Fatalf("source failure/backoff or prefix lost: state=%+v window=%+v", state, window)
	}
	if err := m.storeDB.Callback().Create().Remove("slow_stream_prefix"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(window.NextAt+1, 0)); err != nil {
		t.Fatal(err)
	}
	state = tokenMeteringStateRead(t, m)
	var count, unique int64
	if err := m.storeDB.Raw("SELECT COUNT(*),COUNT(DISTINCT log_id) FROM token_metering_facts").Row().Scan(&count, &unique); err != nil || count != 256 || unique != 256 {
		t.Fatal("deadline recovery duplicated or lost rows", count, unique, err)
	}
	if state.FactCount != 256 || state.ScanThrough != start+300 || state.CompleteThrough != start+300 {
		t.Fatalf("deadline recovery failed to complete source population: %+v", state)
	}
}

func TestTokenMeteringLegacyTimeoutBackoffSeedsReducedPage(t *testing.T) {
	tokenCheckLegacyReducedPage(t, "source_read_failed", 7)
}

func TestTokenMeteringLegacyExhaustedBackoffSeedsReducedPage(t *testing.T) {
	tokenCheckLegacyReducedPage(t, "retry_exhausted", 12)
}

func tokenCheckLegacyReducedPage(t *testing.T, reason string, attempts int) {
	t.Helper()
	m, now, _, source := tokenStreamFixture(t, "normal")
	state := tokenMeteringStateRead(t, m)
	window := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: state.StartAt, EndAt: state.StartAt + 300, CursorAt: state.StartAt, Attempts: attempts, NextAt: now + 300, LastReason: reason}
	if err := m.storeDB.Create(&window).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&state).Update("last_reason", reason).Error; err != nil {
		t.Fatal(err)
	}
	if pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err != nil || pages != 0 {
		t.Fatal("legacy failure backoff bypassed", pages, err)
	}
	if len(source.limits) != 0 {
		t.Fatal("legacy failed window read before its scheduled retry")
	}
	got := tokenMeteringStateRead(t, m)
	if got.ReadPageRows != 8 || got.FactCount != 0 || got.ScanThrough != state.ScanThrough {
		t.Fatal("legacy retry changed facts/coverage or ignored failure history", got)
	}
	var checkpoint TokenMeteringWindow
	if err := m.storeDB.First(&checkpoint).Error; err != nil || checkpoint.NextAt != window.NextAt || checkpoint.Attempts != attempts || checkpoint.CursorID != 0 || checkpoint.Done {
		t.Fatal("legacy backoff/cursor changed", checkpoint, err)
	}
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(window.NextAt, 0)); err != nil {
		t.Fatal(err)
	}
	if len(source.limits) == 0 || source.limits[0] != 8 || source.cursors[0] != 0 || tokenMeteringStateRead(t, m).FactCount == 0 {
		t.Fatal("due legacy retry did not start its reduced page at the durable cursor")
	}
}

func TestTokenMeteringSlowStreamShrinksBelowSixteenAndResumes(t *testing.T) {
	m, now, _, source := tokenStreamFixture(t, "slow_small_page")
	state := tokenMeteringStateRead(t, m)
	if err := m.storeDB.Model(&state).Update("read_page_rows", 16).Error; err != nil {
		t.Fatal(err)
	}
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if pages != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected a timed-out sixteen-row stream with a durable prefix", pages, err)
	}
	var window TokenMeteringWindow
	if err := m.storeDB.First(&window).Error; err != nil {
		t.Fatal(err)
	}
	partial := tokenMeteringStateRead(t, m)
	if partial.FactCount != 12 || partial.ScanThrough != state.StartAt || partial.CompleteThrough != state.StartAt || window.ReadPageRows != 8 || window.CursorID != 12 || window.Done || window.SourceComplete || window.Attempts != 1 || window.NextAt <= now {
		t.Fatal("slow stream did not retain its exact prefix and reduced retry", partial, window)
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	for turn := int64(0); turn < 2; turn++ {
		if _, err := restarted.runTokenMeteringRound(context.Background(), time.Unix(window.NextAt+turn*5, 0)); err != nil {
			t.Fatal("smaller page repeated the transport failure", err)
		}
	}
	complete := tokenMeteringStateRead(t, m)
	if complete.FactCount != 20 || complete.ScanThrough != state.StartAt+300 || complete.CompleteThrough != state.StartAt+300 || complete.ReadPageRows != 8 {
		t.Fatal("small-page restart lost rows, coverage or the learned width", complete)
	}
	if len(source.limits) != 3 || source.limits[0] != 16 || source.limits[1] != 8 || source.limits[2] != 8 || source.cursors[1] != 12 || source.cursors[2] != 20 {
		t.Fatal("restart reread the committed prefix or expanded an unmeasured page", source.limits, source.cursors)
	}
}

func TestTokenMeteringStreamErrorsPreserveOnlyCommittedPrefix(t *testing.T) {
	for _, mode := range []string{"cancel", "corrupt", "empty_timeout"} {
		t.Run(mode, func(t *testing.T) {
			m, now, _, _ := tokenStreamFixture(t, mode)
			start := tokenMeteringStateRead(t, m).StartAt
			ctx := context.Background()
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			if _, err := m.runTokenMeteringRound(ctx, time.Unix(now, 0)); err == nil {
				t.Fatal("failed source page became success")
			}
			state := tokenMeteringStateRead(t, m)
			wantRows := tokenMeteringPageRows
			if mode == "empty_timeout" {
				wantRows = 128
			}
			wantFacts := int64(0)
			if mode == "corrupt" {
				wantFacts = tokenStreamPrefix
			}
			if state.FactCount != wantFacts || state.ScanThrough != start || state.CompleteThrough != start || state.ReadPageRows != wantRows {
				t.Fatalf("failed/cancelled stream changed durable facts: %+v", state)
			}
			probe, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			release, err := m.acquireBackgroundSource(probe)
			if err != nil {
				t.Fatal("source slot leaked", err)
			}
			release()
		})
	}
}
