package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"
)

func tokenRecentBacklog(t *testing.T, m *Monitor, now int64) (int64, int64) {
	t.Helper()
	sealed := (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	start := sealed - 86400
	if err := m.tokenStore().Where("last_reason=?", "fixture_unrelated_recent_cooldown").Delete(&TokenMeteringWindow{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).
		Updates(map[string]any{"start_at": start, "scan_through": start, "complete_through": start, "read_page_rows": 256}).Error; err != nil {
		t.Fatal(err)
	}
	return start, sealed
}

func TestTokenMeteringRecentResumesWhileHistoryBacksOff(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start, sealed := tokenRecentBacklog(t, m, now)
	history := TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: start, EndAt: start + 300, CursorAt: start, NextAt: now + 3600, LastReason: "source_read_failed"}
	if err := m.tokenStore().Create(&history).Error; err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 12; id++ {
		tokenMeteringSourceRow(t, m, id, now-300+id, "")
	}
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	state := tokenMeteringStateRead(t, m)
	if err != nil || pages < 1 || state.FactCount != 12 || state.ScanThrough != start || state.CompleteThrough != start {
		t.Fatal("recent collection waited for or skipped history", pages, err, state)
	}
	health, err := m.tokenMeteringHealth(context.Background(), m.cfg.TokenFactsSourceEpoch, now)
	if err != nil || health.PendingWindows <= 0 {
		t.Fatal("open observation hid the historical gap", health.PendingWindows, err)
	}
	if complete, err := tokenMeteringSelectedRangeComplete(m.tokenStore(), state, now-300, now); err != nil || complete {
		t.Fatal("open EOF was promoted to sealed coverage", complete, err)
	}
	if complete, err := tokenMeteringSelectedRangeComplete(m.tokenStore(), state, start, sealed); err != nil || complete {
		t.Fatal("recent proof hid the historical gap", complete, err)
	}
	if delay := m.tokenMeteringRoundDelay(context.Background(), 0, nil, now); delay <= 0 || delay > 10*time.Second {
		t.Fatal("tail poll was delayed by historical backoff", delay)
	}
	// Restart from the stored window, then advance two sealed boundaries.
	// Both windows must be read in order, including the one missed in downtime.
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	tokenMeteringSourceRow(t, m, 13, sealed+1, "")
	tokenMeteringSourceRow(t, m, 14, sealed+301, "")
	if delay := restarted.tokenMeteringRoundDelay(context.Background(), 0, nil, now+600); delay != 2*time.Second {
		t.Fatal("history backoff delayed runnable recent work", delay)
	}
	for round := int64(0); round < 6; round++ {
		if _, err := restarted.runTokenMeteringRound(context.Background(), time.Unix(now+600+round*10, 0)); err != nil {
			t.Fatal(err)
		}
	}
	state = tokenMeteringStateRead(t, m)
	if state.FactCount != 14 || state.ScanThrough != start || state.CompleteThrough != start {
		t.Fatal("restart skipped an interval or advanced history", state)
	}
	if complete, err := tokenMeteringSelectedRangeComplete(m.tokenStore(), state, sealed, sealed+600); err != nil || !complete {
		t.Fatal("recent restart left an unrecorded gap", complete, err)
	}
}

func TestTokenMeteringRecentTimeoutKeepsPrefixAndIndependentWidth(t *testing.T) {
	m, now, _, source := tokenStreamFixture(t, "timeout")
	start, _ := tokenRecentBacklog(t, m, now)
	tail := now
	now += 25
	source.created = tail + 1
	m.tokenMeteringRounds.Store(1) // Recent gets the first bounded source turn.
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err == nil {
		t.Fatal("recent timeout hidden")
	}
	var w TokenMeteringWindow
	if err := m.tokenStore().First(&w, "source_epoch=? AND start_at=?", m.cfg.TokenFactsSourceEpoch, tail).Error; err != nil {
		t.Fatal(err)
	}
	state := tokenMeteringStateRead(t, m)
	if w.CursorID != tokenStreamPrefix || w.Done || w.SourceComplete || w.ReadPageRows != 128 || w.Attempts != 1 ||
		state.FactCount != tokenStreamPrefix || state.ReadPageRows != 256 || state.ScanThrough != start+300 {
		t.Fatal("recent failure lost prefix, fabricated EOF or throttled history", w, state)
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	for i := int64(0); i < 3; i++ {
		if _, err := restarted.runTokenMeteringRound(context.Background(), time.Unix(w.NextAt+i*2, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.tokenStore().First(&w, "source_epoch=? AND start_at=?", w.SourceEpoch, w.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	var unique int64
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Distinct("log_id").Count(&unique).Error; err != nil {
		t.Fatal(err)
	}
	state = tokenMeteringStateRead(t, m)
	if w.Done || w.SourceComplete || unique != 256 || state.FactCount != 256 || state.ScanThrough >= w.StartAt || state.CompleteThrough >= w.StartAt {
		t.Fatal("recent retry did not resume its persisted prefix", unique, w, state)
	}
}

type tokenRecentBlockedSource struct {
	tokenStreamSource
	blocked int64
	starts  []int64
}

func (s *tokenRecentBlockedSource) Connect(context.Context) (driver.Conn, error) {
	return &tokenRecentBlockedConn{s}, nil
}

type tokenRecentBlockedConn struct{ source *tokenRecentBlockedSource }

func (*tokenRecentBlockedConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*tokenRecentBlockedConn) Close() error                        { return nil }
func (*tokenRecentBlockedConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *tokenRecentBlockedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	start := args[1].Value.(int64)
	c.source.starts = append(c.source.starts, start)
	if start == c.source.blocked {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return (&tokenStreamConn{&c.source.tokenStreamSource}).QueryContext(ctx, query, args)
}

func TestTokenMeteringRecentGetsTurnAfterSlowHistory(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start, _ := tokenRecentBacklog(t, m, now)
	source := &tokenRecentBlockedSource{tokenStreamSource: tokenStreamSource{created: now - 5, rows: 8, mode: "normal"}, blocked: start}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err == nil {
		t.Fatal("history failure hidden")
	}
	if len(source.starts) < 3 || len(source.starts) > 4 || source.starts[0] != now || source.starts[1] != now-300 || source.starts[len(source.starts)-1] != start || tokenMeteringStateRead(t, m).FactCount != 8 {
		t.Fatal("source budget or historical checkpoint changed", source.starts)
	}
	if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+2, 0)); err != nil {
		t.Fatal(err)
	}
	state := tokenMeteringStateRead(t, m)
	if state.FactCount != 8 || state.ScanThrough != start {
		t.Fatal("slow history starved current data", source.starts, state)
	}
}

func TestTokenMeteringRecentCachedSuffixCommitsBeforeSource(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start, _ := tokenRecentBacklog(t, m, now)
	ctx := context.Background()
	state, claimed, err := m.claimTokenMetering(ctx, now)
	if err != nil || !claimed {
		t.Fatal(claimed, err)
	}
	w, found, err := m.nextTokenMeteringWork(ctx, state.SourceEpoch, state.LeaseToken, now, tokenMeteringLaneRecent)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	for id := int64(1); id <= 12; id++ {
		tokenMeteringSourceRow(t, m, id, w.StartAt+id, "")
	}
	rows, done, err := m.readTokenMeteringPage(ctx, w)
	if err != nil || !done || len(rows) != 12 {
		t.Fatal(done, len(rows), err)
	}
	facts, refs := []TokenMeteringFact{}, []TokenMeteringRef{}
	for _, row := range rows[:4] {
		f, r := parseTokenMetering(m.cfg, row, now)
		facts, refs = append(facts, f), append(refs, r...)
	}
	if err := m.commitTokenMeteringPage(ctx, state.LeaseToken, w, rows[:4], facts, refs, false, now); err != nil {
		t.Fatal(err)
	}
	w.CursorAt, w.CursorID = rows[3].CreatedAt, rows[3].ID
	m.tokenMeteringReadCache = &tokenMeteringCachedRead{Window: w, Rows: rows[4:], Done: true, NextReadRows: 256, ObservedAt: now, ExpiresAt: time.Now().Add(30 * time.Second)}
	m.tokenMeteringFinishedLease = state.LeaseToken
	if err := m.tokenStore().Create(&TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: start, EndAt: start + 300, NextAt: now + 60}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.prodDB.Close(); err != nil {
		t.Fatal(err)
	}
	_, _ = m.runTokenMeteringRound(ctx, time.Unix(now, 0)) // Other windows may observe the closed source; the cached prefix must still commit.
	state = tokenMeteringStateRead(t, m)
	if state.FactCount != 12 || state.ScanThrough != start || m.tokenMeteringReadCache != nil {
		t.Fatal("recent suffix was lost or moved the global frontier", state)
	}
}

func TestTokenMeteringForwardReusesRecentEOFWithoutCrossingGap(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	now += 600
	ctx := context.Background()
	state, claimed, err := m.claimTokenMetering(ctx, now)
	if err != nil || !claimed {
		t.Fatal(claimed, err)
	}
	for offset := int64(300); offset <= 600; offset += 300 {
		w := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: state.StartAt + offset, EndAt: state.StartAt + offset + 300, Done: true, SourceComplete: true, NextAt: now + 1800}
		if err := m.tokenStore().Create(&w).Error; err != nil {
			t.Fatal(err)
		}
	}
	// The first, absent window still needs a real read despite the later EOFs.
	w, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found || w.StartAt != state.StartAt || tokenMeteringStateRead(t, m).ScanThrough != state.StartAt {
		t.Fatal("forward collection jumped an absent interval", found, w, err)
	}
	if err := m.commitTokenMeteringPage(ctx, state.LeaseToken, w, nil, nil, nil, true, now); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false); err != nil || found {
			t.Fatal("EOF required a duplicate source read", found, err)
		}
	}
	got := tokenMeteringStateRead(t, m)
	if got.ScanThrough != state.StartAt+900 || got.CompleteThrough != got.ScanThrough {
		t.Fatal("contiguous recent coverage did not join history", got)
	}
}
