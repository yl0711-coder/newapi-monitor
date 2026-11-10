package monitor

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTokenMeteringIdleWakesAtNextSealedWindow(t *testing.T) {
	m, _, _ := tokenMeteringFixture(t)
	defer m.Close()
	state := tokenMeteringStateRead(t, m)
	now := state.ScanThrough + tokenMeteringLagSeconds + tokenMeteringWindowSeconds - 20
	tokenMeteringSourceRow(t, m, 1, state.ScanThrough+1, "")
	tokenMeteringSourceRow(t, m, 2, now-5, "open-tail")
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if err != nil || pages < 1 || pages > 4 || tokenMeteringStateRead(t, m).FactCount != 1 || tokenMeteringStateRead(t, m).ScanThrough != state.ScanThrough {
		t.Fatal("open tail blocked or unsealed history certified", pages, err)
	}
	for _, runErr := range []error{nil, &tokenMeteringReadError{err: context.DeadlineExceeded, queryStarted: true}} {
		if delay := m.tokenMeteringRoundDelay(context.Background(), 0, runErr, now); delay > 10*time.Second || delay <= 0 {
			t.Fatal("idle collector slept across the live tail", delay)
		}
	}
	if delay := m.tokenMeteringRoundDelay(context.Background(), 0, context.DeadlineExceeded, now); delay != time.Minute {
		t.Fatal("local failure was accelerated by the idle wakeup", delay)
	}
	if delay := m.tokenMeteringRoundDelay(context.Background(), 0, nil, now-100); delay > 10*time.Second || delay <= 0 {
		t.Fatal("tail poll lost its cadence", delay)
	}
	pages, err = m.runTokenMeteringRound(context.Background(), time.Unix(now+20, 0))
	state = tokenMeteringStateRead(t, m)
	if err != nil || pages < 1 || pages > 4 || state.FactCount != 2 || state.ScanThrough != state.StartAt+300 || state.CompleteThrough != state.ScanThrough {
		t.Fatal("newly sealed window did not persist and advance", pages, err, state)
	}
}

func TestTokenMeteringCatchupWithContinuousArrivalsAndPageRecovery(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	start := (now-tokenMeteringLagSeconds)/300*300 - 3600
	if err := m.storeDB.Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Updates(map[string]any{"start_at": start, "scan_through": start}).Error; err != nil {
		t.Fatal(err)
	}
	// Twelve dense historical windows, each requiring four keyset pages.
	// The reserved forward slot must leave a durable partial window while the
	// other slots serve recent observations and independent replay.
	const initialRows = 10800
	id := int64(0)
	appendRows := func(n int, first int64) {
		t.Helper()
		tx, err := m.prodDB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			id++
			_, err = tx.Exec("INSERT INTO logs(id,created_at,type,user_id,channel_id,model_name,`group`,prompt_tokens,completion_tokens,quota,is_stream,use_time,other,request_id) VALUES(?,?,2,2,3,'synthetic-model','actual-group',100,10,200,1,1,'{}','')", id, first+int64(i)/3)
			if err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	appendRows(initialRows, start)
	initialLag := int64(3600)
	initialTarget := start + initialLag
	var lastCursor int64
	for round := 0; round < 120; round++ {
		begin := time.Now()
		pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
		if err != nil {
			t.Fatal(err)
		}
		state := tokenMeteringStateRead(t, m)
		if state.FactCount > id || state.ScanThrough > (now-tokenMeteringLagSeconds)/300*300 {
			t.Fatal("fabricated progress")
		}
		if round == 0 {
			var w TokenMeteringWindow
			if err := m.storeDB.First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, start).Error; err != nil {
				t.Fatal(err)
			}
			lastCursor = w.CursorID
			if lastCursor != 256 || state.ScanThrough != start {
				t.Fatalf("dense page checkpoint: cursor=%d scan=%d", lastCursor, state.ScanThrough)
			}
			if _, err := m.prodDB.Exec("ALTER TABLE logs RENAME TO unavailable_logs"); err != nil {
				t.Fatal(err)
			}
			if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+1, 0)); err == nil {
				t.Fatal("source outage hidden")
			}
			if s := tokenMeteringStateRead(t, m); s.FactCount != lastCursor || s.ScanThrough != start {
				t.Fatal("outage advanced cursor")
			}
			if _, err := m.prodDB.Exec("ALTER TABLE unavailable_logs RENAME TO logs"); err != nil {
				t.Fatal(err)
			}
			now += 31 // Reach the persisted backoff; no cursor/next_at edits.
		}
		lag := (now-tokenMeteringLagSeconds)/300*300 - state.ScanThrough
		if round%5 == 0 {
			t.Logf("round=%d facts=%d scan_lag_seconds=%d", round, state.FactCount, lag)
		}
		if lag == 0 {
			if state.ScanThrough <= initialTarget || state.FactCount <= initialRows {
				t.Fatal("catchup did not include newly arriving rows across a moving closed boundary")
			}
			if state.CompleteThrough != state.ScanThrough {
				t.Fatal("catchup has a hidden incomplete prefix")
			}
			var expected int64
			if err := m.prodDB.QueryRow("SELECT COUNT(*) FROM logs WHERE created_at>=? AND created_at<?", start, state.ScanThrough).Scan(&expected); err != nil {
				t.Fatal(err)
			}
			var covered int64
			if err := m.storeDB.Model(&TokenMeteringFact{}).Where("source_epoch=? AND created_at>=? AND created_at<?", state.SourceEpoch, start, state.ScanThrough).Count(&covered).Error; err != nil || expected != covered {
				t.Fatalf("keyset coverage loss/duplicate: source=%d local=%d err=%v", expected, covered, err)
			}
			if err := m.storeDB.Model(&TokenMeteringFact{}).Where("source_epoch=?", state.SourceEpoch).Count(&expected).Error; err != nil || expected != state.FactCount {
				t.Fatalf("durable count: %d %v", expected, err)
			}
			t.Logf("caught up: initial_lag=%d facts=%d arrivals_total=%d arrivals_persisted=%d target_advanced_seconds=%d failed_page_cursor=%d", initialLag, state.FactCount, id-initialRows, state.FactCount-initialRows, state.ScanThrough-initialTarget, lastCursor)
			return
		}
		delay := m.tokenMeteringRoundDelay(context.Background(), pages, nil, now)
		if delay != 2*time.Second {
			t.Fatalf("backlog sleeps %s", delay)
		}
		elapsed := int64((time.Since(begin)+time.Second-1)/time.Second) + int64(delay/time.Second)
		// Advance at least thirty source seconds per round so arrivals become
		// eligible during catch-up, rather than all remaining beyond its cutoff.
		elapsed = max(elapsed, 30)
		now += elapsed
		// Ongoing traffic is added at the newly closed edge, never overwrites IDs.
		appendRows(20, now-tokenMeteringLagSeconds-15)
	}
	t.Fatal(fmt.Sprintf("continuous arrivals failed to catch up: %+v", tokenMeteringStateRead(t, m)))
}

func TestTokenMeteringBudgetWaitIsDueButFailureBackoffIsNot(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	if got := m.tokenMeteringRoundDelay(context.Background(), 0, nil, now); got != 2*time.Second {
		t.Fatalf("due source work slept: %s", got)
	}
	state := tokenMeteringStateRead(t, m)
	if err := m.storeDB.Create(&TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: state.ScanThrough, EndAt: state.ScanThrough + 300, NextAt: now + 30, LastReason: "source_read_failed"}).Error; err != nil {
		t.Fatal(err)
	}
	if got := m.tokenMeteringRoundDelay(context.Background(), 0, nil, now); got > 10*time.Second || got <= 0 {
		t.Fatalf("historical failure blocked independent latest work: %s", got)
	}
	var failed TokenMeteringWindow
	if err := m.storeDB.First(&failed, "source_epoch=? AND start_at=?", state.SourceEpoch, state.ScanThrough).Error; err != nil || failed.NextAt != now+30 || failed.CursorID != 0 {
		t.Fatal("failed window backoff changed", failed, err)
	}
}
