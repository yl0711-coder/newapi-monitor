package monitor

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenMeteringRecentWindowLongOutageKeepsHistoryAndCollectsTail(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	start, sealed := tokenRecentBacklog(t, m, now)
	epoch := m.cfg.TokenFactsSourceEpoch
	old := TokenMeteringWindow{SourceEpoch: epoch, StartAt: sealed - 18*3600, EndAt: sealed - 18*3600 + 300,
		CursorAt: sealed - 18*3600 + 100, CursorID: 17, Attempts: 3, NextAt: now + 60,
		LastReason: "source_read_failed", Lane: tokenMeteringLaneRealtimeTail}
	if err := m.tokenStore().Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Create(&TokenMeteringLaneState{SourceEpoch: epoch, Lane: tokenMeteringLaneRealtimeTail, DiscoveryAt: old.StartAt}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Create(&TokenMeteringWindow{SourceEpoch: epoch, StartAt: start, EndAt: start + 300, CursorAt: start, NextAt: now + 3600}).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringSourceRow(t, m, 100, sealed-250, "")
	for i := 0; i < 2; i++ {
		// The second process must resume the same durable tail window, without
		// losing the old failed prefix or inserting the latest fact twice.
		restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
		if _, err := restarted.runTokenMeteringRound(context.Background(), time.Unix(now, 0)); err != nil {
			t.Fatal(err)
		}
	}
	state := tokenMeteringStateRead(t, m)
	if state.FactCount != 1 || state.ScanThrough != start || state.CompleteThrough != start {
		t.Fatal("latest collection lost facts or fabricated historical coverage", state)
	}
	var saved TokenMeteringWindow
	if err := m.tokenStore().First(&saved, "source_epoch=? AND start_at=?", epoch, old.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	if saved.CursorAt != old.CursorAt || saved.CursorID != old.CursorID || saved.Attempts != old.Attempts || saved.NextAt != old.NextAt || saved.Done || saved.SourceComplete {
		t.Fatal("aged recent prefix was changed or marked complete", saved)
	}
	if complete, err := tokenMeteringSelectedRangeComplete(m.tokenStore(), state, sealed-300, sealed); err != nil || !complete {
		t.Fatal("fresh sealed window did not finish independently", complete, err)
	}
}

// The latest lane can pass an old prefix while preserving that exact durable
// retry. Ordered historical recovery is a separate reservation.
func TestTokenMeteringRecentWindowKeepsOldestUnfinishedPrefix(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	state := tokenMeteringStateRead(t, m)
	sealed := (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	state.ScanThrough = sealed - 7200
	if !tokenMeteringNeedsRecent(state.ScanThrough, now) {
		t.Fatal("fixture does not have a recent backlog")
	}
	old := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: sealed - 900, EndAt: sealed - 600,
		CursorAt: sealed - 900, ReadPageRows: tokenMeteringPageRows, NextAt: 0}
	newer := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: sealed - 600, EndAt: sealed - 300,
		CursorAt: sealed - 600, ReadPageRows: tokenMeteringPageRows, Done: true, SourceComplete: true,
		LastScannedAt: now, NextAt: now + 1800}
	if err := m.tokenStore().Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Create(&newer).Error; err != nil {
		t.Fatal(err)
	}
	var got TokenMeteringWindow
	var eligible bool
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error {
		var err error
		got, eligible, err = tokenMeteringRecentWindow(tx, state, time.Unix(now, 0).Unix())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !eligible || got.StartAt != now/300*300 || got.Done {
		t.Fatalf("old prefix blocked latest reservation: eligible=%t got=%+v", eligible, got)
	}
	var saved TokenMeteringWindow
	if err := m.tokenStore().First(&saved, "source_epoch=? AND start_at=?", old.SourceEpoch, old.StartAt).Error; err != nil || tokenDetectionJSON(saved) != tokenDetectionJSON(old) {
		t.Fatal("old prefix changed", saved, err)
	}
}
