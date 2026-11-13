package monitor

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenMeteringLocalBudgetCommitsPrefixAndResumes(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	state, ok, err := m.claimTokenMetering(context.Background(), now)
	if err != nil || !ok {
		t.Fatal(err)
	}
	w, ok, err := m.nextTokenMeteringWindow(context.Background(), state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !ok {
		t.Fatal(err)
	}
	for id := int64(1); id <= 150; id++ {
		tokenMeteringSourceRow(t, m, id, w.StartAt+id, "budget-prefix-"+strconv.FormatInt(id, 10))
	}
	raw, done, err := m.readTokenMeteringPage(context.Background(), w)
	if err != nil || !done || len(raw) != 150 {
		t.Fatal(len(raw), done, err)
	}
	facts, refs := []TokenMeteringFact{}, []TokenMeteringRef{}
	for _, row := range raw {
		f, r := parseTokenMetering(m.cfg, row, now)
		facts = append(facts, f)
		refs = append(refs, r...)
	}
	var delayed atomic.Bool
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("prefix_budget_delay", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" && delayed.CompareAndSwap(false, true) {
			time.Sleep(1300 * time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	n, err := m.commitTokenMeteringReadPage(context.Background(), state.LeaseToken, w, raw, facts, refs, done, now)
	if err != nil || n <= 0 || n >= len(raw) {
		t.Fatalf("budget lost committed prefix: %d %v", n, err)
	}
	prefix := n
	if err := m.storeDB.Callback().Create().Remove("prefix_budget_delay"); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, w.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	got := tokenMeteringStateRead(t, m)
	if w.Done || w.SourceComplete || w.CursorID != int64(prefix) || got.ScanThrough != w.StartAt || got.FactCount != int64(prefix) {
		t.Fatal("partial commit falsely completed the source window")
	}
	raw, done, err = m.readTokenMeteringPage(context.Background(), w)
	if err != nil || !done || len(raw) != 150-prefix || raw[0].ID != int64(prefix+1) {
		t.Fatal("source did not resume at durable prefix")
	}
	facts, refs = nil, nil
	for _, row := range raw {
		f, r := parseTokenMetering(m.cfg, row, now)
		facts = append(facts, f)
		refs = append(refs, r...)
	}
	n, err = m.commitTokenMeteringReadPage(context.Background(), state.LeaseToken, w, raw, facts, refs, done, now)
	if err != nil || n != 150-prefix {
		t.Fatal(n, err)
	}
	got = tokenMeteringStateRead(t, m)
	if got.FactCount != 150 || got.ScanThrough != w.EndAt || got.CompleteThrough != w.EndAt {
		t.Fatal("resumed page did not publish complete durable window exactly once")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.commitTokenMeteringReadPage(ctx, state.LeaseToken, w, raw, facts, refs, done, now); !errors.Is(err, context.Canceled) {
		t.Fatal("caller cancellation was hidden", err)
	}
}

func TestTokenDetectionMaintenanceCatchesNewArrivals(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	now := time.Now().Unix()
	m.cfg.TokenFactsEnabled = false
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 50, 10
	base := detectionFact(t, m, 1, now-3600, 3, 20000, 0, 0)
	add := func(start, end, createdAt int64) {
		rows := []TokenMeteringFact{}
		for id := start; id <= end; id++ {
			f := base
			f.LogID, f.CreatedAt = id, createdAt
			rows = append(rows, f)
		}
		if err := m.storeDB.CreateInBatches(rows, 64).Error; err != nil {
			t.Fatal(err)
		}
	}
	add(2, 500, now-3600)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.startTokenMeteringMaintenance(ctx)
	deadline := time.Now().Add(28 * time.Second)
	added := false
	for time.Now().Before(deadline) {
		var count int64
		if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count > 0 && !added {
			// These requests arrived after maintenance started. Give them their
			// actual recent event time rather than copying the backlog's timestamp.
			add(501, 525, time.Now().Unix()-30)
			added = true
		}
		if added {
			var recent int64
			if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("log_id>=501 AND log_id<=525").Count(&recent).Error; err != nil {
				t.Fatal(err)
			}
			if recent == 25 {
				var historical int64
				if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("log_id<=500").Count(&historical).Error; err != nil {
					t.Fatal(err)
				}
				if historical == 0 || historical >= 500 {
					t.Fatal("new arrivals waited for history to drain or history never progressed", historical)
				}
				var retained int64
				if err := m.storeDB.Model(&TokenMeteringFact{}).Count(&retained).Error; err != nil || retained != 525 {
					t.Fatal("latest priority removed retained work", retained, err)
				}
				t.Logf("maintenance detected recent=%d while historical=%d/500 remained durable", recent, historical)
				cancel()
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("maintenance did not detect the new arrivals while history remained queued")
}
