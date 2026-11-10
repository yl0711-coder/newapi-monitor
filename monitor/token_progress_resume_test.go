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
	m, now, _ := detectionFixture(t)
	defer m.Close()
	now = time.Now().Unix()
	m.cfg.TokenFactsEnabled = false
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 250, 10
	base := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	add := func(start, end int64) {
		rows := []TokenMeteringFact{}
		for id := start; id <= end; id++ {
			f := base
			f.LogID = id
			rows = append(rows, f)
		}
		if err := m.storeDB.CreateInBatches(rows, 64).Error; err != nil {
			t.Fatal(err)
		}
	}
	add(2, 500)
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
			add(501, 525)
			added = true
		}
		if count == 525 {
			var span struct{ First, Last int64 }
			if err := m.storeDB.Model(&TokenDetectionCurrent{}).Select("MIN(first_evaluated_at) AS first, MAX(first_evaluated_at) AS last").Scan(&span).Error; err != nil {
				t.Fatal(err)
			}
			if span.Last-span.First >= 20 {
				t.Fatal("catch-up still waits idle cadence between full batches")
			}
			t.Logf("maintenance detected historical=500 ongoing_arrivals=25 elapsed_between_first_and_last=%ds", span.Last-span.First)
			cancel()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("maintenance did not drain historical and newly arrived work")
}
