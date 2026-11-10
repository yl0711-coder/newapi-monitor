package monitor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenAssociationLearnedBatchYieldsAndResumes(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	for id := int64(1); id <= 24; id++ {
		request := fmt.Sprintf("association-yield-%d", id)
		tokenMeteringSourceRow(t, m, id, start+id, request)
		tokenMeteringEvidence(t, m, request, start+id, 1000+id, request)
	}
	tokenMeteringRun(t, m, now)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("link_next_at>0").Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	m.tokenLinkCommitRows.Store(128)
	entered := make(chan struct{})
	var once sync.Once
	if err := m.tokenStore().Callback().Update().Before("gorm:update").Register("costly_association", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" {
			once.Do(func() { close(entered) })
			if !waitSourceLifecycle(tx.Statement.Context, 60*time.Millisecond) {
				tx.AddError(tx.Statement.Context.Err())
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer m.tokenStore().Callback().Update().Remove("costly_association")
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		count int
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		n, err := m.linkTokenMeteringBatch(ctx, now)
		done <- outcome{n, err}
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("association did not start")
	}
	queued, stopQueue := context.WithTimeout(context.Background(), 550*time.Millisecond)
	defer stopQueue()
	w := TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: now + 300, EndAt: now + 600, CursorID: 321}
	if err := m.tokenLocalWriteTransaction(queued, func(tx *gorm.DB) error { return tx.Create(&w).Error }); err != nil {
		t.Fatal("learned association batch blocked a queued collector", err)
	}
	cancel()
	r := <-done
	joined = true
	if r.err != nil && !errors.Is(r.err, context.Canceled) {
		t.Fatal(r.err)
	}
	var persisted int64
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("link_attempts>0").Count(&persisted).Error; err != nil || persisted != int64(r.count) || persisted < 1 || persisted >= 16 {
		t.Fatal("association reported an uncommitted prefix or failed to yield", persisted, r.count, err)
	}
	if err := m.tokenStore().Callback().Update().Remove("costly_association"); err != nil {
		t.Fatal(err)
	}
	if n, err := m.linkTokenMeteringBatch(context.Background(), now); err != nil || n+r.count != 24 {
		t.Fatal("association skipped or repeated the cancelled suffix", n, r.count, err)
	}
	var facts []TokenMeteringFact
	if err := m.tokenStore().Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	for _, f := range facts {
		if f.LinkAttempts != 1 || f.LinkState != "unique" || f.RequestLength == nil || *f.RequestLength != 1000+f.LogID {
			t.Fatal("association evidence changed during resume", f.LogID, f.LinkAttempts)
		}
	}
}
