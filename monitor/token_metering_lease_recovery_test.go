package monitor

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenMeteringActiveOwnerCannotOverlapRounds(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "")
	held, release := make(chan struct{}), make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("hold_metering_owner", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_windows" {
			holdOnce.Do(func() {
				close(held)
				select {
				case <-release:
				case <-tx.Statement.Context.Done():
					tx.AddError(tx.Statement.Context.Err())
				}
			})
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer m.storeDB.Callback().Create().Remove("hold_metering_owner")
	type outcome struct {
		pages int
		err   error
	}
	finished := make(chan outcome, 1)
	go func() {
		pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
		finished <- outcome{pages, err}
	}()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		result := <-finished
		if result.err != nil || result.pages != 1 || tokenMeteringStateRead(t, m).FactCount != 1 {
			t.Error("active owner failed or overlapping calls duplicated progress", result)
		}
	}()
	select {
	case <-held:
	case <-time.After(3 * time.Second):
		t.Fatal("owner did not enter its transaction")
	}
	started := time.Now()
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	if pages != 0 || err != nil || time.Since(started) > time.Second {
		t.Error("overlapping round did not yield to the active owner", pages, err)
	}
}

func TestTokenMeteringCompletedOwnerResumesAfterReleaseDeadline(t *testing.T) {
	m, now, _ := tokenMeteringHistoryFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	source := &tokenRoundBudgetSource{created: start + 1}
	m.prodDB = sql.OpenDB(source)
	t.Cleanup(func() { _ = m.prodDB.Close() })
	var releaseFailed bool
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("expire_metering_release", func(tx *gorm.DB) {
		fields, ok := tx.Statement.Dest.(map[string]any)
		if tx.Statement.Table == "token_metering_states" && ok && len(fields) == 1 && fields["lease_until"] == 0 {
			<-tx.Statement.Context.Done()
			tx.AddError(tx.Statement.Context.Err())
			releaseFailed = true
		}
	}); err != nil {
		t.Fatal(err)
	}
	pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	_ = m.storeDB.Callback().Update().Remove("expire_metering_release")
	state := tokenMeteringStateRead(t, m)
	if err != nil || pages != 1 || !releaseFailed || state.FactCount != 256 || state.LeaseUntil != now+30 {
		t.Fatalf("release-deadline fixture did not retain the completed owner's lease: pages=%d release_failed=%t error=%v state=%+v", pages, releaseFailed, err, state)
	}
	// A different process still cannot steal an unexpired durable lease.
	other := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	if pages, err := other.runTokenMeteringRound(context.Background(), time.Unix(now+2, 0)); pages != 0 || err != nil {
		t.Fatal("another worker bypassed the lease", pages, err)
	}
	source.queries.Store(2)
	pages, err = m.runTokenMeteringRound(context.Background(), time.Unix(now+2, 0))
	state = tokenMeteringStateRead(t, m)
	if err != nil || pages == 0 || state.FactCount != 256 || state.ScanThrough != start+300 || state.CompleteThrough != start+300 {
		t.Fatalf("finished owner waited for its own lease instead of resuming: pages=%d error=%v state=%+v", pages, err, state)
	}
	// The in-process completion proof must stop applying once another worker
	// has acquired the row, even if this Monitor remembers its previous token.
	foreign, claimed, err := other.claimTokenMetering(context.Background(), now+3)
	if err != nil || !claimed {
		t.Fatal("foreign claim", claimed, err)
	}
	if pages, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+4, 0)); pages != 0 || err != nil {
		t.Fatal("completed owner bypassed a newer foreign lease", pages, err)
	}
	if got := tokenMeteringStateRead(t, m); got.LeaseToken != foreign.LeaseToken {
		t.Fatal("newer lease was overwritten")
	}
}
