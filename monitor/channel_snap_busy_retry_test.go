package monitor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"
)

// A SQLITE_BUSY on the read-then-write snapshot transaction is retried and
// the final state equals a single clean replace.
func TestChannelSnapReplaceRetriesSQLiteBusy(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.replaceChannelSnapsAuthoritativeContext(context.Background(), []ChannelSnap{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}, 100); err != nil {
		t.Fatal(err)
	}
	var failures atomic.Int32
	failures.Store(2)
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("test:busy_once", func(db *gorm.DB) {
		if db.Statement.Table == "channel_snaps" && failures.Add(-1) >= 0 {
			db.AddError(errors.New("database is locked (5) (SQLITE_BUSY)"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.replaceChannelSnapsAuthoritativeContext(context.Background(), []ChannelSnap{{ID: 1, Name: "a2"}, {ID: 3, Name: "c"}}, 200); err != nil {
		t.Fatalf("busy replace not retried: %v", err)
	}
	if err := m.storeDB.Callback().Create().Remove("test:busy_once"); err != nil {
		t.Fatal(err)
	}
	var snaps []ChannelSnap
	if err := m.storeDB.Order("id").Find(&snaps).Error; err != nil {
		t.Fatal(err)
	}
	got := map[int]ChannelSnap{}
	for _, s := range snaps {
		got[s.ID] = s
	}
	if got[1].Name != "a2" || got[1].DeletedAt != 0 || got[3].Name != "c" || got[3].DeletedAt != 0 || got[2].DeletedAt != 200 {
		t.Fatalf("state after retried replace: %+v", snaps)
	}
	if failures.Load() > -1 {
		t.Fatalf("injected busy errors were not all hit: %d left", failures.Load())
	}
}
