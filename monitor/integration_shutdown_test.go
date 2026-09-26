package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// Both release branches add work that must finish before SQLite closes.
// Exercise the merged Close path with both kinds of task running together.
func TestIntegrationCloseDrainsInvestigationAndFinanceBeforeDatabase(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	investigationCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &Monitor{storeDB: db, investigationTasks: map[string]*logChainInvestigationTask{
		"active": {Status: "running", Cancel: cancel},
	}}
	completed := make(chan error, 2)
	m.investigationWG.Add(1)
	go func() {
		defer m.investigationWG.Done()
		<-investigationCtx.Done()
		completed <- pool.Ping()
	}()
	financeStarted := make(chan struct{})
	m.financeAsyncQueue.submit(context.Background(), "active", false, func(ctx context.Context) error {
		close(financeStarted)
		<-ctx.Done()
		completed <- pool.Ping()
		return ctx.Err()
	})
	<-financeStarted
	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("merged shutdown did not cancel and drain both task types")
	}
	for range 2 {
		if err := <-completed; err != nil {
			t.Fatalf("database closed while a cancelled task was still draining: %v", err)
		}
	}
	if err := pool.Ping(); err == nil {
		t.Fatal("database remained open after tasks drained")
	}
	if state := m.financeAsyncQueue.submit(context.Background(), "later", false, nil); state != "stopped" {
		t.Fatalf("shutdown accepted new finance work: %s", state)
	}
	m.Close() // repeated shutdown must remain safe
}
