package monitor

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestStartUserDirectorySyncUsesOneLowPriorityTask(t *testing.T) {
	m := newTestMonitor(t)
	prod, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "prod.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	if _, err := prod.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, "group" TEXT); INSERT INTO users VALUES (7,'alice','paid')`); err != nil {
		t.Fatal(err)
	}
	m.prodDB = prod
	release, err := m.acquireBackgroundSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			release()
		}
	}()

	started := time.Now()
	m.startUserDirectorySync(context.Background())
	m.startUserDirectorySync(context.Background())
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("用户名缓存不得阻塞主采样调用方")
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, low := m.backgroundSourceWaiterCounts()
		if low == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("低优先等待者数量异常: %d", low)
		}
		time.Sleep(time.Millisecond)
	}
	release()
	held = false
	deadline = time.Now().Add(time.Second)
	for m.userDirectorySyncing.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	rows, err := lookupUsersByName(m.storeDB, "alice")
	if err != nil || len(rows) != 1 || rows[0].UserID != 7 {
		t.Fatalf("用户名缓存同步结果错误: rows=%+v err=%v", rows, err)
	}
}
