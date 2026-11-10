package monitor

import (
	"context"
	"errors"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"path/filepath"
	"testing"
	"time"
)

func TestTokenReportRecheckAvoidsRedundantProjectionWrites(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	h := TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 1, CreatedAt: now - 60, SourceComplete: false, DetectionState: "evidence_insufficient", ResultJSON: `{"rules":[]}`}
	if err := db.Create(&h).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("CREATE TABLE token_write_audit (event TEXT)").Error; err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
		if err := db.Exec("CREATE TRIGGER audit_entry_" + event + " AFTER " + event + " ON " + tokenReportIndexTable + " BEGIN INSERT INTO token_write_audit VALUES ('" + event + "'); END").Error; err != nil {
			t.Fatal(err)
		}
	}
	timing := &tokenWriteTiming{}
	ctx := context.WithValue(context.Background(), tokenWriteTimingKey{}, timing)
	if err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		for i := 0; i < 8; i++ {
			h.NextAt++
			if err := tx.Save(&h).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Table("token_write_audit").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("unchanged rechecks=8 projection_writes=%d timing=%+v", count, *timing)
	if count != 0 {
		t.Fatalf("unchanged detection rechecks rewrote report index %d times", count)
	}
	if err := db.Model(&h).Update("candidate", true).Error; err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.Table(tokenReportIndexTable).Select("status").Where("source_epoch=? AND log_id=1", h.SourceEpoch).Scan(&status).Error; err != nil || status != "candidate" {
		t.Fatal(status, err)
	}
}

func TestTokenWriteTimingSeparatesQueuePoolLockAndExecution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.db")
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(path+"?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=busy_timeout(40)"), &gorm.Config{Logger: logger.Discard, PrepareStmt: true})
		if err != nil {
			t.Fatal(err)
		}
		p, _ := db.DB()
		p.SetMaxOpenConns(1)
		t.Cleanup(func() { p.Close() })
		return db
	}
	db, other := open(), open()
	if err := db.Exec("CREATE TABLE writes (id INTEGER PRIMARY KEY)").Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	run := func(budget time.Duration, fn func(*gorm.DB) error) (tokenWriteTiming, error) {
		timing := &tokenWriteTiming{}
		ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), tokenWriteTimingKey{}, timing), budget)
		defer cancel()
		err := m.tokenLocalWriteTransaction(ctx, fn)
		return *timing, err
	}
	noSQL := func(*gorm.DB) error { t.Fatal("transaction ran while blocked"); return nil }
	m.tokenLocalWriteMu.Lock()
	timing, err := run(25*time.Millisecond, noSQL)
	m.tokenLocalWriteMu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || timing.Stage != "writer_wait" || timing.Connection != 0 {
		t.Fatal(timing, err)
	}
	pool, _ := db.DB()
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	timing, err = run(25*time.Millisecond, noSQL)
	conn.Close()
	if !errors.Is(err, context.DeadlineExceeded) || timing.Stage != "connection_wait" || timing.Lock != 0 {
		t.Fatal(timing, err)
	}
	locked := other.Begin()
	if locked.Error != nil {
		t.Fatal(locked.Error)
	}
	timing, err = run(200*time.Millisecond, noSQL)
	locked.Rollback()
	if err == nil || timing.Stage != "database_lock" || timing.Lock < 20*time.Millisecond || timing.Work != 0 {
		t.Fatal(timing, err)
	}
	if err := db.Exec("PRAGMA busy_timeout=5000").Error; err != nil {
		t.Fatal(err)
	}
	locked = other.Begin()
	if locked.Error != nil {
		t.Fatal(locked.Error)
	}
	started := time.Now()
	timing, err = run(80*time.Millisecond, noSQL)
	locked.Rollback()
	if err == nil || time.Since(started) > 500*time.Millisecond || timing.Work != 0 {
		t.Fatal("database lock ignored cancellation budget", timing, err)
	}
	timing, err = run(time.Second, func(tx *gorm.DB) error {
		time.Sleep(30 * time.Millisecond)
		return tx.Exec("INSERT INTO writes VALUES(1)").Error
	})
	if err != nil || timing.Stage != "complete" || timing.Work < 30*time.Millisecond || timing.Statements != 1 {
		t.Fatal(timing, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec("INSERT INTO writes VALUES(2)").Error; err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := m.tokenLocalWriteTransaction(context.Background(), func(tx *gorm.DB) error { return tx.Exec("INSERT INTO writes VALUES(2)").Error }); err != nil {
		t.Fatal("cancel failed to roll back/recover", err)
	}
}
