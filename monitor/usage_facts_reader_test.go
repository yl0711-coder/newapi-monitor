package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// The user-usage matrix must not queue behind a background writer holding the
// single facts connection, and must read only committed facts.
func TestUsageFactsInteractiveReadContinuesDuringWriterTransaction(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                      filepath.Join(dir, "main.db"),
		UsageFactsStorePath:            filepath.Join(dir, "facts.db"),
		UsageFactsReadIsolationEnabled: true,
		LocalSnapshotOnly:              true,
	}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, usageCST).Unix()
	fact := UsageHourFact{HourTs: day + 3600, UserID: 7, DayTs: day, Requests: 3, ConsumeQuota: 1500}
	if err := m.usageFactsDB.Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	reader := m.usageFactsInteractiveReadStore()
	if reader == nil || reader == m.usageFactsDB || m.usageFactsReadDB.Load() == nil {
		t.Fatal("usage read was not isolated from the one-connection facts writer")
	}
	readPool, err := reader.DB()
	if err != nil || readPool.Stats().MaxOpenConnections != usageFactsReadCapacity {
		t.Fatalf("read-only pool is not bounded: err=%v stats=%+v", err, readPool.Stats())
	}
	if err := reader.Exec("INSERT INTO usage_hour_facts(hour_ts,user_id,channel_id,grp,model_name,token_id,day_ts) VALUES(?,?,?,?,?,?,?)", day+7200, 7, 0, "", "", 0, day).Error; err == nil {
		t.Fatal("usage read handle accepted a write")
	}

	// A background writer holds the only writer connection with uncommitted facts.
	tx := m.usageFactsDB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Create(&UsageHourFact{HourTs: day + 7200, UserID: 7, DayTs: day, Requests: 100, ConsumeQuota: 99_000}).Error; err != nil {
		t.Fatal(err)
	}
	blocked, blockedCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	var n int64
	err = m.usageFactsDB.WithContext(blocked).Model(&UsageHourFact{}).Count(&n).Error
	blockedCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fixture did not occupy the single facts writer connection: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	mx, err := m.computeUsageMatrixFromFacts(ctx, []int64{7}, day, day+86400)
	if err != nil {
		t.Fatalf("matrix queued behind writer or failed: %v (after %s)", err, time.Since(started))
	}
	if len(mx.Cells) != 1 || mx.Cells[0].Requests != 3 || mx.Cells[0].ConsumeQuota != 1500 {
		t.Fatalf("matrix saw uncommitted or missing facts: %+v", mx.Cells)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	mx, err = m.computeUsageMatrixFromFacts(context.Background(), []int64{7}, day, day+86400)
	if err != nil || len(mx.Cells) != 1 || mx.Cells[0].Requests != 103 {
		t.Fatalf("committed facts not visible to read handle: %+v err=%v", mx, err)
	}
}

func TestUsageFactsInteractiveReadOptInAndSharedStoreFallback(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:           filepath.Join(dir, "main.db"),
		UsageFactsStorePath: filepath.Join(dir, "facts.db"),
		LocalSnapshotOnly:   true,
	}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if got := m.usageFactsInteractiveReadStore(); got != m.usageFactsStore() || m.usageFactsReadDB.Load() != nil {
		t.Fatal("disabled isolation opened a new connection")
	}
	shared := newStabilityTestMonitor(t)
	shared.cfg.UsageFactsReadIsolationEnabled = true
	if got := shared.usageFactsInteractiveReadStore(); got != shared.usageFactsStore() {
		t.Fatal("shared legacy main/facts store unexpectedly opened a second connection")
	}
}

func TestUsageReadTimingDiagnosticPassesThroughWithoutReadPool(t *testing.T) {
	m := newStabilityTestMonitor(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/slow", m.usageReadTimingDiagnostic("matrix", func(c *gin.Context) {
		time.Sleep(usageReadDiagnosticThreshold + 50*time.Millisecond)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/slow", nil))
	if w.Code != http.StatusOK || w.Body.String() != `{"ok":true}` {
		t.Fatalf("diagnostic changed the response: %d %s", w.Code, w.Body.String())
	}
}
