package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestStabilityHealthCancellationDoesNotWaitForLocalPool(t *testing.T) {
	m := newStabilityTestMonitor(t)
	t.Cleanup(m.Close)
	pool, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan stabilityHealthResponse, 1)
	go func() { done <- m.stabilityHealth(ctx, time.Now()) }()
	returned := false
	defer func() {
		_ = conn.Close()
		if !returned {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("health reader failed to terminate")
			}
		}
	}()
	select {
	case got := <-done:
		returned = true
		if got.Status != "degraded" || got.ProblemMigration.Status != "error" || got.StoreReachable {
			t.Fatalf("cancelled health must not claim complete state: %+v", got)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("cancelled health request still waiting for local SQLite pool")
	}
}

func TestStabilityBackfillStatusDeadlineDoesNotWaitForLocalPool(t *testing.T) {
	m := newStabilityTestMonitor(t)
	t.Cleanup(m.Close)
	pool, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/stability/backfill/status", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.stabilityBackfillStatusHandler(c)
	}()
	defer func() {
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("backfill status reader failed to terminate")
		}
	}()
	select {
	case <-done:
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("expired read must not return successful status: %d", recorder.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("backfill status ignored request deadline while waiting for SQLite pool")
	}
}
