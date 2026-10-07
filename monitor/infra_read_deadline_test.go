package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestCapacityInfraCancelledWhileAggregateBusy(t *testing.T) {
	m := newTestMonitor(t)
	t.Cleanup(m.Close)
	m.infraAggregateMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan capacitySource, 1)
	go func() {
		defer close(done)
		_, source := m.readCapacityInfra(ctx, 60, 120, 60, 120)
		done <- source
	}()
	defer func() {
		m.infraAggregateMu.Unlock()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("reader failed to exit after lock release")
		}
	}()
	select {
	case source := <-done:
		if source.Available || source.Watermark != 0 || source.Rows != 0 {
			t.Fatalf("cancelled read must be unavailable: %+v", source)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("cancelled capacity request is still waiting for aggregate lock")
	}
}

func TestInfraAggregateLockDeadlineAndRecovery(t *testing.T) {
	m := &Monitor{}
	m.infraAggregateMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := m.lockInfraAggregate(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		m.infraAggregateMu.Unlock()
		t.Fatalf("waiter must respect deadline: %v", err)
	}
	if m.infraAggregateMu.TryLock() {
		m.infraAggregateMu.Unlock()
		t.Fatal("cancelled waiter released the owner's lock")
	}
	m.infraAggregateMu.Unlock()
	if err := m.lockInfraAggregate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired context must not acquire even a free lock: %v", err)
	}
	if err := m.lockInfraAggregate(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.infraAggregateMu.Unlock()
}

func TestInfraReadsRespectRequestDeadlineAtLocalPool(t *testing.T) {
	reads := map[string]func(*Monitor, context.Context) error{
		"latest": func(m *Monitor, ctx context.Context) error { _, err := m.storeInfraLatestContext(ctx); return err },
		"snapshot": func(m *Monitor, ctx context.Context) error {
			_, err := m.computeInfraSnapshotContext(ctx, 120)
			return err
		},
		"series": func(m *Monitor, ctx context.Context) error {
			_, err := m.storeInfraSeriesContext(ctx, "node", "cpu", 0)
			return err
		},
		"containers": func(m *Monitor, ctx context.Context) error {
			_, err := m.hostContainerSnapshotContext(ctx, "node", 120)
			return err
		},
		"registry": func(m *Monitor, ctx context.Context) error {
			_, _, err := m.infraAssetProjectionContext(ctx)
			return err
		},
		"recent_alerts": func(m *Monitor, ctx context.Context) error {
			_, err := m.recentInfraAlertsContext(ctx, 120, 20)
			return err
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			m := newTestMonitor(t)
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
			done := make(chan error, 1)
			go func() { defer close(done); done <- read(m, ctx) }()
			defer func() {
				_ = conn.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("reader did not terminate")
				}
			}()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("deadline lost: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("reader ignored request deadline")
			}
		})
	}
}

func TestInfraHTTPStorageFailureIsNotEmptySuccess(t *testing.T) {
	for name, table := range map[string]any{"metrics": &InfraSample{}, "containers": &HostContainerSnapshot{}, "recent_alerts": &AlertLog{}} {
		t.Run(name, func(t *testing.T) {
			m := newTestMonitor(t)
			t.Cleanup(m.Close)
			m.cfg.InfraEnabled = true
			if err := m.upsertInfra([]InfraSample{{BucketTs: time.Now().Unix() / 60 * 60, Resource: "node", RType: "instance", Metric: "cpu", Value: 30}}); err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Migrator().DropTable(table); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/infra", nil)
			m.serveInfra(c)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("storage failure returned %d, want 503", w.Code)
			}
		})
	}
}

func TestInfraSeriesHTTPStorageFailureIsNotEmptySuccess(t *testing.T) {
	m := newTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.InfraEnabled = true
	if err := m.storeDB.Migrator().DropTable(&InfraSample{}); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/infra/series?resource=node&metrics=cpu", nil)
	m.serveInfraSeries(c)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed series read returned %d, want 503", w.Code)
	}
}

func TestInfraAlertEvaluationDoesNotRequireDisplayHistory(t *testing.T) {
	m := newTestMonitor(t)
	t.Cleanup(m.Close)
	now := time.Now().Unix()
	if err := m.upsertInfra([]InfraSample{{BucketTs: now / 60 * 60, Resource: "node", RType: "instance", Metric: "cpu", Value: 99}}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Migrator().DropTable(&AlertLog{}); err != nil {
		t.Fatal(err)
	}
	snap, err := m.buildInfraSnapshotContext(context.Background(), now, false)
	if err != nil || len(snap.Instances) != 1 || snap.Instances[0].Metrics["cpu"] != 99 {
		t.Fatalf("display history must not block resource evaluation: snap=%+v err=%v", snap, err)
	}
}
