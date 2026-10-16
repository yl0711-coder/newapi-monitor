package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/usagebill/exportjobs"
)

func billLifecycleOwner(t *testing.T, m *Monitor, gid int64) exportjobs.Owner {
	t.Helper()
	snap, err := m.loadPortalUsageAuthorizationSnapshot(context.Background(), gid)
	if err != nil {
		t.Fatal(err)
	}
	return portalBillOwner(snap)
}

func TestPortalBillLifecycleSingletonAndShutdownOrder(t *testing.T) {
	m, _, _, gid := billHTTPFixture(t)
	var wg sync.WaitGroup
	services := make(chan *portalBillTasks, 20)
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s, err := m.initPortalBillTasks(); services <- s; errs <- err }()
	}
	wg.Wait()
	close(services)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var s *portalBillTasks
	for got := range services {
		if s == nil {
			s = got
		} else if s != got {
			t.Fatal("multiple workers initialized")
		}
	}
	started := make(chan struct{})
	sourceAlive := make(chan bool, 1)
	_, err := s.jobs.Submit(context.Background(), exportjobs.Request{Owner: billLifecycleOwner(t, m, gid), Key: strings.Repeat("a", 64), ExpectedRows: 1, Build: func(ctx context.Context, _ io.Writer, _ func(int64) error) (int64, error) {
		close(started)
		<-ctx.Done()
		check, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sourceAlive <- m.prodDB.PingContext(check) == nil
		return 0, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("not started")
	}
	m.Close()
	if !<-sourceAlive {
		t.Fatal("source closed before task stopped")
	}
	select {
	case <-s.maintenanceDone:
	default:
		t.Fatal("maintenance goroutine leaked")
	}
	if _, err := m.initPortalBillTasks(); !errors.Is(err, exportjobs.ErrClosed) {
		t.Fatal("initialized after shutdown")
	}
}

func TestPortalBillLifecycleSlowStopKeepsSource(t *testing.T) {
	m, _, _, gid := billHTTPFixture(t)
	s, err := m.initPortalBillTasks()
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	_, err = s.jobs.Submit(context.Background(), exportjobs.Request{Owner: billLifecycleOwner(t, m, gid), Key: strings.Repeat("b", 64), ExpectedRows: 1, Build: func(ctx context.Context, _ io.Writer, _ func(int64) error) (int64, error) {
		close(started)
		<-release
		return 0, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if m.stopPortalBillTasks(ctx) {
		t.Fatal("ignored slow worker")
	}
	if err := m.prodDB.Ping(); err != nil {
		t.Fatal("source closed while worker alive")
	}
	releaseOnce.Do(func() { close(release) })
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if !m.stopPortalBillTasks(ctx2) {
		t.Fatal("failed to finish shutdown")
	}
}

func TestPortalBillLifecyclePreservesDownload(t *testing.T) {
	m, _, _, gid := billHTTPFixture(t)
	s, err := m.initPortalBillTasks()
	if err != nil {
		t.Fatal(err)
	}
	owner := billLifecycleOwner(t, m, gid)
	v, err := s.jobs.Submit(context.Background(), exportjobs.Request{Owner: owner, Key: strings.Repeat("c", 64), ExpectedRows: 1, Build: func(_ context.Context, w io.Writer, p func(int64) error) (int64, error) {
		_, err := io.WriteString(w, "verified-complete-bill")
		if err != nil {
			return 0, err
		}
		return 1, p(1)
	}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for v.State != "ready" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		v, err = s.jobs.View(context.Background(), owner, v.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	a, err := s.jobs.Open(context.Background(), owner, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !m.stopPortalBillTasks(ctx) {
		t.Fatal("download unnecessarily holds database shutdown")
	}
	b, err := io.ReadAll(a)
	if err != nil || string(b) != "verified-complete-bill" {
		t.Fatal("active download removed")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(s.base)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "bill-jobs-v1-") {
			t.Fatal("download completion left session directory")
		}
	}
}

func TestPortalBillLifecycleRejectsUnsafeDirectory(t *testing.T) {
	m, _, _, _ := billHTTPFixture(t)
	parent, err := m.portalExportTempDir()
	if err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	link := filepath.Join(parent, "bill-tasks-v1")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := m.initPortalBillTasks(); !errors.Is(err, exportjobs.ErrInvalid) {
		t.Fatalf("symlink accepted: %v", err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatal("outside directory changed")
	}
}

func TestPortalBillLifecyclePeriodicRecovery(t *testing.T) {
	m, _, _, _ := billHTTPFixture(t)
	active, err := m.initPortalBillTasks()
	if err != nil {
		t.Fatal(err)
	}
	name := "bill-jobs-v1-" + strings.Repeat("d", 32)
	stale := filepath.Join(active.base, name)
	if err := os.Mkdir(stale, 0700); err != nil {
		t.Fatal(err)
	}
	for file, content := range map[string]string{".lease": "", "session.v1": "nexusapi-usage-bill-ephemeral-v1\n" + name + "\n", "partial": "synthetic-old-data"} {
		if err := os.WriteFile(filepath.Join(stale, file), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	unknown := filepath.Join(active.base, "unrelated")
	if err := os.Mkdir(unknown, 0700); err != nil {
		t.Fatal(err)
	}
	// The same maintenance function on a short test clock; it has no worker
	// or task ownership. The actual active service retains its own lease.
	sweep := &portalBillTasks{base: active.base, stop: make(chan struct{}), maintenanceDone: make(chan struct{})}
	go sweep.maintain(5 * time.Millisecond)
	defer func() { close(sweep.stop); <-sweep.maintenanceDone }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(stale); os.IsNotExist(err) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("timer did not reclaim stale session")
	}
	if _, err := os.Stat(unknown); err != nil {
		t.Fatal("unknown directory removed")
	}
	entries, err := os.ReadDir(active.base)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "bill-jobs-v1-") {
			count++
		}
	}
	if count != 1 {
		t.Fatal("active session not preserved")
	}
}

func TestPortalBillLifecycleRejectsHTTPWhileClosing(t *testing.T) {
	s, h, ck, ticket := billTasksFixture(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	w := billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks", `{"ticket":"`+ticket+`"}`)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("closing accepted request: %d %s", w.Code, w.Body.String())
	}
}
