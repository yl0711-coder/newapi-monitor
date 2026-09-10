package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"github.com/yl0711-coder/newapi-monitor/internal/ecslogagent"
)

// Disk storage lives outside the disposable collector directory. Notifications
// follow durable writes and do not depend on a successful Monitor request.
type notifyingArchiveFixture struct {
	mu      sync.Mutex
	disk    archiveDiskFixture
	written chan string
	objects map[string]ecsarchive.Object
}

func (s *notifyingArchiveFixture) Binding() string { return s.disk.Binding() }
func (s *notifyingArchiveFixture) Put(ctx context.Context, key string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.disk.Put(ctx, key, body)
	if err == nil {
		if s.objects == nil {
			s.objects = make(map[string]ecsarchive.Object)
		}
		s.objects[key] = s.disk.last
		s.written <- key
	}
	return err
}
func (s *notifyingArchiveFixture) Get(ctx context.Context, key string) (ecsarchive.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, err := s.disk.Get(ctx, key)
	if err == nil {
		o.Modified = s.objects[key].Modified
	}
	return o, err
}

func TestECSLogArchiveDeferredRealProcessContinuesAndRecovers(t *testing.T) {
	binary := os.Getenv("MONITOR_TEST_REJECT_COLLECTOR_BIN")
	if binary == "" {
		t.Skip("real reject binary mandatory in ecs-local-acceptance.sh")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute binary required")
	}
	m := newECSLogTestMonitor(t)
	taskID := strings.Repeat("f", 32)
	agent, cfg, _, _ := ecsAgentFixture(t, m, "reject", taskID, func(path string, _ []byte, _ *httptest.ResponseRecorder) bool {
		if strings.Contains(path, "/ingest/") {
			t.Error("deferred mode sent live facts")
		}
		return false
	}, func(cfg *ecslogagent.Config) { cfg.DeferredArchiveACK = true })
	archive := &notifyingArchiveFixture{disk: archiveDiskFixture{root: t.TempDir()}, written: make(chan string, 16)}
	if err := agent.ConfigureArchive(archive, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+taskID); err != nil {
		t.Fatal(err)
	}
	if err := agent.Renew(context.Background(), "reject"); err != nil {
		t.Fatal(err)
	}
	listener, err := agent.Listen()
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: agent, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	path := filepath.Join(cfg.StateRoot, "new-api.log")
	line := "[ERR] " + time.Now().UTC().Format("2006/01/02 - 15:04:05") + " | fixture | user 7 | No available channel for model fixture under group fixture-group (distributor)\n"
	if err := os.WriteFile(path, []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary)
	cmd.Env = agent.ChildEnvironment([]string{"COLLECTOR_LOG_GLOB=" + path, "COLLECTOR_LOG_TIMEZONE=UTC", "COLLECTOR_FLUSH_SECONDS=5"})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	keys := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case key := <-archive.written:
			keys = append(keys, key)
		case <-time.After(20 * time.Second):
			t.Fatal("offline archive blocked subsequent batch")
		}
		if i == 0 {
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteString(line)
			closeErr := f.Close()
			if err != nil || closeErr != nil {
				t.Fatal("append fixture failed", err, closeErr)
			}
		}
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	_ = server.Close()
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 0 {
		t.Fatal("archive receipt invented live facts")
	}
	if !strings.HasPrefix(cfg.StateRoot, "/tmp/eai-") {
		t.Fatal("unexpected disposable fixture root")
	}
	if err := os.RemoveAll(cfg.StateRoot); err != nil {
		t.Fatal(err)
	}
	r := newECSArchiveReplayer(m, "isolated/")
	for _, key := range keys {
		o, err := archive.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			if result := r.replay(context.Background(), o, time.Now()); !result.Accepted || result.Duplicate != (attempt == 1) {
				t.Fatalf("recovery %+v", result)
			}
		}
	}
	var count int64
	for _, row := range m.storeRejections(time.Now().Unix() - 120) {
		count += row.Count
	}
	if count != 2 {
		t.Fatalf("lost or duplicated facts: %d", count)
	}
}
