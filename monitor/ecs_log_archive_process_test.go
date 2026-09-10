package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestECSLogArchiveRealRejectProcessDestroyed(t *testing.T) {
	binary := os.Getenv("MONITOR_TEST_REJECT_COLLECTOR_BIN")
	if binary == "" {
		t.Skip("requires locally built reject collector; mandatory in ecs-local-acceptance.sh")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute test binary required")
	}
	m := newECSLogTestMonitor(t)
	received := make(chan struct{}, 1)
	agent, cfg, _, _ := ecsAgentFixture(t, m, "reject", strings.Repeat("e", 32), func(path string, _ []byte, w *httptest.ResponseRecorder) bool {
		if path == "/internal/ecs/v1/ingest/reject" {
			if w.Code != 503 {
				t.Errorf("expected offline receiver, got %d", w.Code)
			}
			select {
			case received <- struct{}{}:
			default:
			}
		}
		return false
	})
	archive := &archiveDiskFixture{root: t.TempDir()}
	if err := agent.ConfigureArchive(archive, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("e", 32)); err != nil {
		t.Fatal(err)
	}
	if err := agent.Renew(context.Background(), "reject"); err != nil {
		t.Fatal(err)
	}
	m.cfg.ECSLogScope = "offline-fixture"
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
	select {
	case <-received:
	case <-time.After(15 * time.Second):
		t.Fatal("real collector failed to archive batch")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	_ = server.Close()
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cfg.StateRoot, "/tmp/eai-") {
		t.Fatal("unexpected test task directory")
	}
	if err := os.RemoveAll(cfg.StateRoot); err != nil {
		t.Fatal(err)
	}
	m.cfg.ECSLogScope = "isolated"
	if archive.last.Key == "" {
		t.Fatal("archive missing after process destruction")
	}
	r := newECSArchiveReplayer(m, "isolated/")
	for i := 0; i < 2; i++ {
		o, err := archive.Get(context.Background(), archive.last.Key)
		if err != nil {
			t.Fatal(err)
		}
		if result := r.replay(context.Background(), o, time.Now()); !result.Accepted || result.Duplicate != (i == 1) {
			t.Fatalf("independent recovery %+v", result)
		}
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 1 || rows[0].Count != 1 {
		t.Fatalf("lost/duplicated recovered facts %+v", rows)
	}
}
