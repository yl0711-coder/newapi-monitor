package monitor

import (
	"bytes"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Optional cross-repository acceptance: use an explicitly built LOCAL binary.
// The child receives only synthetic logs, a temp CA and this temporary TLS
// receiver. It has no AWS credentials or production endpoint/configuration.
func TestRejectCollectorProcessRestartAgainstRealMonitor(t *testing.T) {
	binary := os.Getenv("MONITOR_TEST_REJECT_COLLECTOR_BIN")
	if binary == "" {
		t.Skip("set MONITOR_TEST_REJECT_COLLECTOR_BIN to the locally built reject collector")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("collector binary must be absolute")
	}
	m := newTestMonitor(t)
	m.cfg.IngestToken = "local-process-fixture"
	router := gin.New()
	router.POST("/internal/rejections/v2", m.ingestRejectionsV2)
	var mu sync.Mutex
	var attempts [][]byte
	received := make(chan struct{}, 10)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		stored := httptest.NewRecorder()
		router.ServeHTTP(stored, r)
		mu.Lock()
		attempts = append(attempts, body)
		first := len(attempts) == 1
		mu.Unlock()
		if first {
			http.Error(w, "simulated ACK loss after database commit", 502)
		} else {
			w.WriteHeader(stored.Code)
			_, _ = w.Write(stored.Body.Bytes())
		}
		received <- struct{}{}
	}))
	defer server.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "new-api.log")
	line := "[ERR] " + time.Now().UTC().Format("2006/01/02 - 15:04:05") + " | fixture | user 7 | No available channel for model fixture under group fixture-group (distributor)\n"
	if err := os.WriteFile(path, []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	start := func() *exec.Cmd {
		cmd := exec.Command(binary)
		cmd.Env = []string{"COLLECTOR_NODE=ecs-process-fixture", "COLLECTOR_STATE_PATH=" + filepath.Join(dir, "state", "checkpoint.json"), "COLLECTOR_LOG_GLOB=" + path, "COLLECTOR_LOG_TIMEZONE=UTC", "COLLECTOR_FLUSH_SECONDS=5", "COLLECTOR_SINK_URL=" + server.URL + "/internal/rejections/v2", "COLLECTOR_SINK_TOKEN=" + m.cfg.IngestToken, "COLLECTOR_CA_FILE=" + ca}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		return cmd
	}
	waitReport := func() {
		select {
		case <-received:
		case <-time.After(15 * time.Second):
			t.Fatal("collector did not report")
		}
	}
	first := start()
	waitReport()
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait() // ungraceful process death, not a flush-on-shutdown test.
	second := start()
	waitReport()
	if err := second.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 || !bytes.Equal(attempts[0], attempts[1]) {
		t.Fatalf("process retry changed payload: %d attempts", len(attempts))
	}
	rows := m.storeRejections(time.Now().Unix() - 120)
	if len(rows) != 1 || rows[0].Count != 1 {
		t.Fatalf("real Monitor double counted: %+v", rows)
	}
	checkpoint, err := os.ReadFile(filepath.Join(dir, "state", "checkpoint.json"))
	if err != nil || strings.Contains(string(checkpoint), "local-process-fixture") {
		t.Fatal("checkpoint unreadable or contains token")
	}
}
