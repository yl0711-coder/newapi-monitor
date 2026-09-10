package ecslogagent

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func TestKilledCollectorNeverEmitsClosure(t *testing.T) {
	c, meta := testConfig(t)
	c.DeferredArchiveACK, c.ArchiveClosure = true, true
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") }))
	store := &memoryArchive{objects: map[string][]byte{}}
	if err := a.ConfigureArchive(store, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32)); err != nil {
		t.Fatal(err)
	}
	a.leases["reject"] = time.Now().Unix() + 600
	if err := a.archiveFrozen(context.Background(), "reject", []byte(`{"node":"`+a.node+`","batch_id":"preserved-before-crash"}`)); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(a.dir, "crash-test-child")
	t.Setenv("ECSLOG_TEST_CHILD", "1")
	t.Setenv("ECSLOG_TEST_READY", ready)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.RunChild(ctx, []string{os.Args[0], "-test.run=^TestECSAgentChildHelper$"}) }()
	deadline := time.Now().Add(5 * time.Second)
	var pid int
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(ready); err == nil {
			pid, _ = strconv.Atoi(string(raw))
			if pid > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid <= 0 {
		t.Fatal("test child did not start")
	}
	child, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("SIGKILL reported as successful drain")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("killed collector not reaped")
	}
	if len(store.objects) != 1 {
		t.Fatal("unexpected archive changes", len(store.objects))
	}
	for _, raw := range store.objects {
		e, err := ecsarchive.Decode(raw)
		if err != nil || e.Kind != "" {
			t.Fatal("crash invented closure", err)
		}
	}
}
