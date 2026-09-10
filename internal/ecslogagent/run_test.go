package ecslogagent

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestECSAgentChildHelper(t *testing.T) {
	if os.Getenv("ECSLOG_TEST_CHILD") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	if err := os.WriteFile(os.Getenv("ECSLOG_TEST_READY"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
}

func TestRunChildGracefulParentCancellationAndFailure(t *testing.T) {
	c, meta := testConfig(t)
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("fixture offline") }))
	if err := a.RunChild(context.Background(), []string{"/fixture-does-not-exist"}); err == nil {
		t.Fatal("missing child executable accepted")
	}
	ready := filepath.Join(a.dir, "test-child-ready")
	t.Setenv("ECSLOG_TEST_CHILD", "1")
	t.Setenv("ECSLOG_TEST_READY", ready)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.RunChild(ctx, []string{os.Args[0], "-test.run=^TestECSAgentChildHelper$"}) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	waiting := true
	for waiting {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("early child exit %v", err)
		case <-deadline.C:
			t.Fatal("child not ready")
		case <-ticker.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("child/proxy not stopped after SIGTERM")
	}
	if _, err := os.Lstat(a.SocketPath()); !os.IsNotExist(err) {
		t.Fatal("socket retained after graceful shutdown")
	}
}
