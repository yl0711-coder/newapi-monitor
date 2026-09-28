//go:build unix

package monitor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinanceGiftAuthorizedConcurrentReceivers(t *testing.T) {
	m, _, a := giftAuthorizedLocalFixture(t)
	// Independent object/DB handles: an in-memory mutex alone cannot protect it.
	facts, closeFacts, err := giftLocalDatabase(m.cfg.UsageFactsStorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFacts()
	other := &Monitor{cfg: m.cfg, storeDB: m.storeDB, usageFactsDB: facts}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := m.runFinanceGiftAuthorizedLocal(ctx, a.TaskID, func(ctx context.Context, _ time.Duration) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		})
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal("first run did not reach cooldown", err)
	case <-time.After(10 * time.Second):
		t.Fatal("first run stalled")
	}
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("local run did not stop")
		}
	}()
	before := giftBoundaryFingerprint(t, m.usageFactsDB)
	if _, err := other.runFinanceGiftAuthorizedLocal(context.Background(), a.TaskID, handoffNoWait); err == nil || !strings.Contains(err.Error(), "already in use") {
		t.Fatal("second receiver admitted", err)
	}
	if giftBoundaryFingerprint(t, m.usageFactsDB) != before {
		t.Fatal("blocked run mutated facts")
	}
}

func TestFinanceGiftReceiverLockChild(t *testing.T) {
	path := os.Getenv("MONITOR_TEST_GIFT_RECEIVER_LOCK_PATH")
	if path == "" {
		t.Skip("subprocess helper")
	}
	db, closeDB, err := giftLocalDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	lock, err := lockFinanceGiftLocalReceiver(context.Background(), db, path)
	if os.Getenv("MONITOR_TEST_GIFT_RECEIVER_LOCK_BLOCKED") == "1" {
		if err == nil {
			lock.Close()
			t.Fatal("child unexpectedly acquired lock")
		}
		if !strings.Contains(err.Error(), "already in use") {
			t.Fatal(err)
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if os.Getenv("MONITOR_TEST_GIFT_RECEIVER_LOCK_HOLD") == "1" {
			fmt.Fprintln(os.Stdout, "receiver-lock-held")
			_, _ = io.Copy(io.Discard, os.Stdin)
		}
	}
}

func TestFinanceGiftReceiverLockProcessExit(t *testing.T) {
	m, _, _ := giftAuthorizedLocalFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFinanceGiftReceiverLockChild$")
	cmd.Env = append(os.Environ(), "MONITOR_TEST_GIFT_RECEIVER_LOCK_PATH="+m.cfg.UsageFactsStorePath, "MONITOR_TEST_GIFT_RECEIVER_LOCK_BLOCKED=0", "MONITOR_TEST_GIFT_RECEIVER_LOCK_HOLD=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	scanner := bufio.NewScanner(stdout)
	ready := false
	for scanner.Scan() {
		if scanner.Text() == "receiver-lock-held" {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatal("child did not acquire lock", scanner.Err())
	}
	if lock, err := lockFinanceGiftLocalReceiver(context.Background(), m.usageFactsDB, m.cfg.UsageFactsStorePath); err == nil {
		lock.Close()
		t.Fatal("child lock ignored")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child should have been terminated")
	}
	lock, err := lockFinanceGiftLocalReceiver(context.Background(), m.usageFactsDB, m.cfg.UsageFactsStorePath)
	if err != nil {
		t.Fatal("process exit left a stale lock", err)
	}
	lock.Close()
}

func TestFinanceGiftReceiverLockAcrossProcesses(t *testing.T) {
	m, _, _ := giftAuthorizedLocalFixture(t)
	lock, err := lockFinanceGiftLocalReceiver(context.Background(), m.usageFactsDB, m.cfg.UsageFactsStorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	child := func(blocked string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFinanceGiftReceiverLockChild$", "-test.v")
		cmd.Env = append(os.Environ(), "MONITOR_TEST_GIFT_RECEIVER_LOCK_PATH="+m.cfg.UsageFactsStorePath, "MONITOR_TEST_GIFT_RECEIVER_LOCK_BLOCKED="+blocked)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v %s", err, output)
		}
	}
	child("1")
	lock.Close()
	child("0") // the remaining lock file is not a stale lock
}

func TestFinanceGiftReceiverLockRejectsUnsafePaths(t *testing.T) {
	for _, mode := range []string{"symlink", "permissions", "different_database"} {
		t.Run(mode, func(t *testing.T) {
			m, _, _ := giftAuthorizedLocalFixture(t)
			path := m.cfg.UsageFactsStorePath
			var err error
			switch mode {
			case "symlink":
				err = os.Symlink(path, path+".gift-handoff.lock")
			case "permissions":
				err = os.WriteFile(path+".gift-handoff.lock", nil, 0600)
				if err == nil {
					err = os.Chmod(path+".gift-handoff.lock", 0644)
				}
			case "different_database":
				path = filepath.Join(t.TempDir(), "wrong.db")
				err = os.WriteFile(path, nil, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			lock, err := lockFinanceGiftLocalReceiver(context.Background(), m.usageFactsDB, path)
			if err == nil {
				lock.Close()
				t.Fatal("unsafe receiver lock accepted")
			}
		})
	}
}
