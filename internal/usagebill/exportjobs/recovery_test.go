package exportjobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type recoverySignal struct{ Root, JobID string }

// Invoked only as our own test subprocess. The parent kills this process after
// reading its readiness signal; no real server/container process is signaled.
func TestRecoveryHelperProcess(t *testing.T) {
	mode := os.Getenv("BILL_RECOVERY_HELPER")
	if mode == "" {
		t.Skip("subprocess fixture")
	}
	m, err := New(os.Getenv("BILL_RECOVERY_BASE"), testLimits(), allow)
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan struct{})
	build := buildText("request,actual\nfixture,0.002424\n", 1)
	if mode == "partial" {
		build = func(ctx context.Context, w io.Writer, p func(int64) error) (int64, error) {
			if _, err := io.WriteString(w, "unfinished synthetic CSV\n"); err != nil {
				return 0, err
			}
			close(written)
			<-ctx.Done()
			return 0, ctx.Err()
		}
	}
	v, err := m.Submit(context.Background(), Request{testOwner(1), testKey("subprocess"), 1, build})
	if err != nil {
		t.Fatal(err)
	}
	if mode == "partial" {
		<-written
	} else {
		waitState(t, m, testOwner(1), v.ID, "ready")
	}
	if mode == "download" {
		// A closed manager with an open download must still retain its lease.
		artifact, err := m.Open(context.Background(), testOwner(1), v.ID)
		if err != nil {
			t.Fatal(err)
		}
		defer artifact.Close()
		if err := m.Close(); !errors.Is(err, ErrBusy) {
			t.Fatal("closed a live download")
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(recoverySignal{m.root, v.ID}); err != nil {
		t.Fatal(err)
	}
	select {} // killed by parent; do not call normal cleanup
}

func launchRecoveryChild(t *testing.T, base, mode string) (recoverySignal, func()) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestRecoveryHelperProcess$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "BILL_RECOVERY_HELPER="+mode, "BILL_RECOVERY_BASE="+base)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }) }
	t.Cleanup(stop)
	type response struct {
		signal recoverySignal
		err    error
	}
	read := make(chan response, 1)
	go func() {
		var signal recoverySignal
		err := json.NewDecoder(out).Decode(&signal)
		read <- response{signal, err}
	}()
	select {
	case r := <-read:
		if r.err != nil {
			t.Fatalf("helper failed before signal: %v", r.err)
		}
		return r.signal, stop
	case <-time.After(5 * time.Second):
		t.Fatal("helper did not become ready")
	}
	return recoverySignal{}, stop
}

func TestRecoveryProtectsLiveProcessThenReclaimsKilledOwner(t *testing.T) {
	for _, mode := range []string{"partial", "ready", "download"} {
		t.Run(mode, func(t *testing.T) {
			base := t.TempDir()
			if err := os.Chmod(base, 0700); err != nil {
				t.Fatal(err)
			}
			signal, kill := launchRecoveryChild(t, base, mode)
			report, err := Recover(base)
			if err != nil || report.Active != 1 || report.Removed != 0 {
				t.Fatalf("live session unsafe: %+v %v", report, err)
			}
			if _, err := os.Stat(signal.Root); err != nil {
				t.Fatal("live directory removed")
			}
			kill() // exercise OS-released locks after abrupt exit, not manual unlock
			if mode == "partial" {
				// New performs the same recovery before creating its own session.
				m, err := New(base, testLimits(), allow)
				if err != nil {
					t.Fatal(err)
				}
				defer m.Close()
				if _, err := os.Stat(signal.Root); !os.IsNotExist(err) {
					t.Fatal("startup retained crashed partial file")
				}
				if _, err := m.View(context.Background(), testOwner(1), signal.JobID); !errors.Is(err, ErrNotFound) {
					t.Fatal("old task ID resurrected")
				}
			} else {
				report, err = Recover(base)
				if err != nil || report.Removed != 1 || report.Active != 0 {
					t.Fatalf("crashed session not reclaimed: %+v %v", report, err)
				}
				if _, err := os.Stat(signal.Root); !os.IsNotExist(err) {
					t.Fatal("dead owner directory retained")
				}
				again, err := Recover(base)
				if err != nil || again.Removed != 0 {
					t.Fatal("recovery not idempotent")
				}
			}
		})
	}
}

func recoveryFixture(t *testing.T, base string, n int) string {
	t.Helper()
	name := sessionPrefix + fmt.Sprintf("%032x", n)
	root := filepath.Join(base, name)
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, sessionMarker), []byte(markerText(name)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, sessionLease), nil, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestRecoveryPreservesUnknownPartialAndSymlinkEntries(t *testing.T) {
	base := t.TempDir()
	_ = os.Chmod(base, 0700)
	outside := t.TempDir()
	marker := filepath.Join(outside, "keep")
	if err := os.WriteFile(marker, []byte("not a bill artifact"), 0600); err != nil {
		t.Fatal(err)
	}
	unknown := recoveryFixture(t, base, 1)
	_ = os.WriteFile(filepath.Join(unknown, sessionMarker), []byte("unknown future format"), 0600)
	partial := recoveryFixture(t, base, 2)
	_ = os.Remove(filepath.Join(partial, sessionLease))
	linkedMarker := recoveryFixture(t, base, 3)
	_ = os.Remove(filepath.Join(linkedMarker, sessionMarker))
	if err := os.Symlink(marker, filepath.Join(linkedMarker, sessionMarker)); err != nil {
		t.Fatal(err)
	}
	linkedLease := recoveryFixture(t, base, 4)
	_ = os.Remove(filepath.Join(linkedLease, sessionLease))
	if err := os.Symlink(marker, filepath.Join(linkedLease, sessionLease)); err != nil {
		t.Fatal(err)
	}
	linkedRoot := filepath.Join(base, sessionPrefix+fmt.Sprintf("%032x", 5))
	if err := os.Symlink(outside, linkedRoot); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(base, "bill-jobs-legacy")
	if err := os.Mkdir(legacy, 0700); err != nil {
		t.Fatal(err)
	}
	good := recoveryFixture(t, base, 6)
	// A symlink inside a recognized abandoned session is unlinked, not followed.
	if err := os.Symlink(outside, filepath.Join(good, "external")); err != nil {
		t.Fatal(err)
	}
	report, err := Recover(base)
	if err != nil || report.Removed != 1 || report.Skipped != 6 {
		t.Fatalf("scope mismatch: %+v %v", report, err)
	}
	for _, p := range []string{unknown, partial, linkedMarker, linkedLease, linkedRoot, legacy, marker} {
		if _, err := os.Lstat(p); err != nil {
			t.Fatalf("foreign/unknown path removed: %s", p)
		}
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "not a bill artifact" {
		t.Fatal("symlink target changed")
	}
}

func TestRecoveryGuardAndBoundedScan(t *testing.T) {
	base := t.TempDir()
	_ = os.Chmod(base, 0700)
	guard, err := lockFile(filepath.Join(base, baseGuard), true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Recover(base); !errors.Is(err, ErrBusy) {
		t.Fatal("concurrent initialization not excluded")
	}
	unlockFile(guard)
	old := recoveryFixture(t, base, 1)
	for i := 0; i < maxRecoveryEntries; i++ {
		if err := os.WriteFile(filepath.Join(base, fmt.Sprintf("foreign-%03d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if report, err := Recover(base); !errors.Is(err, ErrBusy) || report.Removed != 0 {
		t.Fatal("overfull scan partially deleted files")
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatal("overfull scan deleted known file before stopping")
	}
}

func TestRecoverySpecialFilesDoNotBlock(t *testing.T) {
	base := t.TempDir()
	_ = os.Chmod(base, 0700)
	for i, name := range []string{sessionMarker, sessionLease} {
		root := recoveryFixture(t, base, i+1)
		path := filepath.Join(root, name)
		_ = os.Remove(path)
		if err := syscall.Mkfifo(path, 0600); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		report, err := Recover(base)
		if err == nil && (report.Skipped != 2 || report.Removed != 0) {
			err = errors.New("special files accepted")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("special file blocked recovery")
	}
}

func TestRecoveryRejectsGuardSymlinkAndInvalidNames(t *testing.T) {
	base := t.TempDir()
	_ = os.Chmod(base, 0700)
	outside := filepath.Join(t.TempDir(), "keep")
	_ = os.WriteFile(outside, []byte("keep"), 0600)
	if err := os.Symlink(outside, filepath.Join(base, baseGuard)); err != nil {
		t.Fatal(err)
	}
	if _, err := Recover(base); err == nil {
		t.Fatal("guard symlink accepted")
	}
	if _, err := New(base, testLimits(), allow); err == nil {
		t.Fatal("manager followed guard symlink")
	}
	if data, _ := os.ReadFile(outside); string(data) != "keep" {
		t.Fatal("guard target changed")
	}
	for _, name := range []string{"bill-jobs-1", sessionPrefix + strings.Repeat("A", 32), sessionPrefix + strings.Repeat("0", 31), sessionPrefix + "../data"} {
		if validSessionName(name) {
			t.Fatal("invalid session name accepted")
		}
	}
}

func TestRepeatedCloseCannotDeleteReusedPath(t *testing.T) {
	m := newManager(t, testLimits(), allow)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(m.root, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(m.root, "foreign-data")
	if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep" {
		t.Fatal("repeated close deleted a reused path")
	}
}
