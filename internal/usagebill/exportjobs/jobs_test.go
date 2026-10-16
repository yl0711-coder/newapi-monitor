package exportjobs

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLimits() Limits {
	return Limits{MaxJobs: 3, MaxRows: 200000, MaxBytes: 8 << 20, GenerateTimeout: 5 * time.Second, Retention: time.Minute}
}

func testOwner(n int64) Owner            { return Owner{n, 1, "server-members-v1"} }
func testKey(s string) string            { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func allow(context.Context, Owner) error { return nil }

func newManager(t *testing.T, limits Limits, auth Authorize) *Manager {
	t.Helper()
	base := t.TempDir()
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	m, err := New(base, limits, auth)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return m
}

func noArtifacts(t *testing.T, m *Manager) {
	t.Helper()
	entries, err := os.ReadDir(m.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("artifact retained: %v", entries)
	}
	for _, entry := range entries {
		if entry.Name() != sessionMarker && entry.Name() != sessionLease {
			t.Fatal("unexpected staged file")
		}
	}
}

func buildText(text string, rows int64) Build {
	return func(ctx context.Context, w io.Writer, progress func(int64) error) (int64, error) {
		if _, err := io.WriteString(w, text); err != nil {
			return 0, err
		}
		return rows, progress(rows)
	}
}

func submit(t *testing.T, m *Manager, group int64, key string, rows int64, build Build) View {
	t.Helper()
	v, err := m.Submit(context.Background(), Request{testOwner(group), testKey(key), rows, build})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func waitState(t *testing.T, m *Manager, owner Owner, id, want string) View {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		v, err := m.View(context.Background(), owner, id)
		if err != nil {
			t.Fatal(err)
		}
		if v.State == want {
			return v
		}
		if v.State == "failed" {
			t.Fatalf("unexpected failure: %+v", v)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("task never reached " + want)
	return View{}
}

func TestDuplicateSubmitAndClientDisconnect(t *testing.T) {
	m := newManager(t, testLimits(), allow)
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	build := func(ctx context.Context, w io.Writer, report func(int64) error) (int64, error) {
		calls.Add(1)
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		return buildText("header\nrow\n", 1)(ctx, w, report)
	}
	ctx, cancel := context.WithCancel(context.Background())
	v, err := m.Submit(ctx, Request{testOwner(1), testKey("same"), 1, build})
	if err != nil {
		t.Fatal(err)
	}
	cancel() // Browser create request ending must not kill the background task.
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := m.Submit(context.Background(), Request{testOwner(1), testKey("same"), 1, build})
			if err != nil || got.ID != v.ID {
				t.Errorf("duplicate: %+v %v", got, err)
			}
		}()
	}
	wg.Wait()
	waitState(t, m, testOwner(1), v.ID, "running")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("builder never started")
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate generation")
	}
	if _, err := m.Open(context.Background(), testOwner(1), v.ID); !errors.Is(err, ErrNotReady) {
		t.Fatal("partial file downloadable")
	}
}

func TestReadyArtifactIdentityAndScope(t *testing.T) {
	m := newManager(t, testLimits(), allow)
	text := "request,actual\nr1,0.002424\n"
	v := submit(t, m, 1, "one", 1, buildText(text, 1))
	v = waitState(t, m, testOwner(1), v.ID, "ready")
	hash := sha256.Sum256([]byte(text))
	if v.Bytes != int64(len(text)) || v.Rows != 1 || v.SHA256 != hex.EncodeToString(hash[:]) {
		t.Fatal("artifact manifest mismatch")
	}
	for _, owner := range []Owner{testOwner(2), {1, 2, "server-members-v1"}, {1, 1, "changed-members"}} {
		if _, err := m.View(context.Background(), owner, v.ID); !errors.Is(err, ErrNotFound) {
			t.Fatal("status exposed to wrong scope")
		}
		if _, err := m.Open(context.Background(), owner, v.ID); !errors.Is(err, ErrNotFound) {
			t.Fatal("artifact exposed to wrong scope")
		}
	}
	f, err := m.Open(context.Background(), testOwner(1), v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := m.Open(context.Background(), testOwner(1), v.ID); !errors.Is(err, ErrBusy) {
		t.Fatal("unbounded parallel readers")
	}
	body, err := io.ReadAll(f)
	if err != nil || string(body) != text {
		t.Fatal("changed artifact")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal("Close is not idempotent")
	}
}

func TestFailuresNeverExposePartialArtifactAndCanRetry(t *testing.T) {
	for _, name := range []string{"builder", "panic", "count", "progress", "limit", "ignored-limit", "timeout"} {
		t.Run(name, func(t *testing.T) {
			limits := testLimits()
			limits.MaxBytes = 32
			if name == "timeout" {
				limits.GenerateTimeout = 20 * time.Millisecond
			}
			m := newManager(t, limits, allow)
			build := func(ctx context.Context, w io.Writer, report func(int64) error) (int64, error) {
				_, _ = io.WriteString(w, "partial")
				switch name {
				case "builder":
					return 0, errors.New("SECRET upstream payload")
				case "panic":
					panic("SECRET panic")
				case "count":
					return 0, nil
				case "progress":
					_ = report(2)
					return 1, nil
				case "limit":
					_, err := io.WriteString(w, strings.Repeat("x", 64))
					return 1, err
				case "ignored-limit":
					_, _ = io.WriteString(w, strings.Repeat("x", 64))
					return 1, nil
				case "timeout":
					<-ctx.Done()
					return 1, nil
				}
				return 1, nil
			}
			v := submit(t, m, 1, "fail", 1, build)
			v = waitState(t, m, testOwner(1), v.ID, "failed")
			if v.Bytes != 0 || v.SHA256 != "" || strings.Contains(fmt.Sprint(v), "SECRET") {
				t.Fatal("partial/error content leaked")
			}
			if _, err := m.Open(context.Background(), testOwner(1), v.ID); !errors.Is(err, ErrNotReady) {
				t.Fatal("failed artifact opened")
			}
			noArtifacts(t, m)
			retry := submit(t, m, 1, "fail", 1, buildText("complete", 1))
			if retry.ID == v.ID {
				t.Fatal("failed generation falsely reused")
			}
			waitState(t, m, testOwner(1), retry.ID, "ready")
		})
	}
}

func TestAuthorizationBeforeAfterAndDownload(t *testing.T) {
	var calls atomic.Int32
	m := newManager(t, testLimits(), func(context.Context, Owner) error {
		if calls.Add(1) == 3 {
			return ErrUnauthorized
		} // Submit, worker start, worker finish.
		return nil
	})
	v := submit(t, m, 1, "auth", 1, buildText("private", 1))
	// Do not call View before worker's finish authorization; that also verifies access.
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		state := m.jobs[v.ID].view.State
		m.mu.Unlock()
		if state == "failed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("authorization check not completed")
		}
		time.Sleep(time.Millisecond)
	}
	v = waitState(t, m, testOwner(1), v.ID, "failed")
	if v.FailureCode != "authorization_changed" {
		t.Fatalf("%+v", v)
	}

	var deny atomic.Bool
	m2 := newManager(t, testLimits(), func(context.Context, Owner) error {
		if deny.Load() {
			return ErrUnauthorized
		}
		return nil
	})
	v2 := submit(t, m2, 2, "download", 1, buildText("private", 1))
	waitState(t, m2, testOwner(2), v2.ID, "ready")
	deny.Store(true)
	if _, err := m2.Open(context.Background(), testOwner(2), v2.ID); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked authorization still downloads")
	}
	if _, err := m2.View(context.Background(), testOwner(2), v2.ID); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked authorization still reads status")
	}
	if _, err := m2.Submit(context.Background(), Request{testOwner(3), testKey("new"), 1, buildText("x", 1)}); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("unauthorized task created")
	}
}

func TestCapacitySerialGenerationCancelAndShutdown(t *testing.T) {
	limits := testLimits()
	limits.MaxJobs = 2
	m := newManager(t, limits, allow)
	var active, maxActive atomic.Int32
	build := func(ctx context.Context, w io.Writer, report func(int64) error) (int64, error) {
		n := active.Add(1)
		defer active.Add(-1)
		maxActive.Store(max(maxActive.Load(), n))
		<-ctx.Done()
		return 0, ctx.Err()
	}
	v := submit(t, m, 1, "one", 1, build)
	waitState(t, m, testOwner(1), v.ID, "running")
	queued := submit(t, m, 2, "two", 1, build)
	if queued.State != "queued" {
		t.Fatal("generation is not serial")
	}
	if _, err := m.Submit(context.Background(), Request{testOwner(3), testKey("three"), 1, build}); !errors.Is(err, ErrBusy) {
		t.Fatal("queue is unbounded")
	}
	if err := m.Cancel(context.Background(), testOwner(1), v.ID); err != nil {
		t.Fatal(err)
	}
	waitState(t, m, testOwner(2), queued.ID, "running")
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if maxActive.Load() != 1 || active.Load() != 0 {
		t.Fatal("background worker leak or overlap")
	}
	if _, err := os.Stat(m.root); !os.IsNotExist(err) {
		t.Fatal("close retained manager directory")
	}
}

func TestExpiryAndCancellationPreserveOpenLeaseOnly(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			m := newManager(t, testLimits(), allow)
			v := submit(t, m, 1, "lease", 1, buildText("complete", 1))
			waitState(t, m, testOwner(1), v.ID, "ready")
			f, err := m.Open(context.Background(), testOwner(1), v.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if cancel {
				if err := m.Cancel(context.Background(), testOwner(1), v.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				m.mu.Lock()
				m.jobs[v.ID].view.ExpiresAt = time.Now().Add(-time.Second)
				m.sweepLocked(time.Now())
				m.mu.Unlock()
			}
			if _, err := m.Open(context.Background(), testOwner(1), v.ID); !errors.Is(err, ErrNotFound) {
				t.Fatal("expired/canceled download opened")
			}
			if _, err := m.Submit(context.Background(), Request{testOwner(1), testKey("lease"), 1, buildText("replacement", 1)}); !errors.Is(err, ErrBusy) {
				t.Fatal("live file lease overwritten")
			}
			body, err := io.ReadAll(f)
			if err != nil || string(body) != "complete" {
				t.Fatal("existing lease damaged")
			}
			_ = f.Close()
			noArtifacts(t, m)
		})
	}
}

func TestIdleExpiryWithoutAPIRequests(t *testing.T) {
	for _, lease := range []bool{false, true} {
		t.Run(fmt.Sprint(lease), func(t *testing.T) {
			limits := testLimits()
			limits.GenerateTimeout = 100 * time.Millisecond
			limits.Retention = 250 * time.Millisecond
			m := newManager(t, limits, allow)
			v := submit(t, m, 1, "idle-expiry", 1, buildText("retained-until-download-close", 1))
			waitState(t, m, testOwner(1), v.ID, "ready")
			m.mu.Lock()
			path := m.jobs[v.ID].dir
			m.mu.Unlock()
			var f *Artifact
			if lease {
				var err error
				f, err = m.Open(context.Background(), testOwner(1), v.ID)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
			}
			// No View/Open/Submit/sweep call after this point: exercise worker timer.
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				m.mu.Lock()
				j, exists := m.jobs[v.ID]
				retired := exists && j.retired
				m.mu.Unlock()
				if (!lease && !exists) || (lease && retired) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			m.mu.Lock()
			j, exists := m.jobs[v.ID]
			retired := exists && j.retired
			m.mu.Unlock()
			if lease {
				if !retired {
					t.Fatal("idle timer did not retire open artifact")
				}
				body, err := io.ReadAll(f)
				if err != nil || string(body) != "retained-until-download-close" {
					t.Fatal("expiry damaged download")
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			} else if exists {
				t.Fatal("idle timer did not remove artifact")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("expired artifact directory retained")
			}
		})
	}
}

func TestPrivateRootAndParameterBounds(t *testing.T) {
	base := t.TempDir()
	_ = os.Chmod(base, 0755)
	if _, err := New(base, testLimits(), allow); !errors.Is(err, ErrInvalid) {
		t.Fatal("public staging directory accepted")
	}
	_ = os.Chmod(base, 0700)
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(base, link); err != nil {
		t.Fatal(err)
	}
	if _, err := New(link, testLimits(), allow); !errors.Is(err, ErrInvalid) {
		t.Fatal("symlink staging directory accepted")
	}
	limits := testLimits()
	limits.MaxBytes = 128 << 20
	if _, err := New(base, limits, allow); !errors.Is(err, ErrInvalid) {
		t.Fatal("aggregate storage bound ignored")
	}
	m := newManager(t, testLimits(), allow)
	for _, r := range []Request{
		{Owner{}, testKey("x"), 1, buildText("x", 1)},
		{testOwner(1), "../somewhere", 1, buildText("x", 1)},
		{testOwner(1), testKey("x"), -1, buildText("x", 1)},
		{testOwner(1), testKey("x"), 200001, buildText("x", 1)},
		{testOwner(1), testKey("x"), 1, nil},
	} {
		if _, err := m.Submit(context.Background(), r); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid request accepted")
		}
	}
}

func TestHundredThousandRowsConserveAmountAndOrder(t *testing.T) {
	limits := testLimits()
	limits.GenerateTimeout = 20 * time.Second
	m := newManager(t, limits, allow)
	const rows = 100000
	build := func(ctx context.Context, dst io.Writer, progress func(int64) error) (int64, error) {
		w := csv.NewWriter(dst)
		if err := w.Write([]string{"Request ID", "actual_micro_cny"}); err != nil {
			return 0, err
		}
		for i := int64(1); i <= rows; i++ {
			if err := ctx.Err(); err != nil {
				return i - 1, err
			}
			// Synthetic quota=1212 => 2424 micro-CNY, not floating arithmetic.
			if err := w.Write([]string{strconv.FormatInt(i, 10), "2424"}); err != nil {
				return i - 1, err
			}
			if i%500 == 0 {
				w.Flush()
				if err := w.Error(); err != nil {
					return i, err
				}
				if err := progress(i); err != nil {
					return i, err
				}
			}
		}
		w.Flush()
		return rows, w.Error()
	}
	start := time.Now()
	v := submit(t, m, 1, "100k", rows, build)
	v = waitState(t, m, testOwner(1), v.ID, "ready")
	f, err := m.Open(context.Background(), testOwner(1), v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil {
		t.Fatal(err)
	}
	var count, total int64
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
		if row[0] != strconv.FormatInt(count, 10) || row[1] != "2424" {
			t.Fatal("row/order/amount changed")
		}
		total += 2424
	}
	if count != rows || total != 242400000 || v.Rows != rows {
		t.Fatal("incomplete bill")
	}
	t.Logf("synthetic rows=%d bytes=%d amount_micro_cny=%d elapsed=%s", count, v.Bytes, total, time.Since(start))
}

func TestTransientAuthorizationAndRetryCooldown(t *testing.T) {
	var unavailable atomic.Bool
	limits := testLimits()
	limits.RetryDelay = time.Minute
	m := newManager(t, limits, func(context.Context, Owner) error {
		if unavailable.Load() {
			return errors.New("transient local directory unavailable")
		}
		return nil
	})
	v := submit(t, m, 1, "scope", 1, buildText("complete", 1))
	waitState(t, m, testOwner(1), v.ID, "ready")
	unavailable.Store(true)
	if _, err := m.Open(context.Background(), testOwner(1), v.ID); err == nil {
		t.Fatal("opened despite unavailable authorization")
	}
	unavailable.Store(false)
	f, err := m.Open(context.Background(), testOwner(1), v.ID)
	if err != nil {
		t.Fatal("transient failure destroyed artifact")
	}
	_ = f.Close()
	if _, err := m.Submit(context.Background(), Request{testOwner(1), testKey("different"), 1, buildText("x", 1)}); !errors.Is(err, ErrBusy) {
		t.Fatal("cooldown bypass")
	}
	if got := submit(t, m, 1, "scope", 1, buildText("unused", 1)); got.ID != v.ID {
		t.Fatal("duplicate not reusable during cooldown")
	}
}

func TestLateQueuedJobCannotExtendExpiredGenerationWindow(t *testing.T) {
	m := newManager(t, testLimits(), allow)
	started, release := make(chan struct{}), make(chan struct{})
	first := submit(t, m, 1, "first", 1, func(ctx context.Context, w io.Writer, p func(int64) error) (int64, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		return buildText("x", 1)(ctx, w, p)
	})
	<-started
	var hadBound atomic.Bool
	second := submit(t, m, 2, "second", 1, func(ctx context.Context, w io.Writer, p func(int64) error) (int64, error) {
		deadline, ok := ctx.Deadline()
		hadBound.Store(ok && time.Until(deadline) < time.Second)
		<-ctx.Done()
		return 1, nil // even an ignored cancellation cannot publish ready
	})
	m.mu.Lock()
	m.jobs[second.ID].view.ExpiresAt = time.Now().Add(100 * time.Millisecond)
	m.mu.Unlock()
	close(release)
	waitState(t, m, testOwner(1), first.ID, "ready")
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		state := m.jobs[second.ID].view.State
		m.mu.Unlock()
		if state == "failed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queued expiry ignored")
		}
		time.Sleep(time.Millisecond)
	}
	if !hadBound.Load() {
		t.Fatal("queue waiting time not respected")
	}
	if v := waitState(t, m, testOwner(2), second.ID, "failed"); v.FailureCode != "generation_timeout" {
		t.Fatal(v)
	}
}

func TestShutdownWithDownloadAndSiblingDirectories(t *testing.T) {
	m := newManager(t, testLimits(), allow)
	base := filepath.Dir(m.root)
	marker := filepath.Join(base, "unrelated-backup")
	if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	other, err := New(base, testLimits(), allow)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	v := submit(t, m, 1, "lease", 1, buildText("complete", 1))
	waitState(t, m, testOwner(1), v.ID, "ready")
	if _, err := other.View(context.Background(), testOwner(1), v.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("task identity survived another process/session")
	}
	f, err := m.Open(context.Background(), testOwner(1), v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := m.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("shutdown deleted live download")
	}
	data, err := io.ReadAll(f)
	if err != nil || string(data) != "complete" {
		t.Fatal("leased file damaged")
	}
	_ = f.Close()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep" {
		t.Fatal("unrelated file removed")
	}
	if _, err := os.Stat(other.root); err != nil {
		t.Fatal("sibling manager removed")
	}
}

func TestLowSpaceStopsBeforeBuilder(t *testing.T) {
	if os.Getenv("BILL_JOBS_LOW_SPACE") != "1" {
		t.Skip("isolated Docker tmpfs acceptance only")
	}
	m := newManager(t, testLimits(), allow)
	var called atomic.Bool
	v := submit(t, m, 1, "disk", 1, func(context.Context, io.Writer, func(int64) error) (int64, error) { called.Store(true); return 1, nil })
	v = waitState(t, m, testOwner(1), v.ID, "failed")
	if v.FailureCode != "insufficient_storage" || called.Load() {
		t.Fatalf("reserve was not enforced: %+v", v)
	}
	noArtifacts(t, m)
}
