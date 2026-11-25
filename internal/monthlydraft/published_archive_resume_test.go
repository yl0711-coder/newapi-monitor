package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func TestPublishedArchivePartialResumeRevalidatesAuthorityAndSource(t *testing.T) {
	for _, change := range []string{"authority", "source", "withdrawal"} {
		t.Run(change, func(t *testing.T) {
			f, task, _ := publishedArchiveFixture(t)
			dir := publishedArchiveDirectory(t)
			name := task.Request.InputSHA256 + ".plan"
			writer := func(ctx context.Context, root *os.Root, file string, raw []byte) error {
				if err := writeArchiveFile(ctx, root, file, raw[:len(raw)/2]); err != nil {
					return err
				}
				return syscall.ENOSPC
			}
			if _, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, dir, true, writer); !errors.Is(err, syscall.ENOSPC) {
				t.Fatal("missing interruption", err)
			}
			before, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "authority":
				f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
			case "source":
				if err := os.WriteFile(filepath.Join(f.g.root.Name(), "2026-09-original.json"), []byte(`[]`), 0600); err != nil {
					t.Fatal(err)
				}
			case "withdrawal":
				if err := f.auth.Exec(`DELETE FROM portal_monthly_bills`).Error; err != nil {
					t.Fatal(err)
				}
			}
			if receipt, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, dir, true, writeArchiveFile); err == nil || receipt != (ArchiveReceipt{}) {
				t.Fatal("partial output bypassed reauthorization", receipt, err)
			}
			after, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("rejected recovery appended retained partial file", err)
			}
			if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 || evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 {
				t.Fatal("rejected recovery wrote extra files or released hot input", entries, err)
			}
		})
	}
}

func TestPublishedArchiveRepeatedInterruptionsRestartAndCancellation(t *testing.T) {
	f, task, bill := publishedArchiveFixture(t)
	journal, err := loadPublication(t.Context(), f.g.receiver, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	request, own, foreign := publishedCustomerHTTP(t, f)
	page, book := publishedCustomerSnapshot(t, request, own, foreign)
	parent := privateCapturedOutput(t)
	dir := filepath.Join(parent, "input-"+task.Request.InputSHA256)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	initialOwner := capacityFixture(t, f, parent)
	if err := initialOwner.close(); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.g.copyPublishedGenerationArchive(canceled, f.auth, task.Request.Owner, task.ID, dir, true, writeArchiveFile); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled copy did not stop", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatal("cancelled copy wrote output", entries, err)
	}
	names := []string{task.Request.InputSHA256 + ".plan", task.Request.InputSHA256 + ".preparation", task.Request.InputSHA256 + ".artifact", "manifest.pending"}
	for _, target := range names {
		interrupted := false
		writer := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
			if name == target {
				interrupted = true
				if err := writeArchiveFile(ctx, root, name, raw[:len(raw)/2]); err != nil {
					return err
				}
				return syscall.ENOSPC
			}
			return writeArchiveFile(ctx, root, name, raw)
		}
		if _, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, dir, true, writer); !errors.Is(err, syscall.ENOSPC) || !interrupted {
			t.Fatal("next file did not interrupt in the same directory", target, interrupted, err)
		}
		if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 || publishedPermitCount(t, f) != 0 {
			t.Fatal("interruption released original", target)
		}
		// Close/recreate the maintenance owner after every interruption. No
		// retry counter or in-memory success flag may authorize the next copy.
		owner := capacityFixture(t, f, parent)
		if err := owner.close(); err != nil {
			t.Fatal(err)
		}
	}
	owner := capacityFixture(t, f, parent)
	owner.hotCount = 1
	if worked, err := owner.step(t.Context()); err != nil || !worked {
		t.Fatal("fresh owner failed to recover four interruptions", worked, err)
	}
	if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 0 {
		t.Fatal("verified recovery did not release hot payload")
	}
	assertPublishedArchiveUnchanged(t, f, task, bill, journal)
	nextPage, nextBook := publishedCustomerSnapshot(t, request, own, foreign)
	if !bytes.Equal(page, nextPage) || !reflect.DeepEqual(book, nextBook) {
		t.Fatal("recovery changed customer page or downloadable amounts")
	}
	if entries, err := os.ReadDir(parent); err != nil || len(entries) != 1 {
		t.Fatal("retries multiplied archive directories", entries, err)
	}
}

func TestPublishedArchiveManagedRecoveryAfterPartialENOSPC(t *testing.T) {
	for _, stop := range []int{1, 2, 3, 4} {
		t.Run(string(rune('0'+stop)), func(t *testing.T) {
			f, task, _ := publishedArchiveFixture(t)
			parent := privateCapturedOutput(t)
			m := capacityFixture(t, f, parent)
			dir := filepath.Join(parent, "input-"+task.Request.InputSHA256)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			calls := 0
			write := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				calls++
				if calls == stop {
					if err := writeArchiveFile(ctx, root, name, raw[:len(raw)/2]); err != nil {
						return err
					}
					return syscall.ENOSPC
				}
				return writeArchiveFile(ctx, root, name, raw)
			}
			if _, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, dir, true, write); !errors.Is(err, syscall.ENOSPC) {
				t.Fatal("failure not injected", err)
			}
			if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 || publishedPermitCount(t, f) != 0 {
				t.Fatal("partial copy released original")
			}
			// Removing the failure is sufficient: the real maintenance turn
			// resumes the SAME directory and releases only after full re-audit.
			m.hotCount = 1
			if worked, err := m.step(t.Context()); err != nil || !worked || m.paused.Load() {
				t.Fatal("maintenance did not resume", worked, err)
			}
			if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 0 {
				t.Fatal("verified archive did not release payload")
			}
			entries, err := os.ReadDir(parent)
			if err != nil || len(entries) != 1 {
				t.Fatal("recovery multiplied directories", entries, err)
			}
		})
	}
}

func TestPublishedArchiveManagedResumeEveryPartialFile(t *testing.T) {
	f, task, _ := publishedArchiveFixture(t)
	dir := publishedArchiveDirectory(t)
	ref, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, dir, true, writeArchiveFile)
	if err != nil {
		t.Fatal(err)
	}
	id := task.Request.InputSHA256
	names := []string{id + ".plan", id + ".preparation", id + ".artifact", "manifest.pending"}
	contents := make(map[string][]byte)
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		contents[name] = raw
	}
	for last, name := range names {
		t.Run(name, func(t *testing.T) {
			out := publishedArchiveDirectory(t)
			for i := 0; i <= last; i++ {
				raw := contents[names[i]]
				if i == last {
					raw = raw[:len(raw)/2]
				}
				if err := os.WriteFile(filepath.Join(out, names[i]), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			// The public/manual API still rejects a partial directory.
			if _, err := f.g.CopyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, out); err == nil {
				t.Fatal("manual copy silently resumed partial output")
			}
			got, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, out, true, writeArchiveFile)
			if err != nil || got.ManifestSHA256 != ref.ManifestSHA256 {
				t.Fatal("managed partial recovery differs", got, err)
			}
			again, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, out, true, writeArchiveFile)
			if err != nil || again != got {
				t.Fatal("replay changed complete copy", again, err)
			}
			for _, name := range names {
				raw, err := os.ReadFile(filepath.Join(out, name))
				if err != nil || !bytes.Equal(raw, contents[name]) {
					t.Fatal("recovered bytes differ", name, err)
				}
			}
		})
	}
}

func TestPublishedArchiveManagedResumeRejectsUnknownConflictsAndLinks(t *testing.T) {
	f, task, _ := publishedArchiveFixture(t)
	for _, kind := range []string{"unknown", "conflict", "symlink", "hardlink", "public"} {
		t.Run(kind, func(t *testing.T) {
			out := publishedArchiveDirectory(t)
			name := task.Request.InputSHA256 + ".plan"
			path := filepath.Join(out, name)
			external := filepath.Join(t.TempDir(), "untouched")
			if err := os.WriteFile(external, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "unknown":
				path = filepath.Join(out, "unknown")
				if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "conflict":
				if err := os.WriteFile(path, []byte("conflicting bytes"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(external, path); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(external, path); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.WriteFile(path, nil, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, out, true, writeArchiveFile); err == nil {
				t.Fatal("unsafe resume accepted", kind)
			}
			entries, err := os.ReadDir(out)
			if err != nil || len(entries) != 1 {
				t.Fatal("failure mutated directory", entries, err)
			}
			raw, err := os.ReadFile(external)
			if err != nil || string(raw) != "keep" {
				t.Fatal("external file changed", err)
			}
		})
	}
}
