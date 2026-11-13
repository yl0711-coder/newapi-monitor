package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func archiveDirectory(t *testing.T) string {
	t.Helper()
	p := t.TempDir()
	if err := os.Chmod(p, 0700); err != nil {
		t.Fatal(err)
	}
	return p
}

func archiveInvalidated(t *testing.T, f generationFixture) monthlytasks.Task {
	t.Helper()
	j := generationSubmit(t, f)
	if err := f.g.tasks.Invalidate(context.Background(), j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	return j
}

func archiveInventory(t *testing.T, f generationFixture, asOf time.Time) RetentionInspection {
	t.Helper()
	r, err := InspectGenerationRetention(context.Background(), f.receiver, asOf)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestArchiveCopiesOnlyCandidatesReadonlyAndReopensOffline(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	older := retentionReady(t, f, 1)
	latest := retentionReady(t, f, 2)
	invalid := retentionSubmitVersion(t, f, 3)
	if err := f.g.tasks.Invalidate(ctx, invalid.Request.Owner, invalid.ID); err != nil {
		t.Fatal(err)
	}
	active := retentionSubmitVersion(t, f, 4)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	report := archiveInventory(t, f, asOf)
	retentionItem(t, report, older, "archive_copy_candidate")
	retentionItem(t, report, latest, "keep_latest_ready")
	before, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	dir := archiveDirectory(t)
	ro := generationDB(t, f.receiverPath, true)
	receipt, err := CopyGenerationArchive(ctx, ro, dir, report.SnapshotSHA256, asOf)
	if err != nil || receipt.InputCount != 2 || receipt.PayloadBytes <= 0 {
		t.Fatal("copy failed", receipt, err)
	}
	if err := VerifyGenerationArchive(ctx, receipt); err != nil {
		t.Fatal("offline verification", err)
	}
	after, err := os.ReadFile(f.receiverPath)
	if err != nil || !bytes.Equal(before, after) || !reflect.DeepEqual(report, archiveInventory(t, f, asOf)) {
		t.Fatal("copy wrote receiver", err)
	}
	funding, err := os.ReadFile(f.path)
	if err != nil || generationHash(funding) != f.sourceHash {
		t.Fatal("copy altered funding evidence", err)
	}
	for _, j := range []monthlytasks.Task{latest, active} {
		if _, err := os.Stat(filepath.Join(receipt.Directory, j.Request.InputSHA256+".plan")); !os.IsNotExist(err) {
			t.Fatal("protected input copied", err)
		}
	}
	for _, j := range []monthlytasks.Task{older, invalid} {
		var plan, preparation, artifact []byte
		if err := f.receiver.QueryRow(`SELECT plan,preparation,artifact FROM portal_monthly_generation_inputs WHERE id=?`, j.Request.InputSHA256).
			Scan(&plan, &preparation, &artifact); err != nil {
			t.Fatal(err)
		}
		for suffix, want := range map[string][]byte{"plan": plan, "preparation": preparation, "artifact": artifact} {
			if want == nil {
				continue
			}
			got, err := os.ReadFile(filepath.Join(receipt.Directory, j.Request.InputSHA256+"."+suffix))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatal("rehydrated bytes differ", suffix, err)
			}
		}
	}
	// Repetition is copy-only, no overwrite or invalidated task resurrection.
	second, err := CopyGenerationArchive(ctx, ro, dir, report.SnapshotSHA256, asOf)
	if err != nil || second.Directory == receipt.Directory || second.ManifestSHA256 != receipt.ManifestSHA256 {
		t.Fatal("repeat changed content or overwrote archive", err)
	}
	p := f.plan
	p.Owner.AuthVersion = 3
	if _, err := f.g.Submit(ctx, p, f.raw); !errors.Is(err, monthlytasks.ErrStale) {
		t.Fatal("copy resurrected invalidated task", err)
	}
	// Verification needs no receiver, funding connection, or evidence directory.
	if err := VerifyGenerationArchive(ctx, second); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(receipt.Directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		st, err := e.Info()
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
			t.Fatal("private file permission", err)
		}
	}
}

func TestArchiveRejectsStaleInventoryAndProtectedInputs(t *testing.T) {
	f := generationFixtureNew(t)
	archiveInvalidated(t, f)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	r := archiveInventory(t, f, asOf)
	retentionSubmitVersion(t, f, 2)
	dir := archiveDirectory(t)
	receipt, err := CopyGenerationArchive(context.Background(), f.receiver, dir, r.SnapshotSHA256, asOf)
	if !errors.Is(err, ErrGenerationChanged) || receipt != (ArchiveReceipt{}) {
		t.Fatal("stale inventory accepted", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("stale attempt created files", err)
	}
	// A recent observation cannot bypass retention age by editing report fields.
	r = archiveInventory(t, f, time.Now())
	if _, err := CopyGenerationArchive(context.Background(), f.receiver, dir, r.SnapshotSHA256, time.Now()); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("no eligible candidates should fail", err)
	}
}

func TestArchiveRejectsTamperTruncationSymlinkPermissionsAndIncompleteCopy(t *testing.T) {
	f := generationFixtureNew(t)
	j := archiveInvalidated(t, f)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	r := archiveInventory(t, f, asOf)
	receipt, err := CopyGenerationArchive(context.Background(), f.receiver, archiveDirectory(t), r.SnapshotSHA256, asOf)
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"plan", "preparation"} {
		path := filepath.Join(receipt.Directory, j.Request.InputSHA256+"."+suffix)
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, raw := range [][]byte{original[:len(original)-1], append(bytes.Clone(original), ' ')} {
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err := VerifyGenerationArchive(context.Background(), receipt); err == nil {
				t.Fatal("changed payload accepted", suffix)
			}
		}
		if err := os.WriteFile(path, original, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0644); err != nil {
			t.Fatal(err)
		}
		if err := VerifyGenerationArchive(context.Background(), receipt); err == nil {
			t.Fatal("world readable payload accepted")
		}
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path, path+".saved"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(path+".saved", path); err != nil {
			t.Fatal(err)
		}
		if err := VerifyGenerationArchive(context.Background(), receipt); err == nil {
			t.Fatal("symlink payload accepted")
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path+".saved", path); err != nil {
			t.Fatal(err)
		}
	}
	bad := receipt
	bad.ManifestSHA256 = strings.Repeat("0", 64)
	if err := VerifyGenerationArchive(context.Background(), bad); err == nil {
		t.Fatal("wrong manifest digest accepted")
	}
	bad = receipt
	bad.PayloadBytes++
	if err := VerifyGenerationArchive(context.Background(), bad); err == nil {
		t.Fatal("wrong byte count accepted")
	}
	// Interrupted copies have no completion marker; a pending manifest alone
	// cannot pass. Repeating the operation must preserve those original bytes.
	if err := os.Remove(filepath.Join(receipt.Directory, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err := VerifyGenerationArchive(context.Background(), receipt); err == nil {
		t.Fatal("incomplete copy accepted")
	}
	parent := filepath.Dir(receipt.Directory)
	if _, err := CopyGenerationArchive(context.Background(), f.receiver, parent, r.SnapshotSHA256, asOf); err != nil {
		t.Fatal("retry after interruption failed", err)
	}
	if _, err := os.Stat(filepath.Join(receipt.Directory, "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("retry activated earlier incomplete copy")
	}
}

func TestArchiveInputBoundsCancellationAndExclusiveWrites(t *testing.T) {
	f := generationFixtureNew(t)
	archiveInvalidated(t, f)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	r := archiveInventory(t, f, asOf)
	dir := archiveDirectory(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out, err := CopyGenerationArchive(ctx, f.receiver, dir, r.SnapshotSHA256, asOf); !errors.Is(err, context.Canceled) || out != (ArchiveReceipt{}) {
		t.Fatal("cancel not respected", err)
	}
	archiveSlots <- struct{}{}
	_, err := CopyGenerationArchive(context.Background(), f.receiver, dir, r.SnapshotSHA256, asOf)
	<-archiveSlots
	if !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("concurrency not bounded", err)
	}
	for _, path := range []string{"relative", "/", dir + "/../bad"} {
		if _, err := openArchiveRoot(path); err == nil {
			t.Fatal("unsafe root accepted", path)
		}
	}
	root, err := openArchiveRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := writeArchiveFile(context.Background(), root, "existing", []byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := writeArchiveFile(context.Background(), root, "existing", []byte("replacement")); !errors.Is(err, os.ErrExist) {
		t.Fatal("overwrote existing private file", err)
	}
	if _, err := readArchiveFile(context.Background(), root, "existing", 3); err == nil {
		t.Fatal("oversize file accepted")
	}
	if err := os.Symlink(dir, dir+"-link"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(dir + "-link") })
	if _, err := openArchiveRoot(dir + "-link"); err == nil {
		t.Fatal("symlink root accepted")
	}
}

func TestArchiveActualInterruptedWritesAndCorruptReadback(t *testing.T) {
	f := generationFixtureNew(t)
	archiveInvalidated(t, f)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	r := archiveInventory(t, f, asOf)
	for _, scenario := range []string{"disk_failure", "cancel_after_payload", "bad_payload", "bad_pending_manifest", "marker_already_exists"} {
		t.Run(scenario, func(t *testing.T) {
			dir := archiveDirectory(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writes := 0
			out, err := copyGenerationArchive(ctx, f.receiver, dir, r.SnapshotSHA256, asOf,
				func(ctx context.Context, root *os.Root, name string, raw []byte) error {
					writes++
					if scenario == "disk_failure" && writes == 2 {
						return errors.New("injected disk failure")
					}
					if scenario == "bad_payload" && writes == 1 || scenario == "bad_pending_manifest" && name == "manifest.pending" {
						raw = []byte("corrupt")
					}
					if err := writeArchiveFile(ctx, root, name, raw); err != nil {
						return err
					}
					if scenario == "cancel_after_payload" && writes == 1 {
						cancel()
					}
					if scenario == "marker_already_exists" && name == "manifest.pending" {
						return writeArchiveFile(ctx, root, "manifest.json", []byte("do not overwrite"))
					}
					return nil
				})
			if err == nil || out != (ArchiveReceipt{}) {
				t.Fatal("partial or corrupt copy returned success", err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatal("expected one retained partial copy", err)
			}
			marker := filepath.Join(dir, entries[0].Name(), "manifest.json")
			if scenario == "marker_already_exists" {
				raw, err := os.ReadFile(marker)
				if err != nil || string(raw) != "do not overwrite" {
					t.Fatal("existing marker overwritten", err)
				}
			} else if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("interrupted copy marked complete", err)
			}
			if !reflect.DeepEqual(r, archiveInventory(t, f, asOf)) {
				t.Fatal("failed copy changed receiver")
			}
			retry, err := CopyGenerationArchive(context.Background(), f.receiver, dir, r.SnapshotSHA256, asOf)
			if err != nil || VerifyGenerationArchive(context.Background(), retry) != nil {
				t.Fatal("retry did not produce a valid separate copy", err)
			}
		})
	}
}

func TestArchiveFullRecordBoundRoundtrip(t *testing.T) {
	if raceDetector {
		// The production bounds are sized for native speed; the race detector
		// multiplies the cost of hashing the maximum record set. A busy CI runner
		// must not turn that into a spurious deadline failure.
		inspect, archive := retentionInspectionTimeout, archiveOperationTimeout
		retentionInspectionTimeout, archiveOperationTimeout = 6*inspect, 6*archive
		t.Cleanup(func() { retentionInspectionTimeout, archiveOperationTimeout = inspect, archive })
	}
	f := generationFixtureNew(t)
	f.raw = append(bytes.Clone(f.raw), bytes.Repeat([]byte(" "), 512<<10)...)
	f.plan.PreparationSHA256 = generationHash(f.raw)
	for i := range generationRecordLimit {
		f.plan.Owner.AuthVersion = int64(i + 1)
		archiveInvalidated(t, f)
	}
	asOf := time.Now().Add(45 * 24 * time.Hour)
	r := archiveInventory(t, f, asOf)
	start := time.Now()
	out, err := CopyGenerationArchive(context.Background(), generationDB(t, f.receiverPath, true), archiveDirectory(t), r.SnapshotSHA256, asOf)
	if err != nil || out.InputCount != 32 || out.PayloadBytes < 16<<20 {
		t.Fatal("full record archive failed", err)
	}
	if err := VerifyGenerationArchive(context.Background(), out); err != nil {
		t.Fatal("full record readback failed", err)
	}
	t.Logf("32 retained inputs, %d bytes: private copy plus two readback passes %s (local sample only)", out.PayloadBytes, time.Since(start))
}

func TestArchiveConcurrentAdmissionKeepsOneReadonlySnapshot(t *testing.T) {
	f := generationFixtureNew(t)
	if _, err := f.receiver.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	archiveInvalidated(t, f)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	r := archiveInventory(t, f, asOf)
	inserted := false
	out, err := copyGenerationArchive(context.Background(), f.receiver, archiveDirectory(t), r.SnapshotSHA256, asOf,
		func(ctx context.Context, root *os.Root, name string, raw []byte) error {
			if !inserted {
				inserted = true
				f.plan.Owner.AuthVersion = 2
				archiveInvalidated(t, f)
			}
			return writeArchiveFile(ctx, root, name, raw)
		})
	if err != nil || out.InputCount != 1 || !inserted {
		t.Fatal("copy did not retain original read snapshot", err)
	}
	if err := VerifyGenerationArchive(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	newer := archiveInventory(t, f, asOf)
	if newer.InputCount != 2 || newer.SnapshotSHA256 == r.SnapshotSHA256 {
		t.Fatal("concurrent admission missing")
	}
	// This is a point-in-time copy, not current-state evidence for deletion.
	if _, err := CopyGenerationArchive(context.Background(), f.receiver, archiveDirectory(t), r.SnapshotSHA256, asOf); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("old snapshot reused after concurrent admission", err)
	}
}
