package monthlydraft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestPublishedArchiveCompletedWriteIOErrorCannotReleaseOriginal(t *testing.T) {
	f, task, _ := publishedArchiveFixture(t)
	parent := privateCapturedOutput(t)
	owner := capacityFixture(t, f, parent)
	dir := filepath.Join(parent, "input-"+task.Request.InputSHA256)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// Inject an I/O error at the write-completion boundary even though the
	// bytes are present. This is NOT a hardware/fsync-error qualification.
	writer := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
		if err := writeArchiveFile(ctx, root, name, raw); err != nil {
			return err
		}
		return syscall.EIO
	}
	receipt, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, dir, true, writer)
	if !errors.Is(err, syscall.EIO) || receipt != (ArchiveReceipt{}) || monthlyResourceWait(err) {
		t.Fatal("I/O failure treated as successful or automatic capacity recovery", receipt, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("I/O failure created completed manifest", err)
	}
	if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 || publishedPermitCount(t, f) != 0 {
		t.Fatal("write-completion error released original data")
	}
	if err := owner.close(); err != nil {
		t.Fatal(err)
	}
	// Only an explicit subsequent invocation, after the fault is removed,
	// may revalidate the retained bytes and obtain a complete receipt.
	recovered, err := f.g.copyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, dir, true, writeArchiveFile)
	if err != nil || recovered == (ArchiveReceipt{}) || evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 {
		t.Fatal("explicit revalidation failed or itself deleted original", recovered, err)
	}
}
