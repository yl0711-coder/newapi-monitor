package monthlydraft

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func evidenceSparse(t *testing.T, dir, name string, size int64) monitor.MonthlyPreparationSource {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := io.CopyBuffer(h, f, make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	return monitor.MonthlyPreparationSource{ID: name, Path: path, SHA256: hex.EncodeToString(h.Sum(nil))}
}

func TestGenerationEvidenceLargeSnapshotWithinTotalBudget(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	g := Generation{root: root}
	large := evidenceSparse(t, dir, "historical.sqlite", 126902272)
	small := evidenceSparse(t, dir, "remaining.json", 7118167)
	start := time.Now()
	if err := g.verifyFiles(context.Background(), []monitor.MonthlyPreparationSource{large, small}); err != nil {
		t.Fatal("real-sized evidence rejected", err)
	}
	t.Logf("streamed %d bytes within unchanged %d-byte total budget in %s", 126902272+7118167, generationEvidenceLimit, time.Since(start))
	large.SHA256 = generationHash([]byte("wrong digest"))
	if err := g.verifyFiles(context.Background(), []monitor.MonthlyPreparationSource{large}); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("wrong hash accepted", err)
	}
}

func TestGenerationEvidenceTotalBudgetAndCancellation(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	g := Generation{root: root}
	exact := evidenceSparse(t, dir, "exact.sqlite", generationEvidenceLimit)
	if err := g.verifyFiles(context.Background(), []monitor.MonthlyPreparationSource{exact}); err != nil {
		t.Fatal("exact budget rejected", err)
	}
	one := evidenceSparse(t, dir, "one.json", 1)
	for _, sources := range [][]monitor.MonthlyPreparationSource{{exact, one}, {one, exact}, {exact, exact}} {
		if err := g.verifyFiles(context.Background(), sources); !errors.Is(err, ErrGenerationCapacity) {
			t.Fatal("over budget accepted", err)
		}
	}
	if _, err := g.verifyFile(context.Background(), exact, generationEvidenceLimit-1); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("remaining budget ignored", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.verifyFiles(ctx, []monitor.MonthlyPreparationSource{exact}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f, err := os.OpenFile(exact.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = f.Truncate(generationEvidenceLimit + 1)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if err := g.verifyFiles(context.Background(), []monitor.MonthlyPreparationSource{exact}); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("oversized file accepted", err)
	}
}
