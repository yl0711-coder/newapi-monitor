package monthlydraft

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Explicit opt-in; retained evidence is only opened as read-only files. This
// test copies exact bytes to its own private temporary directory. It does NOT
// fabricate funding observations, attest history, generate tasks or publish.
func TestGenerationRetainedEvidenceCompatibility(t *testing.T) {
	path := os.Getenv("MONTHLY_GENERATION_RETAINED_INPUT")
	if path == "" {
		t.Skip("explicit retained input path required")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("absolute input required")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal("retained input unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(f, monitor.MonthlyPreparationInputLimit+1))
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal("cannot read input")
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal("invalid retained input")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	var total, maxSize int64
	for _, s := range input.Sources {
		info, err := os.Lstat(s.Path)
		if err != nil || !info.Mode().IsRegular() || info.Size() > generationEvidenceLimit-total {
			t.Fatal("source missing or outside total budget")
		}
		total += info.Size()
		if info.Size() > maxSize {
			maxSize = info.Size()
		}
	}
	copyStarted := time.Now()
	for i, s := range input.Sources {
		before, err := os.Lstat(s.Path)
		if err != nil || !before.Mode().IsRegular() {
			t.Fatal("source changed before copy")
		}
		src, err := os.Open(s.Path)
		if err != nil {
			t.Fatal("source unreadable")
		}
		opened, err := src.Stat()
		if err != nil || !os.SameFile(before, opened) {
			_ = src.Close()
			t.Fatal("source replaced")
		}
		copyPath := filepath.Join(dir, fmt.Sprintf("evidence-%03d.bin", i))
		dst, err := os.OpenFile(copyPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			_ = src.Close()
			t.Fatal(err)
		}
		h := sha256.New()
		n, copyErr := io.CopyBuffer(io.MultiWriter(dst, h), io.LimitReader(src, before.Size()+1), make([]byte, 64<<10))
		after, statErr := src.Stat()
		sourceCloseErr := src.Close()
		destCloseErr := dst.Close()
		current, pathErr := os.Lstat(s.Path)
		if copyErr != nil || statErr != nil || sourceCloseErr != nil || destCloseErr != nil || pathErr != nil || n != before.Size() || after.Size() != n ||
			!current.Mode().IsRegular() || !os.SameFile(before, current) || !before.ModTime().Equal(after.ModTime()) || hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
			t.Fatalf("source integrity failed at index %d; no amount reinterpreted", i)
		}
		input.Sources[i].Path = copyPath
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	g := Generation{root: root}
	verifyStarted := time.Now()
	if err := g.verifyFiles(context.Background(), input.Sources); err != nil {
		t.Fatal(err)
	}
	if len(input.Sources) != 44 || total != 134020439 || maxSize != 126902272 {
		t.Fatal("retained inventory changed; review before updating acceptance")
	}
	t.Logf("44 exact retained source copies verified; total=%d max=%d copy=%s validation=%s; funding scan coverage NOT asserted", total, maxSize, verifyStarted.Sub(copyStarted), time.Since(verifyStarted))
}
