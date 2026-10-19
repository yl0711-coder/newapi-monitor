package monitor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

func TestFinanceCURFingerprintTracksContentNotFileMetadata(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "cur.example")
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cur.json")
	m.cfg.FinanceCURArtifactEnabled, m.cfg.FinanceCURArtifactPath = true, path
	first := financeCURTestArtifactWithCost(t, true, 1_000_000_000)
	second := financeCURTestArtifactWithCost(t, true, 2_000_000_000)
	var a, b bytes.Buffer
	if err := financecur.EncodeArtifact(&a, first); err != nil {
		t.Fatal(err)
	}
	if err := financecur.EncodeArtifact(&b, second); err != nil {
		t.Fatal(err)
	}
	if a.Len() != b.Len() {
		t.Fatal("fixture must preserve byte size")
	}
	stamp := time.Unix(1789000000, 0)
	write := func(data []byte, modified time.Time) {
		t.Helper()
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, modified, modified); err != nil {
			t.Fatal(err)
		}
	}
	day := first.Timeline.Days[0]
	version := func(period bool) string {
		t.Helper()
		v, err := m.financeReportSourceFingerprintForScope(ctx, day.FromUnix, day.ToUnix, !period)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	write(a.Bytes(), stamp)
	before, basePeriod := version(false), version(true)
	write(b.Bytes(), stamp)
	after := version(false)
	if before == after {
		t.Fatal("same size/mtime cost correction did not invalidate report cache")
	}
	if version(true) != basePeriod {
		t.Fatal("CUR edit invalidated base monthly user/upstream facts")
	}
	view := m.loadFinanceCURCost(day.FromUnix, day.ToUnix)
	if view.ExactCost == nil || view.ExactCost.MicroUSD != "2000000" {
		t.Fatal("corrected artifact did not load", view)
	}
	write(b.Bytes(), stamp.Add(time.Hour))
	if version(false) != after {
		t.Fatal("metadata-only touch invalidated identical financial evidence")
	}
	write([]byte("broken"), stamp)
	invalid := version(false)
	if invalid == after {
		t.Fatal("invalid artifact kept valid cached cost")
	}
	view = m.loadFinanceCURCost(day.FromUnix, day.ToUnix)
	if view.Loaded || view.ExactCost != nil {
		t.Fatal("invalid evidence was promoted", view)
	}
	write(b.Bytes(), stamp)
	if version(false) != after {
		t.Fatal("identical recovered evidence changed version")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if version(false) == after {
		t.Fatal("missing artifact kept valid cached cost")
	}
}

func TestFinanceCURSourceVersionHonorsCancellationAndFileType(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := financeCURArtifactSourceVersion(ctx, "not-read"); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled source read ignored", err)
	}
	reader := financeCURContextReader{ctx: ctx, reader: bytes.NewReader([]byte("not-read"))}
	if n, err := reader.Read(make([]byte, 32)); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled reader accessed bytes", n, err)
	}
	reader.ctx = context.Background()
	if body, err := io.ReadAll(reader); err != nil || string(body) != "not-read" {
		t.Fatal("normal reader changed bytes", err)
	}
	if version, err := financeCURArtifactSourceVersion(context.Background(), t.TempDir()); err != nil || version != "invalid-type" {
		t.Fatal("directory was treated as an artifact", version, err)
	}
}
