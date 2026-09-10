package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

type archiveStalledKeyFixture struct {
	*archiveReaderFixture
	stalledKey string
}

func (f archiveStalledKeyFixture) Get(ctx context.Context, key string) (ecsarchive.Object, error) {
	if key == f.stalledKey {
		<-ctx.Done()
		return ecsarchive.Object{}, ctx.Err()
	}
	return f.archiveReaderFixture.Get(ctx, key)
}

func TestECSLogArchiveSlowPageDoesNotStarveReadySource(t *testing.T) {
	m := newECSLogTestMonitor(t)
	r := newECSArchiveReplayer(m, "isolated/")
	blocked := archiveChainFixture(t, m, strings.Repeat("a", 32), 1)
	ready := archiveChainFixture(t, m, strings.Repeat("b", 32), 2)
	base := pagedArchiveFixture(append(blocked, ready...))
	ctx := context.Background()
	if err := r.scanEntry(ctx, base, archiveEntry(ready[1]), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := r.scanEntry(ctx, base, archiveEntry(ready[0]), time.Now()); err != nil {
		t.Fatal(err)
	}
	reader := archiveStalledKeyFixture{base, blocked[0].Key}
	start := time.Now()
	if err := r.scan(ctx, reader, "slow-binding", time.Now()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout missing: %v", err)
	}
	if time.Since(start) > ecsArchivePhaseBudget+ecsArchiveFailureBudget+3*time.Second {
		t.Fatal("unbounded stalled scan")
	}
	var receipt ECSLogArchiveReceipt
	if err := m.storeDB.First(&receipt, "object_key = ?", ready[1].Key).Error; err != nil || receipt.Status != 200 {
		t.Fatalf("other source starved: status=%d err=%v", receipt.Status, err)
	}
}

func TestECSLogArchiveSlowReadsProgressAcrossDeadlines(t *testing.T) {
	m := newECSLogTestMonitor(t)
	chain := archiveChainFixture(t, m, strings.Repeat("c", 32), 10)
	reader := pagedArchiveFixture(chain)
	reader.delay = 20 * time.Millisecond
	r := newECSArchiveReplayer(m, "isolated/")
	for turn := 0; turn < 15; turn++ {
		ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
		err := r.scan(ctx, reader, "slow-reader", time.Now())
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		var count int64
		if err := m.storeDB.Model(&ECSLogArchiveReceipt{}).Where("status=200").Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count == 10 {
			return
		}
	}
	t.Fatal("slow page never progressed across poll deadlines")
}
