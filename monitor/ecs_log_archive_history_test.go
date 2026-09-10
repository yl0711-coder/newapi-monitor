package monitor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func TestECSLogArchiveVerifiedHistoryDoesNotDelayNewSource(t *testing.T) {
	m := newECSLogTestMonitor(t)
	r := newECSArchiveReplayer(m, "isolated/")
	history := archiveChainFixture(t, m, strings.Repeat("a", 32), 250)
	fresh := archiveChainFixture(t, m, strings.Repeat("b", 32), 1)
	reader := pagedArchiveFixture(append(history, fresh...))
	ctx := context.Background()
	for _, object := range history {
		if err := r.scanEntry(ctx, reader, archiveEntry(object), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	gets := reader.gets
	if err := r.scan(ctx, reader, "history-regression", time.Now()); err != nil {
		t.Fatal(err)
	}
	var receipt ECSLogArchiveReceipt
	if err := m.storeDB.First(&receipt, "object_key=?", fresh[0].Key).Error; err != nil || receipt.Status != 200 {
		t.Fatalf("new source delayed behind verified history: receipt=%+v err=%v", receipt, err)
	}
	if reader.gets-gets != 1 || reader.lists != 3 {
		t.Fatalf("history downloaded again or unexpected listing work: gets=%d lists=%d", reader.gets-gets, reader.lists)
	}
	// A second sweep recognizes every immutable receipt without recounting it.
	if err := r.scan(ctx, reader, "history-regression", time.Now()); err != nil {
		t.Fatal(err)
	}
	if reader.gets-gets != 1 {
		t.Fatal("verified objects downloaded during duplicate sweep")
	}
	var total int64
	if err := m.storeDB.Model(&RejectionSample{}).Select("COALESCE(SUM(count),0)").Scan(&total).Error; err != nil || total != 251 {
		t.Fatalf("facts duplicated or missing: count=%d err=%v", total, err)
	}
}

func TestECSLogArchiveDiscoveryDownloadBudgetStillBounded(t *testing.T) {
	m := newECSLogTestMonitor(t)
	objects := archiveChainFixture(t, m, strings.Repeat("c", 32), 100)
	reader := pagedArchiveFixture(objects)
	r := newECSArchiveReplayer(m, "isolated/")
	state := ECSLogArchiveScan{ID: 1, Binding: "budget"}
	if err := r.scanPages(context.Background(), reader, &state, time.Now()); err != nil {
		t.Fatal(err)
	}
	if reader.gets != ecsArchivePhaseEntries || state.PageIndex != ecsArchivePhaseEntries || state.PageJSON == "" {
		t.Fatalf("download cap/checkpoint changed: gets=%d index=%d", reader.gets, state.PageIndex)
	}
}

func TestECSLogArchiveDiscoveryListingBudgetStillBounded(t *testing.T) {
	m := newECSLogTestMonitor(t)
	reader := &archiveReaderFixture{pages: map[string]ecsarchive.Page{}}
	token := ""
	for i := 0; i <= ecsArchiveDiscoveryPages; i++ {
		next := fmt.Sprint(i + 1)
		reader.pages[token] = ecsarchive.Page{Next: next}
		token = next
	}
	state := ECSLogArchiveScan{ID: 1, Binding: "listing-budget"}
	r := newECSArchiveReplayer(m, "isolated/")
	if err := r.scanPages(context.Background(), reader, &state, time.Now()); err != nil {
		t.Fatal(err)
	}
	if reader.lists != ecsArchiveDiscoveryPages || reader.gets != 0 || state.NextToken != fmt.Sprint(ecsArchiveDiscoveryPages) {
		t.Fatalf("unbounded listing or lost continuation: lists=%d gets=%d next=%q", reader.lists, reader.gets, state.NextToken)
	}
}
