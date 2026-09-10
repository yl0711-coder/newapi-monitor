package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func archiveChainFixture(t *testing.T, m *Monitor, task string, count int) []ecsarchive.Object {
	t.Helper()
	s, key := registerECSTestSource(t, m, task, "reject")
	objects := make([]ecsarchive.Object, 0, count)
	previous := ""
	for i := 0; i < count; i++ {
		var body map[string]any
		if err := json.Unmarshal(ecsLaneTestBody(t, s.Node, s.Lane, 1), &body); err != nil {
			t.Fatal(err)
		}
		body["batch_id"] = fmt.Sprintf("archive-progress-%08d", i)
		raw, _ := json.Marshal(body)
		envelope, err := ecsarchive.SealAfter(m.cfg.ECSLogAudience, s.Node, s.Lane, raw, previous, key)
		if err != nil {
			t.Fatal(err)
		}
		objectKey, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+task, envelope)
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(envelope)
		objects = append(objects, ecsarchive.Object{Key: objectKey, ETag: "fixture-" + envelope.Hash, Modified: time.Now(), Body: encoded})
		previous = envelope.Hash
	}
	return objects
}

func pagedArchiveFixture(objects []ecsarchive.Object) *archiveReaderFixture {
	r := &archiveReaderFixture{objects: map[string]ecsarchive.Object{}, pages: map[string]ecsarchive.Page{}}
	for start := 0; start < len(objects); start += ecsarchive.PageSize {
		page := ecsarchive.Page{}
		for _, o := range objects[start:min(start+ecsarchive.PageSize, len(objects))] {
			r.objects[o.Key] = o
			page.Entries = append(page.Entries, archiveEntry(o))
		}
		token := ""
		if start > 0 {
			token = fmt.Sprint(start)
		}
		if start+ecsarchive.PageSize < len(objects) {
			page.Next = fmt.Sprint(start + ecsarchive.PageSize)
		}
		r.pages[token] = page
	}
	return r
}

func TestECSLogArchiveInterruptedPagePersistsAndResumes(t *testing.T) {
	m := newECSLogTestMonitor(t)
	m.cfg.ECSArchiveEnabled = true
	objects := archiveChainFixture(t, m, strings.Repeat("a", 32), 8)
	reader := pagedArchiveFixture(objects)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader.cancelAt, reader.cancel = 3, cancel
	r := newECSArchiveReplayer(m, "isolated/")
	if err := r.scan(ctx, reader, "binding", time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("interruption hidden: %v", err)
	}
	var checkpoint ECSLogArchiveScan
	if err := m.storeDB.First(&checkpoint, 1).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.PageIndex != 2 || checkpoint.PageJSON == "" || checkpoint.LastFailure == 0 || checkpoint.LastError == "" {
		t.Fatalf("checkpoint/diagnostic missing: %+v", checkpoint)
	}
	if h := m.ecsArchiveHealth(context.Background(), time.Now().Unix()); h.Status != "scan_failed" || h.LastFailure == 0 {
		t.Fatalf("cancelled scan shown healthy: %+v", h)
	}
	// New replayer has no in-memory progress; persisted page must be reused even
	// when subsequent listing fails or changes due to an active ECS task.
	reader.failList = true
	r = newECSArchiveReplayer(m, "isolated/")
	if err := r.scan(context.Background(), reader, "binding", time.Now()); err != nil {
		t.Fatal(err)
	}
	if reader.lists != 1 || reader.reads[objects[0].Key] != 1 || reader.reads[objects[1].Key] != 1 {
		t.Fatal("completed portion was reread/relisted")
	}
	if rows := m.storeRejections(time.Now().Add(-time.Minute).Unix()); len(rows) != 1 || rows[0].Count != 8 {
		t.Fatalf("resumed facts: %+v", rows)
	}
}

func TestECSLogArchiveLongReversedChainsBoundedAndFair(t *testing.T) {
	m := newECSLogTestMonitor(t)
	first := archiveChainFixture(t, m, strings.Repeat("a", 32), 140)
	second := archiveChainFixture(t, m, strings.Repeat("b", 32), 60)
	slices.Reverse(first)
	slices.Reverse(second)
	reader := pagedArchiveFixture(append(first, second...))
	r := newECSArchiveReplayer(m, "isolated/")
	for turn := 0; turn < 25; turn++ {
		before := reader.gets
		if err := r.scan(context.Background(), reader, "binding", time.Now()); err != nil {
			t.Fatal(err)
		}
		if reader.gets-before > 2*ecsArchivePhaseEntries {
			t.Fatal("unbounded GET fanout")
		}
	}
	var accepted int64
	if err := m.storeDB.Model(&ECSLogArchiveReceipt{}).Where("status = 200").Count(&accepted).Error; err != nil || accepted != 200 {
		t.Fatalf("chain starvation: accepted %d error %v", accepted, err)
	}
	var rows []struct {
		Node  string
		Count int64
	}
	if err := m.storeDB.Model(&RejectionSample{}).Select("node, SUM(count) AS count").Group("node").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, row := range rows {
		total += row.Count
	}
	if len(rows) != 2 || total != 200 {
		t.Fatalf("cross-source facts wrong: %+v", rows)
	}
	// A known blocked batch is not fetched every sweep. Completed receipts also
	// remain deduplicated across new replayers and repeated listings.
	if reader.gets > 400 {
		t.Fatalf("blocked batches repeatedly downloaded: %d", reader.gets)
	}
	gets := reader.gets
	for range 10 {
		if err := newECSArchiveReplayer(m, "isolated/").scan(context.Background(), reader, "binding", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if reader.gets != gets {
		t.Fatal("completed objects redownloaded")
	}
}

func TestECSLogArchiveRetryCannotBypassRevocation(t *testing.T) {
	m := newECSLogTestMonitor(t)
	chain := archiveChainFixture(t, m, strings.Repeat("c", 32), 2)
	r := newECSArchiveReplayer(m, "isolated/")
	reader := pagedArchiveFixture(chain)
	if err := r.scanEntry(context.Background(), reader, archiveEntry(chain[1]), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := r.scanEntry(context.Background(), reader, archiveEntry(chain[0]), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ECSLogSource{}).Where("lane = ?", "reject").Update("revoked", true).Error; err != nil {
		t.Fatal(err)
	}
	if err := r.retryReady(context.Background(), reader, time.Now()); err != nil {
		t.Fatal(err)
	}
	var receipt ECSLogArchiveReceipt
	if err := m.storeDB.First(&receipt, "object_key = ?", chain[1].Key).Error; err != nil || receipt.Status != 403 {
		t.Fatalf("revoked pending batch admitted: %+v %v", receipt, err)
	}
}
