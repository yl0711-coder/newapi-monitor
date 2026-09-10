package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func TestECSLogArchiveClosureRequiresWholeChainAndDoesNotCountRequests(t *testing.T) {
	m := newECSLogTestMonitor(t)
	m.cfg.ECSArchiveEnabled = true
	s, key := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
	object := func(e ecsarchive.Envelope) ecsarchive.Object {
		t.Helper()
		k, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32), e)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(e)
		return ecsarchive.Object{Key: k, ETag: "fixture", Modified: time.Now(), Body: raw}
	}
	first, err := ecsarchive.Seal(m.cfg.ECSLogAudience, s.Node, s.Lane, ecsLaneTestBody(t, s.Node, s.Lane, 1), key)
	if err != nil {
		t.Fatal(err)
	}
	closure, err := ecsarchive.SealClosure(m.cfg.ECSLogAudience, s.Node, s.Lane, first.Hash, key)
	if err != nil {
		t.Fatal(err)
	}
	f, c := object(first), object(closure)
	r := newECSArchiveReplayer(m, "isolated/")
	ctx := context.Background()
	if got := r.replay(ctx, c, time.Now()); got.Status != 425 {
		t.Fatalf("premature closure %+v", got)
	}
	if got := r.replay(ctx, f, time.Now()); !got.Accepted {
		t.Fatal(got)
	}
	if got := r.replay(ctx, c, time.Now()); !got.Accepted {
		t.Fatal(got)
	}
	if got := r.replay(ctx, c, time.Now()); !got.Accepted || !got.Duplicate {
		t.Fatal("closure retry", got)
	}
	if got := r.replay(ctx, f, time.Now()); !got.Accepted || !got.Duplicate {
		t.Fatal("ancestor retry", got)
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 1 || rows[0].Count != 1 {
		t.Fatal("closure counted as request", rows)
	}
	if err := m.storeDB.Model(&s).Updates(map[string]any{"stopped_at": time.Now().Unix(), "stop_code": "ServiceSchedulerInitiated"}).Error; err != nil {
		t.Fatal(err)
	}
	sources, _, err := m.ecsLogSnapshot(ctx)
	if err != nil || len(sources) != 1 || sources[0].ArchiveStatus != "collector_chain_closed" {
		t.Fatal("closure projection", sources, err)
	}
	if got := ecsLogSourceStatus(sources[0], time.Now().Unix()+ecsLogReplaySeconds+1); got != "stopped_archive_drained" {
		t.Fatal(got)
	}
	sources[0].StopCode = "EssentialContainerExited"
	if got := ecsLogSourceStatus(sources[0], time.Now().Unix()); got != "delivery_gap_unresolved" {
		t.Fatal("abnormal exit falsely complete", got)
	}
	lateBody := ecsLaneTestBody(t, s.Node, s.Lane, 2)
	if w := postSignedECS(m, s, key, lateBody, nil); w.Code != 409 {
		t.Fatal("live transport bypassed closed chain", w.Code)
	}
	late, err := ecsarchive.SealAfter(m.cfg.ECSLogAudience, s.Node, s.Lane, lateBody, closure.Hash, key)
	if err != nil {
		t.Fatal(err)
	}
	lateObj := object(late)
	got := r.replay(ctx, lateObj, time.Now())
	if got.Status != 409 {
		t.Fatal("post-closure data accepted", got)
	}
	if err := r.record(ctx, lateObj, got); err != nil {
		t.Fatal(err)
	}
	sources, _, err = m.ecsLogSnapshot(ctx)
	if err != nil || sources[0].ArchiveStatus != "archive_delivery_conflict" {
		t.Fatal("conflict hidden", sources, err)
	}
	h := m.projectECSLogHealth(sources, nil, time.Now().Unix())
	if h.DeliveryGaps != 1 || h.CoverageStatus != "unverified" {
		t.Fatal("coverage incorrectly certified", h)
	}
}

func TestECSLogArchiveClosureRejectsForkedHistory(t *testing.T) {
	m := newECSLogTestMonitor(t)
	s, key := registerECSTestSource(t, m, strings.Repeat("b", 32), "access")
	var head string
	r := newECSArchiveReplayer(m, "isolated/")
	makeObject := func(e ecsarchive.Envelope) ecsarchive.Object {
		k, _ := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("b", 32), e)
		raw, _ := json.Marshal(e)
		return ecsarchive.Object{Key: k, Body: raw, ETag: "fixture", Modified: time.Now()}
	}
	for n := 1; n <= 2; n++ {
		var payload nginxIngestRequest
		if err := json.Unmarshal(ecsLaneTestBody(t, s.Node, s.Lane, int64(n)), &payload); err != nil {
			t.Fatal(err)
		}
		payload.BatchID = fmt.Sprintf("fork-root-abcdefgh-%d", n)
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		e, err := ecsarchive.Seal(m.cfg.ECSLogAudience, s.Node, s.Lane, body, key)
		if err != nil {
			t.Fatal(err)
		}
		head = e.Hash
		if got := r.replay(context.Background(), makeObject(e), time.Now()); !got.Accepted {
			t.Fatal(got)
		}
	}
	e, err := ecsarchive.SealClosure(m.cfg.ECSLogAudience, s.Node, s.Lane, head, key)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.replay(context.Background(), makeObject(e), time.Now()); got.Status != 409 {
		t.Fatal("forked chain certified", got)
	}
}
