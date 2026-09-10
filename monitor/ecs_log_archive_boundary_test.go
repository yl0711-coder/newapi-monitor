package monitor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func TestECSLogEmptyClosureNeverCertifiesRequestCoverage(t *testing.T) {
	m := newECSLogTestMonitor(t)
	m.cfg.ECSArchiveEnabled = true
	s, key := registerECSTestSource(t, m, strings.Repeat("c", 32), "reject")
	e, err := ecsarchive.SealClosure(m.cfg.ECSLogAudience, s.Node, s.Lane, "", key)
	if err != nil {
		t.Fatal(err)
	}
	objectKey, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("c", 32), e)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	r := newECSArchiveReplayer(m, "isolated/")
	o := ecsarchive.Object{Key: objectKey, ETag: "fixture", Modified: time.Now(), Body: raw}
	if result := r.replay(context.Background(), o, time.Now()); !result.Accepted {
		t.Fatal(result)
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 0 {
		t.Fatal("empty closure invented usage", rows)
	}
	sources, _, err := m.ecsLogSnapshot(context.Background())
	if err != nil || sources[0].ArchiveStatus != "collector_chain_closed" {
		t.Fatal("missing empty-chain evidence", err)
	}
	for _, code := range []string{"ServiceSchedulerInitiated", "EssentialContainerExited", "", "TaskFailedToStart"} {
		sources[0].StoppedAt, sources[0].StopCode = time.Now().Unix(), code
		health := m.projectECSLogHealth(sources, nil, time.Now().Unix())
		if health.CoverageStatus != "unverified" {
			t.Fatal("collector proof became business proof", health)
		}
		if code != "ServiceSchedulerInitiated" && health.DeliveryGaps != 1 {
			t.Fatal("abnormal or unknown stop code hidden", code, health)
		}
	}
}

func TestECSLogStoppedWithoutClosureStillExpiresToGap(t *testing.T) {
	now := time.Now().Unix()
	source := ECSLogSource{StoppedAt: now - ecsLogReplaySeconds - 1, StopCode: "ServiceSchedulerInitiated", ArchiveStatus: "awaiting_closure"}
	if status := ecsLogSourceStatus(source, now); status != "delivery_gap_unresolved" {
		t.Fatal("missing final proof hidden", status)
	}
	source.StoppedAt, source.ArchiveStatus = 0, "archive_delivery_conflict"
	if status := ecsLogSourceStatus(source, now); status != "archive_delivery_conflict" {
		t.Fatal("active archive conflict hidden", status)
	}
}
