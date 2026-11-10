package monitor

import (
	"context"
	"testing"
	"time"
)

func TestPreRouteLiveCadenceRetainsReplayAndContinuousWatermark(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	now := time.Now().Truncate(time.Minute)
	_, target := cloudWatchPreRouteRange(now, 168)
	s := CloudWatchPreRouteCursor{ID: 1, CoverageFromTs: target - 7200, NextTs: target - 300, ThroughTs: target - 300, Status: "caught_up", SemanticsVersion: cloudWatchPreRouteVersion}
	nextReplay := now.Add(3 * time.Minute)
	for i := 0; i < 3; i++ {
		from, to, replay := cloudWatchPreRouteNextWindow(s, target+int64(i*60), now.Add(time.Duration(i)*time.Minute), nextReplay)
		if replay || from != s.ThroughTs || to != target+int64(i*60) {
			t.Fatalf("live gap: %d %d %t state=%+v", from, to, replay, s)
		}
		if err := m.publishCloudWatchPreRouteWindow(context.Background(), &s, from, to, nil, to); err != nil {
			t.Fatal(err)
		}
	}
	from, to, replay := cloudWatchPreRouteNextWindow(s, target+180, nextReplay, nextReplay)
	if !replay || from != s.ThroughTs-1200 {
		t.Fatalf("late arrival replay lost: %d %d %t", from, to, replay)
	}
	// Query failure does not publish/advance, and the failed replay remains due.
	previous := s.ThroughTs
	s.Status = "degraded"
	retryFrom, _, again := cloudWatchPreRouteNextWindow(s, target+240, nextReplay.Add(time.Minute), nextReplay)
	if !again || retryFrom != from || s.ThroughTs != previous {
		t.Fatal("failed replay skipped")
	}
	late := []RejectionSample{{Node: cloudWatchPreRouteNode, BucketTs: from + 60, Count: 3}}
	if err := m.publishCloudWatchPreRouteWindow(context.Background(), &s, from, to, late, to); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := m.storeDB.Model(&RejectionSample{}).Where("node=? AND bucket_ts=?", cloudWatchPreRouteNode, from+60).Count(&n).Error; err != nil || n != 1 {
		t.Fatalf("late data not retained n=%d err=%v", n, err)
	}
	// Restart has no in-memory replay deadline and must replay immediately.
	_, _, replay = cloudWatchPreRouteNextWindow(s, to+60, nextReplay.Add(time.Minute), time.Time{})
	if !replay {
		t.Fatal("restart omitted replay")
	}
}

func TestSourceCadenceDoesNotAddQueryDurationToEveryCycle(t *testing.T) {
	start := time.Unix(1000, 0)
	for _, tt := range []struct{ work, want time.Duration }{{8 * time.Second, 52 * time.Second}, {59 * time.Second, 2 * time.Second}, {90 * time.Second, 2 * time.Second}} {
		if got := sourceCadenceDelay(start, start.Add(tt.work), time.Minute); got != tt.want {
			t.Fatalf("work=%v got=%v want=%v", tt.work, got, tt.want)
		}
	}
}
