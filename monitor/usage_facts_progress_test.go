package monitor

import (
	"math"
	"testing"
	"time"
)

func TestUsageFactHistoryObservedETARequiresDurableSample(t *testing.T) {
	now := time.Unix(10_000, 0)
	if eta, status, rate, sample := usageFactHistoryObservedETA(10, 100, now.Unix()-299, now.Unix(), now); eta != nil || status != "warming" || rate != 0 || sample != 299 {
		t.Fatalf("short sample must stay warming: eta=%v status=%q rate=%v sample=%d", eta, status, rate, sample)
	}
	if eta, status, _, _ := usageFactHistoryObservedETA(10, 100, now.Unix()-600, now.Unix()-3601, now); eta != nil || status != "stalled" {
		t.Fatalf("stale progress must not produce a fake ETA: eta=%v status=%q", eta, status)
	}
}

func TestUsageFactHistoryObservedETAUsesPersistedProgress(t *testing.T) {
	now := time.Unix(20_000, 0)
	eta, status, rate, sample := usageFactHistoryObservedETA(50, 200, now.Unix()-600, now.Unix()-30, now)
	if eta == nil || *eta != 1800 || status != "observed" || sample != 600 {
		t.Fatalf("unexpected observed estimate: eta=%v status=%q sample=%d", eta, status, sample)
	}
	if math.Abs(rate-(50.0/600.0)) > 1e-12 {
		t.Fatalf("unexpected durable throughput: %v", rate)
	}
}
