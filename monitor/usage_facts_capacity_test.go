package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestUsageFactDiskPressureTiers(t *testing.T) {
	const gib = int64(1024 * 1024 * 1024)
	tests := []struct {
		name        string
		total, free int64
		want        usageFactDiskPressureLevel
	}{
		{name: "below warning", total: 100 * gib, free: 41 * gib, want: usageFactDiskNormal},
		{name: "60 percent warning", total: 100 * gib, free: 40 * gib, want: usageFactDiskWarning},
		{name: "70 percent throttle", total: 100 * gib, free: 30 * gib, want: usageFactDiskThrottled},
		{name: "80 percent blocks cold", total: 100 * gib, free: 20 * gib, want: usageFactDiskColdBlocked},
		{name: "85 percent stops derived writes", total: 100 * gib, free: 15 * gib, want: usageFactDiskCritical},
		{name: "absolute warning reserve", total: 5 * gib, free: 3 * gib, want: usageFactDiskWarning},
		{name: "absolute cold reserve", total: 5 * gib, free: 2*gib - 1, want: usageFactDiskColdBlocked},
		{name: "absolute critical reserve", total: 5 * gib, free: gib - 1, want: usageFactDiskCritical},
		{name: "invalid filesystem", total: 0, free: 0, want: usageFactDiskCritical},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := usageFactDiskPressure(tt.total, tt.free, 80, 2*gib); got != tt.want {
				t.Fatalf("pressure=%s want %s", got, tt.want)
			}
		})
	}
}

func TestReadyStatusExposesDiskPressureWithoutForcingRestart(t *testing.T) {
	m := &Monitor{cfg: Settings{UsageFactsEnabled: true}}
	m.localStoreProbeOK.Store(true)
	m.storeIntegrityOK.Store(true)
	m.localFactsProbeOK.Store(true)
	m.usageFactsIntegrityOK.Store(true)
	m.usageFactsHistoryDiskLevel.Store(int64(usageFactDiskCritical))
	m.usageFactsHistoryDiskFreeBytes.Store(512 * 1024 * 1024)
	m.usageFactsHistoryDiskUsedBPS.Store(8_700)
	m.processStartedAt.Store(1)
	status, code := m.readyStatus(time.Unix(100, 0))
	if code != 200 || status.Status != "degraded" {
		t.Fatalf("critical disk must preserve old-snapshot serving: code=%d status=%s", code, status.Status)
	}
	if status.FactsDisk.Pressure != "critical" || status.FactsDisk.UsedPercent != 87 {
		t.Fatalf("disk status=%+v", status.FactsDisk)
	}
	found := false
	for _, reason := range status.DegradedReasons {
		found = found || reason == "facts_disk_critical"
	}
	if !found {
		t.Fatalf("reasons=%v", status.DegradedReasons)
	}
}

func TestCriticalOrUnreadableFactsVolumeStopsIndependentDerivedWriters(t *testing.T) {
	m := &Monitor{cfg: Settings{
		UsageFactsEnabled: true, UsageFactsFullHistoryEnabled: true,
		StorePath: filepath.Join(t.TempDir(), "missing", "monitor.db"),
	}}

	if err := m.syncUsageFactsTail(context.Background(), time.Now()); !errors.Is(err, errUsageFactHistoryDiskPressure) {
		t.Fatalf("high-priority Tail crossed an unreadable critical volume: %v", err)
	}
	if level := usageFactDiskPressureLevel(m.usageFactsHistoryDiskLevel.Load()); level != usageFactDiskCritical {
		t.Fatalf("unreadable capacity was not fail-closed: %s", level)
	}
	if err := m.syncUsageProfiles(context.Background(), time.Now()); !errors.Is(err, errUsageFactHistoryDiskPressure) {
		t.Fatalf("profile snapshot crossed an unreadable critical volume: %v", err)
	}
}

func TestLocalReadinessProbeSamplesFactsCapacityBeforeWorker(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsFullHistoryEnabled = true
	m.cfg.UsageFactsStorePath = filepath.Join(t.TempDir(), "missing", "facts.db")
	m.usageFactsHistoryDiskLevel.Store(int64(usageFactDiskNormal))

	m.probeLocalStores(context.Background())

	if level := usageFactDiskPressureLevel(m.usageFactsHistoryDiskLevel.Load()); level != usageFactDiskCritical {
		t.Fatalf("startup readiness probe did not fail closed on an unreadable facts mount: %s", level)
	}
	status, code := m.readyStatus(time.Now())
	if code != 200 || status.FactsDisk.Pressure != "critical" {
		t.Fatalf("startup disk pressure was not exposed by /ready: code=%d status=%+v", code, status)
	}
}
