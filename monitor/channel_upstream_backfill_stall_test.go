package monitor

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A slow source where each whole-hour page uses most of the lane budget:
// every resume re-reads page 1 and then yields on page 2, so the parent cursor
// never advances. After the stall window the hour must switch to durable
// five-minute children instead of looping forever without an error.
func TestNewAPIHistoryStalledParentCheckpointSwitchesToSegments(t *testing.T) {
	from := time.Date(2026, 11, 1, 1, 0, 0, 0, cstLocation).Unix()
	rows := usageFixtureRows(366, from, 3600)
	server, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31, BalanceUnit: 500000,
	}
	credential := newAPICredential{AccessToken: "usage-token"}
	countSegments := func() int64 {
		t.Helper()
		var n int64
		if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ?", row.Domain, from).Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Round 1: only page 1 fits in the budget.
	if _, _, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(1, 0)); err != nil || complete {
		t.Fatalf("round 1 complete=%v err=%v", complete, err)
	}
	var checkpoint NewAPIUsageBackfillCheckpoint
	if err := m.storeDB.First(&checkpoint, "domain = ?", row.Domain).Error; err != nil || checkpoint.NextPage != 2 {
		t.Fatalf("parent checkpoint=%+v err=%v", checkpoint, err)
	}

	// Round 2 (fresh checkpoint): the re-verification probe consumes the
	// budget again. A recent cursor is not a stall, so topology is unchanged.
	if _, _, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(1, 0)); err != nil || complete {
		t.Fatalf("round 2 complete=%v err=%v", complete, err)
	}
	if n := countSegments(); n != 0 {
		t.Fatalf("fresh parent cursor was split: segments=%d", n)
	}

	// The cursor has not advanced for longer than the stall window, but the
	// lane did not run in between (paused): not a stall, keep the parent cursor.
	stale := time.Now().Add(-newAPIBackfillParentStallAfter - time.Minute).Unix()
	if err := m.storeDB.Model(&NewAPIUsageBackfillCheckpoint{}).Where("domain = ?", row.Domain).Update("updated_at", stale).Error; err != nil {
		t.Fatal(err)
	}
	row.UsageBackfillLastAttemptAt = stale
	if _, _, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(1, 0)); err != nil || complete {
		t.Fatalf("paused-lane round complete=%v err=%v", complete, err)
	}
	if n := countSegments(); n != 0 {
		t.Fatalf("paused lane was treated as a stalled cursor: segments=%d", n)
	}
	if err := m.storeDB.Model(&NewAPIUsageBackfillCheckpoint{}).Where("domain = ?", row.Domain).Update("updated_at", stale).Error; err != nil {
		t.Fatal(err)
	}

	// The lane kept attempting for the whole window without the cursor moving.
	row.UsageBackfillLastAttemptAt = time.Now().Unix() - 60
	_, progress, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0))
	if err != nil || complete || !strings.Contains(progress, "慢查询降档") {
		t.Fatalf("stalled round complete=%v progress=%q err=%v", complete, progress, err)
	}
	if n := countSegments(); n != 12 {
		t.Fatalf("stalled parent segments=%d, want 12", n)
	}
	var done int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ? AND status = ?", row.Domain, from, newAPIBackfillSegmentComplete).Count(&done).Error; err != nil || done == 0 {
		t.Fatalf("segmented round made no durable progress: done=%d err=%v", done, err)
	}
	var parent int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillCheckpoint{}).Where("domain = ? AND window_from = ? AND window_to = ?", row.Domain, from, from+3600).Count(&parent).Error; err != nil || parent != 0 {
		t.Fatalf("stale parent accumulation kept: rows=%d err=%v", parent, err)
	}

	result, progress, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0))
	if err != nil || !complete || progress != "" {
		t.Fatalf("final round complete=%v progress=%q err=%v", complete, progress, err)
	}
	if len(result.Hours) != 1 || result.Hours[0].Requests != int64(len(rows)) {
		t.Fatalf("segmented aggregate=%+v", result.Hours)
	}
}

func TestNewAPIBackfillParentStalledBoundaries(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	from := int64(1_799_996_400)
	stall := int64(newAPIBackfillParentStallAfter / time.Second)
	cp := NewAPIUsageBackfillCheckpoint{WindowFrom: from, WindowTo: from + 3600, UpdatedAt: now.Unix() - stall}
	attempt := now.Unix()
	if !newAPIBackfillParentStalled(true, cp, from, from+3600, attempt, now) {
		t.Fatal("stall window boundary not detected")
	}
	if newAPIBackfillParentStalled(true, cp, from, from+3600, cp.UpdatedAt+stall-1, now) {
		t.Fatal("lane that did not keep attempting treated as stalled")
	}
	cp.UpdatedAt++
	if newAPIBackfillParentStalled(true, cp, from, from+3600, attempt, now) {
		t.Fatal("recent cursor treated as stalled")
	}
	cp.UpdatedAt = 1
	if newAPIBackfillParentStalled(false, cp, from, from+3600, attempt, now) {
		t.Fatal("missing checkpoint treated as stalled")
	}
	if newAPIBackfillParentStalled(true, cp, from+3600, from+7200, attempt, now) {
		t.Fatal("checkpoint of another hour treated as stalled")
	}
	cp.UpdatedAt = 0
	if newAPIBackfillParentStalled(true, cp, from, from+3600, attempt, now) {
		t.Fatal("unset timestamp treated as stalled")
	}
}
