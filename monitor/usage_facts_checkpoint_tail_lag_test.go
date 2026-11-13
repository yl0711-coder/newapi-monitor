package monitor

import (
	"context"
	"testing"
)

// The publisher advances the global right edge from per-hour proofs and may
// keep a member whose Tail job cursor is briefly behind. The post-publish
// checkpoint must apply the same rule; requiring the job cursor closed the
// whole user-usage page right after such a publish ("checkpoint incomplete").
func TestPublishedCheckpointAcceptsTailCursorLagWithCompleteHourProofs(t *testing.T) {
	m, _, published := newUsageServingAvailabilityFixture(t)
	through := published.PublishedThrough
	day := usageFactDayStart(through)
	floor := day - usageFactDaySeconds
	db := m.usageFactsStore()
	// Promote both members to the full-history signature path.
	for _, id := range []int64{88, 152} {
		if err := db.Model(&UsageFactMemberState{}).Where("user_id = ?", id).Updates(map[string]any{
			"verified_through_hour": day, "tail_through_hour": through,
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Model(&UsageFactPublishedMember{}).Where("user_id = ?", id).Updates(map[string]any{
			"source_floor_hour": floor, "verified_through_hour": day,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Model(&UsageFactSyncState{}).Where("id = 1").Update("published_range_start", floor).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := m.validateUsageFactFullHistoryCheckpoint(ctx, through); err != nil {
		t.Fatalf("baseline full-history checkpoint failed: %v", err)
	}

	// Tail job cursor one hour behind, hour proofs complete: must stay readable.
	if err := db.Model(&UsageFactMemberState{}).Where("user_id = ?", 88).Update("tail_through_hour", through-usageFactHourSeconds).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.validateUsageFactFullHistoryCheckpoint(ctx, through); err != nil {
		t.Fatalf("tail cursor lag with complete hour proofs closed the read gate: %v", err)
	}

	// A real missing hour proof still fails closed.
	last := through - usageFactHourSeconds
	if err := db.Where("user_id = ? AND hour_ts = ?", 88, last).Delete(&UsageFactMemberHourState{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.validateUsageFactFullHistoryCheckpoint(ctx, through); err == nil {
		t.Fatal("missing hour proof was accepted")
	}

	// Restore the proof but corrupt the fact behind it: content audit fails.
	if err := db.Model(&UsageFactMemberState{}).Where("user_id = ?", 88).Update("tail_through_hour", through).Error; err != nil {
		t.Fatal(err)
	}
	row := UsageHourFact{HourTs: last, DayTs: day, UserID: 88, ChannelID: 1, Grp: "g1", ModelName: "m1", Requests: 1, PromptTokens: 10, CompletionTokens: 2, ConsumeQuota: 100}
	if err := db.Create(&UsageFactMemberHourState{
		UserID: 88, HourTs: last, Status: "complete", Rows: 1, Requests: 1, Tokens: 12,
		ContentHash: usageFactContentHash([]UsageHourFact{row}), SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.validateUsageFactFullHistoryCheckpoint(ctx, through); err != nil {
		t.Fatalf("restored proof not accepted: %v", err)
	}
	if err := db.Model(&UsageHourFact{}).Where("user_id = ? AND hour_ts = ?", 88, last).Update("consume_quota", 999).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.validateUsageFactFullHistoryCheckpoint(ctx, through); err == nil {
		t.Fatal("tampered fact behind a complete proof was accepted")
	}
}
