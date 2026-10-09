package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Use the real publisher and read fence: a healthy second member must remain
// readable while the first member's already-published hour is refreshed.
func newUsageServingAvailabilityFixture(t *testing.T) (*Monitor, time.Time, UsageFactSyncState) {
	t.Helper()
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsReadEnabled = true
	m.cfg.UsageFactsRawPageImportEnabled = true
	now := time.Date(2026, 8, 18, 2, 20, 0, 0, usageCST)
	through := m.usageFactFinalizedHour(now)
	from := usageFactDayStart(through)
	for _, id := range []int64{88, 152} {
		prepareUsageHistoryCommitMember(t, m, id, 1)
		if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", id).Updates(map[string]any{
			"source_floor_hour": from - usageFactDaySeconds, "source_first_log_hour": from - usageFactDaySeconds,
			"source_floor_checked_at": now.Unix(), "source_history_status": "complete_hot",
			"classification_version": userTrafficClassificationVersion, "query_semantics_version": usageFactQuerySemanticsVersion,
			"source_epoch":   m.cfg.UsageFactsHistorySourceEpoch,
			"live_from_hour": from, "live_through_hour": through, "live_target_hour": through,
			"live_status": "ready", "live_last_success_at": now.Unix(),
		}).Error; err != nil {
			t.Fatal(err)
		}
		for hour := from; hour < through; hour += usageFactHourSeconds {
			row := UsageHourFact{HourTs: hour, DayTs: from, UserID: id, ChannelID: 1,
				Grp: "g1", ModelName: "m1", Requests: 1, PromptTokens: 10, CompletionTokens: 2, ConsumeQuota: 100}
			if err := m.usageFactsStore().Create(&row).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.usageFactsStore().Create(&UsageFactMemberHourState{
				UserID: id, HourTs: hour, Status: "complete", Rows: 1, Requests: 1, Tokens: 12,
				ContentHash: usageFactContentHash([]UsageHourFact{row}), SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
				CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
			}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	published, err := m.publishUsageFactFullHistorySnapshot(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if published.PublishedRangeStart != from || published.PublishedThrough != through {
		t.Fatalf("fixture did not publish expected window: %+v", published)
	}
	if _, err := m.loadUsageFactServingReadSnapshot(context.Background()); err != nil {
		t.Fatalf("fixture must start readable: %v", err)
	}
	return m, now, published
}

func TestUsageServingAvailabilityDuringHourRefresh(t *testing.T) {
	m, now, published := newUsageServingAvailabilityFixture(t)
	hour := published.PublishedThrough - usageFactHourSeconds
	claim, err := m.claimUsageFactHour(hour, []int64{88})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.failUsageFactHourClaim(hour, []int64{88}, claim, context.Canceled, false); err != nil {
			t.Error(err)
		}
	}()
	if _, err := m.claimUsageFactHour(hour, []int64{88}); !errors.Is(err, errUsageFactLeaseBusy) {
		t.Fatalf("refresh lease must still exclude a second writer: %v", err)
	}
	m.refreshUsageFactsFullHistoryReadiness(context.Background(), now, UsageFactsStatus{
		SnapshotUsable: true, ReadEnabled: true,
		PublishedRangeStart: published.PublishedRangeStart, PublishedThrough: published.PublishedThrough,
	})
	router := gin.New()
	router.GET("/usage/stats", m.usageAggregateAuthorizationGuard(func(c *gin.Context) {
		stats, err := m.computeUsageStatsFromFacts(c.Request.Context(), []int64{152}, published.PublishedRangeStart, published.PublishedThrough, 0)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if stats.Summary.Requests != 2 || stats.Summary.ConsumeQuota != 200 || stats.Summary.Tokens != 24 {
			t.Errorf("healthy member totals changed during refresh: %+v", stats.Summary)
		}
		c.JSON(http.StatusOK, stats)
	}))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/usage/stats", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("background refresh took healthy member offline: status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestUsageServingRefreshFailureRetainsCommittedProof(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errUsageFactSourceBusy} {
		t.Run(cause.Error(), func(t *testing.T) {
			m, _, published := newUsageServingAvailabilityFixture(t)
			hour := published.PublishedThrough - usageFactHourSeconds
			claim, err := m.claimUsageFactHour(hour, []int64{88})
			if err != nil {
				t.Fatal(err)
			}
			if err := m.failUsageFactHourClaim(hour, []int64{88}, claim, cause, true); err != nil {
				t.Fatal(err)
			}
			var proof UsageFactMemberHourState
			if err := m.usageFactsStore().First(&proof, "user_id = ? AND hour_ts = ?", 88, hour).Error; err != nil {
				t.Fatal(err)
			}
			if proof.Status != "complete" || proof.LeaseToken != "" || proof.LeaseUntil != 0 ||
				proof.ContentHash != claim.previous[88].ContentHash || proof.CompletedAt != claim.previous[88].CompletedAt {
				t.Fatalf("failed refresh lost the old proof or retained its lease: %+v", proof)
			}
			if err := m.validateUsageFactFullHistoryCheckpoint(context.Background(), published.PublishedThrough); err != nil {
				t.Fatalf("failed refresh invalidated committed data: %v", err)
			}
		})
	}
}

func TestUsageServingRefreshExpiredOwnerCannotClobberNewLease(t *testing.T) {
	m, _, published := newUsageServingAvailabilityFixture(t)
	hour := published.PublishedThrough - usageFactHourSeconds
	first, err := m.claimUsageFactHour(hour, []int64{88})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactMemberHourState{}).Where("user_id = ? AND hour_ts = ?", 88, hour).
		Update("lease_until", time.Now().Add(-time.Minute).Unix()).Error; err != nil {
		t.Fatal(err)
	}
	second, err := m.claimUsageFactHour(hour, []int64{88})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.failUsageFactHourClaim(hour, []int64{88}, first, context.Canceled, false); err != nil {
		t.Fatal(err)
	}
	var current UsageFactMemberHourState
	if err := m.usageFactsStore().First(&current, "user_id = ? AND hour_ts = ?", 88, hour).Error; err != nil {
		t.Fatal(err)
	}
	if current.Status != "running" || current.LeaseToken != second.leaseToken {
		t.Fatalf("expired owner changed the new owner's proof/lease: %+v", current)
	}
	if err := m.failUsageFactHourClaim(hour, []int64{88}, second, context.Canceled, false); err != nil {
		t.Fatal(err)
	}
	if err := m.validateUsageFactFullHistoryCheckpoint(context.Background(), published.PublishedThrough); err != nil {
		t.Fatalf("cancelled successor lost the last committed proof: %v", err)
	}
}

func TestUsageServingRefreshStillFailsClosed(t *testing.T) {
	for _, damage := range []string{"content", "missing_proof", "old_epoch", "membership", "repair_hold"} {
		t.Run(damage, func(t *testing.T) {
			m, now, published := newUsageServingAvailabilityFixture(t)
			hour := published.PublishedThrough - usageFactHourSeconds
			if _, err := m.claimUsageFactHour(hour, []int64{88}); err != nil {
				t.Fatal(err)
			}
			db := m.usageFactsStore()
			var err error
			switch damage {
			case "content":
				err = db.Model(&UsageHourFact{}).Where("user_id = ? AND hour_ts = ?", 88, hour).Update("consume_quota", 999).Error
			case "missing_proof":
				err = db.Where("user_id = ? AND hour_ts = ?", 88, hour).Delete(&UsageFactMemberHourState{}).Error
			case "old_epoch":
				err = db.Model(&UsageFactMemberHourState{}).Where("user_id = ? AND hour_ts = ?", 88, hour).Update("source_epoch", "old-source").Error
			case "membership":
				err = m.storeDB.Model(&UsageMemberControl{}).Where("user_id = ?", 88).Update("active", false).Error
			case "repair_hold":
				id := int64(88)
				err = db.Create(&UsageFactJob{ID: "test-pending-repair", UserID: &id, Kind: usageFactHistoryKindLocalAudit,
					Status: usageFactHistoryJobQueued, LastError: usageFactAuditRepairHoldPendingPrefix + "test"}).Error
			}
			if err != nil {
				t.Fatal(err)
			}
			m.refreshUsageFactsFullHistoryReadiness(context.Background(), now, UsageFactsStatus{
				SnapshotUsable: true, ReadEnabled: true,
				PublishedRangeStart: published.PublishedRangeStart, PublishedThrough: published.PublishedThrough,
			})
			if _, err := m.loadUsageFactServingReadSnapshot(context.Background()); err == nil {
				t.Fatal("integrity/authority failure was hidden by refresh tolerance")
			}
		})
	}
}

func TestUsageServingRefreshCommitsUpdatedFactsAndGeneration(t *testing.T) {
	m, _, published := newUsageServingAvailabilityFixture(t)
	hour := published.PublishedThrough - usageFactHourSeconds
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,"group",token_id,token_name,other)
VALUES (1,88,1,?,2,'m1',250,20,4,'g1',0,'','')`, hour+60); err != nil {
		t.Fatal(err)
	}
	result, err := m.syncUsageFactHourWithOptions(context.Background(), hour, []int64{88}, usageFactHourSyncOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed || !result.HadPriorFingerprint {
		t.Fatalf("refresh must atomically replace the previous fact: %+v", result)
	}
	snapshot, err := m.loadUsageFactServingReadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var global UsageFactSyncState
	if err := m.usageFactsStore().First(&global, 1).Error; err != nil {
		t.Fatal(err)
	}
	if global.ServingGeneration <= published.ServingGeneration {
		t.Fatal("changed published facts did not invalidate serving caches")
	}
	stats, err := m.computeUsageStatsFromFacts(context.Background(), []int64{88}, published.PublishedRangeStart, published.PublishedThrough, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Summary.ConsumeQuota != 350 || stats.Summary.Tokens != 36 || stats.Summary.Requests != 2 {
		t.Fatalf("new and old facts were mixed or duplicated: %+v", stats.Summary)
	}
	if err := m.validateUsageFactFullHistoryCheckpoint(context.Background(), published.PublishedThrough); err != nil {
		t.Fatal(err)
	}
	result, err = m.syncUsageFactHourWithOptions(context.Background(), hour, []int64{88}, usageFactHourSyncOptions{})
	if err != nil || result.Changed {
		t.Fatalf("identical refresh should be idempotent: %+v err=%v", result, err)
	}
	after, err := m.loadUsageFactServingReadSnapshot(context.Background())
	if err != nil || !snapshot.equal(after) {
		t.Fatalf("identical refresh changed the serving snapshot: %v", err)
	}
}

func TestUsageServingRefreshOnlyPreservesValidCompletedHours(t *testing.T) {
	for _, kind := range []string{"empty_complete", "new", "incomplete", "damaged", "old_epoch"} {
		t.Run(kind, func(t *testing.T) {
			m := newUsageHistoryTestMonitor(t)
			hour := usageFactTestDay().Unix()
			proof := UsageFactMemberHourState{UserID: 88, HourTs: hour, Status: "complete",
				ContentHash: usageFactContentHash(nil), CompletedAt: time.Now().Unix(), SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch}
			switch kind {
			case "incomplete":
				proof.Status, proof.CompletedAt = "failed", 0
			case "damaged":
				proof.ContentHash = "invalid-hash"
			case "old_epoch":
				proof.SourceEpoch = "old-source"
			}
			if kind != "new" {
				if err := m.usageFactsStore().Create(&proof).Error; err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.claimUsageFactHour(hour, []int64{88}); err != nil {
				t.Fatal(err)
			}
			var current UsageFactMemberHourState
			if err := m.usageFactsStore().First(&current, "user_id = ? AND hour_ts = ?", 88, hour).Error; err != nil {
				t.Fatal(err)
			}
			if current.Status != "running" || current.LeaseToken == "" {
				t.Fatalf("writer must retain its running lease: %+v", current)
			}
			err := auditUsageFactTrailingHoursForEpochWithRefresh(m.usageFactsStore(), hour, hour+usageFactHourSeconds,
				[]int64{88}, m.cfg.UsageFactsHistorySourceEpoch, true)
			if (err == nil) != (kind == "empty_complete") {
				t.Fatalf("refresh proof eligibility incorrect for %s: %v", kind, err)
			}
			if err := auditUsageFactTrailingHoursForEpoch(m.usageFactsStore(), hour, hour+usageFactHourSeconds,
				[]int64{88}, m.cfg.UsageFactsHistorySourceEpoch); err == nil {
				t.Fatal("a running refresh must not qualify as a completed new candidate")
			}
		})
	}
}

func TestUsageServingCheckpointRefreshBoundaries(t *testing.T) {
	for _, kind := range []string{"full_history", "partial_first_day", "expired_lease", "missing_lease"} {
		t.Run(kind, func(t *testing.T) {
			m, _, published := newUsageServingAvailabilityFixture(t)
			hour := published.PublishedThrough - usageFactHourSeconds
			if _, err := m.claimUsageFactHour(hour, []int64{88}); err != nil {
				t.Fatal(err)
			}
			db := m.usageFactsStore()
			var err error
			switch kind {
			case "full_history":
				err = db.Model(&UsageFactMemberState{}).Where("user_id = ?", 88).Updates(map[string]any{
					"source_floor_hour": published.PublishedRangeStart, "source_first_log_hour": published.PublishedRangeStart,
					"tail_through_hour": published.PublishedThrough, "verified_through_hour": published.PublishedRangeStart,
				}).Error
			case "partial_first_day":
				err = db.Model(&UsageFactMemberState{}).Where("user_id = ?", 88).Update("live_from_hour", hour).Error
				if err == nil {
					err = db.Model(&UsageFactPublishedMember{}).Where("user_id = ?", 88).Update("source_floor_hour", hour).Error
				}
			case "expired_lease":
				err = db.Model(&UsageFactMemberHourState{}).Where("user_id = ? AND hour_ts = ?", 88, hour).
					Update("lease_until", time.Now().Add(-time.Minute).Unix()).Error
			case "missing_lease":
				err = db.Model(&UsageFactMemberHourState{}).Where("user_id = ? AND hour_ts = ?", 88, hour).Update("lease_token", "").Error
			}
			if err != nil {
				t.Fatal(err)
			}
			err = m.validateUsageFactFullHistoryCheckpoint(context.Background(), published.PublishedThrough)
			if (err == nil) != (kind != "missing_lease") {
				t.Fatalf("unexpected serving eligibility for %s: %v", kind, err)
			}
			ready, err := m.completedUsageFactHourUsersForEpoch(hour, []int64{88}, m.cfg.UsageFactsHistorySourceEpoch)
			if err != nil || ready[88] {
				t.Fatalf("running refresh must not advance a worker cursor: %v, %v", ready, err)
			}
		})
	}
}
