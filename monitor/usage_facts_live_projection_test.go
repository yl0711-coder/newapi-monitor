package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func seedUsageLiveProjectionMember(t *testing.T, m *Monitor, userID, floor, through, baselineNet, usedQuota, capturedAt int64) {
	t.Helper()
	m.cfg.UsageFactsFullHistoryEnabled = true
	m.cfg.UsageFactsHistorySourceEpoch = "projection-test-epoch"
	// Projection is a local-only read path. Running it with no prodDB proves the
	// helper cannot accidentally reach the source logs or users tables.
	m.cfg.LocalSnapshotOnly = true
	m.cfg.UsageFactsLocalReadOnly = true
	seedPublishedUsageFactsForTest(t, m, []int64{userID}, floor, through)
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_floor_hour": floor, "source_epoch": m.cfg.UsageFactsHistorySourceEpoch,
		"tracked_revision":       1,
		"classification_version": userTrafficClassificationVersion, "query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactMemberState{
		UserID: userID, Active: true, TrackedRevision: 1,
		SourceFloorHour: &floor, CoverageThroughHour: &through, TailThroughHour: &through,
		VerifiedThroughHour: &through, SourceHistoryStatus: "complete_hot", CoverageStatus: "ready",
		VerificationStatus: "complete", SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if baselineNet > 0 {
		if err := m.usageFactsStore().Create(&UsageDailyFact{
			DateTs: usageFactDayStart(floor), UserID: userID, ChannelID: 1, Grp: "baseline", ModelName: "baseline", TokenID: 1,
			ConsumeQuota: baselineNet,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.usageFactsStore().Create(&UsageUserSnapshot{
		UserID: userID, Username: "projection", Email: "projection@example.test",
		UsedQuota: usedQuota, Exists: true, CapturedAt: capturedAt,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageUserQuotaWatermark{
		UserID: userID, UsedQuota: baselineNet, Exists: true, CapturedAt: through + 60,
	}).Error; err != nil {
		t.Fatal(err)
	}
}

func TestUsageLiveProjectionIgnoresLifetimeCounterDriftBeforeAnchor(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Date(2026, 8, 19, 19, 18, 0, 0, usageCST)
	today := usageFactDayStart(now.Unix())
	// Historical facts intentionally differ from the cumulative counter by 20.
	// The projection must use the nearby hourly anchor, not misclassify that old
	// discrepancy as today's consumption.
	floor := today - 12*usageFactHourSeconds
	through := today + 19*usageFactHourSeconds
	seedUsageLiveProjectionMember(t, m, 11, floor, through, 80, 150, now.Add(-time.Minute).Unix())
	if err := m.usageFactsStore().Model(&UsageUserQuotaWatermark{}).Where("user_id = ?", 11).
		Update("used_quota", 100).Error; err != nil {
		t.Fatal(err)
	}
	projection, err := m.loadUsageLiveProjection(context.Background(), []int64{11}, floor, through, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection == nil || projection.TodayNetByUser[11] != 50 {
		t.Fatalf("pre-anchor lifetime drift leaked into today: %+v", projection)
	}
}

func TestUsageLiveProjectionAcceptsOriginalRevisionOneLegacySignature(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Date(2026, 8, 19, 19, 18, 0, 0, usageCST)
	today := usageFactDayStart(now.Unix())
	through := today + 19*usageFactHourSeconds
	seedUsageLiveProjectionMember(t, m, 12, today-usageFactDaySeconds, through, 100, 150, now.Add(-time.Minute).Unix())
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", 12).
		Update("tracked_revision", 0).Error; err != nil {
		t.Fatal(err)
	}
	projection, err := m.loadUsageLiveProjection(context.Background(), []int64{12}, today, today+usageFactDaySeconds, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection == nil || projection.TodayNetByUser[12] != 50 {
		t.Fatalf("legacy revision-one publication should remain compatible: %+v", projection)
	}
}

func TestUsageLiveProjectionMatchesHourlyAnchorWithoutSourceRead(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Date(2026, 8, 19, 19, 18, 0, 0, usageCST)
	today := usageFactDayStart(now.Unix())
	floor := today - usageFactDaySeconds
	through := today + 19*usageFactHourSeconds
	seedUsageLiveProjectionMember(t, m, 102, floor, through, 50_287_604_164, 52_525_296_239, now.Add(-5*time.Minute).Unix())

	projection, err := m.loadUsageLiveProjection(context.Background(), []int64{102}, floor, through, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection == nil || projection.TodayNetByUser[102] != 2_237_692_075 {
		t.Fatalf("projection=%+v", projection)
	}
	mx := newUsageMatrixRange(floor, through)
	mx.Cells = []UsageMatrixCell{{UserID: 102, Date: "2026-08-19", UsageBilling: UsageBilling{ConsumeQuota: 2_219_922_287}}}
	if !applyUsageMatrixLiveProjection(mx, projection) {
		t.Fatal("fresh complete projection was not applied")
	}
	if got := mx.Cells[0].NetQuota; got != 2_237_692_075 {
		t.Fatalf("today net=%d", got)
	}
	if !mx.LiveProjectionApplied || mx.LiveProjectionThrough != now.Add(-5*time.Minute).Unix() || mx.FinalizedThrough != through {
		t.Fatalf("projection metadata=%+v", mx)
	}
}

func TestUsageLiveProjectionFailsClosedWhenProfileStaleOrHistoryIncomplete(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stale    bool
		coverage string
	}{
		{name: "stale profile", stale: true, coverage: "ready"},
		{name: "history incomplete", coverage: "backfilling"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestMonitor(t)
			enableUsageFactsForTest(m)
			now := time.Date(2026, 8, 19, 19, 18, 0, 0, usageCST)
			today := usageFactDayStart(now.Unix())
			captured := now.Add(-5 * time.Minute).Unix()
			if tc.stale {
				captured = now.Add(-usageLiveProjectionMaxAge - time.Second).Unix()
			}
			seedUsageLiveProjectionMember(t, m, 7, today-usageFactDaySeconds, today+19*usageFactHourSeconds, 100, 150, captured)
			if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 7).Update("coverage_status", tc.coverage).Error; err != nil {
				t.Fatal(err)
			}
			projection, err := m.loadUsageLiveProjection(context.Background(), []int64{7}, today-usageFactDaySeconds, today+19*usageFactHourSeconds, now)
			if err != nil {
				t.Fatal(err)
			}
			if projection != nil {
				t.Fatalf("unsafe projection escaped fail-closed gate: %+v", projection)
			}
		})
	}
}

func TestUsageLiveProjectionIsAtomicAcrossSelectedMembers(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Date(2026, 8, 19, 19, 18, 0, 0, usageCST)
	today := usageFactDayStart(now.Unix())
	floor := today - usageFactDaySeconds
	through := today + 19*usageFactHourSeconds
	seedUsageLiveProjectionMember(t, m, 1, floor, through, 100, 150, now.Add(-time.Minute).Unix())

	if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
		UserID: 2, SourceFloorHour: floor, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, PublishedAt: now.Unix(),
		TrackedRevision:       1,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactMemberState{
		UserID: 2, Active: true, TrackedRevision: 1, SourceFloorHour: &floor, CoverageThroughHour: &through,
		SourceHistoryStatus: "complete_hot", CoverageStatus: "ready", VerificationStatus: "complete",
		SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, ClassificationVersion: userTrafficClassificationVersion,
		QuerySemanticsVersion: usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	// A stale second member must suppress the entire company projection; mixing
	// one live member with one finalized-only member would make totals incoherent.
	if err := m.usageFactsStore().Create(&UsageUserSnapshot{
		UserID: 2, UsedQuota: 25, Exists: true,
		CapturedAt: now.Add(-usageLiveProjectionMaxAge - time.Second).Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageUserQuotaWatermark{
		UserID: 2, UsedQuota: 0, Exists: true, CapturedAt: through + 60,
	}).Error; err != nil {
		t.Fatal(err)
	}
	projection, err := m.loadUsageLiveProjection(context.Background(), []int64{1, 2}, floor, through, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection != nil {
		t.Fatalf("mixed-freshness company escaped atomic gate: %+v", projection)
	}
	if err := m.usageFactsStore().Model(&UsageUserSnapshot{}).Where("user_id = ?", 2).
		Update("captured_at", now.Add(-time.Minute).Unix()).Error; err != nil {
		t.Fatal(err)
	}
	projection, err = m.loadUsageLiveProjection(context.Background(), []int64{2, 1, 2}, floor, through, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection == nil || len(projection.TodayNetByUser) != 2 || projection.TodayNetByUser[1] != 50 || projection.TodayNetByUser[2] != 25 {
		t.Fatalf("complete company projection=%+v", projection)
	}
}

func TestUsageLiveProjectionAllowsVerifiedEmptyMemberButRejectsFirstActivity(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Date(2026, 8, 19, 19, 18, 0, 0, usageCST)
	today := usageFactDayStart(now.Unix())
	through := today + 19*usageFactHourSeconds
	seedUsageLiveProjectionMember(t, m, 3, today-usageFactDaySeconds, through, 0, 0, now.Add(-time.Minute).Unix())
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 3).
		Update("source_history_status", "no_history").Error; err != nil {
		t.Fatal(err)
	}
	projection, err := m.loadUsageLiveProjection(context.Background(), []int64{3}, today, today+usageFactDaySeconds, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection == nil || projection.TodayNetByUser[3] != 0 {
		t.Fatalf("verified empty member should not block projection: %+v", projection)
	}
	if err := m.usageFactsStore().Model(&UsageUserSnapshot{}).Where("user_id = ?", 3).
		Update("used_quota", 1).Error; err != nil {
		t.Fatal(err)
	}
	projection, err = m.loadUsageLiveProjection(context.Background(), []int64{3}, today, today+usageFactDaySeconds, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection != nil {
		t.Fatalf("first activity must wait for no-history invalidation and verification: %+v", projection)
	}
}

func TestUsageProfileSyncPersistsAndPrunesQuotaWatermarks(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	if err := m.storeDB.Create(&TrackedUser{UserID: 5, Username: "watermark-user"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email,quota,used_quota) VALUES (5,'watermark-user','',0,100)"); err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 8, 19, 12, 0, 0, 0, usageCST)
	if err := m.syncUsageProfiles(context.Background(), started); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("UPDATE users SET used_quota=175 WHERE id=5"); err != nil {
		t.Fatal(err)
	}
	later := started.Add(usageLiveProjectionWatermarkRetention + time.Hour)
	if err := m.syncUsageProfiles(context.Background(), later); err != nil {
		t.Fatal(err)
	}
	var rows []UsageUserQuotaWatermark
	if err := m.usageFactsStore().Where("user_id = ?", 5).Order("captured_at").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].CapturedAt != later.Unix() || rows[0].UsedQuota != 175 || !rows[0].Exists {
		t.Fatalf("watermark retention=%+v", rows)
	}
}

func TestUsageLiveProjectionCrossMidnightUsesCompletedPriorDay(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Date(2026, 8, 20, 0, 12, 0, 0, usageCST)
	today := usageFactDayStart(now.Unix())
	floor := today - usageFactDaySeconds
	// At 00:12 the 23:00-00:00 hour has finalized, so today's cumulative delta
	// is safe even though no hour belonging to today has closed yet.
	seedUsageLiveProjectionMember(t, m, 9, floor, today, 100, 105, now.Add(-time.Minute).Unix())
	projection, err := m.loadUsageLiveProjection(context.Background(), []int64{9}, today, today+usageFactDaySeconds, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection == nil || projection.TodayNetByUser[9] != 5 {
		t.Fatalf("midnight projection=%+v", projection)
	}
	mx := newUsageMatrixRange(today, today+usageFactDaySeconds)
	if !applyUsageMatrixLiveProjection(mx, projection) || len(mx.Cells) != 1 {
		t.Fatalf("midnight matrix=%+v", mx)
	}
	daySet := make(map[string]bool, len(mx.Days))
	for _, day := range mx.Days {
		daySet[day] = true
	}
	for _, cell := range mx.Cells {
		if !daySet[cell.Date] {
			t.Fatalf("projection cell %q is not present in days %v", cell.Date, mx.Days)
		}
	}
}

func TestUsageLiveProjectionWaitsForPriorDayFinalHourAtMidnight(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Date(2026, 8, 20, 0, 9, 0, 0, usageCST)
	today := usageFactDayStart(now.Unix())
	floor := today - usageFactDaySeconds
	through := today - usageFactHourSeconds
	seedUsageLiveProjectionMember(t, m, 9, floor, through, 100, 105, now.Add(-time.Minute).Unix())
	projection, err := m.loadUsageLiveProjection(context.Background(), []int64{9}, today, today+usageFactDaySeconds, now)
	if err != nil {
		t.Fatal(err)
	}
	if projection != nil {
		t.Fatalf("projection must wait for the prior day's final hour: %+v", projection)
	}
}

func TestUsageLiveProjectionNeverMovesAuthoritativeFactsBackward(t *testing.T) {
	projection := &usageLiveProjection{
		DayTs: 100, Through: 200, FinalizedThrough: 150,
		TodayNetByUser: map[int64]int64{1: 90},
	}
	mx := &UsageMatrix{Cells: []UsageMatrixCell{{UserID: 1, Date: time.Unix(100, 0).In(usageCST).Format("2006-01-02"), UsageBilling: UsageBilling{ConsumeQuota: 100}}}}
	before := mx.Cells[0].ConsumeQuota
	if applyUsageMatrixLiveProjection(mx, projection) {
		t.Fatal("older cumulative snapshot must not replace newer facts")
	}
	if mx.Cells[0].ConsumeQuota != before || mx.LiveProjectionApplied {
		t.Fatalf("authoritative fact changed: %+v", mx)
	}
}

func TestUsageStatsLiveProjectionKeepsTotalsReconciledAndIdempotent(t *testing.T) {
	day := time.Date(2026, 8, 19, 0, 0, 0, 0, usageCST)
	projection := &usageLiveProjection{
		DayTs: day.Unix(), Through: day.Add(19*time.Hour + 15*time.Minute).Unix(), FinalizedThrough: day.Add(19 * time.Hour).Unix(),
		TodayNetByUser: map[int64]int64{1: 300, 2: 200},
	}
	st := newUsageStatsRange(day.Unix(), day.Add(24*time.Hour).Unix())
	st.Daily = []UsageDaily{{Date: "2026-08-19", UsageBilling: UsageBilling{ConsumeQuota: 450}}}
	st.Summary.ConsumeQuota = 450
	st.Summary.finalize()
	if !applyUsageStatsLiveProjection(st, projection) {
		t.Fatal("projection was not applied")
	}
	if st.Daily[0].NetQuota != 500 || st.Summary.NetQuota != 500 {
		t.Fatalf("daily=%+v summary=%+v", st.Daily[0], st.Summary)
	}
	if len(st.ByGroup) != 1 || st.ByGroup[0].Key != usageLiveProjectionDimension || st.ByGroup[0].NetQuota != 50 ||
		len(st.ByModel) != 1 || len(st.DailyByModel) != 1 {
		t.Fatalf("unclassified projection missing: groups=%+v models=%+v daily_models=%+v", st.ByGroup, st.ByModel, st.DailyByModel)
	}
	// 缓存对象只会投影一次；即使调用方意外重复应用，也不得再次累计。
	if !applyUsageStatsLiveProjection(st, projection) || st.Summary.NetQuota != 500 {
		t.Fatalf("projection is not idempotent: %+v", st.Summary)
	}
}

func TestUsageFactReadWrappersApplyProjectionWithoutSourceConnection(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Now()
	today := usageFactDayStart(now.Unix())
	through := m.usageFactFinalizedHour(now)
	if through <= today {
		t.Skip("test requires at least one finalized hour today")
	}
	floor := today - usageFactDaySeconds
	seedUsageLiveProjectionMember(t, m, 88, floor, through, 100, 450, now.Add(-time.Minute).Unix())
	if err := m.usageFactsStore().Create(&UsageHourFact{
		HourTs: through - usageFactHourSeconds, DayTs: today, UserID: 88,
		ChannelID: 1, Grp: "closed", ModelName: "closed", TokenID: 1, ConsumeQuota: 200,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageUserQuotaWatermark{}).
		Where("user_id = ?", 88).Update("used_quota", 300).Error; err != nil {
		t.Fatal(err)
	}

	mx, err := m.computeUsageMatrixForRead(context.Background(), []int64{88}, floor, through)
	if err != nil {
		t.Fatal(err)
	}
	var todayNet int64
	for _, cell := range mx.Cells {
		if cell.UserID == 88 && cell.Date == time.Unix(today, 0).In(usageCST).Format("2006-01-02") {
			todayNet = cell.NetQuota
		}
	}
	if todayNet != 350 || !mx.LiveProjectionApplied {
		t.Fatalf("matrix today=%d metadata=%+v", todayNet, mx)
	}

	st, err := m.computeUsageStatsForRead(context.Background(), []int64{88}, floor, through, 0)
	if err != nil {
		t.Fatal(err)
	}
	if st.Summary.NetQuota != 450 || !st.LiveProjectionApplied {
		// The requested range includes the prior-day baseline (100) plus projected
		// today (350), while the detailed live delta remains explicitly unclassified.
		t.Fatalf("stats summary=%+v metadata=%+v", st.Summary, st)
	}
}

func TestUsageLiveProjectionIsExposedByAdminRoutesWithoutSourceConnection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	now := time.Now()
	today := usageFactDayStart(now.Unix())
	through := m.usageFactFinalizedHour(now)
	if through <= today {
		t.Skip("test requires the prior day to be fully finalized")
	}
	g := CustomerGroup{Name: "projection-company"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 77, GroupID: g.ID, Username: "projection-user", AddedAt: now.Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	seedUsageLiveProjectionMember(t, m, 77, today-usageFactDaySeconds, through, 100, 450, now.Add(-time.Minute).Unix())

	day := now.In(usageCST).Format("2006-01-02")
	request := func(path string, handler gin.HandlerFunc) map[string]any {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, path, nil)
		handler(c)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, w.Code, w.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	for path, handler := range map[string]gin.HandlerFunc{
		"/usage/matrix?from=" + day + "&to=" + day:           m.serveUsageMatrix,
		"/usage/stats?user_id=77&from=" + day + "&to=" + day: m.serveUsageStats,
	} {
		body := request(path, handler)
		if applied, _ := body["live_projection_applied"].(bool); !applied {
			t.Fatalf("%s did not expose projection metadata: %+v", path, body)
		}
		if body["live_projection_message"] == "" || body["finalized_through"] == nil {
			t.Fatalf("%s projection metadata incomplete: %+v", path, body)
		}
	}
}
