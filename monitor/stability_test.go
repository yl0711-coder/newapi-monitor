package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newStabilityTestMonitor(t *testing.T) *Monitor {
	t.Helper()
	m := &Monitor{cfg: Settings{RetentionDays: 7, StabilityEnabled: true, StabilityRetentionDays: 90}}
	if err := m.openStore(t.TempDir() + "/stability.db"); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	return m
}

func TestReadyStatusKeepsDisabledRawProblemMigrationVisible(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, cstLocation).Unix()
	state := StabilityProblemClassificationMigration{
		ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
		FromTs: now - 24*3600, ThroughTs: now, NextTs: now - 12*3600,
		Status: "queued", CreatedAt: now - 3600, UpdatedAt: now - 60,
	}
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}

	m.probeLocalOperationalState(context.Background(), now)
	if !m.stabilityProblemMigrationIncomplete.Load() {
		t.Fatal("durable incomplete raw migration was hidden when execution flag was disabled")
	}

	// Isolate the migration reason from the other readiness inputs. The live
	// cursor and hourly coverage are healthy; only the persisted cold cursor is
	// incomplete.
	m.localStoreProbeOK.Store(true)
	m.localStoreProbeAt.Store(now)
	m.storeIntegrityOK.Store(true)
	m.storeIntegrityCheckedAt.Store(now)
	m.stabilityCoverageCheckedAt.Store(now)
	m.stabilityCoverageBPS.Store(10000)
	m.stabilityBackfillStalled.Store(false)
	m.problemLastSuccess.Store(now)
	m.problemLastFailure.Store(0)
	m.stabilityProblemCoverageTo.Store(now)
	m.stabilityProblemPending.Store(0)
	m.processStartedAt.Store(now - 60)

	status, code := m.readyStatus(time.Unix(now, 0))
	if code != http.StatusOK || status.Status != "degraded" {
		t.Fatalf("incomplete raw migration must degrade readiness: code=%d status=%+v", code, status)
	}
	found := false
	for _, reason := range status.DegradedReasons {
		found = found || reason == "stability_problem_migration_incomplete"
	}
	if !found {
		t.Fatalf("raw migration reason missing: %v", status.DegradedReasons)
	}

	if err := m.storeDB.Model(&StabilityProblemClassificationMigration{}).
		Where("id = ?", 1).Updates(map[string]any{"status": "complete", "next_ts": now, "completed_at": now}).Error; err != nil {
		t.Fatal(err)
	}
	m.probeLocalOperationalState(context.Background(), now)
	if m.stabilityProblemMigrationIncomplete.Load() {
		t.Fatal("completed raw migration remained degraded after local probe")
	}
}

func TestStabilityRollupAndReportKeepsGroupChannelSemantics(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	currentHour := day + 10*3600
	previousHour := day - 24*3600 + 10*3600
	rows := []MetricSample{
		{BucketTs: currentHour, ChannelID: 33, ModelName: "gpt-5", Grp: "codex", Success: 90, Anomaly: 5, Failed: 5, Tokens: 1000, Quota: 500000, SumUseTime: 190, MaxUseTime: 8, Err5xx: 5},
		{BucketTs: previousHour, ChannelID: 33, ModelName: "gpt-5", Grp: "codex", Success: 80, Failed: 20, Tokens: 800, Quota: 400000, Err5xx: 20},
		{BucketTs: currentHour, ChannelID: 44, ModelName: "claude", Grp: "claude", Success: 50, Tokens: 500},
	}
	if err := m.upsertSamples(rows); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 33, Name: "route-a", Type: 1, Vendor: newAPIChannelTypeName(1), Status: 1, UpdatedAt: day}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 44, Name: "route-b", Type: 14, Vendor: newAPIChannelTypeName(14), Status: 1, UpdatedAt: day}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&RejectionSample{BucketTs: currentHour, Node: "master", Reason: "no_available_channel", Model: "gpt-5", Grp: "codex", Count: 2}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.rollupStabilityHours(previousHour - 60); err != nil {
		t.Fatal(err)
	}
	if err := m.rollupStabilityRejections(previousHour - 60); err != nil {
		t.Fatal(err)
	}
	// 重复 rollup 必须覆盖而不是翻倍。
	if err := m.rollupStabilityHours(previousHour - 60); err != nil {
		t.Fatal(err)
	}
	if err := m.rollupStabilityRejections(previousHour - 60); err != nil {
		t.Fatal(err)
	}

	report, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 24*3600}, day+20*3600)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Requests != 152 {
		t.Fatalf("summary requests=%d want 152", report.Summary.Requests)
	}
	if len(report.Groups) != 2 {
		t.Fatalf("groups=%d want 2", len(report.Groups))
	}
	var codex StabilityGroup
	for _, g := range report.Groups {
		if g.Name == "codex" {
			codex = g
		}
	}
	if codex.Requests != 102 || codex.Rejected != 2 {
		t.Fatalf("codex metrics=%+v", codex.StabilityMetrics)
	}
	if codex.Stability == nil || math.Abs(*codex.Stability-90.0/102.0*100) > 0.0001 {
		t.Fatalf("codex stability=%v", codex.Stability)
	}
	if len(codex.Channels) != 1 || codex.Channels[0].Requests != 100 || codex.Channels[0].Rejected != 0 {
		t.Fatalf("channel must exclude pre-route rejection: %+v", codex.Channels)
	}
	if codex.DeltaPP == nil || *codex.DeltaPP <= 0 {
		t.Fatalf("expected positive delta, got %v", codex.DeltaPP)
	}
	if len(codex.Daily) != 1 || codex.Daily[0].Rejected != 2 {
		t.Fatalf("daily rejection missing: %+v", codex.Daily)
	}
	if report.Meta.TimelineBucketSec != 3600 || len(codex.Timeline) != 24 || codex.Timeline[10].Requests != 102 || codex.Timeline[10].Problems != 12 {
		t.Fatalf("timeline/rejection missing: step=%d timeline=%+v", report.Meta.TimelineBucketSec, codex.Timeline)
	}

	vendorReport, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 24*3600, Vendor: "OpenAI"}, day+20*3600)
	if err != nil {
		t.Fatal(err)
	}
	if vendorReport.Summary.Requests != 100 || vendorReport.Summary.Rejected != 0 {
		t.Fatalf("vendor filter must not invent rejection ownership: %+v", vendorReport.Summary)
	}
}

func TestNewAPIChannelTypeNameUsesOfficialMappingAndNoGuessing(t *testing.T) {
	for channelType, want := range map[int]string{1: "OpenAI", 14: "Anthropic", 26: "ZhipuV4", 43: "DeepSeek"} {
		if got := newAPIChannelTypeName(channelType); got != want {
			t.Fatalf("type=%d vendor=%q want %q", channelType, got, want)
		}
	}
	if got := newAPIChannelTypeName(9999); got != "未标记" {
		t.Fatalf("unknown type must not be guessed: %q", got)
	}
}

func TestStabilityTimelineKeepsNarrowStripCountAcrossRanges(t *testing.T) {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, cstLocation).Unix()
	for _, tc := range []struct {
		days int
		step int64
		bars int
	}{{7, 2 * 3600, 84}, {15, 4 * 3600, 90}, {30, 8 * 3600, 90}, {90, 24 * 3600, 90}} {
		scope := stabilityScope{FromTs: start, ToTs: start + int64(tc.days)*86400}
		step := stabilityTimelineBucketSec(scope)
		if step != tc.step || len(stabilityBucketKeys(scope, step)) != tc.bars {
			t.Fatalf("days=%d step=%d bars=%d", tc.days, step, len(stabilityBucketKeys(scope, step)))
		}
	}
}

func TestStabilityProblemTextAndRawGrouping(t *testing.T) {
	m := newStabilityTestMonitor(t)
	from := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	raw := "status_code=524, The origin did not return a complete response."
	msg, truncated := stabilityProblemText(raw)
	if truncated || msg != raw {
		t.Fatalf("raw error changed: %q truncated=%v", msg, truncated)
	}
	if got := stabilityProblemCode(raw); got != "524" {
		t.Fatalf("code=%q", got)
	}
	if got := stabilityProblemCode("bad response status code 504"); got != "504" {
		t.Fatalf("space separated code=%q", got)
	}
	if got := stabilityProblemCode("unexpected status 403 Forbidden"); got != "403" {
		t.Fatalf("unexpected status code=%q", got)
	}
	long := strings.Repeat("错", maxStabilityProblemMessage+1)
	msg, truncated = stabilityProblemText(long)
	if !truncated || len([]rune(msg)) != maxStabilityProblemMessage {
		t.Fatalf("safe truncation failed: len=%d truncated=%v", len([]rune(msg)), truncated)
	}
	rows := []StabilityProblemSample{
		{BucketTs: from + 60, Source: "newapi", SignatureHash: stabilityProblemHash("newapi", raw), ChannelID: 33, ModelName: "gpt", Grp: "codex", Code: "524", Message: raw, Count: 2, FirstTs: from + 61, LastTs: from + 90},
		{BucketTs: from + 120, Source: "newapi", SignatureHash: stabilityProblemHash("newapi", raw), ChannelID: 33, ModelName: "gpt", Grp: "codex", Code: "524", Message: raw, Count: 3, FirstTs: from + 121, LastTs: from + 150},
	}
	if err := m.upsertStabilityProblems(rows); err != nil {
		t.Fatal(err)
	}
	result, err := m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: from, ToTs: from + 3600}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Problems) != 1 || result.Problems[0].Message != raw || result.Problems[0].Count != 5 {
		t.Fatalf("problem aggregation=%+v", result.Problems)
	}
	if result.Problems[0].AdviceStatus != "knowledge_base_pending_review" {
		t.Fatalf("unexpected advice status: %s", result.Problems[0].AdviceStatus)
	}
}

func TestStabilityProblemTextRedactsSensitiveValuesBeforeStorage(t *testing.T) {
	raw := "upstream 10.8.0.12:443 user@example.com Authorization: Bearer sk-secret-1234567890 request 550e8400-e29b-41d4-a716-446655440000 api_key=abcdefghijklmnopqrstuvwxyz012345"
	message, truncated := stabilityProblemText(raw)
	if truncated {
		t.Fatalf("unexpected truncation: %q", message)
	}
	for _, secret := range []string{"10.8.0.12", "user@example.com", "sk-secret-1234567890", "550e8400-e29b-41d4-a716-446655440000", "abcdefghijklmnopqrstuvwxyz012345"} {
		if strings.Contains(message, secret) {
			t.Fatalf("sensitive value leaked: %q in %q", secret, message)
		}
	}
	if !strings.Contains(message, "<ip>") || !strings.Contains(message, "<email>") || !strings.Contains(message, "<redacted>") {
		t.Fatalf("missing redaction markers: %q", message)
	}
}

func TestStabilityProblemSamplerKeepsRawTextAndIsIdempotent(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	from := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	raw := "unexpected status 403 Forbidden: affinity channel disabled"
	for _, row := range []struct {
		createdAt int64
		typ       int
	}{
		{from + 5, 5}, {from + 35, 5}, {from + 70, 5}, {from + 80, 2},
	} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(user_id,created_at,type,model_name,`+"`group`"+`,channel_id,content,request_id)
			VALUES (1,?,?,?,?,?,?,'user-request')`, row.createdAt, row.typ, "gpt-test", "codex", 33, raw); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ { // 重叠窗口重跑必须覆盖，不能把同一批错误翻倍。
		if _, err := m.sampleStabilityProblems(context.Background(), from, from+180); err != nil {
			t.Fatal(err)
		}
	}
	result, err := m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: from, ToTs: from + 180}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if result.CapturedTotal != 3 || len(result.Problems) != 1 {
		t.Fatalf("sampled problems=%+v", result)
	}
	problem := result.Problems[0]
	if problem.Message != raw || problem.Code != "403" || problem.Count != 3 {
		t.Fatalf("raw problem changed or double counted: %+v", problem)
	}
}

func TestStabilityProblemSamplerResumesOverflowWithoutPublishingPartialMinute(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	from := time.Date(2026, 8, 5, 11, 0, 0, 0, cstLocation).Unix()
	tx, err := m.prodDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO logs (user_id,created_at,type,model_name,` + "`group`" + `,channel_id,content,request_id)
		VALUES (1,?,5,'gpt-test','codex',33,'status_code=503, service unavailable','user-request')`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxStabilityProblemRowsPerRun+1; i++ {
		if _, err := stmt.Exec(from + int64(i%60)); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// 首轮只探测到超限并建立本地续采状态，不发布半截分钟。
	if _, err := m.sampleStabilityProblems(context.Background(), from, from+60); err != nil {
		t.Fatal(err)
	}
	result, err := m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: from, ToTs: from + 60}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if result.CapturedTotal != 0 || result.PendingMinutes != 1 || result.CoverageComplete {
		t.Fatalf("partial minute was exposed as complete: %+v", result)
	}
	// Live catch-up has a hard budget of three one-minute source queries per
	// sampler turn.  The first turn used one overflow probe plus two pages.  Two
	// more turns may add at most six pages (3,000 rows), so the minute must still
	// be pending and no turn may consume more than its page budget.
	var state StabilityProblemIngestState
	if err := m.storeDB.First(&state, "bucket_ts = ?", from).Error; err != nil {
		t.Fatal(err)
	}
	if state.RowsScanned != int64(2*stabilityProblemPageSize) {
		t.Fatalf("first live turn exceeded its three-query budget: %+v", state)
	}
	for i := 0; i < 2; i++ {
		before := state.RowsScanned
		if _, err := m.sampleStabilityProblems(context.Background(), from, from+60); err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.First(&state, "bucket_ts = ?", from).Error; err != nil {
			t.Fatal(err)
		}
		if delta := state.RowsScanned - before; delta > int64(stabilityProblemLiveWindowsPerTurn*stabilityProblemPageSize) {
			t.Fatalf("one live turn exceeded query/page budget: delta=%d state=%+v", delta, state)
		}
	}
	if m.stabilityProblemPendingCount() != 1 {
		t.Fatal("overflow minute should still be pending before the final bounded turn")
	}
	if _, err := m.sampleStabilityProblems(context.Background(), from, from+60); err != nil {
		t.Fatal(err)
	}
	result, err = m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: from, ToTs: from + 60}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if result.CapturedTotal != maxStabilityProblemRowsPerRun+1 || result.PendingMinutes != 0 || !result.CoverageComplete {
		t.Fatalf("overflow minute was not fully resumed: %+v", result)
	}
}

func TestStabilityProblemSamplerCatchesUpWithoutSkippingElapsedMinutes(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	base := time.Date(2026, 8, 5, 12, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&StabilityProblemIngestState{BucketTs: base, Complete: true, CompletedAt: base + 60}).Error; err != nil {
		t.Fatal(err)
	}
	for _, createdAt := range []int64{base + 5*60, base + 25*60} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(user_id,created_at,type,model_name,`+"`group`"+`,channel_id,content,request_id)
			VALUES (1,?,5,'gpt-test','codex',33,'status_code=503, catchup','user-request')`, createdAt); err != nil {
			t.Fatal(err)
		}
	}
	// 调度器当前窗口是 20~30 分钟，但上一次覆盖只到第 1 分钟。
	// 每轮只允许追赶三个分钟；首轮必须从第 1 分钟连续推到第 4
	// 分钟，而不是跳到第 20 分钟。第二轮再要捕获第 5 分钟的日志。
	if _, err := m.sampleStabilityProblems(context.Background(), base+20*60, base+30*60); err != nil {
		t.Fatal(err)
	}
	result, err := m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: base, ToTs: base + 30*60}, 50)
	if err != nil {
		t.Fatal(err)
	}
	var cursor StabilityProblemLiveCursor
	if err := m.storeDB.First(&cursor, 1).Error; err != nil {
		t.Fatal(err)
	}
	if result.CapturedTotal != 0 || result.CoverageComplete || result.UncoveredMinutes != 26 ||
		cursor.NextTs != base+4*60 || cursor.TargetThroughTs != base+30*60 ||
		!m.stabilityProblemNeedsCatchup(base+30*60) {
		t.Fatalf("first bounded catch-up turn skipped or falsely completed elapsed range: result=%+v cursor=%+v", result, cursor)
	}
	if _, err := m.sampleStabilityProblems(context.Background(), base+20*60, base+30*60); err != nil {
		t.Fatal(err)
	}
	result, err = m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: base, ToTs: base + 30*60}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if result.CapturedTotal != 1 || result.CoverageComplete || result.UncoveredMinutes != 23 ||
		!m.stabilityProblemNeedsCatchup(base+30*60) {
		t.Fatalf("second bounded catch-up turn did not capture the next contiguous log: %+v", result)
	}
	var currentMinute int64
	if err := m.storeDB.Model(&StabilityProblemIngestState{}).Where("bucket_ts = ?", base+25*60).Count(&currentMinute).Error; err != nil {
		t.Fatal(err)
	}
	if currentMinute != 0 {
		t.Fatal("current window was sampled before the older gap was covered")
	}
}

func TestStabilityProblemLiveCursorSurvivesLongGapAndAtomicReset(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	base := time.Date(2026, 8, 5, 13, 0, 0, 0, cstLocation).Unix()
	for _, createdAt := range []int64{base + 2*60 + 1, base + 25*60 + 1} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(user_id,created_at,type,model_name,`+"`group`"+`,channel_id,content,request_id)
			VALUES (1,?,5,'gpt-test','codex',33,'status_code=503 durable live cursor','customer')`, createdAt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.sampleStabilityProblems(context.Background(), base, base+60); err != nil {
		t.Fatal(err)
	}
	// Simulate process-local state loss. The next call presents only the current
	// rolling window, but the durable cursor must resume the preceding 20-minute
	// gap instead of jumping to it.
	m.problemLiveThrough.Store(0)
	m.problemLastSuccess.Store(0)
	if _, err := m.sampleStabilityProblems(context.Background(), base+20*60, base+30*60); err != nil {
		t.Fatal(err)
	}
	var cursor StabilityProblemLiveCursor
	if err := m.storeDB.First(&cursor, 1).Error; err != nil {
		t.Fatal(err)
	}
	if cursor.NextTs != base+4*60 || cursor.TargetThroughTs != base+30*60 || cursor.Status != "running" {
		t.Fatalf("durable live cursor skipped or falsely caught up: %+v", cursor)
	}
	var oldRows, futureRows int64
	if err := m.storeDB.Model(&StabilityProblemSample{}).Where("bucket_ts = ?", base+2*60).Count(&oldRows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&StabilityProblemSample{}).Where("bucket_ts = ?", base+25*60).Count(&futureRows).Error; err != nil {
		t.Fatal(err)
	}
	if oldRows != 1 || futureRows != 0 {
		t.Fatalf("live cursor sampled out of order: old=%d future=%d", oldRows, futureRows)
	}
}

func TestStabilityProblemMigrationAdaptiveBackoffPauseAndRetry(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityClassificationMigrationEnabled = true
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, cstLocation).Unix()
	state := StabilityProblemClassificationMigration{
		ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
		FromTs: base, ThroughTs: base + 12*60, NextTs: base, Status: "running",
		CurrentSpanMinutes: 12, CreatedAt: base, UpdatedAt: base,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 5; attempt++ {
		if err := m.storeDB.First(&state, 1).Error; err != nil {
			t.Fatal(err)
		}
		now := base + int64(attempt)*100
		_ = m.recordStabilityProblemMigrationFailure(&state, context.DeadlineExceeded, now)
		if err := m.storeDB.First(&state, 1).Error; err != nil {
			t.Fatal(err)
		}
		if state.Attempts != attempt {
			t.Fatalf("attempt=%d state=%+v", attempt, state)
		}
		if attempt == 1 && state.CurrentSpanMinutes != 6 || attempt == 2 && state.CurrentSpanMinutes != 3 ||
			attempt >= 3 && state.CurrentSpanMinutes != 1 {
			t.Fatalf("adaptive span did not shrink at attempt %d: %+v", attempt, state)
		}
	}
	if state.Status != "paused" || state.NextRetryAt != 0 {
		t.Fatalf("five source-window failures must pause cold migration: %+v", state)
	}
	retried, err := m.retryStabilityProblemMigration(time.Unix(base+1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if retried.Status != "queued" || retried.Attempts != 0 || retried.CurrentSpanMinutes != 1 || retried.NextRetryAt != 0 {
		t.Fatalf("root retry did not resume from the safest span: %+v", retried)
	}
}

func TestStabilityProblemLiveFailureBackoffPersistsAndClearsOnProgress(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	now := time.Now().Unix()
	base := now/60*60 - 10*60
	cursor := StabilityProblemLiveCursor{
		ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
		NextTs: base, TargetThroughTs: base + 60, Status: "running", UpdatedAt: now,
	}
	if err := m.storeDB.Create(&cursor).Error; err != nil {
		t.Fatal(err)
	}

	m.recordStabilityProblemLiveFailure(&cursor, context.DeadlineExceeded, now)
	var stored StabilityProblemLiveCursor
	if err := m.storeDB.First(&stored, 1).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Attempts != 1 || stored.NextRetryAt != now+60 || stored.LastFailureAt != now {
		t.Fatalf("live failure did not persist its retry fence: %+v", stored)
	}
	m.recordStabilityProblemLiveFailure(&stored, fmt.Errorf("%w: busy", errStabilityProblemSourceGateWait), now+1)
	var afterGate StabilityProblemLiveCursor
	if err := m.storeDB.First(&afterGate, 1).Error; err != nil {
		t.Fatal(err)
	}
	if afterGate.Attempts != 1 || afterGate.NextRetryAt != now+60 {
		t.Fatalf("protected gate wait consumed a live attempt: %+v", afterGate)
	}
	if _, err := m.sampleStabilityProblems(context.Background(), base, base+60); !errors.Is(err, errStabilityProblemLiveBackoff) {
		t.Fatalf("restart-visible retry fence was ignored: %v", err)
	}

	if err := m.storeDB.Create(&StabilityProblemIngestState{
		BucketTs: base, TrafficClassVersion: userTrafficClassificationVersion, Complete: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	afterGate.NextRetryAt = 0
	if err := m.storeDB.Model(&StabilityProblemLiveCursor{}).Where("id = ?", 1).Update("next_retry_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.advanceStabilityProblemLiveCursor(&afterGate, 60, now+61, true); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&stored, 1).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Attempts != 0 || stored.NextRetryAt != 0 || stored.LastFailureAt != 0 || stored.LastError != "" {
		t.Fatalf("successful live progress did not clear backoff: %+v", stored)
	}
}

func TestStabilityProblemMigrationEstimateUsesDurableProgress(t *testing.T) {
	now := int64(10_000)
	state := StabilityProblemClassificationMigration{
		FromTs: 1_000, ThroughTs: 8_200, NextTs: 4_600,
		CreatedAt: now - 3_600, LastSuccessAt: now - 10,
	}
	estimated, status, rate, sample := stabilityProblemMigrationEstimate(state, "running", now)
	if estimated == nil || *estimated != 3_600 || status != "observed" || sample != 3_600 || math.Abs(rate-1.0/60.0) > 0.000001 {
		t.Fatalf("durable ETA mismatch: estimated=%v status=%q rate=%f sample=%d", estimated, status, rate, sample)
	}
	state.NextRetryAt = now + 60
	if estimated, status, _, _ := stabilityProblemMigrationEstimate(state, "running", now); estimated != nil || status != "backoff" {
		t.Fatalf("retry wait must not publish a false ETA: estimated=%v status=%q", estimated, status)
	}
	state.NextRetryAt = 0
	if estimated, status, _, _ := stabilityProblemMigrationEstimate(state, "paused_disabled", now); estimated != nil || status != "blocked" {
		t.Fatalf("disabled incomplete migration must be blocked: estimated=%v status=%q", estimated, status)
	}
	state.NextTs = state.ThroughTs
	if estimated, status, _, _ := stabilityProblemMigrationEstimate(state, "complete", now); estimated == nil || *estimated != 0 || status != "complete" {
		t.Fatalf("complete migration ETA mismatch: estimated=%v status=%q", estimated, status)
	}
}

func TestStabilityProblemMigrationWorkerDoesNotWaitForMainSamplerTick(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.cfg.StabilityClassificationMigrationEnabled = true
	m.cfg.StabilityBackfillDelayMS = -1
	m.cfg.BackgroundSourceMinStartIntervalMS = -1
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, cstLocation).Unix()
	now := time.Now().Unix()
	if err := m.storeDB.Create(&StabilityProblemClassificationMigration{
		ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
		FromTs: base, ThroughTs: base + 60, NextTs: base, Status: "running",
		CurrentSpanMinutes: 1, CreatedAt: now, UpdatedAt: now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.problemLastSuccess.Store(now)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	started := time.Now()
	m.runStabilityProblemMigrationLoop(ctx)
	if elapsed := time.Since(started); elapsed >= 6*time.Second {
		t.Fatalf("cold worker remained tied to the main sampler tick: elapsed=%s", elapsed)
	}
	var state StabilityProblemClassificationMigration
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.Status != "complete" || state.NextTs != base+60 {
		t.Fatalf("dedicated low-priority worker did not finish bounded window: %+v", state)
	}
}

func TestStabilityProblemMigrationGateWaitDoesNotConsumeAttempt(t *testing.T) {
	m := newStabilityTestMonitor(t)
	base := time.Now().Unix()
	state := StabilityProblemClassificationMigration{
		ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
		FromTs: base - 600, ThroughTs: base, NextTs: base - 600, Status: "running",
		CurrentSpanMinutes: 3, Attempts: 4, CreatedAt: base - 1000, UpdatedAt: base - 100,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	cause := fmt.Errorf("%w: deadline", errStabilityProblemSourceGateWait)
	_ = m.recordStabilityProblemMigrationFailure(&state, cause, base)
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.Attempts != 4 || state.Status == "paused" || state.CurrentSpanMinutes != 3 {
		t.Fatalf("yielding at the source gate consumed a window failure: %+v", state)
	}
	if state.NextRetryAt != base+int64(stabilityProblemMigrationGateYieldDelay/time.Second) {
		t.Fatalf("protected gate yield must retry promptly without spinning: %+v", state)
	}
	if state.LastFailureAt != base || !strings.Contains(state.LastError, errStabilityProblemSourceGateWait.Error()) {
		t.Fatalf("protected gate yield must remain observable: %+v", state)
	}
}

func TestStabilityProblemMigrationOnlyGateWaitUsesShortYield(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "context canceled", cause: context.Canceled},
		{name: "source not ready", cause: errSourceNotReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			base := time.Now().Unix()
			state := StabilityProblemClassificationMigration{
				ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
				FromTs: base - 600, ThroughTs: base, NextTs: base - 600, Status: "running",
				CurrentSpanMinutes: 3, Attempts: 4, CreatedAt: base - 1000, UpdatedAt: base - 100,
			}
			if err := m.storeDB.Create(&state).Error; err != nil {
				t.Fatal(err)
			}
			_ = m.recordStabilityProblemMigrationFailure(&state, tc.cause, base)
			if err := m.storeDB.First(&state, 1).Error; err != nil {
				t.Fatal(err)
			}
			if state.NextRetryAt != base+60 || state.Attempts != 4 || state.CurrentSpanMinutes != 3 {
				t.Fatalf("non-gate scheduler interruption lost its conservative retry fence: %+v", state)
			}
		})
	}
}

func TestStabilityProblemLiveLanePreemptsIndependentColdMigration(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.cfg.StabilityClassificationMigrationEnabled = true
	m.cfg.StabilityBackfillDelayMS = -1
	oldMinute := time.Date(2026, 2, 1, 3, 0, 0, 0, cstLocation).Unix()
	liveMinute := time.Date(2026, 8, 17, 6, 0, 0, 0, cstLocation).Unix()
	now := time.Now().Unix()
	if err := m.storeDB.Create(&StabilityProblemClassificationMigration{
		ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
		FromTs: oldMinute, ThroughTs: oldMinute + 60, NextTs: oldMinute,
		Status: "running", CreatedAt: now, UpdatedAt: now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityProblemIngestState{
		BucketTs: oldMinute, TrafficClassVersion: userTrafficClassificationVersion, Complete: false,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		created int64
		content string
	}{{oldMinute + 1, "status_code=502 cold"}, {liveMinute + 1, "status_code=503 live"}} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(user_id,created_at,type,model_name,`+"`group`"+`,channel_id,content,request_id)
			VALUES (1,?,5,'gpt-test','codex',33,?,'customer')`, row.created, row.content); err != nil {
			t.Fatal(err)
		}
	}

	m.problemLastSuccess.Store(777)
	if _, err := m.sampleStabilityProblems(context.Background(), liveMinute, liveMinute+60); err != nil {
		t.Fatal(err)
	}
	var liveRows, coldRows int64
	if err := m.storeDB.Model(&StabilityProblemSample{}).Where("bucket_ts = ?", liveMinute).Count(&liveRows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&StabilityProblemSample{}).Where("bucket_ts = ?", oldMinute).Count(&coldRows).Error; err != nil {
		t.Fatal(err)
	}
	if liveRows != 1 || coldRows != 0 || m.problemLiveThrough.Load() != liveMinute+60 {
		t.Fatalf("cold cursor hijacked live lane: live=%d cold=%d through=%d", liveRows, coldRows, m.problemLiveThrough.Load())
	}
	var before StabilityProblemClassificationMigration
	if err := m.storeDB.First(&before, 1).Error; err != nil || before.NextTs != oldMinute {
		t.Fatalf("live lane advanced cold cursor: state=%+v err=%v", before, err)
	}
	liveSuccess := m.problemLastSuccess.Load()

	if _, err := m.sampleStabilityProblemMigration(context.Background(), 60); err != nil {
		t.Fatal(err)
	}
	var after StabilityProblemClassificationMigration
	if err := m.storeDB.First(&after, 1).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&StabilityProblemSample{}).Where("bucket_ts = ?", oldMinute).Count(&coldRows).Error; err != nil {
		t.Fatal(err)
	}
	if after.Status != "complete" || after.NextTs != oldMinute+60 || coldRows != 1 {
		t.Fatalf("cold migration did not advance independently: state=%+v rows=%d", after, coldRows)
	}
	if got := m.problemLastSuccess.Load(); got != liveSuccess {
		t.Fatalf("cold migration manufactured live success: %d", got)
	}
}

func TestStabilityProblemPendingUpgradeReplacesOldVersionCursorAndStage(t *testing.T) {
	m := newStabilityTestMonitor(t)
	bucket := time.Date(2026, 3, 1, 4, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&StabilityProblemIngestState{
		BucketTs: bucket, TrafficClassVersion: userTrafficClassificationVersion - 1,
		Complete: true, LastCreatedAt: bucket + 59, LastID: 99, RowsScanned: 5001,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityProblemStage{
		BucketTs: bucket, TrafficClassVersion: userTrafficClassificationVersion - 1,
		Source: "newapi", SignatureHash: "old", ChannelID: 1, ModelName: "m", Grp: "g", Count: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.ensureProblemWindowPending(bucket, bucket+60, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	var state StabilityProblemIngestState
	if err := m.storeDB.First(&state, "bucket_ts = ?", bucket).Error; err != nil {
		t.Fatal(err)
	}
	var stages int64
	if err := m.storeDB.Model(&StabilityProblemStage{}).Where("bucket_ts = ?", bucket).Count(&stages).Error; err != nil {
		t.Fatal(err)
	}
	if state.TrafficClassVersion != userTrafficClassificationVersion || state.Complete ||
		state.LastCreatedAt != 0 || state.LastID != 0 || state.RowsScanned != 0 || stages != 0 {
		t.Fatalf("old cursor/stage blocked v5 paging: state=%+v stages=%d", state, stages)
	}
}

func TestCompactStabilityReportKeepsSummaryButDefersNestedDetails(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&[]StabilityHourSample{
		{HourTs: day, ChannelID: 1, ModelName: "m1", Grp: "g", Success: 9, Failed: 1},
		{HourTs: day, ChannelID: 2, ModelName: "m2", Grp: "g", Success: 8, Failed: 2},
	}).Error; err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: day, ToTs: day + 24*3600}
	compact, err := m.buildStabilityReportWithDetails(context.Background(), scope, day+24*3600, false)
	if err != nil {
		t.Fatal(err)
	}
	full, err := m.buildStabilityReportWithDetails(context.Background(), scope, day+24*3600, true)
	if err != nil {
		t.Fatal(err)
	}
	if compact.Summary.Requests != full.Summary.Requests || len(compact.Groups) != 1 || len(compact.Groups[0].Channels) != 2 {
		t.Fatalf("compact report changed summary semantics: compact=%+v full=%+v", compact.Summary, full.Summary)
	}
	if compact.Groups[0].ModelCount != 2 || len(compact.Groups[0].Daily) != 1 || len(compact.Groups[0].Timeline) == 0 {
		t.Fatalf("compact report lost first-screen data: %+v", compact.Groups[0])
	}
	for _, channel := range compact.Groups[0].Channels {
		if channel.ModelCount != 1 || len(channel.Daily) != 0 || len(channel.Timeline) != 0 || len(channel.Models) != 0 {
			t.Fatalf("compact report eagerly returned nested channel details: %+v", channel)
		}
	}
	if len(full.Groups[0].Channels[0].Daily) == 0 || len(full.Groups[0].Channels[0].Timeline) == 0 || len(full.Groups[0].Channels[0].Models) == 0 {
		t.Fatalf("detail report did not retain nested data: %+v", full.Groups[0].Channels[0])
	}
}

func TestStabilityReportFallsBackToLegacyHoursWithoutDoubleCountingV5(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 13, 0, 0, 0, 0, cstLocation).Unix()

	// 模拟升级现场：第一天只有旧口径，第二天新旧事实同时存在。
	// 读取应展示第一天的旧数据，但第二天只能选 v5，不能叠加。
	if err := m.storeDB.Exec(`INSERT INTO stability_hour_samples
		(hour_ts,channel_id,model_name,grp,traffic_class_version,success,failed)
		VALUES (?,?,?,?,NULL,?,?)`, day, 1, "legacy-model", "g", 90, 10).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`INSERT INTO stability_hour_ingest_states
		(hour_ts,status,traffic_class_version,completed_at) VALUES (?,'complete',NULL,?)`, day, day+3600).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: day, ChannelID: 3, ModelName: "incomplete-v5-model", Grp: "g", Success: 888,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&[]StabilityRejectHour{
		{HourTs: day, Node: "n", Reason: "legacy", Model: "m", Grp: "g", Count: 7},
		{HourTs: day + 86400, Node: "n", Reason: "v5", Model: "m", Grp: "g", Count: 5},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`INSERT INTO stability_hour_samples
		(hour_ts,channel_id,model_name,grp,traffic_class_version,success,failed)
		VALUES (?,?,?,?,NULL,?,?)`, day+86400, 2, "stale-legacy-model", "g", 999, 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: day + 86400, ChannelID: 1, ModelName: "v5-model", Grp: "g", Success: 90, Failed: 10,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{
		HourTs: day + 86400, Status: "complete",
	}).Error; err != nil {
		t.Fatal(err)
	}

	report, err := m.buildStabilityReport(context.Background(), stabilityScope{
		FromTs: day, ToTs: day + 2*86400,
	}, day+2*86400)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Requests != 212 || report.Summary.Rejected != 12 {
		t.Fatalf("legacy fallback or v5 precedence is wrong: %+v", report.Summary)
	}
	if len(report.Groups) != 1 || len(report.Groups[0].Daily) != 2 {
		t.Fatalf("unexpected report groups: %+v", report.Groups)
	}
	first, second := report.Groups[0].Daily[0], report.Groups[0].Daily[1]
	if first.Requests != 107 || first.Stability == nil || math.Abs(*first.Stability-(90.0/107.0*100)) > 0.0001 {
		t.Fatalf("legacy fallback day is wrong: %+v", first)
	}
	if second.Requests != 105 || second.Stability == nil || math.Abs(*second.Stability-(90.0/105.0*100)) > 0.0001 {
		t.Fatalf("v5 day was not preferred over stale legacy facts: %+v", second)
	}
	if got := report.Meta.DataCoverage.LegacyFallbackHours; got != 1 {
		t.Fatalf("legacy fallback coverage=%d, want 1", got)
	}
	if got := report.Meta.DataCoverage.EffectiveHours; got != 2 {
		t.Fatalf("effective coverage=%d, want 2", got)
	}
}

func TestStabilityReportMarksDeletedChannelAsHistorical(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: day, ChannelID: 33, ModelName: "m", Grp: "g", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 33, Name: "deleted-route", Vendor: "OpenAI", Status: 1, DeletedAt: day + 3600}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 86400}, day+86400)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Groups) != 1 || len(report.Groups[0].Channels) != 1 || report.Groups[0].Channels[0].Current {
		t.Fatalf("deleted channel was presented as current: %+v", report.Groups)
	}
	if report.Groups[0].Channels[0].Name != "deleted-route" {
		t.Fatalf("deleted channel lost last snapshot metadata: %+v", report.Groups[0].Channels[0])
	}
}

func TestStabilityProblemCoverageReportsUncoveredHistory(t *testing.T) {
	m := newStabilityTestMonitor(t)
	from := time.Date(2026, 8, 5, 8, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&StabilityProblemIngestState{BucketTs: from + 60, Complete: true, CompletedAt: from + 180}).Error; err != nil {
		t.Fatal(err)
	}
	result, err := m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: from, ToTs: from + 180}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if result.CoverageComplete || result.PendingMinutes != 0 || result.UncoveredMinutes != 2 {
		t.Fatalf("missing history was reported as complete: %+v", result)
	}
}

func TestStabilityHealthRequiresFreshProblemCoverageWhenEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityEnabled = true
	m.cfg.SampleSeconds = 60
	m.cfg.StabilityProblemSampleSec = 300
	now := time.Now().Unix()
	m.lastRun.Store(now)

	record := func() stabilityHealthResponse {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/stability/health", nil)
		m.serveStabilityHealth(c)
		var got stabilityHealthResponse
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	if got := record(); got.Status != "degraded" || got.ProblemSamplerLastSuccess != 0 {
		t.Fatalf("uninitialized problem sampler must be degraded: %+v", got)
	}
	bucket := now/60*60 - 60
	if err := m.storeDB.Create(&StabilityProblemIngestState{
		BucketTs: bucket, Complete: true, UpdatedAt: now, CompletedAt: now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.problemLastSuccess.Store(now)
	m.problemLiveThrough.Store(bucket + 60)
	if got := record(); got.Status != "ok" || got.ProblemCoverageTo != bucket+60 || got.ProblemSamplerAgeSec > 2 {
		t.Fatalf("fresh complete problem coverage must be healthy: %+v", got)
	}
	m.problemLastSuccess.Store(now - 2000)
	if got := record(); got.Status != "degraded" {
		t.Fatalf("stale problem sampler must be degraded: %+v", got)
	}
}

func TestStabilityHealthUsesDurableLiveLagAndDisabledMigrationState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityEnabled = true
	m.cfg.StabilityProblemSampleSec = 300
	m.cfg.SampleSeconds = 60
	now := time.Now().Unix()
	m.lastRun.Store(now)
	cursor := StabilityProblemLiveCursor{
		ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
		NextTs: now/60*60 - 30*60, TargetThroughTs: now/60*60 - 10*60,
		Status: "running", LastSuccessAt: now, UpdatedAt: now,
	}
	if err := m.storeDB.Create(&cursor).Error; err != nil {
		t.Fatal(err)
	}
	migration := StabilityProblemClassificationMigration{
		ID: 1, TrafficClassVersion: userTrafficClassificationVersion,
		FromTs: cursor.NextTs - 86400, ThroughTs: cursor.NextTs, NextTs: cursor.NextTs - 86400,
		Status: "queued", CurrentSpanMinutes: 12, CreatedAt: now, UpdatedAt: now,
	}
	if err := m.storeDB.Create(&migration).Error; err != nil {
		t.Fatal(err)
	}
	m.problemLastSuccess.Store(now)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/stability/health", nil)
	m.serveStabilityHealth(c)
	var got stabilityHealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "degraded" || got.ProblemLiveLagSec != 20*60 || got.ProblemMigration.Status != "paused_disabled" {
		t.Fatalf("fresh page success hid durable live/cold backlog: %+v", got)
	}
}

func TestStabilityHealthIncludesHourlyClassificationMigration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityEnabled = false
	now := time.Now().Unix()
	job := StabilityBackfillJob{
		ID: "classification-progress", Kind: stabilityMigrationJobKind, Status: "running",
		TotalHours: 100, CompletedHours: 37, FailedHours: 2, RemainingHours: 61,
		ProgressPercent: 37, EstimatedRemainingSeconds: 1234, UpdatedAt: now,
		FailedHourTs: []int64{now - 3600}, LastError: "source detail must stay root-only",
	}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityBackfillJob{
		ID: "newer-manual-job", Kind: stabilityManualJobKind, Status: "complete",
		TotalHours: 999, CompletedHours: 999, ProgressPercent: 100, UpdatedAt: now + 60,
	}).Error; err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/stability/health", nil)
	m.serveStabilityHealth(c)
	var got stabilityHealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(w.Body.String(), "failed_hour_ts") || strings.Contains(w.Body.String(), "source detail") {
		t.Fatalf("frequently-polled health response leaked unbounded/root-only job details: %s", w.Body.String())
	}
	if got.HourlyMigration == nil || got.HourlyMigration.Status != "running" ||
		got.HourlyMigration.CompletedHours != 37 || got.HourlyMigration.TotalHours != 100 ||
		got.HourlyMigration.FailedHours != 2 || got.HourlyMigration.ProgressPercent != 37 {
		t.Fatalf("hourly migration progress missing from local health endpoint: %+v", got.HourlyMigration)
	}
}

func TestStabilityHealthIncludesNginxCollectorState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityEnabled = false
	m.cfg.SampleSeconds = 60
	m.cfg.NginxEnabled = true
	m.cfg.NginxAllowedNodes = []string{"master", "slave"}
	now := time.Now().Unix()
	m.lastRun.Store(now)
	if err := m.storeDB.Create(&NginxSourceState{
		Node: "master", LastEventTs: now, LastIngestTs: now, BacklogKnown: true, BacklogBytes: 512,
		CursorDiscontinuities: 1, LastCursorDiscontinuityAt: now, DiscardedLines: 2, LastDiscardedAt: now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	record := func() stabilityHealthResponse {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/stability/health", nil)
		m.serveStabilityHealth(c)
		var got stabilityHealthResponse
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := record(); got.Status != "degraded" || got.NginxSourceCount != 2 || got.NginxUnhealthySources != 2 ||
		got.NginxBacklogBytes != 512 || got.NginxCursorDiscontinuities != 1 || got.NginxDiscardedLines != 2 || got.NginxRecentDataLossSources != 1 {
		t.Fatalf("缺少 slave 必须在健康接口客观显示降级: %+v", got)
	}
	if err := m.storeDB.Create(&NginxSourceState{Node: "slave", LastEventTs: now, LastIngestTs: now, BacklogKnown: true}).Error; err != nil {
		t.Fatal(err)
	}
	if got := record(); got.Status != "degraded" || got.NginxSourceCount != 2 || got.NginxUnhealthySources != 1 ||
		got.NginxRecentDataLossSources != 1 {
		t.Fatalf("新鲜心跳不能掩盖近期游标断裂或丢行: %+v", got)
	}
	if err := m.storeDB.Model(&NginxSourceState{}).Where("node = ?", "master").Updates(map[string]any{
		"backlog_bytes": 0, "last_cursor_discontinuity_at": now - nginxRecentDataLossWindowSec - 1,
		"last_discarded_at": now - nginxRecentDataLossWindowSec - 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if got := record(); got.Status != "ok" || got.NginxSourceCount != 2 || got.NginxUnhealthySources != 0 || got.NginxBacklogUnknown != 0 {
		t.Fatalf("历史累计异常超过观察窗后不应永久保持降级: %+v", got)
	}
	if err := m.storeDB.Model(&NginxSourceState{}).Where("node = ?", "master").Updates(map[string]any{
		"backlog_bytes": nginxBacklogWarnBytes, "last_event_ts": now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if got := record(); got.Status != "degraded" || got.NginxLargeBacklogSources != 1 || got.NginxUnhealthySources != 1 {
		t.Fatalf("大积压必须使健康状态降级: %+v", got)
	}
}

func TestRefreshChannelsPersistsOfficialTypeVendorWithoutSecrets(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	if _, err := m.prodDB.Exec("CREATE TABLE channels (id INTEGER PRIMARY KEY,name TEXT,type INTEGER,status INTEGER,`group` TEXT,models TEXT,base_url TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO channels (id,name,type,status,`group`,models,base_url) VALUES (33,'route-a',14,1,'claude','claude-opus','https://temp.last-api.ai/v1')"); err != nil {
		t.Fatal(err)
	}
	m.refreshChannels()
	var snap ChannelSnap
	if err := m.storeDB.First(&snap, "id = ?", 33).Error; err != nil {
		t.Fatal(err)
	}
	if snap.Name != "route-a" || snap.Type != 14 || snap.Vendor != "Anthropic" || snap.Status != 1 {
		t.Fatalf("channel snapshot=%+v", snap)
	}
	if snap.BaseDomain != "last-api.ai" {
		t.Fatalf("base domain=%q", snap.BaseDomain)
	}
	if snap.BaseHost != "temp.last-api.ai" {
		t.Fatalf("base host=%q", snap.BaseHost)
	}
}

func TestStabilityComparisonUsesSameClockTimeOnPreviousCalendarDay(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	rows := []StabilityHourSample{
		{HourTs: day + 10*3600, ChannelID: 1, ModelName: "m", Grp: "g", Success: 90, Failed: 10},
		{HourTs: day - 86400 + 10*3600, ChannelID: 1, ModelName: "m", Grp: "g", Success: 80, Failed: 20},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	states := make([]StabilityHourIngestState, 0, 12)
	for hour := int64(0); hour < 12; hour++ {
		states = append(states, StabilityHourIngestState{HourTs: day - 86400 + hour*3600, Status: "complete"})
	}
	if err := m.storeDB.Create(&states).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 12*3600}, day+12*3600)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Meta.ComparisonAvailable || !report.Meta.ComparisonCoverage.Complete || report.Previous.Requests != 100 || report.DeltaPP == nil || math.Abs(*report.DeltaPP-10) > 0.0001 {
		t.Fatalf("calendar comparison=%+v delta=%v", report.Previous, report.DeltaPP)
	}
}

func TestStabilityComparisonDoesNotPresentPartialPreviousPeriod(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: day - 86400, ChannelID: 1, ModelName: "m", Grp: "g", Success: 100}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 12*3600}, day+12*3600)
	if err != nil {
		t.Fatal(err)
	}
	if report.Meta.ComparisonAvailable || report.Meta.ComparisonCoverage.Complete || report.Meta.ComparisonCoverage.MissingHours != 12 {
		t.Fatalf("有部分历史行但无完整台账时不得展示环比: %+v", report.Meta)
	}
}

func TestStabilityReadErrorSeparatesClientCancelTimeoutAndServerError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"client_cancel", context.Canceled, 499},
		{"query_timeout", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"server_error", errors.New("sqlite broken"), http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			writeStabilityReadError(c, tc.err)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func TestStabilityEqualRequestRankingsAreDeterministic(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	rows := []StabilityHourSample{
		{HourTs: day, ChannelID: 2, ModelName: "z-model", Grp: "z-group", Success: 1},
		{HourTs: day, ChannelID: 1, ModelName: "a-model", Grp: "a-group", Success: 1},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 3600}, day+3600)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Groups) != 2 || report.Groups[0].Name != "a-group" || report.Rankings.Models[0].Name != "a-model" || report.Rankings.Channels[0].ID != 1 {
		t.Fatalf("同请求数排序必须有稳定次序: groups=%+v models=%+v channels=%+v", report.Groups, report.Rankings.Models, report.Rankings.Channels)
	}
}

func TestStabilityProblemShareUsesAllCapturedRowsNotOnlyTopN(t *testing.T) {
	m := newStabilityTestMonitor(t)
	from := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	rows := []StabilityProblemSample{
		{BucketTs: from, Source: "newapi", SignatureHash: "a", ChannelID: 1, ModelName: "m", Grp: "g", Code: "500", Message: "a", Count: 9, FirstTs: from, LastTs: from},
		{BucketTs: from, Source: "newapi", SignatureHash: "b", ChannelID: 1, ModelName: "m", Grp: "g", Code: "502", Message: "b", Count: 1, FirstTs: from, LastTs: from},
	}
	if err := m.upsertStabilityProblems(rows); err != nil {
		t.Fatal(err)
	}
	result, err := m.queryStabilityProblems(context.Background(), stabilityScope{FromTs: from, ToTs: from + 3600}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Truncated || result.CapturedTotal != 10 || len(result.Problems) != 1 || math.Abs(result.Problems[0].SharePct-90) > 0.0001 {
		t.Fatalf("top-N denominator is not exact: %+v", result)
	}
}

func TestStabilityRetentionPrunesAllLocalTables(t *testing.T) {
	m := newStabilityTestMonitor(t)
	cutoff := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	old, fresh := cutoff-3600, cutoff+3600
	if err := m.storeDB.Create(&[]StabilityHourSample{
		{HourTs: old, ChannelID: 1, ModelName: "old", Grp: "g", Success: 1},
		{HourTs: fresh, ChannelID: 1, ModelName: "fresh", Grp: "g", Success: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&[]StabilityRejectHour{
		{HourTs: old, Node: "n", Reason: "old", Model: "m", Grp: "g", Count: 1},
		{HourTs: fresh, Node: "n", Reason: "fresh", Model: "m", Grp: "g", Count: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&[]StabilityProblemSample{
		{BucketTs: old, Source: "newapi", SignatureHash: "old", ChannelID: 1, ModelName: "m", Grp: "g", Message: "old", Count: 1},
		{BucketTs: fresh, Source: "newapi", SignatureHash: "fresh", ChannelID: 1, ModelName: "m", Grp: "g", Message: "fresh", Count: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneStabilityOlderThan(cutoff); err != nil {
		t.Fatal(err)
	}
	for name, model := range map[string]any{
		"hour": &StabilityHourSample{}, "reject": &StabilityRejectHour{}, "problem": &StabilityProblemSample{},
	} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s rows=%d want 1", name, count)
		}
	}
}

func TestStabilityRetentionCutoffMatchesFinalizedCoverageBoundary(t *testing.T) {
	now := time.Date(2026, 8, 28, 21, 23, 45, 0, cstLocation).Unix()
	finalizedTo := finalizedStabilityHourTo(now)
	got := stabilityRetentionCutoff(now, 181)
	want := finalizedTo - 181*86400
	if got != want {
		t.Fatalf("retention cutoff=%s want finalized coverage boundary %s",
			time.Unix(got, 0).In(cstLocation), time.Unix(want, 0).In(cstLocation))
	}
	if raw := now - 181*86400; got == raw || raw-got != 23*60+45 {
		t.Fatalf("retention unexpectedly followed wall clock: cutoff=%d raw=%d", got, raw)
	}
	if got := stabilityRetentionCutoff(now, 0); got != 0 {
		t.Fatalf("disabled retention cutoff=%d want 0", got)
	}
}

func TestStabilityProblemIntervalBounds(t *testing.T) {
	if got := stabilityProblemIntervalSeconds(0); got != 300 {
		t.Fatalf("zero interval=%d want 300", got)
	}
	if got := stabilityProblemIntervalSeconds(30); got != 300 {
		t.Fatalf("short interval=%d want 300", got)
	}
	if got := stabilityProblemIntervalSeconds(600); got != 600 {
		t.Fatalf("normal interval=%d want 600", got)
	}
	if got := stabilityProblemIntervalSeconds(7200); got != 3600 {
		t.Fatalf("long interval=%d want 3600", got)
	}
}

func TestStabilityDimensionGuardFailsClosedInsteadOfReturningPartialTotals(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	rows := make([]StabilityHourSample, 0, 2001)
	for i := 0; i < 2001; i++ {
		rows = append(rows, StabilityHourSample{HourTs: day, ChannelID: i + 1, ModelName: fmt.Sprintf("m-%04d", i), Grp: "g", Success: 1})
	}
	if err := m.storeDB.CreateInBatches(rows, 200).Error; err != nil {
		t.Fatal(err)
	}
	_, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 3600}, day+3600)
	if err == nil || !strings.Contains(err.Error(), "为保证准确性") {
		t.Fatalf("维度超限应拒绝返回部分统计，err=%v", err)
	}
}

func TestStabilityPageIntegrationKeepsExistingMonitorAndPortalBoundaries(t *testing.T) {
	for _, id := range []string{`id="tab-usage"`, `id="tab-model"`, `id="tab-server"`, `id="tab-stability"`, `id="tab-channels"`} {
		if !strings.Contains(pageHTML, id) {
			t.Fatalf("Monitor page missing %s", id)
		}
	}
	order := []string{`data-tab="usage"`, `data-tab="channels"`, `data-tab="stability"`, `data-tab="model"`, `data-tab="server"`}
	last := -1
	for _, marker := range order {
		at := strings.Index(pageHTML, marker)
		if at <= last {
			t.Fatalf("Monitor navigation order incorrect at %s", marker)
		}
		last = at
	}
	if !strings.Contains(pageHTML, "usageRefresh(true)") || !strings.Contains(pageHTML, "resizeModelCharts") || !strings.Contains(pageHTML, "loadInfra") {
		t.Fatal("新页面接入时不应移除原有三页的关键交互")
	}
	if strings.Contains(portalHTML, "stability/report") || strings.Contains(portalHTML, "tab-stability") || strings.Contains(portalHTML, "channels/report") || strings.Contains(portalHTML, "tab-channels") {
		t.Fatal("Usage Portal 不应接入 Monitor 稳定性报表或渠道管理")
	}
}

func TestWithdrawnClientEvidenceFeatureIsNotExposed(t *testing.T) {
	for _, forbidden := range []string{
		"/internal/client-outcomes",
		"/stability/delivery-evidence",
		"/stability/delivery-timeline",
		"/stability/delivery-issues",
		"MONITOR_CLIENT_EVIDENCE_",
		"可选客户端技术结果",
		"未接入受控客户端",
	} {
		if strings.Contains(pageHTML, forbidden) || strings.Contains(string(stabilityJS), forbidden) {
			t.Fatalf("已撤回的客户端证据功能仍暴露在前端: %q", forbidden)
		}
	}

	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	router := gin.New()
	m.RegisterRoutes(router)
	for _, route := range router.Routes() {
		if strings.Contains(route.Path, "client-outcomes") || strings.Contains(route.Path, "delivery-evidence") || strings.Contains(route.Path, "delivery-timeline") || strings.Contains(route.Path, "delivery-issues") {
			t.Fatalf("已撤回的客户端证据路由仍存在: %s %s", route.Method, route.Path)
		}
	}
}

func TestStabilityPageIncludesBusinessDateShortcuts(t *testing.T) {
	for _, shortcut := range []string{
		`data-stability-hours="24">近 24 小时`,
		`data-stability-preset="today">今天`,
		`data-stability-preset="yesterday">昨天`,
		`data-stability-preset="week">本周`,
	} {
		if !strings.Contains(pageHTML, shortcut) {
			t.Fatalf("稳定性报表缺少日期快捷项 %q", shortcut)
		}
	}
	js := string(stabilityJS)
	for _, marker := range []string{`data-stability-hours`, `q.set('hours',String(st.hours))`, `data-stability-preset`, `stPresetRange`, `timeZone:'Asia/Shanghai'`, `hours:0,days:7`} {
		if !strings.Contains(js, marker) {
			t.Fatalf("稳定性报表快捷日期交互缺少 %q", marker)
		}
	}
	if strings.Contains(portalHTML, "data-stability-preset") {
		t.Fatal("Usage Portal 不应接入 Monitor 稳定性报表日期快捷项")
	}
	if !strings.Contains(pageHTML, `class="active" data-stability-days="7">近 7 天`) {
		t.Fatal("稳定性报表默认时间窗口必须是近 7 天")
	}
}

func TestStabilityPageUsesCompactCoverageStatus(t *testing.T) {
	js := string(stabilityJS)
	for _, marker := range []string{"latest_hour_pending", "正常汇总中", "小时待补", "小时为旧口径参考", "v5 重签后将自动替换"} {
		if !strings.Contains(js, marker) {
			t.Fatalf("稳定性页缺少分级完整性状态 %q", marker)
		}
	}
	if strings.Contains(js, "当前日期范围的小时数据完整率为") {
		t.Fatal("正常尾部延迟不应再使用全宽红色告警")
	}
}

func TestStabilityEdgePageKeepsAccessAndErrorEvidenceSeparate(t *testing.T) {
	js := string(stabilityJS)
	for _, marker := range []string{
		"Nginx error 分类",
		"节点级旁证，不做时间邻近的伪精确请求关联",
		"入口 P95 / P99",
		"桶上界估算",
		"edgeSourceRows(sources,'access')",
		"edgeSourceRows(d.error_sources||[],'error')",
	} {
		if !strings.Contains(js, marker) {
			t.Fatalf("稳定性边缘旁证页缺少 %q", marker)
		}
	}
}

func TestMonitorResponsiveShellAndWideTablesStayAdminOnly(t *testing.T) {
	for _, marker := range []string{
		`id="monitorSidebarToggle"`,
		`class="monitor-nav-label"`,
		`class="mxwrap"><table id="usageMxTable"`,
		`class="mxwrap"><table id="usageMemMxTable"`,
		`monitor-table-scroll monitor-table-model"><table><thead id="thGroup"`,
		`monitor-table-scroll monitor-table-model"><table><thead id="thChannel"`,
		`monitor-table-scroll monitor-table-model"><table><thead id="thModel"`,
		`monitor-table-scroll monitor-table-server"><table><thead id="thInst"`,
		`monitor-table-scroll monitor-table-admin"><table><thead id="thGrp"`,
		`monitor-table-scroll monitor-table-admin"><table><thead id="thUsr"`,
		`function sizeUsageMatrix(table,dayCount,users)`,
	} {
		if !strings.Contains(pageHTML, marker) {
			t.Fatalf("Monitor 响应式布局缺少 %q", marker)
		}
	}
	css := string(stabilityCSS)
	for _, marker := range []string{
		`.monitor-shell.sidebar-collapsed{grid-template-columns:76px minmax(0,1fr)}`,
		`.monitor-shell.sidebar-collapsed .monitor-nav.tabs .tab::after`,
		`transition-delay:1s,1s,1s`,
		`.monitor-table-scroll{width:100%;max-width:100%;overflow-x:auto`,
		`.monitor-table-model>table{min-width:1120px}`,
		`.monitor-table-server>table{min-width:1180px}`,
		`.monitor-table-admin>table{min-width:980px}`,
	} {
		if !strings.Contains(css, marker) {
			t.Fatalf("Monitor 响应式样式缺少 %q", marker)
		}
	}
	if !strings.Contains(string(stabilityJS), `nexusapi-monitor-sidebar-collapsed`) {
		t.Fatal("侧栏收起状态没有持久化，刷新后会跳回展开态")
	}
	for _, marker := range []string{`item.dataset.navTooltip=text`, `item.removeAttribute('title')`, `item.setAttribute('aria-label',text)`} {
		if !strings.Contains(string(stabilityJS), marker) {
			t.Fatalf("收起侧栏的延迟文字说明缺少 %q", marker)
		}
	}
	for _, forbidden := range []string{`monitorSidebarToggle`, `nexusapi-monitor-sidebar-collapsed`} {
		if strings.Contains(portalHTML, forbidden) {
			t.Fatalf("Usage Portal 不应被 Monitor 侧栏改造影响: %q", forbidden)
		}
	}
}

func TestStabilityRangeUsesCSTDateBoundariesAndCapsRange(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Date(2026, 8, 5, 12, 30, 0, 0, cstLocation)
	check := func(rawURL string, wantFrom, wantTo time.Time) {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", rawURL, nil)
		s, err := stabilityRange(c, now, 90)
		if err != nil {
			t.Fatal(err)
		}
		if s.FromTs != wantFrom.Unix() || s.ToTs != wantTo.Unix() {
			t.Fatalf("%s range=[%v,%v], want [%v,%v]", rawURL, time.Unix(s.FromTs, 0), time.Unix(s.ToTs, 0), wantFrom, wantTo)
		}
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/stability/report?days=7", nil)
	s, err := stabilityRange(c, now, 90)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 30, 0, 0, 0, 0, cstLocation).Unix()
	if s.FromTs != want || s.ToTs != now.Unix() {
		t.Fatalf("range=[%v,%v]", time.Unix(s.FromTs, 0), time.Unix(s.ToTs, 0))
	}
	check("/stability/report?from=2026-08-05&to=2026-08-05", time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation), now)
	check("/stability/report?from=2026-08-04&to=2026-08-04", time.Date(2026, 8, 4, 0, 0, 0, 0, cstLocation), time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation))
	check("/stability/report?from=2026-08-03&to=2026-08-05", time.Date(2026, 8, 3, 0, 0, 0, 0, cstLocation), now)

	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/stability/report?from=2026-01-01&to=2026-08-05", nil)
	if _, err = stabilityRange(c, now, 90); err == nil {
		t.Fatal("expected >90 day error")
	}
}

func TestStabilityRangeUsesLast24CompletedHours(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Date(2026, 8, 12, 16, 37, 45, 0, cstLocation)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/stability/report?hours=24&group=codex-1.2x&channel=59", nil)

	scope, err := stabilityRange(c, now, 90)
	if err != nil {
		t.Fatal(err)
	}
	wantTo := time.Date(2026, 8, 12, 16, 0, 0, 0, cstLocation)
	wantFrom := wantTo.Add(-24 * time.Hour)
	if scope.FromTs != wantFrom.Unix() || scope.ToTs != wantTo.Unix() || scope.RangeHours != 24 || scope.Group != "codex-1.2x" || scope.ChannelID != 59 {
		t.Fatalf("range=%+v, want [%v,%v] with filters", scope, wantFrom, wantTo)
	}

	for _, rawURL := range []string{
		"/stability/report?hours=24&days=7",
		"/stability/report?hours=24&from=2026-08-11&to=2026-08-12",
		"/stability/report?hours=0",
		"/stability/report?hours=2161",
	} {
		w = httptest.NewRecorder()
		c, _ = gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", rawURL, nil)
		if _, err = stabilityRange(c, now, 90); err == nil {
			t.Fatalf("%s should be rejected", rawURL)
		}
	}
}

func TestStabilityQueriesUseLocalStoreOnly(t *testing.T) {
	m := newStabilityTestMonitor(t) // prodDB 故意为 nil
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	if _, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: day, ToTs: day + 12*3600}, day+12*3600); err != nil {
		t.Fatalf("local-only report failed: %v", err)
	}
}

// 代表性规模基准：90 天 × 12 渠道 × 每小时一行，页面读取仍只落在
// Monitor 本地 SQLite。基准不设脆弱的硬时间阈值，CI 可用 -bench 跟踪回归。
func BenchmarkStabilityReport90Days(b *testing.B) {
	m := &Monitor{cfg: Settings{RetentionDays: 7, StabilityEnabled: true, StabilityRetentionDays: 90}}
	if err := m.openStore(b.TempDir() + "/stability-bench.db"); err != nil {
		b.Fatal(err)
	}
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, cstLocation).Unix()
	rows := make([]StabilityHourSample, 0, 90*24*12)
	for hour := 0; hour < 90*24; hour++ {
		for channel := 1; channel <= 12; channel++ {
			rows = append(rows, StabilityHourSample{
				HourTs: start + int64(hour)*3600, ChannelID: channel,
				ModelName: fmt.Sprintf("model-%d", channel%4), Grp: fmt.Sprintf("group-%d", channel%5),
				Success: 98, Anomaly: 1, Failed: 1, Tokens: 1000, Quota: 500000, SumUseTime: 200,
			})
		}
	}
	if err := m.storeDB.CreateInBatches(rows, 200).Error; err != nil {
		b.Fatal(err)
	}
	scope := stabilityScope{FromTs: start, ToTs: start + 90*86400}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.buildStabilityReport(context.Background(), scope, scope.ToTs); err != nil {
			b.Fatal(err)
		}
	}
}
