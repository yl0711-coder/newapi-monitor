package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
)

func TestStabilityHourSQLUsesBoundedHalfOpenRangeAndSuccessOnlyUsage(t *testing.T) {
	q := stabilityHourSQL()
	if strings.Count(q, "?") != 2 || !strings.Contains(q, "created_at >= ? AND created_at < ?") {
		t.Fatalf("小时补数必须只有一个半开小时区间:\n%s", q)
	}
	for _, want := range []string{
		"SUM(CASE WHEN type=2 THEN prompt_tokens+completion_tokens END)",
		"SUM(CASE WHEN type=2 THEN quota END)",
		"is_channel_test",
		"channel_test_origin",
		"GROUP BY channel_id, model_name, grp, is_channel_test, channel_test_origin",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("小时补数 SQL 缺少 %q:\n%s", want, q)
		}
	}
	if strings.Contains(q, "SUM(quota)") {
		t.Fatal("错误日志的 quota 不得混入消费额")
	}
}

func TestStabilityRangeSQLHasHourBucketHardCapControlAndServerTimeout(t *testing.T) {
	q := stabilityRangeSQL()
	control := stabilityRangeControlSQL()
	for name, sqlText := range map[string]string{"detail": q, "control": control} {
		for _, want := range []string{"MAX_EXECUTION_TIME(8000)", "created_at DIV 3600", "created_at >= ? AND created_at < ?", "GROUP BY hour_ts"} {
			if !strings.Contains(sqlText, want) {
				t.Fatalf("%s SQL missing %q:\n%s", name, want, sqlText)
			}
		}
	}
	if !strings.Contains(q, "LIMIT 20001") {
		t.Fatalf("range SQL must fetch only cap+1 sentinel rows:\n%s", q)
	}
	if strings.Count(control, "user_requests") != 1 || !strings.Contains(control, "internal_requests") {
		t.Fatalf("control query must independently total both traffic classes:\n%s", control)
	}
	if got := normalizeStabilityMaxExecutionMS(20000); got != 8000 {
		t.Fatalf("server timeout must be capped at 8s, got=%d", got)
	}
}

func TestStabilityAdaptiveBatchNeedsThreeHealthyChunksAndDegradesOnRisk(t *testing.T) {
	result := stabilityRangeResult{Rows: 100, SourceQueries: 2, QueryDuration: 2 * time.Second}
	batch, healthy := 2, 0
	for i := 0; i < 2; i++ {
		batch, healthy = adaptStabilityBatchAfterSuccess(batch, healthy, result)
		if batch != 2 || healthy != i+1 {
			t.Fatalf("healthy chunk %d advanced too early: batch=%d healthy=%d", i+1, batch, healthy)
		}
	}
	batch, healthy = adaptStabilityBatchAfterSuccess(batch, healthy, result)
	if batch != 4 || healthy != 0 {
		t.Fatalf("third healthy chunk must advance 2→4: batch=%d healthy=%d", batch, healthy)
	}
	batch, healthy = adaptStabilityBatchAfterSuccess(12, 2, stabilityRangeResult{SourceQueries: 2, QueryDuration: 5 * time.Second})
	if batch != 6 || healthy != 0 {
		t.Fatalf("query >2s average must degrade 12→6: batch=%d healthy=%d", batch, healthy)
	}
	batch, healthy = adaptStabilityBatchAfterSuccess(6, 2, stabilityRangeResult{Rows: maxStabilityRowsPerRange * 8 / 10, SourceQueries: 2})
	if batch != 4 || healthy != 0 {
		t.Fatalf("near-cap rows must degrade 6→4: batch=%d healthy=%d", batch, healthy)
	}
	if nextStabilityBatchHours(12) != 12 {
		t.Fatal("adaptive range must never exceed 12 hours")
	}
}

func TestStabilityServerExecutionTimeoutTriggersAdaptiveFallback(t *testing.T) {
	err := &mysql.MySQLError{Number: 3024, Message: "maximum statement execution time exceeded"}
	if !stabilityRangeShouldFallback(err) {
		t.Fatal("MySQL MAX_EXECUTION_TIME error must shrink the range instead of pausing the whole job")
	}
	if stabilityRangeShouldFallback(&mysql.MySQLError{Number: 1045, Message: "access denied"}) {
		t.Fatal("authentication failures must not be misclassified as an adaptive range timeout")
	}
}

func TestFetchStabilityRangeEqualsSingleHoursAndChecksIndependentControls(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityBackfillDelayMS = -1
	m.cfg.StabilityBackfillSourceDutyPercent = 100
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	base := time.Date(2026, 8, 5, 8, 0, 0, 0, cstLocation).Unix()
	for _, stmt := range []struct {
		id, userID, hour, typ, quota, prompt, completion int64
		tokenName, content, requestID                    string
	}{
		{1, 2, 0, 2, 100, 7, 3, "customer", "ok", "r1"},
		{2, 2, 0, 5, 0, 0, 0, "customer", "status_code=503", "r2"},
		{3, 2, 2, 2, 80, 4, 4, "customer", "ok", "r3"},
		{4, 1, 2, 2, 60, 3, 3, "模型测试", "模型测试", ""},
	} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,use_time,`+"`group`"+`,token_id,token_name,content,other,request_id)
			VALUES (?,?,9,?,?,'range-test',?,?,?,2,'g',1,?,?,?,?)`,
			stmt.id, stmt.userID, base+stmt.hour*3600+stmt.id, stmt.typ, stmt.quota, stmt.prompt, stmt.completion,
			stmt.tokenName, stmt.content, `{}`, stmt.requestID); err != nil {
			t.Fatal(err)
		}
	}

	rangeResult, err := m.fetchStabilityRange(context.Background(), base, base+3*3600)
	if err != nil {
		t.Fatal(err)
	}
	if rangeResult.SourceQueries != stabilitySourceQueriesPerRange {
		t.Fatalf("range source queries=%d want=%d", rangeResult.SourceQueries, stabilitySourceQueriesPerRange)
	}
	for hour := base; hour < base+3*3600; hour += 3600 {
		single, err := m.fetchStabilityHour(context.Background(), hour)
		if err != nil {
			t.Fatal(err)
		}
		rangeHour := rangeResult.Hours[hour]
		rr, rt, rq := stabilityHourTotals(rangeHour.Users)
		sr, st, sq := stabilityHourTotals(single.Users)
		if rr != sr || rt != st || rq != sq || len(rangeHour.Users) != len(single.Users) {
			t.Fatalf("user range/single mismatch hour=%d range=%+v single=%+v", hour, rangeHour.Users, single.Users)
		}
		rr, rt, rq = channelTestHourTotals(rangeHour.InternalTests)
		sr, st, sq = channelTestHourTotals(single.InternalTests)
		if rr != sr || rt != st || rq != sq || len(rangeHour.InternalTests) != len(single.InternalTests) {
			t.Fatalf("internal range/single mismatch hour=%d range=%+v single=%+v", hour, rangeHour.InternalTests, single.InternalTests)
		}
	}
	if _, exists := rangeResult.Hours[base+3600]; exists {
		t.Fatal("zero-traffic hour should have no fabricated source row; runner must create its local proof")
	}

	badControls := map[int64]stabilitySourceControl{base: {UserRequests: 999}}
	if err := verifyStabilitySourceControls(base, base+3600, rangeResult.Hours, badControls); !errors.Is(err, errStabilityControlMismatch) {
		t.Fatalf("independent control mismatch must fail closed, got=%v", err)
	}
}

func TestStabilityRangeRunnerAdaptsAndPersistsZeroTrafficHours(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityBackfillEnabled = true
	m.cfg.StabilityBackfillDelayMS = -1
	m.cfg.StabilityBackfillSourceDutyPercent = 100
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	base := time.Date(2026, 8, 6, 0, 0, 0, 0, cstLocation).Unix()
	for id, hour := range []int64{0, 7} {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,use_time,`+"`group`"+`,token_id,token_name,content,other,request_id)
			VALUES (?,2,9,?,2,'range-run',100,5,5,1,'g',1,'customer','ok','{}',?)`, id+1, base+hour*3600+1, "r"+strconv.Itoa(id)); err != nil {
			t.Fatal(err)
		}
	}
	job := StabilityBackfillJob{ID: "range-zero", Kind: stabilityManualJobKind, FromTs: base, ToTs: base + 8*3600,
		Status: "queued", TotalHours: 8, CurrentBatchHours: 2, UpdatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfill(context.Background(), job.ID)
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != "complete" || job.CompletedHours != 8 || job.FailedHours != 0 || job.ProgressPercent != 100 || job.ProcessedPercent != 100 {
		t.Fatalf("range job did not fully publish: %+v", job)
	}
	// Three healthy 2h chunks are required before the next 4h batch. Eight
	// hours therefore use four chunks, each with detail+control SQL.
	if job.SourceQueries != 4*stabilitySourceQueriesPerRange {
		t.Fatalf("source queries=%d want=%d", job.SourceQueries, 4*stabilitySourceQueriesPerRange)
	}
	var states []StabilityHourIngestState
	if err := m.storeDB.Where("hour_ts >= ? AND hour_ts < ?", base, base+8*3600).Order("hour_ts").Find(&states).Error; err != nil {
		t.Fatal(err)
	}
	if len(states) != 8 {
		t.Fatalf("every hour, including zeros, needs a proof: %+v", states)
	}
	if states[1].Status != "complete" || states[1].Rows != 0 || states[1].Requests != 0 {
		t.Fatalf("zero hour proof invalid: %+v", states[1])
	}
}

func TestStabilityRangeFallbackIsolatesBadHourContinuesAndExplicitRetryRecovers(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityBackfillEnabled = true
	m.cfg.StabilityBackfillDelayMS = -1
	m.cfg.StabilityBackfillSourceDutyPercent = 100
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	base := time.Date(2026, 8, 7, 0, 0, 0, 0, cstLocation).Unix()
	badHour := base + 13*3600
	job := StabilityBackfillJob{ID: "fallback-isolate", Kind: stabilityManualJobKind, FromTs: base, ToTs: base + 14*3600,
		Status: "queued", TotalHours: 14, CurrentBatchHours: 12, UpdatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	var spans []int
	badCalls := 0
	fetch := func(_ context.Context, fromTs, toTs int64) (stabilityRangeResult, error) {
		span := int((toTs - fromTs) / 3600)
		spans = append(spans, span)
		result := stabilityRangeResult{Hours: map[int64]stabilityHourTraffic{}, SourceQueries: 1, QueryDuration: time.Millisecond}
		if len(spans) <= 4 {
			return result, fmt.Errorf("%w: scripted", errStabilityRangeTooLarge)
		}
		if fromTs == badHour && span == 1 {
			badCalls++
			return result, fmt.Errorf("%w: pathological hour", errStabilityRangeTooLarge)
		}
		return result, nil
	}
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfillWithFetcher(context.Background(), job.ID, fetch)
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(spans) < 5 || spans[0] != 12 || spans[1] != 6 || spans[2] != 4 || spans[3] != 2 || spans[4] != 1 {
		t.Fatalf("fallback sequence=%v want prefix [12 6 4 2 1]", spans)
	}
	if badCalls != 2 { // initial isolation + mandatory final retry
		t.Fatalf("isolated hour calls=%d want=2", badCalls)
	}
	if job.Status != "partial" || job.CompletedHours != 13 || job.FailedHours != 1 || len(job.FailedHourTs) != 1 || job.FailedHourTs[0] != badHour {
		t.Fatalf("bad hour must be isolated without pausing healthy history: %+v", job)
	}
	if job.ProcessedPercent != 100 || job.ProgressPercent >= 100 || job.FailedPercent <= 0 {
		t.Fatalf("progress must distinguish complete and failed: %+v", job)
	}

	// Explicit retry clears only the failed set. The real fake-source range and
	// independent control query now prove that zero-traffic hour complete.
	if _, err := m.retryStabilityBackfill(job.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
			t.Fatal(err)
		}
		if job.Status == "complete" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Status != "complete" || job.CompletedHours != 14 || job.FailedHours != 0 || len(job.FailedHourTs) != 0 {
		t.Fatalf("explicit retry did not recover isolated hour: %+v", job)
	}
}

func TestBackgroundSourceEnforcesGlobalStartSpacingAndLowDuty(t *testing.T) {
	m := &Monitor{cfg: Settings{BackgroundSourceMinStartIntervalMS: 30}}
	release, err := m.acquireBackgroundSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first := m.backgroundSourceLastStart.Load()
	release()
	release, err = m.acquireBackgroundSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second := m.backgroundSourceLastStart.Load()
	release()
	if elapsed := time.Duration(second - first); elapsed < 25*time.Millisecond {
		t.Fatalf("background query starts too close: %v", elapsed)
	}

	m = &Monitor{}
	release, err = m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m.deferBackgroundSourceStart(300 * time.Millisecond)
	release()
	started := time.Now()
	// Stability duty is low-lane-only: a Tail/sampler query may use the next
	// globally-spaced window instead of inheriting the bulk cooldown.
	release, err = m.acquireBackgroundSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("high source query inherited low duty window: %v", elapsed)
	}
	release, err = m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	if elapsed := time.Since(started); elapsed < 250*time.Millisecond {
		t.Fatalf("low source duty not-before was bypassed: %v", elapsed)
	}
}

func TestStabilityBackfillResumeSkipsAlreadyCompleteHours(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityBackfillDelayMS = -1
	m.cfg.StabilityBackfillSourceDutyPercent = 100
	base := time.Date(2026, 8, 8, 0, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&[]StabilityHourIngestState{
		{HourTs: base + 2*3600, Status: "complete"},
		{HourTs: base + 3*3600, Status: "complete"},
	}).Error; err != nil {
		t.Fatal(err)
	}
	job := StabilityBackfillJob{ID: "resume-skip", FromTs: base, ToTs: base + 4*3600,
		Status: "queued", TotalHours: 4, CurrentBatchHours: 2, UpdatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	var ranges [][2]int64
	fetch := func(_ context.Context, fromTs, toTs int64) (stabilityRangeResult, error) {
		ranges = append(ranges, [2]int64{fromTs, toTs})
		return stabilityRangeResult{Hours: map[int64]stabilityHourTraffic{}, SourceQueries: 1, QueryDuration: time.Millisecond}, nil
	}
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfillWithFetcher(context.Background(), job.ID, fetch)
	if len(ranges) != 1 || ranges[0] != [2]int64{base, base + 2*3600} {
		t.Fatalf("resume re-read completed hours: %v", ranges)
	}
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != "complete" || job.CompletedHours != 4 {
		t.Fatalf("resume did not finish: %+v", job)
	}
}

func TestReplaceStabilityHourSeparatesUserTrafficAndInternalTestCost(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	users := []StabilityHourSample{{
		HourTs: hour, ChannelID: 37, ModelName: "claude-sonnet-5", Grp: "claude-0.5x",
		Success: 9, Tokens: 900, Quota: 9000,
	}}
	tests := []ChannelTestHourSample{{
		HourTs: hour, ChannelID: 37, ModelName: "claude-sonnet-5", Grp: "internal", Origin: "scheduled",
		Requests: 6, Tokens: 60, Quota: 600,
	}}
	if err := m.replaceStabilityHourTraffic(hour, users, tests, StabilityHourIngestState{JobID: "split"}); err != nil {
		t.Fatal(err)
	}

	var userTotal, testTotal struct{ Requests, Tokens, Quota int64 }
	if err := m.storeDB.Raw(`SELECT COALESCE(SUM(success+anomaly+failed),0) requests,
		COALESCE(SUM(tokens),0) tokens,COALESCE(SUM(quota),0) quota
		FROM stability_hour_samples WHERE hour_ts=? AND traffic_class_version=?`,
		hour, userTrafficClassificationVersion).Scan(&userTotal).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Raw(`SELECT COALESCE(SUM(requests),0) requests,
		COALESCE(SUM(tokens),0) tokens,COALESCE(SUM(quota),0) quota
		FROM channel_test_hour_samples WHERE hour_ts=?`, hour).Scan(&testTotal).Error; err != nil {
		t.Fatal(err)
	}
	if userTotal.Requests != 9 || userTotal.Tokens != 900 || userTotal.Quota != 9000 {
		t.Fatalf("用户流量被内部测试污染: %+v", userTotal)
	}
	if testTotal.Requests != 6 || testTotal.Tokens != 60 || testTotal.Quota != 600 {
		t.Fatalf("内部测试成本没有独立保存: %+v", testTotal)
	}
	var state StabilityHourIngestState
	if err := m.storeDB.First(&state, "hour_ts=?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if state.Requests != 9 || state.InternalTestRequests != 6 || state.TrafficClassVersion != userTrafficClassificationVersion {
		t.Fatalf("小时控制台账未分别核验两类流量: %+v", state)
	}
}

func TestReplaceStabilityHourIsAtomicIdempotentAndMarksZeroTrafficComplete(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	first := []StabilityHourSample{{HourTs: hour, ChannelID: 1, ModelName: "m1", Grp: "g1", Success: 3, Tokens: 10, Quota: 20}}
	if err := m.replaceStabilityHour(hour, first, StabilityHourIngestState{JobID: "test", Attempts: 1}); err != nil {
		t.Fatal(err)
	}
	second := []StabilityHourSample{{HourTs: hour, ChannelID: 2, ModelName: "m2", Grp: "g2", Failed: 2}}
	if err := m.replaceStabilityHour(hour, second, StabilityHourIngestState{JobID: "test", Attempts: 2}); err != nil {
		t.Fatal(err)
	}
	var rows []StabilityHourSample
	if err := m.storeDB.Where("hour_ts = ?", hour).Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChannelID != 2 || rows[0].Failed != 2 {
		t.Fatalf("重复补数必须完整替换而非累加: %+v", rows)
	}
	if err := m.replaceStabilityHour(hour+3600, nil, StabilityHourIngestState{JobID: "test", Attempts: 1}); err != nil {
		t.Fatal(err)
	}
	var state StabilityHourIngestState
	if err := m.storeDB.First(&state, "hour_ts = ?", hour+3600).Error; err != nil {
		t.Fatal(err)
	}
	if state.Status != "complete" || state.Rows != 0 || state.Requests != 0 {
		t.Fatalf("零流量小时也必须有完整性凭证: %+v", state)
	}
}

func TestReplaceStabilityHourRejectsContradictoryZeroTraffic(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 9, 4, 3, 0, 0, 0, time.UTC).Unix()
	if err := m.storeDB.Create(&MetricSample{
		BucketTs: hour + 60, ChannelID: 7, ModelName: "gpt", Grp: "codex", Success: 3,
	}).Error; err != nil {
		t.Fatal(err)
	}
	previous := StabilityHourSample{HourTs: hour, ChannelID: 7, ModelName: "gpt", Grp: "codex", Success: 3}
	if err := m.storeDB.Create(&previous).Error; err != nil {
		t.Fatal(err)
	}
	err := m.replaceStabilityHour(hour, nil, StabilityHourIngestState{JobID: "source-cutover"})
	if !errors.Is(err, errStabilityZeroContradiction) {
		t.Fatalf("positive minute facts must reject a signed zero source hour: %v", err)
	}
	var count int64
	if err := m.storeDB.Model(&StabilityHourSample{}).Where("hour_ts = ?", hour).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("contradictory replacement must roll back existing facts, rows=%d", count)
	}
}

func TestStabilityCoverageAndRepairMapRejectContradictoryZeroTraffic(t *testing.T) {
	m := newStabilityTestMonitor(t)
	from := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC).Unix()
	if err := m.storeDB.Create(&[]StabilityHourIngestState{
		{HourTs: from, Status: "complete", Requests: 0},
		{HourTs: from + 3600, Status: "complete", Requests: 0},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&MetricSample{
		BucketTs: from + 60, ChannelID: 9, ModelName: "gpt", Grp: "codex", Success: 2,
	}).Error; err != nil {
		t.Fatal(err)
	}
	cov := m.stabilityDataCoverage(context.Background(), from, from+2*3600, from+4*3600)
	if cov.CompletedHours != 1 || cov.MissingHours != 1 || cov.Complete {
		t.Fatalf("contradictory signed zero must be a visible coverage gap: %+v", cov)
	}
	complete, err := m.completeStabilityHours(from, from+2*3600)
	if err != nil {
		t.Fatal(err)
	}
	if complete[from] || !complete[from+3600] {
		t.Fatalf("automatic repair map did not isolate the contradictory hour: %+v", complete)
	}
}

func TestStabilityCoverageCountsLedgerNotSamplePresence(t *testing.T) {
	m := newStabilityTestMonitor(t)
	from := time.Date(2026, 8, 5, 0, 0, 0, 0, cstLocation).Unix()
	// 有数据但没有完整台账，不得冒充已覆盖。
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: from, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	cov := m.stabilityDataCoverage(context.Background(), from, from+3*3600, from+5*3600)
	if cov.CompletedHours != 0 || cov.MissingHours != 3 || cov.Complete {
		t.Fatalf("样本存在不能代替覆盖台账: %+v", cov)
	}
	states := []StabilityHourIngestState{
		{HourTs: from, Status: "complete"},
		{HourTs: from + 3600, Status: "complete"},
		{HourTs: from + 2*3600, Status: "failed"},
	}
	if err := m.storeDB.Create(&states).Error; err != nil {
		t.Fatal(err)
	}
	cov = m.stabilityDataCoverage(context.Background(), from, from+3*3600, from+5*3600)
	if cov.CompletedHours != 2 || cov.MissingHours != 1 || cov.Percent < 66 || cov.Percent > 67 || cov.LatestHourPending {
		t.Fatalf("覆盖率应按完整小时计算: %+v", cov)
	}
}

func TestStabilityCoverageSeparatesLatestPendingFromHistoricalGap(t *testing.T) {
	m := newStabilityTestMonitor(t)
	from := time.Date(2026, 8, 9, 0, 0, 0, 0, cstLocation).Unix()
	now := from + 3*3600 + stabilityHourFinalizeDelaySec
	if err := m.storeDB.Create(&[]StabilityHourIngestState{
		{HourTs: from, Status: "complete"},
		{HourTs: from + 3600, Status: "complete"},
	}).Error; err != nil {
		t.Fatal(err)
	}
	cov := m.stabilityDataCoverage(context.Background(), from, from+4*3600, now)
	if cov.CompletedHours != 2 || cov.MissingHours != 1 || !cov.LatestHourPending || cov.PendingHourTs != from+2*3600 {
		t.Fatalf("唯一缺失的最新可归档小时应标记为正常汇总中: %+v", cov)
	}

	// 同一小时已经失败时不能被降级为正常尾部延迟。
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: from + 2*3600, Status: "failed"}).Error; err != nil {
		t.Fatal(err)
	}
	cov = m.stabilityDataCoverage(context.Background(), from, from+4*3600, now)
	if cov.LatestHourPending {
		t.Fatalf("失败小时必须保留真实告警语义: %+v", cov)
	}
}

func TestLocalRollupDoesNotOverwriteAuthoritativeCompleteHour(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&MetricSample{BucketTs: hour, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: 1, ModelName: "m", Grp: "g", Success: 99}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.rollupStabilityHours(hour); err != nil {
		t.Fatal(err)
	}
	var got StabilityHourSample
	if err := m.storeDB.First(&got, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if got.Success != 99 {
		t.Fatalf("分钟级局部数据覆盖了权威小时补数: %+v", got)
	}
}

func TestStabilityBackfillInterruptionIsResumableButQueryTimeoutIsNotShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !stabilityBackfillInterrupted(ctx, context.Canceled) {
		t.Fatal("服务取消必须识别为可续跑中断")
	}
	if !stabilityBackfillInterrupted(context.Background(), context.Canceled) {
		t.Fatal("驱动返回 context canceled 也必须识别为可续跑中断")
	}
	if stabilityBackfillInterrupted(context.Background(), context.DeadlineExceeded) {
		t.Fatal("单小时查询超时不能伪装成服务停机，应按真实失败退避处理")
	}
	if stabilityBackfillInterrupted(context.Background(), errors.New("database unavailable")) {
		t.Fatal("普通数据库错误不能自动排队无限重试")
	}
}

func TestCanceledBackfillJobReturnsToQueuedForRestartResume(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	job := StabilityBackfillJob{ID: "cancel-resume", FromTs: hour, ToTs: hour + 3600, Status: "queued", TotalHours: 1, UpdatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfill(ctx, job.ID)
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != "queued" || !strings.Contains(job.LastError, "等待下次启动续跑") {
		t.Fatalf("停机取消后任务必须回到queued: %+v", job)
	}
}

func TestStabilityStorageCoversTwoMaximumQueryPeriods(t *testing.T) {
	for _, tc := range []struct {
		cfg         Settings
		wantQuery   int
		wantStorage int
	}{
		{Settings{}, 90, 181},
		{Settings{StabilityQueryMaxDays: 90, StabilityRetentionDays: 90}, 90, 181},
		{Settings{StabilityQueryMaxDays: 30, StabilityRetentionDays: 120}, 30, 120},
	} {
		if got := tc.cfg.stabilityQueryDays(); got != tc.wantQuery {
			t.Fatalf("query days=%d want=%d", got, tc.wantQuery)
		}
		if got := tc.cfg.stabilityStorageDays(); got != tc.wantStorage {
			t.Fatalf("storage days=%d want=%d", got, tc.wantStorage)
		}
	}
}

func TestDisabledStabilityBackfillRejectsManualStartAndDoesNotResume(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityBackfillEnabled = false
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	job := StabilityBackfillJob{ID: "must-not-resume", FromTs: hour, ToTs: hour + 3600, Status: "queued", TotalHours: 1, UpdatedAt: time.Now().Unix()}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}

	m.startStabilityBackfillMaintenance(context.Background())
	if m.stabilityBackfillRunning.Load() {
		t.Fatal("补数总开关关闭时不得续跑历史任务")
	}
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != "queued" {
		t.Fatalf("禁用时不得改写历史任务: %+v", job)
	}
	if _, err := m.startStabilityBackfill(hour, hour+3600); !errors.Is(err, errStabilityBackfillDisabled) {
		t.Fatalf("人工调用应返回禁用错误，got=%v", err)
	}

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/stability/backfill?days=1", nil)
	m.startStabilityBackfillHandler(c)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("禁用的管理接口应返回 503，got=%d body=%s", w.Code, w.Body.String())
	}
}

func TestClassificationMigrationDoesNotAutoStartWithoutExplicitFlag(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityBackfillEnabled = true
	m.cfg.StabilityAutoRepair = false
	m.cfg.StabilityClassificationMigrationEnabled = false
	m.prodDB = newFakeProdDB(t)
	m.startStabilityBackfillMaintenance(context.Background())
	var jobs int64
	if err := m.storeDB.Model(&StabilityBackfillJob{}).Count(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || m.stabilityBackfillRunning.Load() {
		t.Fatalf("classification migration started without explicit flag: jobs=%d running=%v", jobs, m.stabilityBackfillRunning.Load())
	}
}

func TestClassificationMigrationDoesNotReplacePausedOrPartialAuditChain(t *testing.T) {
	for _, status := range []string{"paused", "partial"} {
		t.Run(status, func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			m.cfg.StabilityBackfillEnabled = true
			m.cfg.StabilityClassificationMigrationEnabled = true
			m.prodDB = newFakeProdDB(t)
			job := StabilityBackfillJob{
				ID: "existing-" + status, Kind: stabilityMigrationJobKind,
				Status: status, FromTs: 1, ToTs: 3601, TotalHours: 1,
				FailedHours: 1, FailedHourTs: []int64{1}, UpdatedAt: time.Now().Unix(),
			}
			if err := m.storeDB.Create(&job).Error; err != nil {
				t.Fatal(err)
			}
			if m.startStabilityClassificationMigration() {
				t.Fatalf("%s migration must wait for explicit retry", status)
			}
			var jobs int64
			if err := m.storeDB.Model(&StabilityBackfillJob{}).
				Where("kind = ?", stabilityMigrationJobKind).Count(&jobs).Error; err != nil {
				t.Fatal(err)
			}
			if jobs != 1 || m.stabilityBackfillRunning.Load() {
				t.Fatalf("restart created a duplicate migration: jobs=%d running=%v", jobs, m.stabilityBackfillRunning.Load())
			}
		})
	}
}

func TestUsageAdminLabelsAndGroupOrderContract(t *testing.T) {
	for _, want := range []string{"关注客户累计总消耗", "关注客户数", ".sort((a,b)=>"} {
		if !strings.Contains(pageHTML, want) {
			t.Fatalf("管理端用户用量缺少 %q", want)
		}
	}
	if strings.Contains(portalHTML, "关注客户数") {
		t.Fatal("本轮管理端文案不得进入 Usage Portal")
	}
}
