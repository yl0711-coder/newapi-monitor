package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"gorm.io/gorm"
)

func TestCloudWatchNginxRepairTimeoutResumesNarrowedHourAfterRestart(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = strings.Repeat("t", 32), "test-only"
	state := completeEmptyCloudWatchNginxEvidence(t, m, now)
	target := state.EvidenceThroughTs
	left := target/3600*3600 - 12*3600
	state.NextTs, state.ThroughTs, state.Status = target, target, "caught_up"
	state.ReadWindowSeconds, state.RepairNextTs = 120, left
	state.LastRepairAt = now.Add(-30 * time.Minute).Unix()
	lastRepair := state.LastRepairAt
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	for i, at := range []int64{left + 60, left + 480, left + 3600} {
		if err := m.storeDB.Create(&NginxMinuteSample{Node: cloudWatchNginxNode, BucketTs: at,
			Route: "/v1/responses", Method: "POST", Status: 200, Count: int64(i + 7)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	client := &fakeCloudWatchLogsClient{}
	client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		in := client.startInputs[len(client.startInputs)-1]
		if aws.ToInt64(in.EndTime)-aws.ToInt64(in.StartTime) > 420 {
			return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusTimeout}, nil
		}
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusComplete}, nil
	}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	for _, width := range []int64{900, 420} {
		from, to, ok := cloudWatchNginxRepairWindow(state, target)
		if !ok || from != left {
			t.Fatal("repair skipped failed left boundary", from, to)
		}
		_, err := m.runCloudWatchNginxRepairWindow(context.Background(), &state, from, to, target)
		if cloudWatchPreRouteErrorCode(err) != "timeout" {
			t.Fatal("expected repair timeout", err)
		}
		m.recordCloudWatchNginxFailure(&state, err, now.Unix())
		state, err = m.loadCloudWatchNginxCursor(now)
		if err != nil {
			t.Fatal(err)
		}
		from, to, ok = cloudWatchNginxRepairWindow(state, target)
		if !ok || from != left || to-from != width {
			t.Fatal("historical repair did not retain a narrower retry", from, to, width)
		}
		if state.NextTs != target || state.ThroughTs != target || state.ReadWindowSeconds != 120 || state.EvidenceThroughTs != target || state.LastRepairAt != lastRepair {
			t.Fatal("repair timeout changed another lane or certified the hour", state)
		}
	}
	from, to, _ := cloudWatchNginxRepairWindow(state, target)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.runCloudWatchNginxRepairWindow(cancelled, &state, from, to, target); err == nil || state.RepairNextTs != left {
		t.Fatal("cancelled repair advanced its checkpoint", err)
	}
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("repair-checkpoint-failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "cloud_watch_nginx_cursors" {
			_ = tx.AddError(errors.New("injected repair checkpoint failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, writeErr := m.runCloudWatchNginxRepairWindow(context.Background(), &state, from, to, target)
	if err := m.storeDB.Callback().Update().Remove("repair-checkpoint-failure"); err != nil {
		t.Fatal(err)
	}
	if writeErr == nil || !strings.Contains(writeErr.Error(), "injected repair checkpoint failure") || state.RepairNextTs != left {
		t.Fatal("failed local checkpoint published repair progress", writeErr, state.RepairNextTs)
	}
	var preserved int64
	if err := m.storeDB.Model(&NginxMinuteSample{}).Where("node=? AND bucket_ts=?", cloudWatchNginxNode, left+60).Select("count").Scan(&preserved).Error; err != nil || preserved != 7 {
		t.Fatal("failed repair removed original minute facts", preserved, err)
	}
	for next := left; next < left+3600; {
		var err error
		state, err = m.loadCloudWatchNginxCursor(now)
		if err != nil {
			t.Fatal(err)
		}
		from, to, ok := cloudWatchNginxRepairWindow(state, target)
		if !ok || from != next || to != min(next+420, left+3600) {
			t.Fatal("restart lost a split-hour checkpoint or crossed its boundary", next, from, to)
		}
		if _, err := m.runCloudWatchNginxRepairWindow(context.Background(), &state, from, to, target); err != nil {
			t.Fatal(err)
		}
		next = to
		if state.RepairNextTs != next || state.NextTs != target || state.ThroughTs != target || state.ReadWindowSeconds != 120 || state.EvidenceThroughTs != target {
			t.Fatal("split repair rewrote another lane or skipped its own suffix", state)
		}
		if next < left+3600 && state.LastRepairAt != lastRepair {
			t.Fatal("partial hour incorrectly entered the twenty-minute repair pause")
		}
		if next == left+3600 && state.LastRepairAt <= lastRepair {
			t.Fatal("complete hour did not restore the existing repair cadence")
		}
	}
	var inside, outside int64
	if err := m.storeDB.Model(&NginxMinuteSample{}).Where("node=? AND bucket_ts>=? AND bucket_ts<?", cloudWatchNginxNode, left, left+3600).Count(&inside).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NginxMinuteSample{}).Where("node=? AND bucket_ts=?", cloudWatchNginxNode, left+3600).Select("count").Scan(&outside).Error; err != nil || inside != 0 || outside != 9 {
		t.Fatal("repair replacement did not respect its exact hour", inside, outside, err)
	}
}

func TestCloudWatchNginxRepairCannotAgePastFailedInterval(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	state := completeEmptyCloudWatchNginxEvidence(t, m, now)
	_, target := cloudWatchNginxRange(now, m.cfg.CloudWatchNginxLookbackHours)
	left := target/3600*3600 - int64(cloudWatchNginxRepairHorizon/time.Second)
	state.CoverageFromTs = left - 3600
	state.RepairNextTs, state.RepairReadWindowSeconds = left+180, 60
	state.LastRepairAt = now.Add(-48 * time.Hour).Unix()
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	// The source horizon moves while the same failed minute awaits retry.
	// Reload must retain that minute, not turn elapsed time into coverage.
	for _, elapsed := range []time.Duration{time.Hour, 3 * time.Hour} {
		restarted, err := m.loadCloudWatchNginxCursor(now.Add(elapsed))
		if err != nil {
			t.Fatal(err)
		}
		from, to, ok := cloudWatchNginxRepairWindow(restarted, target+int64(elapsed/time.Second))
		if !ok || from != left+180 || to != left+240 || restarted.RepairNextTs != state.RepairNextTs || restarted.LastRepairAt != state.LastRepairAt {
			t.Fatal("moving horizon skipped an uncompleted repair", from, to, restarted.RepairNextTs)
		}
	}
}

func TestCloudWatchNginxSustainedRecoveryGrowsWithoutSkippingGap(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = strings.Repeat("t", 32), "test-only"
	state, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	state.ReadWindowSeconds = 60
	left, target := state.NextTs, state.NextTs+3600
	client := &fakeCloudWatchLogsClient{}
	failed := false
	client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		if failed {
			return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusTimeout}, nil
		}
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusComplete}, nil
	}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	for i := 0; i < 8; i++ {
		from, to := cloudWatchNginxNextWindow(state, target)
		if from != left+int64(i)*60 || to != from+60 {
			t.Fatal("unread prefix skipped", from, to)
		}
		if _, err := m.runCloudWatchNginxWindow(context.Background(), &state, from, to, target); err != nil {
			t.Fatal(err)
		}
		if i < 7 && state.ReadWindowSeconds != 60 {
			t.Fatal("single success expanded width")
		}
	}
	if state.ReadWindowSeconds != 120 || state.ThroughTs != left+8*60 {
		t.Fatal("recovery did not grow or certified unread suffix", state)
	}
	state, err = m.loadCloudWatchNginxCursor(now)
	if err != nil || state.ReadWindowSeconds != 120 {
		t.Fatal("learned width not durable", state, err)
	}
	failed = true
	from, to := cloudWatchNginxNextWindow(state, target)
	if _, err = m.runCloudWatchNginxWindow(context.Background(), &state, from, to, target); cloudWatchPreRouteErrorCode(err) != "timeout" {
		t.Fatal(err)
	}
	if state.ReadWindowSeconds != 60 || state.NextTs != from || state.ThroughTs != from {
		t.Fatal("failed expanded read skipped gap", state)
	}
	failed = false
	from, to = cloudWatchNginxNextWindow(state, target)
	if _, err = m.runCloudWatchNginxWindow(context.Background(), &state, from, to, target); err != nil {
		t.Fatal(err)
	}
	if state.ReadWindowSeconds != 60 || state.ThroughTs != from+60 {
		t.Fatal("timeout did not reset fast-success streak", state)
	}
}

func TestCloudWatchNginxPrimaryTimeoutResumesNarrowedWindowAfterRestart(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = strings.Repeat("t", 32), "test-only"
	state, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	left, target := state.NextTs, state.NextTs+3600
	client := &fakeCloudWatchLogsClient{}
	client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		in := client.startInputs[len(client.startInputs)-1]
		if aws.ToInt64(in.EndTime)-aws.ToInt64(in.StartTime) > 420 {
			return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusTimeout}, nil
		}
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusComplete}, nil
	}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	for _, width := range []int64{900, 420} {
		from, to := cloudWatchNginxNextWindow(state, target)
		_, err := m.runCloudWatchNginxWindow(context.Background(), &state, from, to, target)
		if cloudWatchPreRouteErrorCode(err) != "timeout" {
			t.Fatalf("expected timeout: %v", err)
		}
		m.recordCloudWatchNginxFailure(&state, err, now.Unix())
		state, err = m.loadCloudWatchNginxCursor(now)
		if err != nil || state.NextTs != left || state.ThroughTs != left || state.ReadWindowSeconds != width {
			t.Fatalf("restart lost failed boundary or width: %+v %v", state, err)
		}
	}
	from, to := cloudWatchNginxNextWindow(state, target)
	if _, err := m.runCloudWatchNginxWindow(context.Background(), &state, from, to, target); err != nil {
		t.Fatal(err)
	}
	if state.NextTs != left+420 || state.ThroughTs != left+420 || state.ReadWindowSeconds != 420 {
		t.Fatalf("successful complete prefix not published: %+v", state)
	}
	// A narrowed replay must finish its remaining suffix; caught_up cannot
	// repeatedly reset it to the same twenty-minute left boundary.
	state.NextTs, state.ThroughTs, state.Status = target, target, "caught_up"
	state.CoverageFromTs = left - 3600
	state.ReadWindowSeconds = 120
	state.ReplayReadWindowSeconds = 120
	from, to, state.ReplayThroughTs, _ = cloudWatchNginxReplayWindow(state, now, 5*time.Minute)
	state.ReplayNextTs = from
	if err := m.publishCloudWatchNginxWindowLane(context.Background(), &state, from, to, nil, nil, 0, 0, target, false, true); err != nil {
		t.Fatal(err)
	}
	next, _, _, _ := cloudWatchNginxReplayWindow(state, now, 5*time.Minute)
	if next != to || state.NextTs != target || state.ThroughTs != target || state.Status != "caught_up" || state.ReadWindowSeconds != 120 {
		t.Fatalf("split replay loops or rewinds coverage: next=%d state=%+v", next, state)
	}
}

func TestCloudWatchNginxEvidenceTimeoutWriteFailureRestartRecovery(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = strings.Repeat("t", 32), "test-only"
	state, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	left := from + 600
	if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), nil, from, left, "prefix"); err != nil {
		t.Fatal(err)
	}
	if err := m.publishCloudWatchNginxEvidenceCursor(context.Background(), &state, from, left, target); err != nil {
		t.Fatal(err)
	}
	client := &fakeCloudWatchLogsClient{}
	client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		in := client.startInputs[len(client.startInputs)-1]
		if aws.ToInt64(in.StartTime) != left {
			t.Fatalf("retry skipped failed left boundary: %d != %d", aws.ToInt64(in.StartTime), left)
		}
		if aws.ToInt64(in.EndTime)-aws.ToInt64(in.StartTime) > 420 {
			return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusTimeout}, nil
		}
		rows := make([][]cwtypes.ResultField, 501)
		for i := range rows {
			message := fmt.Sprintf(`{"msec":"%d.000","request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.35","upstream_status":"200","oneapi_request_id":"request-%d","request_completion":"OK"}`, left+1, i)
			rows[i] = []cwtypes.ResultField{{Field: aws.String("@ptr"), Value: aws.String(fmt.Sprintf("event-%d", i))}, {Field: aws.String("@message"), Value: aws.String(message)}}
			var fields map[string]string
			if err := json.Unmarshal([]byte(message), &fields); err != nil {
				t.Fatal(err)
			}
			for key, value := range fields {
				rows[i] = append(rows[i], cwtypes.ResultField{Field: aws.String(key), Value: aws.String(value)})
			}
		}
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusComplete, Results: rows}, nil
	}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	for _, width := range []int64{900, 420} {
		if processed, err := m.processCloudWatchNginxEvidenceChunk(context.Background(), &state, target); !processed || cloudWatchPreRouteErrorCode(err) != "timeout" {
			t.Fatalf("timeout: processed=%v err=%v", processed, err)
		}
		if state.EvidenceNextTs != left || state.EvidenceThroughTs != left || state.EvidenceReadWindowSeconds != width {
			t.Fatalf("timeout changed coverage or failed to narrow: %+v", state)
		}
		state, err = m.loadCloudWatchNginxCursor(now)
		if err != nil || state.EvidenceReadWindowSeconds != width {
			t.Fatalf("restart lost narrowed window: %d %v", state.EvidenceReadWindowSeconds, err)
		}
	}
	insertCalls := 0
	if err := m.nginxEvidenceDB.Callback().Create().Before("gorm:create").Register("test_fail_second_evidence_chunk", func(tx *gorm.DB) {
		if tx.Statement.Table == "nginx_request_evidences" {
			insertCalls++
			if insertCalls == 2 {
				_ = tx.AddError(errors.New("injected local write failure"))
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.processCloudWatchNginxEvidenceChunk(context.Background(), &state, target); err == nil {
		t.Fatal("partial persistence accepted")
	}
	if state.EvidenceThroughTs != left || state.EvidenceNextTs != left {
		t.Fatal("partial persistence published coverage")
	}
	var proof CloudWatchNginxEvidenceCheckpoint
	if err := m.nginxEvidenceDB.First(&proof).Error; err != nil {
		t.Fatal(err)
	}
	if proof.PendingFromTs != left || proof.PendingToTs != left+420 || proof.ThroughTs != left {
		t.Fatalf("partial durable proof: %+v", proof)
	}
	if err := m.nginxEvidenceDB.Callback().Create().Remove("test_fail_second_evidence_chunk"); err != nil {
		t.Fatal(err)
	}
	state, err = m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	if state.EvidenceNextTs != left || state.EvidenceReadWindowSeconds != 420 {
		t.Fatalf("write failure discarded successful prefix: %+v", state)
	}
	if processed, err := m.processCloudWatchNginxEvidenceChunk(context.Background(), &state, target); !processed || err != nil {
		t.Fatalf("recovery: %v %v", processed, err)
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 501 || state.EvidenceNextTs != left+420 || state.EvidenceThroughTs != left+420 {
		t.Fatalf("recovered rows=%d through=%d", count, state.EvidenceThroughTs)
	}
	if err := m.nginxEvidenceDB.First(&proof).Error; err != nil {
		t.Fatal(err)
	}
	if proof.PendingToTs != 0 || !cloudWatchNginxEvidenceProofMatches(state, proof, from) {
		t.Fatalf("complete window lacks durable proof: %+v", proof)
	}
	if state.EvidenceReadWindowSeconds != 420 {
		t.Fatal("successful persisted recovery discarded the proven read width")
	}
	state, err = m.loadCloudWatchNginxCursor(now)
	if err != nil || state.EvidenceReadWindowSeconds != 420 {
		t.Fatal("restart lost the successful read width")
	}
	if got := m.cloudWatchNginxEvidenceRecovery(time.Now()); !got.Verified {
		t.Fatalf("published proof not readable: %+v", got)
	}
}

func TestCloudWatchNginxSchedulerAdvancesPrimaryAndEvidenceWithoutReplayBlocking(t *testing.T) {
	// Exercise the real scheduler: one newly closed minute must not force a
	// twenty-minute replay ahead of an existing historical evidence gap.
	m, clock := newCloudWatchNginxRecoveryTestMonitor(t)
	m.cfg.CloudWatchNginxPollSeconds = 300
	m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = strings.Repeat("s", 32), "scheduler-test"
	state, err := m.loadCloudWatchNginxCursor(clock)
	if err != nil {
		t.Fatal(err)
	}
	_, target := cloudWatchNginxRange(clock, 168)
	// Keep the tested frontier inside retention. The scheduler uses the live
	// clock, whose rounded retention boundary may be one minute newer than
	// the fixture clock; that normal expiry must not move this test's frontier.
	evidenceFrom := state.EvidenceNextTs
	if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), nil, evidenceFrom, evidenceFrom+600, "scheduler-prefix"); err != nil {
		t.Fatal(err)
	}
	if err := m.publishCloudWatchNginxEvidenceCursor(context.Background(), &state, evidenceFrom, evidenceFrom+600, target); err != nil {
		t.Fatal(err)
	}
	state.ThroughTs, state.NextTs, state.Status = target-60, target-60, "caught_up"
	state.EvidenceReadWindowSeconds = 60
	left, primary := state.EvidenceNextTs, state.ThroughTs
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	started := make(chan int64, 4)
	client := &fakeCloudWatchLogsClient{}
	client.startFn = func(_ context.Context, in *cloudwatchlogs.StartQueryInput) (*cloudwatchlogs.StartQueryOutput, error) {
		select {
		case started <- aws.ToInt64(in.StartTime):
		default:
		}
		return &cloudwatchlogs.StartQueryOutput{QueryId: aws.String("scheduler-query")}, nil
	}
	client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusComplete}, nil
	}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		deadline := time.Now().Add(2 * time.Second)
		for m.cloudWatchNginxRunning.Load() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}()
	m.startCloudWatchNginx(ctx)
	select {
	case from := <-started:
		if from != primary {
			t.Fatalf("scheduler rewound fresh work into a replay: %d != %d", from, primary)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler did not run")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var durable CloudWatchNginxCursor
		if err := m.storeDB.First(&durable, 1).Error; err != nil {
			t.Fatal(err)
		}
		if durable.EvidenceThroughTs == left+60 {
			if durable.ThroughTs < target || durable.EvidenceReadWindowSeconds != 60 {
				t.Fatal("scheduler starved primary or failed to persist independent evidence")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("historical evidence query did not publish durable success")
}
