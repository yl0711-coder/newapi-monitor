package monitor

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCloudWatchNginxTimeoutStagePolicyAcrossLanes(t *testing.T) {
	for _, stage := range []string{"start_query", "get_query_results", "poll_query", "poll_scheduled", "query_status"} {
		t.Run(stage, func(t *testing.T) {
			m, now := newCloudWatchNginxRecoveryTestMonitor(t)
			m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = strings.Repeat("t", 32), "stage-test"
			state, err := m.loadCloudWatchNginxCursor(now)
			if err != nil {
				t.Fatal(err)
			}
			from := state.NextTs
			to := from + 3600
			target := to + 3600
			state.ReadWindowSeconds, state.RepairReadWindowSeconds, state.EvidenceReadWindowSeconds = 3600, 3600, 3600
			client := &fakeCloudWatchLogsClient{}
			switch stage {
			case "start_query":
				client.startFn = func(context.Context, *cloudwatchlogs.StartQueryInput) (*cloudwatchlogs.StartQueryOutput, error) {
					return nil, context.DeadlineExceeded
				}
			case "get_query_results":
				client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
					return nil, context.DeadlineExceeded
				}
			case "poll_query":
				client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
					return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusRunning}, nil
				}
			case "poll_scheduled":
				client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
					return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusScheduled}, nil
				}
			case "query_status":
				client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
					return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusTimeout}, nil
				}
			}
			m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
			m.cloudWatchLogs.poll = func(context.Context) error { return context.DeadlineExceeded }
			want := int64(3600)
			if stage == "query_status" || stage == "poll_query" {
				want = 900
			}
			for _, repair := range []bool{false, true} {
				before := state.ThroughTs
				_, err = m.runCloudWatchNginxWindowMode(context.Background(), &state, from, to, target, repair)
				if cloudWatchPreRouteErrorCode(err) != "timeout" || state.ThroughTs != before {
					t.Fatal(err, state)
				}
				width := state.ReadWindowSeconds
				if repair {
					width = state.RepairReadWindowSeconds
				}
				if width != want {
					t.Fatal("wrong shrink policy", stage, repair, width, want)
				}
			}
			_, err = m.runCloudWatchNginxEvidenceWindow(context.Background(), &state, from, to, target)
			narrowCloudWatchNginxEvidenceRead(&state, to-from, err)
			if state.EvidenceReadWindowSeconds != want {
				t.Fatal("evidence policy differs", state.EvidenceReadWindowSeconds, want)
			}
			if stage == "start_query" || stage == "get_query_results" {
				state.ReadWindowSeconds = 0
				state.EvidenceReadWindowSeconds = 0
				m.recordCloudWatchNginxFailure(&state, err, now.Unix())
				m.recordCloudWatchNginxEvidenceFailure(&state, err, now.Unix())
				state, err = m.loadCloudWatchNginxCursor(now)
				if err != nil {
					t.Fatal(err)
				}
				left, right := cloudWatchNginxNextWindow(state, target)
				if right-left != 3600 {
					t.Fatal("restart silently shrank a transport timeout", left, right)
				}
			}
		})
	}
	// A deadline without an AWS stage is not execution evidence.
	if got := cloudWatchNginxTimeoutStage(errors.Join(context.DeadlineExceeded, errors.New("outer budget"))); got != "budget" {
		t.Fatal(got)
	}
}

func TestCloudWatchNginxReplayRestartAndLateRows(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = strings.Repeat("r", 32), "replay-test"
	state := completeEmptyCloudWatchNginxEvidence(t, m, now)
	target := state.EvidenceThroughTs
	state.NextTs, state.ThroughTs, state.Status = target, target, "caught_up"
	state.ReplayNextTs, state.ReplayThroughTs, state.ReplayReadWindowSeconds = target-1200, target, 60
	left := state.ReplayNextTs
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	events := 1
	client := &fakeCloudWatchLogsClient{}
	client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		in := client.startInputs[len(client.startInputs)-1]
		rows := [][]cwtypes.ResultField{}
		if aws.ToInt64(in.StartTime) <= left && aws.ToInt64(in.EndTime) > left {
			for i := 0; i < events; i++ {
				fields := map[string]string{"@ptr": fmt.Sprintf("replay-event-%d", i), "msec": fmt.Sprintf("%d.000", left+1), "request_method": "POST", "uri": "/v1/responses", "status": "200", "request_time": "0.35", "upstream_status": "200", "oneapi_request_id": fmt.Sprintf("replay-request-%d", i), "request_completion": "OK"}
				row := []cwtypes.ResultField{}
				for k, v := range fields {
					row = append(row, cwtypes.ResultField{Field: aws.String(k), Value: aws.String(v)})
				}
				rows = append(rows, row)
			}
		}
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusComplete, Results: rows}, nil
	}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	run := func() {
		t.Helper()
		from, to, _, ok := cloudWatchNginxReplayWindow(state, now, 5*time.Minute)
		if !ok {
			t.Fatal("replay disappeared")
		}
		if _, err := m.runCloudWatchNginxWindowLane(context.Background(), &state, from, to, target, false, true); err != nil {
			t.Fatal(err)
		}
	}
	run()
	var err error
	state, err = m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	if state.ReplayNextTs != left+60 || state.NextTs != target || state.EvidenceThroughTs != target {
		t.Fatal("restart lost independent cursors", state)
	}
	for state.ReplayNextTs < state.ReplayThroughTs {
		run()
	}
	// The following poll must schedule a new replay, including logs delivered
	// after the first completed cycle, without advancing either coverage proof.
	now = time.Unix(state.ReplayLastAt, 0).Add(5 * time.Minute)
	from, _, end, ok := cloudWatchNginxReplayWindow(state, now, 5*time.Minute)
	if !ok || from != left || end != target {
		t.Fatal(from, end, ok)
	}
	state.ReplayNextTs, state.ReplayThroughTs = from, end
	events = 2
	run()
	var count, evidence int64
	if err := m.storeDB.Model(&NginxMinuteSample{}).Where("node=? AND bucket_ts=?", cloudWatchNginxNode, left).Select("COALESCE(SUM(count),0)").Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("node=? AND event_ms>=? AND event_ms<?", cloudWatchNginxNode, left*1000, (left+60)*1000).Count(&evidence).Error; err != nil {
		t.Fatal(err)
	}
	if count != 2 || evidence != 2 || state.ThroughTs != target || state.EvidenceThroughTs != target {
		t.Fatal("late replay lost, duplicated or borrowed coverage", count, evidence, state)
	}
}
func TestCloudWatchNginxScheduleBoundsReplayAndHistory(t *testing.T) {
	now := time.Unix(1793536800, 0)
	target := now.Unix() - 120
	state := CloudWatchNginxCursor{CoverageFromTs: target - 3*86400, NextTs: target - 3600, ThroughTs: target - 3600, ReadWindowSeconds: 60,
		ReplayNextTs: target - 4800, ReplayThroughTs: target - 3600, ReplayReadWindowSeconds: 60, RepairNextTs: target - 86400, RepairReadWindowSeconds: 60,
		EvidenceCoverageFromTs: target - 2*86400, EvidenceNextTs: target - 2*86400, EvidenceThroughTs: target - 2*86400, EvidenceReadWindowSeconds: 60}
	schedule := cloudWatchNginxSchedule{}
	seen := map[cloudWatchNginxLane]int{}
	for i := 0; i < 12; i++ {
		lane, from, to, _, ok := schedule.choose(state, target, now, 5*time.Minute)
		if !ok {
			t.Fatal("ready tasks not scheduled")
		}
		seen[lane]++
		switch lane {
		case nginxForward:
			if from != state.NextTs {
				t.Fatal("new data rewound")
			}
			state.NextTs = to
			state.ThroughTs = to
		case nginxReplay:
			state.ReplayNextTs = to
		case nginxRepair:
			state.RepairNextTs = to
		case nginxEvidence:
			state.EvidenceNextTs = to
		}
	}
	if seen[nginxForward] != 6 || seen[nginxReplay] != 2 || seen[nginxRepair] != 2 || seen[nginxEvidence] != 2 {
		t.Fatal(seen)
	}
	// Submission failure cools only its lane; the other three still get turns.
	schedule.due[nginxReplay] = now.Add(cloudWatchNginxRetryDelay(newCloudWatchLogsError(cwLogsErrTimeout, "start_query", nil), 5*time.Minute))
	for i := 0; i < 6; i++ {
		lane, _, _, _, ok := schedule.choose(state, target, now, 5*time.Minute)
		if !ok || lane == nginxReplay {
			t.Fatal(lane, ok)
		}
	}
	// Caught up forward never rewinds. The next replay fixes a stable interval.
	state.NextTs, state.ThroughTs, state.Status = target, target, "caught_up"
	state.ReplayNextTs, state.ReplayThroughTs, state.ReplayLastAt = target, target, 0
	from, to, end, ok := cloudWatchNginxReplayWindow(state, now, 5*time.Minute)
	if !ok || from != target-1200 || to != from+60 || end != target {
		t.Fatal(from, to, end, ok)
	}
	state.ReplayNextTs, state.ReplayThroughTs = to, end
	fresh, _ := cloudWatchNginxNextWindow(state, target+60)
	next, _, fixed, _ := cloudWatchNginxReplayWindow(state, now.Add(time.Minute), 5*time.Minute)
	if fresh != target || next != to || fixed != end {
		t.Fatal("moving target restarted replay", fresh, next, fixed)
	}
}
func TestCloudWatchNginxSmallWindowRecoversWithNormalNetworkLatency(t *testing.T) {
	var fast atomic.Int64
	width := int64(60)
	for width < 3600 {
		previous := width
		for i := 0; i < 8; i++ {
			width = cloudWatchNginxRecoveredWidth(width, width, 1, 10*time.Second, &fast, 3600)
		}
		if width <= previous {
			t.Fatal("recovered source stranded in small windows", width)
		}
	}
	width = cloudWatchNginxRecoveredWidth(60, 60, 1, 24*time.Second, &fast, 3600)
	if width != 60 {
		t.Fatal("slow query incorrectly expanded")
	}
}
