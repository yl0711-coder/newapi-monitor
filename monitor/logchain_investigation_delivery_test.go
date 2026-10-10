package monitor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestInvestigationEmptyCloudFrontAfterDeliveryDeadlineIsPartial(t *testing.T) {
	to := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		age  time.Duration
		want string
	}{
		{name: "still_waiting", age: time.Hour - time.Second, want: "pending_delivery"},
		{name: "exact_deadline", age: time.Hour, want: "partial"},
		{name: "past_deadline", age: time.Hour + time.Minute, want: "partial"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := logChainInvestigationResult{
				CandidatesComplete: true, Scope: logChainInvestigationScopeView{ToUTC: to.Format(time.RFC3339)},
				SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceCloudFrontAccess, Status: "empty"}},
			}
			if got := investigationCompletionStatusAt(context.Background(), result, to.Add(tc.age)); got != tc.want {
				t.Fatalf("status=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestPendingDeliveryFinalRecheckPastOneHourRetainsGap(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	// Exercise the actual recheck with a real, already expired scope. The old
	// regression used a recent scope and never took the >1h complete branch.
	to := time.Now().UTC().Add(-65 * time.Minute)
	in := logChainInvestigationInput{From: to.Add(-2 * time.Minute), To: to, CloudFrontRequestID: "missing-edge", Timezone: "Asia/Shanghai"}
	task := &logChainInvestigationTask{ID: "inv_0123456789abcdef0123456789abcdef", Input: in, Status: "pending_delivery", CreatedAt: to}
	statuses := make([]logChainCloudWatchSourceStatus, 0, len(logChainInvestigationSourceOrder))
	for _, source := range logChainInvestigationSourceOrder {
		status := logChainCloudWatchSourceStatus{Source: source, Status: "skipped"}
		if source == cwSourceCloudFrontAccess {
			status.Status = "empty"
		}
		statuses = append(statuses, status)
	}
	task.Result = &logChainInvestigationResult{
		Status: "pending_delivery", Scope: m.investigationScopeView(in), CandidatesComplete: true,
		SourceStatus: statuses, BlindSpots: []string{"historical audit gap"},
	}
	m.investigationTasks = map[string]*logChainInvestigationTask{task.ID: task}
	result := m.recheckPendingCloudFront(context.Background(), task)
	if result.Status != "partial" {
		t.Fatalf("expired empty CloudFront source became %s", result.Status)
	}
	final := finalizePendingDeliveryRecheck(result, true)
	gaps := strings.Join(final.BlindSpots, "\n")
	if final.Status != "partial" || !strings.Contains(gaps, "入口证据仍缺失") || !strings.Contains(gaps, "停止自动复查") || !strings.Contains(gaps, "historical audit gap") {
		t.Fatalf("final recheck lost missing-evidence explanation: %+v", final)
	}
	if len(task.Result.BlindSpots) != 1 || task.Result.Status != "pending_delivery" {
		t.Fatalf("recheck mutated the previous result: %+v", task.Result)
	}
	if twice := finalizePendingDeliveryRecheck(final, true); len(twice.BlindSpots) != len(final.BlindSpots) {
		t.Fatal("repeated finalization duplicated the delivery gap")
	}
}

func TestPendingDeliveryFinalRecheckPreservesActualEvidenceAndFailureStates(t *testing.T) {
	for _, state := range []string{"complete", "failed", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			sourceState := "empty"
			if state == "complete" {
				sourceState = "found"
			}
			result := logChainInvestigationResult{Status: state, SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceCloudFrontAccess, Status: sourceState}}}
			if got := finalizePendingDeliveryRecheck(result, true); got.Status != state {
				t.Fatalf("state=%s changed to %s", state, got.Status)
			}
		})
	}
}
