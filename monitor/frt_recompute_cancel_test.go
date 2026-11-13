package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestFRTRecomputeCancellationPersistsAndResumes(t *testing.T) {
	for _, stage := range []string{"query", "backoff", "timeout"} {
		t.Run(stage, func(t *testing.T) {
			m, end := modelFRTFixture(t, true)
			ctx, cancel := context.WithCancel(context.Background())
			if stage == "timeout" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
			}
			defer cancel()
			failures := &frtSourceQueryFailures{}
			failures.beforeQuery = func() {
				if stage == "query" {
					cancel()
				} else if stage == "timeout" {
					<-ctx.Done()
				}
			}
			if stage == "backoff" {
				failures.remaining.Store(1)
				prior := frtRecomputeRetryWait
				frtRecomputeRetryWait = func(context.Context, time.Duration) bool { cancel(); return false }
				t.Cleanup(func() { frtRecomputeRetryWait = prior })
			}
			m.prodDB = frtRecomputeProdDBWithFailures(t, failures)
			state, err := m.RecomputeFRTSum(ctx, end-60, end)
			want := context.Canceled
			if stage == "timeout" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) || state == nil || state.Status != "failed" {
				t.Fatalf("cancellation not returned: state=%+v err=%v", state, err)
			}
			var stored FRTSumRecomputeState
			if err := m.storeDB.First(&stored, "id = ?", 1).Error; err != nil {
				t.Fatal(err)
			}
			if stored.Status != "failed" || !strings.Contains(stored.LastReason, want.Error()) || stored.FinishedAt == 0 || stored.DoneThroughTs != end-60 || stored.LeaseOwner != "" {
				t.Fatalf("terminal state must survive cancellation and lease release: %+v", stored)
			}
			failures.beforeQuery = nil
			resumed, err := m.RecomputeFRTSum(context.Background(), end-60, end)
			if err != nil || resumed.Status != "complete" || resumed.TaskID != stored.TaskID || resumed.LastReason != "" {
				t.Fatalf("resume changed task or retained error: state=%+v err=%v", resumed, err)
			}
		})
	}
}

func TestFRTRecomputeCancellationCannotOverwriteReplacementOwner(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failures := &frtSourceQueryFailures{}
	failures.beforeQuery = func() {
		if err := m.storeDB.Model(&FRTSumRecomputeState{}).Where("id = ?", 1).Updates(map[string]any{"lease_owner": "replacement-owner", "lease_until_ts": time.Now().Unix() + 300, "last_reason": "replacement checkpoint"}).Error; err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	m.prodDB = frtRecomputeProdDBWithFailures(t, failures)
	_, err := m.RecomputeFRTSum(ctx, end-60, end)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("original cancellation lost: %v", err)
	}
	var stored FRTSumRecomputeState
	if err := m.storeDB.First(&stored, "id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if stored.LeaseOwner != "replacement-owner" || stored.LastReason != "replacement checkpoint" || stored.Status != "running" {
		t.Fatalf("stale owner overwrote checkpoint: %+v", stored)
	}
}
