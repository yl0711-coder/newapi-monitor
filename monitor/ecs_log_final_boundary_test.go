package monitor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func TestECSLogFinalBoundaryRequiresIndependentStopAndWholeChain(t *testing.T) {
	for _, scenario := range []string{"normal", "running", "exit_unknown", "crashed", "abnormal_stop", "future_boundary", "wrong_runtime"} {
		t.Run(scenario, func(t *testing.T) {
			m := newECSLogTestMonitor(t)
			m.cfg.ECSArchiveEnabled = true
			s, key := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
			now := time.Now()
			p, _ := m.ecsLogPolicy(s.ServiceARN, s.Container, s.Lane)
			stop := types.Task{TaskArn: aws.String(s.TaskARN), StoppedAt: &now, LastStatus: aws.String("STOPPED"), StopCode: types.TaskStopCodeServiceSchedulerInitiated,
				Containers: []types.Container{{Name: aws.String(s.Container), RuntimeId: aws.String(s.RuntimeID), LastStatus: aws.String("STOPPED"), ExitCode: aws.Int32(0)}}}
			if scenario == "exit_unknown" {
				stop.Containers[0].ExitCode = nil
			}
			if scenario == "crashed" {
				stop.Containers[0].ExitCode = aws.Int32(137)
			}
			if scenario == "abnormal_stop" {
				stop.StopCode = types.TaskStopCodeEssentialContainerExited
			}
			if scenario == "wrong_runtime" {
				stop.Containers[0].RuntimeId = aws.String("replacement-runtime")
			}
			if scenario != "running" {
				if err := observeECSLogTask(m.storeDB, p, stop, now.Unix()); err != nil {
					t.Fatal(err)
				}
			}
			b := &ecsarchive.FinalBoundary{ObservedAt: now.Unix(), Files: []ecsarchive.FinalFile{{Name: "new-api.log", Inode: 1, Size: 8, Offset: 8, SHA256: strings.Repeat("b", 64)}}}
			if scenario == "future_boundary" {
				b.ObservedAt += ecsLogSignatureSkew + 1
			}
			first, err := ecsarchive.Seal(m.cfg.ECSLogAudience, s.Node, s.Lane, ecsLaneTestBody(t, s.Node, s.Lane, 1), key)
			if err != nil {
				t.Fatal(err)
			}
			closure, err := ecsarchive.SealBoundaryClosure(m.cfg.ECSLogAudience, s.Node, s.Lane, first.Hash, b, key)
			if err != nil {
				t.Fatal(err)
			}
			object := func(e ecsarchive.Envelope) ecsarchive.Object {
				k, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32), e)
				if err != nil {
					t.Fatal(err)
				}
				raw, _ := json.Marshal(e)
				return ecsarchive.Object{Key: k, ETag: "fixture", Modified: now, Body: raw}
			}
			r := newECSArchiveReplayer(m, "isolated/")
			ctx := context.Background()
			if got := r.replay(ctx, object(closure), now); got.Status != 425 {
				t.Fatal("missing predecessor hidden", got)
			}
			if got := r.replay(ctx, object(first), now); !got.Accepted {
				t.Fatal(got)
			}
			got := r.replay(ctx, object(closure), now)
			want := 200
			switch scenario {
			case "running", "exit_unknown", "wrong_runtime":
				want = 425
			case "crashed", "abnormal_stop", "future_boundary":
				want = 422
			}
			if got.Status != want || got.FinalBoundaryVerified != (want == 200) {
				t.Fatal("final stop evidence", got, want)
			}
			if err := r.record(ctx, object(closure), got); err != nil {
				t.Fatal(err)
			}
			var sources []ECSLogSource
			if err := m.storeDB.Find(&sources).Error; err != nil {
				t.Fatal(err)
			}
			if err := projectArchiveDelivery(m.storeDB, sources); err != nil {
				t.Fatal(err)
			}
			if (sources[0].FinalBoundaryStatus == "retained_files_verified") != (want == 200) {
				t.Fatalf("false boundary projection %+v", sources)
			}
			if scenario == "normal" {
				if got := r.replay(ctx, object(closure), now); !got.Duplicate {
					t.Fatal("duplicate final boundary", got)
				}
			}
			if scenario == "running" {
				if err := observeECSLogTask(m.storeDB, p, stop, now.Unix()); err != nil {
					t.Fatal(err)
				}
				if got := r.replay(ctx, object(closure), now); !got.Accepted || !got.FinalBoundaryVerified {
					t.Fatal("late AWS stop did not recover", got)
				}
			}
			if rows := m.storeRejections(now.Unix() - 120); len(rows) != 1 || rows[0].Count != 1 {
				t.Fatal("boundary changed request counts", rows)
			}
		})
	}
}
