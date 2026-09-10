package monitor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

type ecsInventoryTestClient struct {
	list     func(*ecs.ListTasksInput) (*ecs.ListTasksOutput, error)
	describe func(*ecs.DescribeTasksInput) (*ecs.DescribeTasksOutput, error)
}

func (f ecsInventoryTestClient) ListTasks(_ context.Context, in *ecs.ListTasksInput, _ ...func(*ecs.Options)) (*ecs.ListTasksOutput, error) {
	return f.list(in)
}

func (f ecsInventoryTestClient) DescribeTasks(_ context.Context, in *ecs.DescribeTasksInput, _ ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error) {
	return f.describe(in)
}

func TestECSTaskInventoryRejectsIncompleteDescriptions(t *testing.T) {
	for name, output := range map[string]*ecs.DescribeTasksOutput{
		"nil":       nil,
		"missing":   {},
		"failure":   {Failures: []ecstypes.Failure{{Arn: aws.String("task-1")}}},
		"foreign":   {Tasks: []ecstypes.Task{{TaskArn: aws.String("task-2")}}},
		"duplicate": {Tasks: []ecstypes.Task{{TaskArn: aws.String("task-1")}, {TaskArn: aws.String("task-1")}}},
	} {
		t.Run(name, func(t *testing.T) {
			client := &fakeECSTaskHealthClient{listOutputs: []*ecs.ListTasksOutput{{TaskArns: []string{"task-1"}}}, describeOutputs: []*ecs.DescribeTasksOutput{output}}
			got, err := ecsServiceTaskState(context.Background(), client, "cluster", "service")
			if err == nil || got != (ecsServiceTaskSnapshot{}) {
				t.Fatalf("partial inventory published: %+v, %v", got, err)
			}
		})
	}
}

func TestECSTaskInventoryPaginationIsBoundedAndAtomic(t *testing.T) {
	for _, mode := range []string{"failure", "repeat", "budget", "nil", "empty_identity", "size"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			client := ecsInventoryTestClient{list: func(in *ecs.ListTasksInput) (*ecs.ListTasksOutput, error) {
				calls++
				if aws.ToString(in.ServiceName) != "service" || in.DesiredStatus != ecstypes.DesiredStatusRunning {
					t.Fatal("incorrect scope")
				}
				if mode == "failure" && calls == 2 {
					return nil, errors.New("AWS unavailable")
				}
				if mode == "nil" {
					return nil, nil
				}
				if mode == "empty_identity" {
					return &ecs.ListTasksOutput{TaskArns: []string{""}}, nil
				}
				if mode == "size" {
					arns := make([]string, maxECSTaskInventorySize+1)
					for i := range arns {
						arns[i] = fmt.Sprint(i)
					}
					return &ecs.ListTasksOutput{TaskArns: arns}, nil
				}
				token := "repeat"
				if mode == "budget" {
					token = fmt.Sprint(calls)
				}
				return &ecs.ListTasksOutput{TaskArns: []string{"task-1"}, NextToken: &token}, nil
			}, describe: func(*ecs.DescribeTasksInput) (*ecs.DescribeTasksOutput, error) {
				t.Fatal("must finish inventory before describing")
				return nil, nil
			}}
			got, err := collectECSTaskInventory(context.Background(), client, "cluster", "service")
			if err == nil || got != nil || calls > maxECSTaskInventoryPages {
				t.Fatalf("got=%v err=%v calls=%d", got, err, calls)
			}
		})
	}
}

func TestECSTaskInventoryDeduplicatesAndBatches(t *testing.T) {
	listCalls, describeCalls := 0, 0
	arns := make([]string, ecsDescribeTaskBatchSize+1)
	for i := range arns {
		arns[i] = fmt.Sprintf("task-%d", i)
	}
	client := ecsInventoryTestClient{
		list: func(in *ecs.ListTasksInput) (*ecs.ListTasksOutput, error) {
			listCalls++
			if listCalls == 1 {
				return &ecs.ListTasksOutput{TaskArns: arns, NextToken: aws.String("page2")}, nil
			}
			if aws.ToString(in.NextToken) != "page2" {
				t.Fatal("pagination lost")
			}
			return &ecs.ListTasksOutput{TaskArns: arns[:1]}, nil
		}, describe: func(in *ecs.DescribeTasksInput) (*ecs.DescribeTasksOutput, error) {
			describeCalls++
			if len(in.Tasks) > ecsDescribeTaskBatchSize {
				t.Fatal("batch exceeds limit")
			}
			out := &ecs.DescribeTasksOutput{}
			for _, arn := range in.Tasks {
				out.Tasks = append(out.Tasks, ecstypes.Task{TaskArn: aws.String(arn), LastStatus: aws.String("RUNNING"), Memory: aws.String("1024")})
			}
			return out, nil
		},
	}
	got, err := ecsServiceTaskState(context.Background(), client, "cluster", "service")
	if err != nil || got.MemoryReservedMB != float64(len(arns)*1024) || describeCalls != 2 {
		t.Fatalf("got=%+v err=%v calls=%d", got, err, describeCalls)
	}
}

func TestECSTaskInventoryCancellationAndUnknownState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := collectECSTaskInventory(ctx, ecsInventoryTestClient{}, "cluster", "service"); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("got=%v err=%v", got, err)
	}
	tasks := []ecstypes.Task{{Memory: aws.String("1024")}, {LastStatus: aws.String("STOPPED"), Memory: aws.String("1024")}, {LastStatus: aws.String("RUNNING"), Memory: aws.String("+Inf")}}
	if got := summarizeECSTasks(tasks); got != (ecsServiceTaskSnapshot{}) {
		t.Fatalf("unknown state or invalid capacity counted: %+v", got)
	}
}

func TestECSTaskDiscoveryFailureIsVisibleWithoutMaskingFailure(t *testing.T) {
	m := newTestMonitor(t)
	r := InfraResource{Type: "ecs_service", Metrics: map[string]float64{"task_discovery_ok": 0, "status_failed": 0, "cpu": 1}}
	if got := m.infraStatus(r); got != "warn" {
		t.Fatalf("discovery failure status=%s", got)
	}
	r.Metrics["status_failed"] = 1
	if got := m.infraStatus(r); got != "bad" {
		t.Fatalf("service failure masked: %s", got)
	}
}
