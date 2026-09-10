package monitor

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

const (
	maxECSTaskInventoryPages = 100
	maxECSTaskInventorySize  = 10000
	ecsDescribeTaskBatchSize = 100
)

// This is an observation of RUNNING-desired tasks, not proof that absent
// tasks have stopped. Never use it alone to retire sources or discard logs.
// Any incomplete page or DescribeTasks failure invalidates the entire result.
func collectECSTaskInventory(ctx context.Context, client ecsTaskHealthAPI, clusterARN, serviceName string) ([]ecstypes.Task, error) {
	arns, err := listECSTaskARNs(ctx, client, clusterARN, serviceName)
	if err != nil {
		return nil, err
	}
	tasks := make([]ecstypes.Task, 0, len(arns))
	for start := 0; start < len(arns); start += ecsDescribeTaskBatchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := arns[start:min(start+ecsDescribeTaskBatchSize, len(arns))]
		out, err := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{Cluster: aws.String(clusterARN), Tasks: batch})
		if err != nil {
			return nil, err
		}
		if out == nil || len(out.Failures) > 0 {
			return nil, fmt.Errorf("ECS task inventory: DescribeTasks returned an incomplete result")
		}
		if err := validateECSTaskBatch(batch, out.Tasks); err != nil {
			return nil, err
		}
		tasks = append(tasks, out.Tasks...)
	}
	return tasks, nil
}

func listECSTaskARNs(ctx context.Context, client ecsTaskHealthAPI, clusterARN, serviceName string) ([]string, error) {
	var arns []string
	var token *string
	seenTokens, seenTasks := map[string]bool{}, map[string]bool{}
	for page := 0; page < maxECSTaskInventoryPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out, err := client.ListTasks(ctx, &ecs.ListTasksInput{
			Cluster: aws.String(clusterARN), ServiceName: aws.String(serviceName),
			DesiredStatus: ecstypes.DesiredStatusRunning, NextToken: token,
		})
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, fmt.Errorf("ECS task inventory: ListTasks returned no response")
		}
		for _, arn := range out.TaskArns {
			if strings.TrimSpace(arn) == "" {
				return nil, fmt.Errorf("ECS task inventory: empty task identity")
			}
			if !seenTasks[arn] {
				seenTasks[arn] = true
				arns = append(arns, arn)
			}
		}
		if len(arns) > maxECSTaskInventorySize {
			return nil, fmt.Errorf("ECS task inventory exceeded %d tasks", maxECSTaskInventorySize)
		}
		next := aws.ToString(out.NextToken)
		if strings.TrimSpace(next) == "" {
			return arns, nil
		}
		if seenTokens[next] {
			return nil, fmt.Errorf("ECS task inventory pagination repeated a token")
		}
		seenTokens[next], token = true, out.NextToken
	}
	return nil, fmt.Errorf("ECS task inventory exceeded %d pages", maxECSTaskInventoryPages)
}

func validateECSTaskBatch(requested []string, tasks []ecstypes.Task) error {
	pending := make(map[string]bool, len(requested))
	for _, arn := range requested {
		pending[arn] = true
	}
	for _, task := range tasks {
		arn := aws.ToString(task.TaskArn)
		if !pending[arn] {
			return fmt.Errorf("ECS task inventory: unexpected or duplicate task identity")
		}
		delete(pending, arn)
	}
	if len(pending) > 0 {
		return fmt.Errorf("ECS task inventory: %d task descriptions missing", len(pending))
	}
	return nil
}

func summarizeECSTasks(tasks []ecstypes.Task) (snapshot ecsServiceTaskSnapshot) {
	for _, task := range tasks {
		if aws.ToString(task.LastStatus) != "RUNNING" {
			continue
		}
		memory, err := strconv.ParseFloat(strings.TrimSpace(aws.ToString(task.Memory)), 64)
		if err == nil && memory > 0 && !math.IsInf(memory, 0) {
			snapshot.MemoryReservedMB += memory
		}
		if task.EphemeralStorage != nil && task.EphemeralStorage.SizeInGiB > 0 {
			snapshot.EphemeralReservedGB += float64(task.EphemeralStorage.SizeInGiB)
		}
		for _, container := range task.Containers {
			switch container.HealthStatus {
			case ecstypes.HealthStatusHealthy:
				snapshot.HealthChecked++
			case ecstypes.HealthStatusUnhealthy:
				snapshot.HealthChecked++
				snapshot.Unhealthy++
			}
		}
	}
	return snapshot
}
