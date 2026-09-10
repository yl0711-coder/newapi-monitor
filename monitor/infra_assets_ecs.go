package monitor

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

func ecsAssetObservations(resource, serviceARN string, tasks []ecstypes.Task) []infraAssetObservation {
	// ARNs come from authenticated AWS Describe responses, not from an
	// untrusted collector's task ID. This does NOT authorize log ingestion.
	if serviceARN == "" {
		return nil
	}
	state := "idle"
	for _, task := range tasks {
		if aws.ToString(task.LastStatus) == "RUNNING" {
			state = "running"
			break
		}
	}
	rows := []infraAssetObservation{{Identity: serviceARN, Resource: resource, Kind: "ecs_service", Platform: "ECS/Fargate", CloudState: state}}
	for _, task := range tasks {
		arn := aws.ToString(task.TaskArn)
		if arn == "" {
			continue
		}
		rows = append(rows, infraAssetObservation{Identity: arn, Resource: "ecs/task/" + arnTail(arn), Parent: resource, Kind: "ecs_task", Platform: "ECS/Fargate", CloudState: strings.ToLower(aws.ToString(task.LastStatus))})
	}
	return rows
}
