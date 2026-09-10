package monitor

import (
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
)

type ecsLogTaskAPI interface {
	DescribeTasks(context.Context, *ecs.DescribeTasksInput, ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error)
	DescribeTaskDefinition(context.Context, *ecs.DescribeTaskDefinitionInput, ...func(*ecs.Options)) (*ecs.DescribeTaskDefinitionOutput, error)
}

type ecsLogRegistration struct {
	// CallerARN is inserted by the trusted AWS_IAM bridge, never accepted from
	// the collector at a public route. Bridge authentication is checked first.
	CallerARN  string `json:"caller_arn"`
	ServiceARN string `json:"service_arn"`
	TaskARN    string `json:"task_arn"`
	Container  string `json:"container"`
	RuntimeID  string `json:"runtime_id"`
	Lane       string `json:"lane"`
	PublicKey  string `json:"public_key"`
}

func validateECSCaller(in ecsLogRegistration, p ECSLogPolicy) bool {
	task, service := ecsLogARNPattern.FindStringSubmatch(in.TaskARN), ecsLogARNPattern.FindStringSubmatch(p.ServiceARN)
	if len(task) == 0 || len(service) == 0 || task[3] != "task" || task[1] != service[1] || task[2] != service[2] || task[4] != service[4] || !ecsTaskIDPattern.MatchString(task[5]) {
		return false
	}
	roleName := p.TaskRoleARN[strings.LastIndex(p.TaskRoleARN, "/")+1:]
	want := "arn:aws:sts::" + task[2] + ":assumed-role/" + roleName + "/" + task[5]
	return in.CallerARN == want
}

func verifyECSLogTask(ctx context.Context, client ecsLogTaskAPI, in ecsLogRegistration, p ECSLogPolicy) (ECSLogSource, error) {
	if !validateECSCaller(in, p) {
		return ECSLogSource{}, errors.New("caller/task binding rejected")
	}
	out, err := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{Cluster: aws.String(p.ClusterARN), Tasks: []string{in.TaskARN}})
	if err != nil {
		return ECSLogSource{}, err
	}
	if out == nil || len(out.Failures) > 0 || len(out.Tasks) != 1 {
		return ECSLogSource{}, errors.New("task verification incomplete")
	}
	task := out.Tasks[0]
	serviceName := p.ServiceARN[strings.LastIndex(p.ServiceARN, "/")+1:]
	if aws.ToString(task.TaskArn) != in.TaskARN || aws.ToString(task.ClusterArn) != p.ClusterARN || aws.ToString(task.Group) != "service:"+serviceName || aws.ToString(task.LastStatus) != "RUNNING" || aws.ToString(task.DesiredStatus) != "RUNNING" || task.TaskDefinitionArn == nil {
		return ECSLogSource{}, errors.New("task is not an active member of the authorized service")
	}
	definition, err := client.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{TaskDefinition: task.TaskDefinitionArn})
	if err != nil {
		return ECSLogSource{}, err
	}
	if definition == nil || definition.TaskDefinition == nil || aws.ToString(definition.TaskDefinition.TaskDefinitionArn) != aws.ToString(task.TaskDefinitionArn) || !ecsLogTaskDefinitionAllowed(p, aws.ToString(task.TaskDefinitionArn)) || aws.ToString(definition.TaskDefinition.TaskRoleArn) != p.TaskRoleARN {
		return ECSLogSource{}, errors.New("task definition role verification failed")
	}
	for _, container := range task.Containers {
		if aws.ToString(container.Name) == in.Container && aws.ToString(container.RuntimeId) == in.RuntimeID && in.RuntimeID != "" && len(in.RuntimeID) <= 256 && aws.ToString(container.LastStatus) == "RUNNING" {
			return ECSLogSource{ServiceARN: p.ServiceARN, TaskARN: in.TaskARN, TaskDefinitionARN: aws.ToString(task.TaskDefinitionArn), TaskRoleARN: p.TaskRoleARN, Container: in.Container, RuntimeID: in.RuntimeID, Lane: in.Lane}, nil
		}
	}
	return ECSLogSource{}, errors.New("producer container incarnation verification failed")
}

func (m *Monitor) verifyECSRegistration(ctx context.Context, in ecsLogRegistration, p ECSLogPolicy) (ECSLogSource, error) {
	if m.ecsLogVerify != nil {
		return m.ecsLogVerify(ctx, in, p)
	} // injected only by isolated tests
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(m.cfg.AWSRegion), awsconfig.WithRetryMaxAttempts(2))
	if err != nil {
		return ECSLogSource{}, err
	}
	return verifyECSLogTask(ctx, ecs.NewFromConfig(cfg), in, p)
}
