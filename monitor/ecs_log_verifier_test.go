package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

func testECSLogPolicy() ECSLogPolicy {
	return ECSLogPolicy{
		ClusterARN:  "arn:aws:ecs:us-west-2:123456789012:cluster/fixture",
		ServiceARN:  "arn:aws:ecs:us-west-2:123456789012:service/fixture/worker",
		TaskRoleARN: "arn:aws:iam::123456789012:role/fixture-task",
		Containers:  map[string][]string{"nginx": {"access", "error", "evidence", "reject"}},
	}
}

func testECSRegistration(taskID, key string) ecsLogRegistration {
	p := testECSLogPolicy()
	return ecsLogRegistration{
		CallerARN:  "arn:aws:sts::123456789012:assumed-role/fixture-task/" + taskID,
		ServiceARN: p.ServiceARN, TaskARN: "arn:aws:ecs:us-west-2:123456789012:task/fixture/" + taskID,
		Container: "nginx", RuntimeID: "fixture-runtime-" + taskID, Lane: "reject", PublicKey: key,
	}
}

type ecsLogTaskFixture struct {
	tasks      *ecs.DescribeTasksOutput
	definition *ecs.DescribeTaskDefinitionOutput
	err        error
}

func (f *ecsLogTaskFixture) DescribeTasks(context.Context, *ecs.DescribeTasksInput, ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error) {
	return f.tasks, f.err
}

func (f *ecsLogTaskFixture) DescribeTaskDefinition(context.Context, *ecs.DescribeTaskDefinitionInput, ...func(*ecs.Options)) (*ecs.DescribeTaskDefinitionOutput, error) {
	return f.definition, f.err
}

func validECSLogTaskFixture(in ecsLogRegistration) *ecsLogTaskFixture {
	p := testECSLogPolicy()
	definition := "arn:aws:ecs:us-west-2:123456789012:task-definition/fixture:1"
	return &ecsLogTaskFixture{
		tasks: &ecs.DescribeTasksOutput{Tasks: []types.Task{{TaskArn: aws.String(in.TaskARN), ClusterArn: aws.String(p.ClusterARN),
			Group: aws.String("service:worker"), LastStatus: aws.String("RUNNING"), DesiredStatus: aws.String("RUNNING"), TaskDefinitionArn: aws.String(definition),
			Containers: []types.Container{{Name: aws.String(in.Container), RuntimeId: aws.String(in.RuntimeID), LastStatus: aws.String("RUNNING")}},
		}}},
		definition: &ecs.DescribeTaskDefinitionOutput{TaskDefinition: &types.TaskDefinition{TaskDefinitionArn: aws.String(definition), TaskRoleArn: aws.String(p.TaskRoleARN)}},
	}
}

func TestECSLogTaskVerifierRequiresAuthoritativeIdentity(t *testing.T) {
	in := testECSRegistration(strings.Repeat("a", 32), "")
	p := testECSLogPolicy()
	if source, err := verifyECSLogTask(context.Background(), validECSLogTaskFixture(in), in, p); err != nil || source.TaskARN != in.TaskARN || source.RuntimeID != in.RuntimeID {
		t.Fatalf("valid task: %+v %v", source, err)
	}
	for name, mutate := range map[string]func(*ecsLogTaskFixture, *ecsLogRegistration){
		"caller-session": func(_ *ecsLogTaskFixture, in *ecsLogRegistration) { in.CallerARN += "x" },
		"caller-role": func(_ *ecsLogTaskFixture, in *ecsLogRegistration) {
			in.CallerARN = strings.ReplaceAll(in.CallerARN, "fixture-task", "wrong-role")
		},
		"cluster": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) { f.tasks.Tasks[0].ClusterArn = aws.String("other") },
		"task":    func(f *ecsLogTaskFixture, _ *ecsLogRegistration) { f.tasks.Tasks[0].TaskArn = aws.String("other") },
		"service": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) {
			f.tasks.Tasks[0].Group = aws.String("service:other")
		},
		"stopped": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) { f.tasks.Tasks[0].LastStatus = aws.String("STOPPED") },
		"stopping": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) {
			f.tasks.Tasks[0].DesiredStatus = aws.String("STOPPED")
		},
		"runtime": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) {
			f.tasks.Tasks[0].Containers[0].RuntimeId = aws.String("previous")
		},
		"container": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) {
			f.tasks.Tasks[0].Containers[0].Name = aws.String("other")
		},
		"definition-role": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) {
			f.definition.TaskDefinition.TaskRoleArn = aws.String("other")
		},
		"definition-identity": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) {
			f.definition.TaskDefinition.TaskDefinitionArn = aws.String("other")
		},
		"empty": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) { f.tasks = nil },
		"partial": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) {
			f.tasks.Failures = []types.Failure{{Reason: aws.String("MISSING")}}
		},
		"aws-error": func(f *ecsLogTaskFixture, _ *ecsLogRegistration) { f.err = errors.New("fixture outage") },
	} {
		t.Run(name, func(t *testing.T) {
			request := in
			f := validECSLogTaskFixture(in)
			mutate(f, &request)
			if _, err := verifyECSLogTask(context.Background(), f, request, p); err == nil {
				t.Fatal("unverified identity accepted")
			}
		})
	}
}

func TestECSLogPolicyFailClosed(t *testing.T) {
	b, err := json.Marshal([]ECSLogPolicy{testECSLogPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	valid := Settings{ECSLogEnabled: true, ECSLogScope: "isolated", ECSLogAudience: "fixture-local-monitor", ECSLogBridgeToken: strings.Repeat("b", 32), ECSLogPoliciesJSON: string(b), AWSRegion: "us-west-2", NginxEnabled: true, NginxErrorEnabled: true, NginxEvidenceMode: "pilot"}
	if _, err := parseECSLogPolicies(valid); err != nil {
		t.Fatal(err)
	}
	if p, err := parseECSLogPolicies(Settings{}); err != nil || len(p) != 0 {
		t.Fatal("legacy default changed")
	}
	for name, mutate := range map[string]func(*Settings){
		"production":       func(s *Settings) { s.ECSLogScope = "production" },
		"prod-dsn":         func(s *Settings) { s.ProdDSN = "fixture-not-a-real-DSN" },
		"source-worker":    func(s *Settings) { s.SourceWorkerEnabled = true },
		"missing-audience": func(s *Settings) { s.ECSLogAudience = "" },
		"offline":          func(s *Settings) { s.LocalSnapshotOnly = true },
		"auth-bypass":      func(s *Settings) { s.LocalAuthBypass = true },
		"shared-secret":    func(s *Settings) { s.IngestToken = s.ECSLogBridgeToken },
		"weak-secret":      func(s *Settings) { s.ECSLogBridgeToken = "weak" },
		"wrong-region":     func(s *Settings) { s.AWSRegion = "us-east-1" },
		"unknown-field": func(s *Settings) {
			s.ECSLogPoliciesJSON = strings.Replace(s.ECSLogPoliciesJSON, `"cluster_arn":`, `"typo":true,"cluster_arn":`, 1)
		},
		"empty": func(s *Settings) { s.ECSLogPoliciesJSON = "[]" },
		"duplicate-lane": func(s *Settings) {
			s.ECSLogPoliciesJSON = strings.Replace(s.ECSLogPoliciesJSON, `"error"`, `"access"`, 1)
		},
		"receiver-disabled": func(s *Settings) { s.NginxErrorEnabled = false },
	} {
		t.Run(name, func(t *testing.T) {
			s := valid
			mutate(&s)
			if _, err := parseECSLogPolicies(s); err == nil {
				t.Fatal("unsafe policy accepted")
			}
		})
	}
}
