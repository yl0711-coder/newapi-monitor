package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/gin-gonic/gin"
)

type ecsLogInventoryFixture struct {
	running []string
	tasks   map[string]types.Task
	err     error
}

func (f *ecsLogInventoryFixture) ListTasks(context.Context, *ecs.ListTasksInput, ...func(*ecs.Options)) (*ecs.ListTasksOutput, error) {
	return &ecs.ListTasksOutput{TaskArns: f.running}, f.err
}

func (f *ecsLogInventoryFixture) DescribeTasks(_ context.Context, in *ecs.DescribeTasksInput, _ ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error) {
	out := &ecs.DescribeTasksOutput{}
	for _, arn := range in.Tasks {
		if task, ok := f.tasks[arn]; ok {
			out.Tasks = append(out.Tasks, task)
		} else {
			out.Failures = append(out.Failures, types.Failure{Arn: aws.String(arn), Reason: aws.String("MISSING")})
		}
	}
	return out, f.err
}

func ecsLogTestTasks(count int) []types.Task {
	tasks := make([]types.Task, 0, count)
	for i := 0; i < count; i++ {
		in := testECSRegistration(fmt.Sprintf("%032x", i+1), "")
		tasks = append(tasks, validECSLogTaskFixture(in).tasks.Tasks[0])
	}
	return tasks
}

func TestECSLogDiscoveryScaleAndFailure(t *testing.T) {
	for _, ownership := range []bool{false, true} {
		t.Run(fmt.Sprintf("ownership_%t", ownership), func(t *testing.T) {
			testECSLogDiscoveryScaleAndFailure(t, ownership)
		})
	}
}

func testECSLogDiscoveryScaleAndFailure(t *testing.T, ownership bool) {
	t.Helper()
	m := newECSLogTestMonitor(t)
	if ownership {
		m.cfg.ECSLogOwnershipEnabled = true
		if err := m.initECSLogOwnership(m.storeDB); err != nil {
			t.Fatal(err)
		}
	}
	p := testECSLogPolicy()
	now := time.Now().Unix()
	f := &ecsLogInventoryFixture{tasks: map[string]types.Task{}}
	all := ecsLogTestTasks(5)
	setRunning := func(indices ...int) {
		f.running = nil
		for _, i := range indices {
			task := all[i]
			f.running = append(f.running, aws.ToString(task.TaskArn))
			f.tasks[aws.ToString(task.TaskArn)] = task
		}
	}
	assertSources := func(want int64) []ECSLogSource {
		t.Helper()
		var sources []ECSLogSource
		if err := m.storeDB.Order("node, lane").Find(&sources).Error; err != nil || int64(len(sources)) != want {
			t.Fatalf("sources=%d want=%d err=%v", len(sources), want, err)
		}
		return sources
	}
	setRunning(0, 1, 2)
	if err := m.reconcileECSLogService(context.Background(), f, p, now); err != nil {
		t.Fatal(err)
	}
	initial := assertSources(12)
	for _, source := range initial {
		if source.PublicKey != "" || source.LeaseUntil != 0 || ecsLogSourceStatus(source, now) != "starting" {
			t.Fatal("discovery implicitly authorized a task")
		}
	}
	registerOwners := func(start, end int) {
		t.Helper()
		if !ownership {
			return
		}
		for i := start; i < end; i++ {
			for _, lane := range []string{"access", "error", "evidence", "reject"} {
				registerECSTestSource(t, m, fmt.Sprintf("%032x", i+1), lane)
			}
		}
	}
	registerOwners(0, 3)
	setRunning(0, 1, 2, 3, 4)
	if err := m.reconcileECSLogService(context.Background(), f, p, now+30); err != nil {
		t.Fatal(err)
	}
	assertSources(20)
	registerOwners(3, 5)
	var ownersBefore []ECSLogOwnership
	if ownership {
		if err := m.storeDB.Order("task_arn, container, lane").Find(&ownersBefore).Error; err != nil || len(ownersBefore) != 20 {
			t.Fatalf("five tasks must own twenty independent lanes: %d %v", len(ownersBefore), err)
		}
	}
	// Scale-in leaves task 1, 2, 4. Only explicit STOPPED confirms task 3/5.
	setRunning(0, 1, 3)
	for _, i := range []int{2, 4} {
		task := all[i]
		task.LastStatus = aws.String("STOPPED")
		task.StoppedAt = aws.Time(time.Unix(now+45, 0))
		task.StopCode = types.TaskStopCodeServiceSchedulerInitiated
		f.tasks[aws.ToString(task.TaskArn)] = task
	}
	if err := m.reconcileECSLogService(context.Background(), f, p, now+60); err != nil {
		t.Fatal(err)
	}
	sources := assertSources(20)
	stopped := 0
	for _, source := range sources {
		if source.StoppedAt != 0 {
			stopped++
			if ecsLogSourceStatus(source, now+360) != "stopped_delivery_unconfirmed" {
				t.Fatal("normal scale-in reported heartbeat failure or false completion")
			}
		}
	}
	if stopped != 8 {
		t.Fatalf("stopped lanes=%d", stopped)
	}
	// An old inventory cannot resurrect retired tasks or move the watermark.
	if err := m.observeECSLogTasks(context.Background(), p, all, now+30); err != nil {
		t.Fatal(err)
	}
	f.err = errors.New("fixture AWS outage")
	if err := m.reconcileECSLogService(context.Background(), f, p, now+90); err == nil {
		t.Fatal("outage hidden")
	}
	var after []ECSLogSource
	if err := m.storeDB.Order("node, lane").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(sources)
	b, _ := json.Marshal(after)
	if string(a) != string(b) {
		t.Fatal("AWS failure changed source lifecycle")
	}
	if ownership {
		var ownersAfter []ECSLogOwnership
		if err := m.storeDB.Order("task_arn, container, lane").Find(&ownersAfter).Error; err != nil {
			t.Fatal(err)
		}
		a, _ := json.Marshal(ownersBefore)
		b, _ := json.Marshal(ownersAfter)
		if string(a) != string(b) {
			t.Fatal("scale-in or discovery failure changed task responsibility")
		}
		for _, source := range after {
			if err := m.checkECSLogOwnership(context.Background(), source); err != nil {
				t.Fatal("discovery lost verified ownership", err)
			}
		}
	}
	var discovery ECSLogDiscovery
	if err := m.storeDB.First(&discovery).Error; err != nil || discovery.LastSuccess != now+60 || discovery.LastFailure != now+90 {
		t.Fatalf("discovery status %+v %v", discovery, err)
	}
}

func TestECSLogMissingIsNotStoppedAndRollingRevisionsCoexist(t *testing.T) {
	m := newECSLogTestMonitor(t)
	p := testECSLogPolicy()
	p.TaskDefinitionARNs = append(p.TaskDefinitionARNs, "arn:aws:ecs:us-west-2:123456789012:task-definition/fixture:2")
	now := time.Now().Unix()
	tasks := ecsLogTestTasks(2)
	tasks[1].TaskDefinitionArn = aws.String("arn:aws:ecs:us-west-2:123456789012:task-definition/fixture:2")
	if err := m.observeECSLogTasks(context.Background(), p, tasks, now); err != nil {
		t.Fatal(err)
	}
	f := &ecsLogInventoryFixture{running: []string{aws.ToString(tasks[1].TaskArn)}, tasks: map[string]types.Task{aws.ToString(tasks[1].TaskArn): tasks[1]}}
	if err := m.reconcileECSLogService(context.Background(), f, p, now+30); err == nil {
		t.Fatal("missing task treated as confirmed exit")
	}
	var count int64
	if err := m.storeDB.Model(&ECSLogSource{}).Where("stopped_at > 0").Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("incorrect retirement=%d %v", count, err)
	}
	// Same task, producer restarted: a new runtime ID is a different source.
	tasks[0].Containers[0].RuntimeId = aws.String("replacement-runtime")
	if err := m.observeECSLogTasks(context.Background(), p, tasks, now+60); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ECSLogSource{}).Count(&count).Error; err != nil || count != 12 {
		t.Fatalf("container incarnation overwritten=%d %v", count, err)
	}
	if err := m.storeDB.Model(&ECSLogSource{}).Where("public_key <> ''").Count(&count).Error; err != nil || count != 0 {
		t.Fatal("discovery authorized container without registration")
	}
}

func TestECSLogDiscoveryDoesNotExpectUnauthorizedTaskDefinition(t *testing.T) {
	m := newECSLogTestMonitor(t)
	p := testECSLogPolicy()
	task := ecsLogTestTasks(1)[0]
	task.TaskDefinitionArn = aws.String("arn:aws:ecs:us-west-2:123456789012:task-definition/fixture:2")
	if err := m.observeECSLogTasks(context.Background(), p, []types.Task{task}, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.storeDB.Model(&ECSLogSource{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("unauthorized rolling task became an expected source: %d %v", count, err)
	}
}

func TestECSLogSourcesStatusAndPrivateFields(t *testing.T) {
	m := newECSLogTestMonitor(t)
	source, _ := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
	r := gin.New()
	r.GET("/sources", m.serveECSLogSources)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/sources", nil))
	if w.Code != 200 || strings.Contains(w.Body.String(), source.PublicKey) || strings.Contains(w.Body.String(), "public_key") || !strings.Contains(w.Body.String(), `"production_ready":false`) {
		t.Fatalf("unsafe status response: %d %s", w.Code, w.Body.String())
	}
	now := time.Now().Unix()
	for _, tc := range []struct {
		code string
		age  int64
		want string
	}{
		{"ServiceSchedulerInitiated", 30, "stopped_delivery_unconfirmed"},
		{"EssentialContainerExited", 30, "delivery_gap_unresolved"},
		{"ServiceSchedulerInitiated", ecsLogReplaySeconds + 1, "delivery_gap_unresolved"},
	} {
		source.StoppedAt, source.StopCode = now-tc.age, tc.code
		if got := ecsLogSourceStatus(source, now); got != tc.want {
			t.Fatalf("stop status=%s want=%s", got, tc.want)
		}
	}
}

func TestECSLogDiscoveryDeadlineFailureIsVisible(t *testing.T) {
	m := newECSLogTestMonitor(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	now := time.Now().Unix()
	if err := m.recordECSLogDiscoveryFailure(ctx, testECSLogPolicy().ServiceARN, now); err != nil {
		t.Fatal(err)
	}
	var discovery ECSLogDiscovery
	if err := m.storeDB.First(&discovery).Error; err != nil || ecsLogDiscoveryStatus(discovery, now) != "discovery_failed" {
		t.Fatalf("timeout not visible: %+v %v", discovery, err)
	}
	for _, tc := range []struct {
		row  ECSLogDiscovery
		want string
	}{
		{ECSLogDiscovery{}, "awaiting_discovery"},
		{ECSLogDiscovery{LastSuccess: now - ecsLogDiscoveryStaleSeconds - 1}, "discovery_stale"},
		{ECSLogDiscovery{LastSuccess: now, LastFailure: now - 30}, "discovered"},
	} {
		if got := ecsLogDiscoveryStatus(tc.row, now); got != tc.want {
			t.Fatalf("status=%s want=%s", got, tc.want)
		}
	}
}

func TestECSLogSourceStatusPaginationIncludesHistoricalSources(t *testing.T) {
	m := newECSLogTestMonitor(t)
	if err := m.observeECSLogTasks(context.Background(), testECSLogPolicy(), ecsLogTestTasks(27), time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.GET("/sources", m.serveECSLogSources)
	for _, tc := range []struct {
		page string
		want int
		more bool
	}{{"1", 100, true}, {"2", 8, false}} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/sources?page="+tc.page, nil))
		var body struct {
			Sources []ecsLogSourceView `json:"sources"`
			Total   int                `json:"total"`
			HasMore bool               `json:"has_more"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || len(body.Sources) != tc.want || body.Total != 108 || body.HasMore != tc.more {
			t.Fatalf("pagination %s: %d %s", tc.page, w.Code, w.Body.String())
		}
	}
}
