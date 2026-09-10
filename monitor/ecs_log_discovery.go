package monitor

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Only successful, complete AWS observations are applied. Absence from a
// RUNNING inventory is never converted into STOPPED. Explicitly describe known
// missing tasks; a MISSING response remains discovery uncertainty, not exit.
func (m *Monitor) reconcileECSLogService(ctx context.Context, client ecsTaskHealthAPI, p ECSLogPolicy, now int64) error {
	tasks, err := m.collectECSLogTasks(ctx, client, p)
	if err != nil {
		if recordErr := m.recordECSLogDiscoveryFailure(ctx, p.ServiceARN, now); recordErr != nil {
			return errors.Join(err, recordErr)
		}
		return err
	}
	if err := m.observeECSLogTasks(ctx, p, tasks, now); err != nil {
		return errors.Join(err, m.recordECSLogDiscoveryFailure(ctx, p.ServiceARN, now))
	}
	return nil
}

func (m *Monitor) collectECSLogTasks(ctx context.Context, client ecsTaskHealthAPI, p ECSLogPolicy) ([]types.Task, error) {
	service := p.ServiceARN[strings.LastIndex(p.ServiceARN, "/")+1:]
	tasks, err := collectECSTaskInventory(ctx, client, p.ClusterARN, service)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(tasks))
	for _, task := range tasks {
		seen[aws.ToString(task.TaskArn)] = true
	}
	var known []string
	if err := m.storeDB.WithContext(ctx).Model(&ECSLogSource{}).Where("service_arn = ? AND (stopped_at = 0 OR (producer_exit_code IS NULL AND stopped_at >= ?)) AND revoked = ?", p.ServiceARN, time.Now().Unix()-ecsLogReplaySeconds, false).Distinct("task_arn").Limit(ecsLogSourceLimit+1).Pluck("task_arn", &known).Error; err != nil {
		return nil, err
	}
	if len(known) > ecsLogSourceLimit {
		return nil, errors.New("ECS log discovery source budget exceeded")
	}
	missing := make([]string, 0, len(known))
	for _, arn := range known {
		if !seen[arn] {
			missing = append(missing, arn)
		}
	}
	for start := 0; start < len(missing); start += ecsDescribeTaskBatchSize {
		batch := missing[start:min(start+ecsDescribeTaskBatchSize, len(missing))]
		out, err := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{Cluster: aws.String(p.ClusterARN), Tasks: batch})
		if err != nil {
			return nil, err
		}
		if out == nil || len(out.Failures) != 0 {
			return nil, errors.New("ECS missing task status unresolved")
		}
		if err := validateECSTaskBatch(batch, out.Tasks); err != nil {
			return nil, err
		}
		tasks = append(tasks, out.Tasks...)
	}
	return tasks, nil
}

func validateECSLogObservation(p ECSLogPolicy, tasks []types.Task, now int64) error {
	service := p.ServiceARN[strings.LastIndex(p.ServiceARN, "/")+1:]
	seen := map[string]bool{}
	for _, task := range tasks {
		arn := aws.ToString(task.TaskArn)
		parts := ecsLogARNPattern.FindStringSubmatch(arn)
		if len(parts) == 0 || parts[3] != "task" || !ecsTaskIDPattern.MatchString(parts[5]) || aws.ToString(task.ClusterArn) != p.ClusterARN || !strings.HasPrefix(arn, strings.Replace(p.ClusterARN, ":cluster/", ":task/", 1)+"/") || aws.ToString(task.Group) != "service:"+service || seen[arn] {
			return errors.New("ECS log discovery returned an unexpected task")
		}
		seen[arn] = true
		if aws.ToString(task.LastStatus) == "STOPPED" && (task.StoppedAt == nil || task.StoppedAt.Unix() <= 0 || task.StoppedAt.Unix() > now+ecsLogSignatureSkew) {
			return errors.New("ECS stopped task has no trustworthy stop timestamp")
		}
	}
	return nil
}

func (m *Monitor) observeECSLogTasks(ctx context.Context, p ECSLogPolicy, tasks []types.Task, now int64) error {
	if err := validateECSLogObservation(p, tasks, now); err != nil {
		return err
	}
	return m.infraAssetWrite(ctx, 5*time.Second, func(tx *gorm.DB) error {
		var watermark ECSLogDiscovery
		err := tx.First(&watermark, "service_arn = ?", p.ServiceARN).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if watermark.LastSuccess >= now {
			return nil
		}
		for _, task := range tasks {
			if err := observeECSLogTask(tx, p, task, now); err != nil {
				return err
			}
		}
		var count int64
		if err := tx.Model(&ECSLogSource{}).Count(&count).Error; err != nil {
			return err
		}
		if count > ecsLogSourceLimit {
			return errors.New("ECS source registry capacity reached")
		}
		watermark.ServiceARN, watermark.LastSuccess = p.ServiceARN, now
		return tx.Save(&watermark).Error
	})
}

func observeECSLogTask(tx *gorm.DB, p ECSLogPolicy, task types.Task, now int64) error {
	arn := aws.ToString(task.TaskArn)
	if aws.ToString(task.LastStatus) == "STOPPED" {
		// Do not renew a stopped task's lease. A separate archive-scoped replay
		// identity is required beyond the current short lease. Archive recovery
		// never renews this task or updates its live heartbeat.
		if err := tx.Model(&ECSLogSource{}).Where("service_arn = ? AND task_arn = ? AND stopped_at = 0", p.ServiceARN, arn).Updates(map[string]any{"stopped_at": task.StoppedAt.Unix(), "stop_code": string(task.StopCode), "last_discovered": now}).Error; err != nil {
			return err
		}
		for _, c := range task.Containers {
			if aws.ToString(c.LastStatus) != "STOPPED" || c.ExitCode == nil || aws.ToString(c.RuntimeId) == "" {
				continue
			}
			if err := tx.Model(&ECSLogSource{}).Where("service_arn = ? AND task_arn = ? AND container = ? AND runtime_id = ?", p.ServiceARN, arn, aws.ToString(c.Name), aws.ToString(c.RuntimeId)).Updates(map[string]any{"producer_exit_code": *c.ExitCode}).Error; err != nil {
				return err
			}
		}
		return nil
	}
	if aws.ToString(task.LastStatus) != "RUNNING" {
		return nil
	}
	// A service can contain old and candidate task definitions during a rolling
	// deployment. Only definitions explicitly authorized by the same policy as
	// registration become expected log sources; otherwise healthy legacy tasks
	// would be misreported as missing collectors during the overlap window.
	if !ecsLogTaskDefinitionAllowed(p, aws.ToString(task.TaskDefinitionArn)) {
		return nil
	}
	for _, container := range task.Containers {
		name, runtimeID := aws.ToString(container.Name), aws.ToString(container.RuntimeId)
		if runtimeID == "" || len(runtimeID) > 256 || aws.ToString(container.LastStatus) != "RUNNING" {
			continue
		}
		for _, lane := range p.Containers[name] {
			source := ECSLogSource{Node: ecsLogNode(arn, name, runtimeID), Lane: lane, TaskARN: arn, ServiceARN: p.ServiceARN, Container: name, RuntimeID: runtimeID, FirstSeen: now, LastDiscovered: now}
			var previous ECSLogSource
			err := tx.First(&previous, "node = ? AND lane = ?", source.Node, lane).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if err := tx.Create(&source).Error; err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if previous.TaskARN != arn || previous.Container != name || previous.RuntimeID != runtimeID || previous.ServiceARN != p.ServiceARN {
				return errors.New("ECS source identity collision")
			}
			// A late RUNNING observation cannot resurrect a retired incarnation.
			if previous.StoppedAt == 0 && previous.LastDiscovered < now {
				if err := tx.Model(&previous).Update("last_discovered", now).Error; err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (m *Monitor) recordECSLogDiscoveryFailure(ctx context.Context, service string, now int64) error {
	// A timed-out AWS request must still leave a visible local failure marker.
	// Detach only for this bounded local write, never for further AWS requests.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
	}
	return m.infraAssetWrite(ctx, 3*time.Second, func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "service_arn"}}, DoUpdates: clause.Assignments(map[string]any{"last_failure": gorm.Expr("MAX(last_failure,excluded.last_failure)")})}).Create(&ECSLogDiscovery{ServiceARN: service, LastFailure: now}).Error
	})
}

// This status is collection liveness, NOT a claim of complete request coverage.
// Stopped tasks need a final boundary even on normal scale-in; without it they
// remain pending/gap instead of silently becoming green or disappearing.
func ecsLogSourceStatus(source ECSLogSource, now int64) string {
	if source.Revoked {
		return "revoked"
	}
	if source.StoppedAt > 0 {
		if source.StopCode != "ServiceSchedulerInitiated" && source.StopCode != "UserInitiated" || source.ArchiveStatus == "archive_delivery_conflict" || source.ProducerExitCode != nil && *source.ProducerExitCode != 0 {
			return "delivery_gap_unresolved"
		}
		if source.ArchiveStatus == "collector_chain_closed" {
			return "stopped_archive_drained" // Still no producer/interval coverage claim.
		}
		if now > source.StoppedAt+ecsLogReplaySeconds {
			return "delivery_gap_unresolved"
		}
		return "stopped_delivery_unconfirmed"
	}
	if source.ArchiveStatus == "archive_delivery_conflict" {
		return "archive_delivery_conflict"
	}
	lastLive := max(source.LastReport, source.LastHeartbeat)
	if now < source.FirstSeen+ecsLogStartupSeconds && lastLive == 0 {
		return "starting"
	}
	if source.LeaseUntil <= now {
		return "lease_expired"
	}
	if lastLive == 0 {
		return "awaiting_first_report"
	}
	if now-lastLive > ecsLogReportStaleSeconds {
		return "report_stale"
	}
	if source.LastReport == 0 {
		return "heartbeating_no_log_batches"
	}
	return "reporting"
}
