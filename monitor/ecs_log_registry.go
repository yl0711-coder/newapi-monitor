package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
)

// Node is task + producer container incarnation; Lane completes the source
// identity. Existing fact stores already separate access/error/evidence/reject.
// Do not rewrite historical ecs-canary nodes or reuse a previous task's cursor.
type ECSLogSource struct {
	Node                string `gorm:"primaryKey;size:64" json:"node"`
	Lane                string `gorm:"primaryKey;size:16" json:"lane"`
	ServiceARN          string `gorm:"index;size:512" json:"service_arn"`
	TaskARN             string `gorm:"index;size:512" json:"task_arn"`
	TaskDefinitionARN   string `gorm:"size:512" json:"task_definition_arn,omitempty"`
	TaskRoleARN         string `gorm:"size:512" json:"-"`
	Container           string `gorm:"size:64" json:"container"`
	RuntimeID           string `gorm:"size:256" json:"runtime_id"`
	PublicKey           string `gorm:"size:64" json:"-"`
	LeaseUntil          int64  `json:"lease_until"`
	FirstSeen           int64  `json:"first_seen"`
	LastDiscovered      int64  `json:"last_discovered"`
	LastReport          int64  `json:"last_report"`
	LastHeartbeat       int64  `json:"last_heartbeat"`
	StoppedAt           int64  `json:"stopped_at"`
	StopCode            string `gorm:"size:64" json:"stop_code,omitempty"`
	ProducerExitCode    *int32 `json:"producer_exit_code,omitempty"`
	FinalBoundaryStatus string `gorm:"-" json:"final_boundary_status,omitempty"`
	Revoked             bool   `json:"revoked"`
	ArchiveStatus       string `gorm:"-" json:"archive_status,omitempty"`
	ArchiveClosedAt     int64  `gorm:"-" json:"archive_closed_at,omitempty"`
}

type ECSLogDiscovery struct {
	ServiceARN  string `gorm:"primaryKey;size:512" json:"service_arn"`
	LastSuccess int64  `json:"last_success"`
	LastFailure int64  `json:"last_failure"`
}

// Preserve gaps between registrations: renewing after an outage must not
// retroactively authorize uploads made while no verified lease existed.
type ECSLogLeaseWindow struct {
	Node      string `gorm:"primaryKey;size:64"`
	Lane      string `gorm:"primaryKey;size:16"`
	StartedAt int64  `gorm:"primaryKey"`
	Until     int64  `gorm:"index"`
}

func ecsLogNode(task, container, runtimeID string) string {
	b, _ := json.Marshal([]string{"ecs-log-source-v1", task, container, runtimeID})
	h := sha256.Sum256(b)
	return "ecs-" + hex.EncodeToString(h[:24])
}

var errECSLogOwnerConflict = errors.New("ECS source has another verification identity or was retired")

func (m *Monitor) registerECSLogLease(ctx context.Context, source ECSLogSource, publicKey string, now int64) (ECSLogSource, error) {
	verifiedRole := source.TaskRoleARN
	verifiedDefinition := source.TaskDefinitionARN
	source.Node = ecsLogNode(source.TaskARN, source.Container, source.RuntimeID)
	var result ECSLogSource
	err := m.infraAssetWrite(ctx, 5*time.Second, func(tx *gorm.DB) error {
		var previous ECSLogSource
		err := tx.First(&previous, "node = ? AND lane = ?", source.Node, source.Lane).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil {
			if previous.ServiceARN != source.ServiceARN || previous.TaskARN != source.TaskARN || previous.Container != source.Container || previous.RuntimeID != source.RuntimeID {
				return errors.New("ECS source identity collision")
			}
			// Lease expiry must not rotate the source's verification key. Frozen
			// batches may outlive the sender, and replay needs its original identity.
			// A lost key requires explicit recovery, not silent ownership takeover.
			if previous.Revoked || previous.StoppedAt > 0 || previous.PublicKey != "" && previous.PublicKey != publicKey {
				return errECSLogOwnerConflict
			}
			if previous.TaskRoleARN != "" && previous.TaskRoleARN != verifiedRole {
				return errECSLogOwnerConflict
			}
			source = previous
		} else {
			var count int64
			if err := tx.Model(&ECSLogSource{}).Count(&count).Error; err != nil {
				return err
			}
			if count >= ecsLogSourceLimit {
				return errors.New("ECS source registry capacity reached")
			}
			source.FirstSeen = now
		}
		source.PublicKey = publicKey
		source.TaskRoleARN = verifiedRole
		source.TaskDefinitionARN = verifiedDefinition
		source.LeaseUntil = now + ecsLogLeaseSeconds
		source.LastDiscovered = max(source.LastDiscovered, now)
		if err := m.claimECSLogOwnership(tx, source, publicKey, now); err != nil {
			return err
		}
		result = source
		if err := tx.Save(&source).Error; err != nil {
			return err
		}
		// Bounded retention exceeds the archive replay horizon. No source/fact
		// data is removed here, only expired authorization windows.
		if err := tx.Where("node = ? AND lane = ? AND until < ?", source.Node, source.Lane, now-2*ecsLogReplaySeconds).Delete(&ECSLogLeaseWindow{}).Error; err != nil {
			return err
		}
		var window ECSLogLeaseWindow
		err = tx.Where("node = ? AND lane = ? AND until >= ?", source.Node, source.Lane, now).Order("started_at DESC").First(&window).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			window = ECSLogLeaseWindow{Node: source.Node, Lane: source.Lane, StartedAt: now}
		} else if err != nil {
			return err
		}
		window.Until = max(window.Until, source.LeaseUntil)
		return tx.Save(&window).Error
	})
	return result, err
}
