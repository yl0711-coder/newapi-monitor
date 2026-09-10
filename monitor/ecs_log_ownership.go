package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"gorm.io/gorm"
)

// Task/container/lane is the responsibility boundary. RuntimeID is deliberately
// a VALUE, not a key: restarting a producer must not silently reread its task's
// retained files under a fresh source. A future handoff needs byte-boundary proof.
type ECSLogOwnership struct {
	TaskARN           string `gorm:"primaryKey;size:512"`
	Container         string `gorm:"primaryKey;size:64"`
	Lane              string `gorm:"primaryKey;size:16;uniqueIndex:idx_ecs_owner_node_lane,priority:2"`
	ServiceARN        string `gorm:"size:512"`
	TaskDefinitionARN string `gorm:"size:512"`
	TaskRoleARN       string `gorm:"size:512"`
	RuntimeID         string `gorm:"size:256"`
	Node              string `gorm:"size:64;uniqueIndex:idx_ecs_owner_node_lane,priority:1"`
	Audience          string `gorm:"size:128"`
	PublicKeyHash     string `gorm:"size:64"`
	Purpose           string `gorm:"size:32"`
	AssignedAt        int64
}

type ECSLogOwnershipBinding struct {
	ID       int `gorm:"primaryKey;autoIncrement:false"`
	Version  int
	Audience string `gorm:"size:128"`
	Purpose  string `gorm:"size:32"`
}

const ecsLogCandidatePurpose = "isolated-candidate"

var errECSLogResponsibilityConflict = errors.New("ECS task lane already has a different collection responsibility; explicit handoff required")
var errECSLogResponsibilityMissing = errors.New("ECS collection responsibility missing or mismatched; retain batch")

func (m *Monitor) ecsOwnershipRequired() bool {
	return m.cfg.ECSLogOwnershipEnabled || m.ecsLogOwnershipActive.Load()
}

// Called before serving. No automatic adoption of old facts and no flag-based
// downgrade once activated. Fresh opt-in uses a single SQLite transaction for
// schema + binding. These tables never live in NewAPI's business database.
func (m *Monitor) initECSLogOwnership(db *gorm.DB) error {
	hasBinding := db.Migrator().HasTable(&ECSLogOwnershipBinding{})
	hasOwners := db.Migrator().HasTable(&ECSLogOwnership{})
	if !hasBinding && !hasOwners && !m.cfg.ECSLogOwnershipEnabled {
		return nil
	}
	if !m.cfg.ECSLogOwnershipEnabled || !ecsLogRuntimeEnabled(m.cfg) || !ecsLogAudiencePattern.MatchString(m.cfg.ECSLogAudience) {
		return errors.New("ownership store cannot disable its gate or change receiver identity")
	}
	if hasBinding != hasOwners {
		return errors.New("incomplete ownership schema; recovery required")
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if !hasBinding {
			var count int64
			if err := tx.Model(&ECSLogSource{}).Where("public_key <> '' OR last_report > 0 OR last_heartbeat > 0").Count(&count).Error; err != nil {
				return err
			}
			if count != 0 {
				return errors.New("ownership requires a fresh receiver; existing sources cannot be silently adopted")
			}
			// Isolated acceptance must start with an empty receiver so its result
			// cannot be confused with old facts. Production deliberately keeps
			// existing Lightsail facts: ownership applies only to newly verified
			// ECS sources and never adopts legacy batches.
			if m.cfg.ECSLogScope == ecsLogScopeIsolated {
				for _, model := range []any{&NginxIngestBatch{}, &NginxErrorIngestBatch{}, &RejectionIngestBatch{}, &MetricSample{}, &UsageHourFact{}, &UsageDailyFact{}, &ChannelUpstreamUsageHour{}, &ECSLogArchiveReceipt{}} {
					if !tx.Migrator().HasTable(model) {
						continue
					}
					var present []int
					if err := tx.Model(model).Select("1").Limit(1).Scan(&present).Error; err != nil {
						return err
					}
					if len(present) > 0 {
						return errors.New("isolated ownership cannot adopt existing business or collection facts")
					}
				}
			}
			if err := tx.AutoMigrate(&ECSLogOwnership{}, &ECSLogOwnershipBinding{}); err != nil {
				return err
			}
			binding := ECSLogOwnershipBinding{ID: 1, Version: 1, Audience: m.cfg.ECSLogAudience, Purpose: ecsLogPurpose(m.cfg)}
			return tx.Create(&binding).Error
		}
		var rows []ECSLogOwnershipBinding
		if err := tx.Limit(2).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) != 1 || rows[0].ID != 1 || rows[0].Version != 1 || rows[0].Audience != m.cfg.ECSLogAudience || rows[0].Purpose != ecsLogPurpose(m.cfg) {
			return errors.New("ownership store identity changed or missing")
		}
		return nil
	})
	if err == nil {
		m.ecsLogOwnershipActive.Store(true)
	}
	return err
}

func ecsLogOwnershipFor(source ECSLogSource, audience, publicKey, purpose string) ECSLogOwnership {
	hash := sha256.Sum256([]byte(publicKey))
	return ECSLogOwnership{TaskARN: source.TaskARN, Container: source.Container, Lane: source.Lane, ServiceARN: source.ServiceARN, TaskDefinitionARN: source.TaskDefinitionARN,
		TaskRoleARN: source.TaskRoleARN, RuntimeID: source.RuntimeID, Node: source.Node, Audience: audience,
		PublicKeyHash: hex.EncodeToString(hash[:]), Purpose: purpose}
}

func sameECSLogOwnership(a, b ECSLogOwnership) bool {
	a.AssignedAt, b.AssignedAt = 0, 0
	return a == b
}

func findECSLogOwnership(tx *gorm.DB, source ECSLogSource) (ECSLogOwnership, error) {
	var owner ECSLogOwnership
	err := tx.First(&owner, "task_arn = ? AND container = ? AND lane = ?", source.TaskARN, source.Container, source.Lane).Error
	return owner, err
}

// Must run inside the source+lease transaction, after independent AWS checking.
// No caller-supplied purpose, audience or approval field is accepted by HTTP.
func (m *Monitor) claimECSLogOwnership(tx *gorm.DB, source ECSLogSource, publicKey string, now int64) error {
	if !m.ecsOwnershipRequired() {
		return nil
	}
	if source.TaskDefinitionARN == "" {
		return errors.New("verified task definition required for responsibility assignment")
	}
	want := ecsLogOwnershipFor(source, m.cfg.ECSLogAudience, publicKey, ecsLogPurpose(m.cfg))
	owner, err := findECSLogOwnership(tx, source)
	if err == nil {
		if !sameECSLogOwnership(owner, want) {
			return errECSLogResponsibilityConflict
		}
		return nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	// A lost ledger row must not be reconstructed from an already registered
	// source: its historical owner may no longer be provable.
	var count int64
	if err := tx.Model(&ECSLogSource{}).Where("task_arn = ? AND container = ? AND lane = ? AND public_key <> ''", source.TaskARN, source.Container, source.Lane).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errECSLogResponsibilityMissing
	}
	if err := tx.Model(&ECSLogOwnership{}).Count(&count).Error; err != nil {
		return err
	}
	if count >= ecsLogSourceLimit {
		return errors.New("ECS responsibility ledger capacity reached; no historical owner was removed")
	}
	want.AssignedAt = now
	return tx.Create(&want).Error
}

func (m *Monitor) checkECSLogOwnership(ctx context.Context, source ECSLogSource) error {
	if !m.ecsOwnershipRequired() {
		return nil
	}
	owner, err := findECSLogOwnership(m.storeDB.WithContext(ctx), source)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errECSLogResponsibilityMissing
	}
	if err != nil {
		return err
	}
	if !sameECSLogOwnership(owner, ecsLogOwnershipFor(source, m.cfg.ECSLogAudience, source.PublicKey, ecsLogPurpose(m.cfg))) {
		return errECSLogResponsibilityMissing
	}
	return nil
}
