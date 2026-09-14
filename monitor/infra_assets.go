package monitor

// Resource membership is durable control-plane state, not a metric. Never
// prune it with time-series retention or change a manual state on discovery.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const infraAssetLimit = 10000

type InfraAsset struct {
	ID             string `gorm:"primaryKey;size:64" json:"id"`
	Identity       string `gorm:"uniqueIndex;size:1024" json:"identity"`
	Scope          string `gorm:"index;size:512" json:"-"`
	Resource       string `gorm:"index;size:253" json:"resource"`
	Parent         string `gorm:"size:253" json:"parent,omitempty"`
	Kind           string `gorm:"size:32" json:"kind"`
	Platform       string `gorm:"size:32" json:"platform"`
	State          string `gorm:"index;size:16" json:"state"`
	CloudState     string `gorm:"size:32" json:"cloud_state"`
	FirstSeen      int64  `json:"first_seen"`
	LastDiscovered int64  `json:"last_discovered"`
	LastChecked    int64  `json:"last_checked"`
	LastLive       int64  `json:"last_live"`
	LastReport     int64  `json:"last_report"`
	LastSampleAt   int64  `json:"last_sample_at"`
	ArchivedAt     int64  `json:"archived_at"`
	RemovedAt      int64  `json:"removed_at"`
	Revision       int64  `json:"revision"`
}

type InfraAssetAudit struct {
	ID       uint64 `gorm:"primaryKey"`
	AssetID  string `gorm:"index;size:64"`
	Action   string `gorm:"size:16"`
	Actor    string `gorm:"size:128"`
	At       int64
	Revision int64
}

// A scope watermark avoids rewriting every missing historical task merely to
// record a successful scan. It also rejects older complete scan results.
type InfraAssetScope struct {
	Scope       string `gorm:"primaryKey;size:512"`
	LastChecked int64
}

func (m *Monitor) infraAssetWrite(ctx context.Context, budget time.Duration, fn func(*gorm.DB) error) error {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return retryStabilityLocalWrite(ctx, func(attempt context.Context) error {
		return m.storeDB.WithContext(attempt).Transaction(fn)
	})
}

type infraAssetObservation struct {
	Identity, Resource, Parent, Kind, Platform, CloudState string
}

func infraAssetID(identity string) string {
	h := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(h[:])
}

// Called only with a complete, successful inventory for this exact scope.
// "missing" is an observation, not proof of termination and never an archive.
func (m *Monitor) observeInfraAssets(ctx context.Context, scope string, observations []infraAssetObservation, now int64) error {
	if scope == "" || len(observations) > infraAssetLimit {
		return errors.New("invalid resource inventory scope or size")
	}
	seen := make(map[string]bool, len(observations))
	for _, o := range observations {
		if o.Identity == "" || o.Resource == "" || len(o.Identity) > 1024 || len(o.Resource) > 253 || seen[o.Identity] {
			return errors.New("invalid or duplicate resource identity")
		}
		seen[o.Identity] = true
	}
	return m.infraAssetWrite(ctx, 5*time.Second, func(tx *gorm.DB) error {
		watermark := InfraAssetScope{Scope: scope, LastChecked: now}
		r := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "scope"}}, DoUpdates: clause.AssignmentColumns([]string{"last_checked"}), Where: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: "last_checked < ?", Vars: []any{now}}}}}).Create(&watermark)
		if r.Error != nil || r.RowsAffected == 0 {
			return r.Error
		}
		// One successful scope scan may mark absence, but cannot change a
		// user's archive/remove decision, even if a removed task reports again.
		if err := tx.Model(&InfraAsset{}).Where("scope = ? AND last_checked <= ? AND cloud_state <> ? AND state NOT IN ?", scope, now, "missing", []string{"removed", "linked"}).Updates(map[string]any{"cloud_state": "missing", "last_checked": now}).Error; err != nil {
			return err
		}
		for _, o := range observations {
			live := int64(0)
			if o.CloudState == "running" {
				live = now
			}
			a := InfraAsset{ID: infraAssetID(o.Identity), Identity: o.Identity, Scope: scope, Resource: o.Resource, Parent: o.Parent, Kind: o.Kind, Platform: o.Platform,
				State: "active", CloudState: o.CloudState, FirstSeen: now, LastDiscovered: now, LastChecked: now, LastLive: live, Revision: 1}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.Assignments(map[string]any{
				"cloud_state": o.CloudState, "last_discovered": now, "last_checked": now, "last_live": gorm.Expr("MAX(last_live, ?)", live),
			}), Where: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: "last_checked <= ?", Vars: []any{now}}}}}).Create(&a).Error; err != nil {
				return err
			}
			if o.Kind == "instance" {
				if err := linkInfraLegacyHost(tx, o.Resource, now); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (m *Monitor) retainMissingLightsailAssets(ctx context.Context, previous []infraLatestRow, discovered []infraTarget, now int64) error {
	present := map[string]bool{}
	for _, t := range discovered {
		present[t.name] = true
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, old := range previous {
			if old.RType != "instance" || present[old.Resource] {
				continue
			}
			var known int64
			if err := tx.Model(&InfraAsset{}).Where("resource = ?", old.Resource).Count(&known).Error; err != nil {
				return err
			}
			if known > 0 {
				continue
			}
			identity := "legacy-lightsail:" + m.cfg.AWSRegion + ":" + old.Resource
			a := InfraAsset{ID: infraAssetID(identity), Identity: identity, Scope: "lightsail:" + m.cfg.AWSRegion + ":instance", Resource: old.Resource, Kind: "instance", Platform: "Lightsail", State: "active", CloudState: "missing", FirstSeen: now, Revision: 1}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&a).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// The existing ingest token remains mandatory. A legacy host name is NOT a
// verified cloud identity: do not bind this report to an ARN supplied by the
// client. Resolve names only against a unique currently-discovered host.
func (m *Monitor) noteInfraHostReport(ctx context.Context, node string, now int64, observedAt ...int64) error {
	sampleAt := now
	if len(observedAt) > 0 {
		sampleAt = observedAt[0]
	}
	return m.acceptInfraHostReport(ctx, node, now, sampleAt, nil)
}

func (m *Monitor) infraAssets(ctx context.Context) ([]InfraAsset, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var assets []InfraAsset
	query := m.monitorOwnedInfraAssetsQuery(m.storeDB.WithContext(ctx))
	err := query.Where("state NOT IN ?", []string{"removed", "linked"}).Order("first_seen DESC, id").Limit(infraAssetLimit + 1).Find(&assets).Error
	if err != nil {
		return nil, err
	}
	if len(assets) > infraAssetLimit {
		return nil, errors.New("resource registry exceeds bounded view; administrative pagination required")
	}
	return assets, nil
}

func (m *Monitor) assetRecovery(a InfraAsset, now int64) bool {
	last := min(a.LastReport, a.LastSampleAt)
	// ECS control-plane freshness is explicitly labelled separately from
	// log delivery; a running task does not certify its logs are complete.
	if a.Platform == "ECS/Fargate" && a.CloudState == "running" {
		last = max(last, a.LastLive)
	}
	return a.State == "archived" && last > a.ArchivedAt && last <= now && now-last <= m.infraMetricFreshnessSec()
}

var errInfraAssetConflict = errors.New("resource state changed or action is not currently allowed")

func (m *Monitor) changeInfraAsset(ctx context.Context, id, action, actor string, revision, now int64) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var a InfraAsset
		if err := tx.First(&a, "id = ?", id).Error; err != nil {
			return err
		}
		if a.Revision != revision {
			return errInfraAssetConflict
		}
		updates := map[string]any{"revision": revision + 1}
		switch action {
		case "archive":
			if a.State != "active" {
				return errInfraAssetConflict
			}
			updates["state"], updates["archived_at"] = "archived", now
		case "restore":
			if !m.assetRecovery(a, now) {
				return errInfraAssetConflict
			}
			updates["state"] = "active"
		case "remove":
			if a.State != "archived" || m.assetRecovery(a, now) {
				return errInfraAssetConflict
			}
			updates["state"], updates["removed_at"] = "removed", now
		default:
			return fmt.Errorf("invalid resource action")
		}
		r := tx.Model(&InfraAsset{}).Where("id = ? AND revision = ? AND state = ?", id, revision, a.State).Updates(updates)
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return errInfraAssetConflict
		}
		return tx.Create(&InfraAssetAudit{AssetID: id, Action: action, Actor: actor, At: now, Revision: revision + 1}).Error
	})
}
