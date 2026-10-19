//go:build unix

package monitor

// Guarded execution kernel, called by the default-off maintenance task workflow.
// No startup task executes it. Production backup/old-image recovery and explicit
// operator authorization remain separate requirements from route availability.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"gorm.io/gorm"
)

const financeRechargeGuardHours = 168

type financeRechargeGuardPlan struct {
	Version               int                             `json:"version"`
	CreatedAt             int64                           `json:"created_at"`
	ExpiresAt             int64                           `json:"expires_at"`
	Request               FinanceRechargeRehearsalRequest `json:"request"`
	RecipientSHA256       string                          `json:"recipient_sha256"`
	OriginalHistorySHA256 string                          `json:"original_history_sha256"`
	Rows                  []ChannelFinanceVersion         `json:"rows"`
	Preview               FinanceRechargeRehearsalResult  `json:"preview"`
}

type financeRechargeGuardReceipt struct {
	PlanSHA256    string                            `json:"plan_sha256"`
	Status        string                            `json:"status"`
	AddedVersions int                               `json:"added_versions"`
	Economics     []FinanceRechargeEconomicsRebuild `json:"economics,omitempty"`
}

func financeRechargeGuardHash(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func validateFinanceRechargeGuardRequest(r FinanceRechargeRehearsalRequest, now int64) (stabilityScope, error) {
	from, e1 := financeStartHour(r.FromDate)
	to, e2 := financeStartHour(r.ToDate)
	if e1 != nil || e2 != nil || from < 0 || to <= from || to > now-now%3600 || to-from > financeRechargeGuardHours*3600 || len(r.Corrections) != 1 || r.SourceSHA256 != "" || !r.RebuildEconomics {
		return stabilityScope{}, errors.New("guarded repair requires one domain, at most seven closed days, mandatory economics validation and no whole-file identity")
	}
	c := r.Corrections[0]
	if c.FromTs < from || c.ToTs > to || c.FromTs%3600 != 0 || c.ToTs%3600 != 0 {
		return stabilityScope{}, errors.New("correction must be whole hours within reviewed dates")
	}
	return stabilityScope{FromTs: from, ToTs: to}, nil
}

// Only identity/configuration and bounded accounting facts are hashed. Never
// read credentials or balance/polling timestamps; ordinary balance refreshes
// must not invalidate a historical repair. Relevant bill changes do invalidate it.
func financeRechargeRecipientHash(ctx context.Context, tx *gorm.DB, r FinanceRechargeRehearsalRequest, excludeMarker string) (string, error) {
	domain := r.Corrections[0].Domain
	scope, err := validateFinanceRechargeGuardRequest(r, time.Now().Unix())
	if err != nil {
		return "", err
	}
	var account ChannelUpstreamAccount
	if err := tx.WithContext(ctx).Select("domain", "provider", "base_url", "account", "user_id", "usage_adapter", "usage_sync_enabled", "balance_unit", "balance_unit_previous", "balance_unit_effective_at", "records_started_at").Where("domain=?", domain).First(&account).Error; err != nil {
		return "", err
	}
	var config []ChannelDomainCost
	if err := tx.WithContext(ctx).Where("domain=?", domain).Find(&config).Error; err != nil {
		return "", err
	}
	var history []ChannelFinanceVersion
	q := tx.WithContext(ctx).Where("domain=?", domain)
	if excludeMarker != "" {
		q = q.Where("updated_by IS NULL OR updated_by != ?", excludeMarker)
	}
	if err := q.Order("effective_at,version").Limit(financeRechargeCorrectionVersionLimit + 1).Find(&history).Error; err != nil {
		return "", err
	}
	if len(history) > financeRechargeCorrectionVersionLimit {
		return "", errors.New("history budget exceeded")
	}
	for i := range history {
		history[i].ID = 0
	}
	var bills []ChannelUpstreamUsageHour
	// Include a daily bucket crossing the range start; conservative invalidation
	// is preferable to missing a source change at a boundary.
	if err := tx.WithContext(ctx).Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, scope.FromTs-86400, scope.ToTs).Order("hour_ts").Limit(financeRechargeGuardHours + 25).Find(&bills).Error; err != nil {
		return "", err
	}
	if len(bills) > financeRechargeGuardHours+24 {
		return "", errors.New("bill budget exceeded")
	}
	return financeRechargeGuardHash(struct {
		Account ChannelUpstreamAccount
		Config  []ChannelDomainCost
		History []ChannelFinanceVersion
		Bills   []ChannelUpstreamUsageHour
	}{account, config, history, bills})
}

func prepareFinanceRechargeGuardPlan(ctx context.Context, db *gorm.DB, r FinanceRechargeRehearsalRequest, now int64) (financeRechargeGuardPlan, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var plan financeRechargeGuardPlan
	scope, err := validateFinanceRechargeGuardRequest(r, now)
	if err != nil {
		return plan, err
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		hash, err := financeRechargeRecipientHash(ctx, tx, r, "")
		if err != nil {
			return err
		}
		preview, rows, err := prepareFinanceRechargeRehearsal(ctx, tx, r, scope, now)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.New("requested interval already has this ratio; no repair plan required")
		}
		history, err := financeRechargeRecoveryHistory(ctx, tx, r.Corrections[0].Domain)
		if err != nil {
			return err
		}
		historyHash, err := financeRechargeGuardHash(history)
		if err != nil {
			return err
		}
		if err := validateFinanceRechargeRecoveryReserve(history, rows, r.Corrections[0], now); err != nil {
			return err
		}
		plan = financeRechargeGuardPlan{Version: 1, CreatedAt: now, ExpiresAt: now + 900, Request: r, RecipientSHA256: hash, OriginalHistorySHA256: historyHash, Rows: rows, Preview: preview}
		return nil
	})
	return plan, err
}

// The approved hash identifies the exact reviewed proposal, not user identity.
// A future caller must authenticate/authorize independently. A SQLite read/write
// transaction rejects conflicting upgrades rather than retrying with new facts.
func executeFinanceRechargeGuardPlan(ctx context.Context, db *gorm.DB, plan financeRechargeGuardPlan, approvedHash string, now int64) (financeRechargeGuardReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var receipt financeRechargeGuardReceipt
	hash, err := financeRechargeGuardHash(plan)
	if err != nil || hash != approvedHash || plan.Version != 1 || plan.CreatedAt <= 0 || now < plan.CreatedAt || plan.ExpiresAt != plan.CreatedAt+900 || now > plan.ExpiresAt {
		return receipt, errors.New("invalid, unapproved or expired repair plan")
	}
	scope, err := validateFinanceRechargeGuardRequest(plan.Request, now)
	if err != nil {
		return receipt, err
	}
	marker := "guarded-recharge:" + hash
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var applied []ChannelFinanceVersion
		if err := tx.Where("domain=? AND updated_by=?", plan.Request.Corrections[0].Domain, marker).Order("version").Limit(financeRechargeCorrectionVersionLimit + 1).Find(&applied).Error; err != nil {
			return err
		}
		if len(applied) > 0 {
			want := append([]ChannelFinanceVersion(nil), plan.Rows...)
			for i := range want {
				want[i].ID = 0
				want[i].UpdatedBy = marker
			}
			for i := range applied {
				applied[i].ID = 0
			}
			if !reflect.DeepEqual(applied, want) {
				return errors.New("persisted repair receipt differs; stop and inspect")
			}
			current, err := financeRechargeRecipientHash(ctx, tx, plan.Request, marker)
			if err != nil {
				return err
			}
			if current != plan.RecipientSHA256 {
				return errors.New("repair was committed but recipient changed; re-inspect without replay")
			}
			receipt = financeRechargeGuardReceipt{PlanSHA256: hash, Status: "already_applied"}
			return nil
		}
		current, err := financeRechargeRecipientHash(ctx, tx, plan.Request, "")
		if err != nil {
			return err
		}
		if current != plan.RecipientSHA256 {
			return errors.New("recipient history, configuration or bill changed; prepare a new review")
		}
		history, err := financeRechargeRecoveryHistory(ctx, tx, plan.Request.Corrections[0].Domain)
		if err != nil {
			return err
		}
		historyHash, err := financeRechargeGuardHash(history)
		if err != nil || historyHash != plan.OriginalHistorySHA256 {
			return errors.New("original history recovery evidence differs")
		}
		preview, rows, err := prepareFinanceRechargeRehearsal(ctx, tx, plan.Request, scope, plan.CreatedAt)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(rows, plan.Rows) || !reflect.DeepEqual(preview, plan.Preview) {
			return errors.New("reviewed rows or money changed")
		}
		if err := validateFinanceRechargeRecoveryReserve(history, rows, plan.Request.Corrections[0], plan.CreatedAt); err != nil {
			return err
		}
		for i := range rows {
			rows[i].UpdatedBy = marker
		}
		if err := tx.CreateInBatches(&rows, 100).Error; err != nil {
			return err
		}
		after, duplicate, err := prepareFinanceRechargeRehearsal(ctx, tx, plan.Request, scope, plan.CreatedAt)
		if err != nil {
			return err
		}
		if len(duplicate) != 0 {
			return errors.New("repair is not idempotent")
		}
		if err := verifyFinanceRechargeRehearsalBalances(preview, after); err != nil {
			return err
		}
		economics, err := rebuildFinanceRechargeEconomics(ctx, tx, plan.Request.Corrections, plan.CreatedAt)
		if err != nil {
			return fmt.Errorf("repair economics rolled back: %w", err)
		}
		receipt = financeRechargeGuardReceipt{PlanSHA256: hash, Status: "applied", AddedVersions: len(rows), Economics: economics}
		return nil
	})
	if err != nil {
		return financeRechargeGuardReceipt{}, err
	}
	return receipt, nil
}
