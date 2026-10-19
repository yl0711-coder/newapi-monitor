//go:build unix

package monitor

// Post-commit compensation is append-only, not database restore or deletion.
// The default-off maintenance workflow requires a separate recovery proposal
// and authorization; application rollback does not invoke this kernel.
import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"time"

	"gorm.io/gorm"
)

type financeRechargeRecoveryPlan struct {
	Original        financeRechargeGuardPlan       `json:"original"`
	OriginalSHA256  string                         `json:"original_sha256"`
	CreatedAt       int64                          `json:"created_at"`
	ExpiresAt       int64                          `json:"expires_at"`
	RecipientSHA256 string                         `json:"recipient_sha256"`
	Rows            []ChannelFinanceVersion        `json:"rows"`
	Preview         FinanceRechargeRehearsalResult `json:"preview"`
}

func financeRechargeRecoveryHistory(ctx context.Context, tx *gorm.DB, domain string) ([]ChannelFinanceVersion, error) {
	var history []ChannelFinanceVersion
	if err := tx.WithContext(ctx).Where("domain=?", domain).Order("effective_at,version").Limit(financeRechargeCorrectionVersionLimit + 1).Find(&history).Error; err != nil {
		return nil, err
	}
	if len(history) == 0 || len(history) > financeRechargeCorrectionVersionLimit {
		return nil, errors.New("invalid recovery history size")
	}
	for i := range history {
		history[i].ID = 0
	}
	return history, nil
}

func prepareFinanceRechargeRecovery(ctx context.Context, db *gorm.DB, original financeRechargeGuardPlan, originalHash string, now int64) (financeRechargeRecoveryPlan, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var plan financeRechargeRecoveryPlan
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		plan, err = buildFinanceRechargeRecovery(ctx, tx, original, originalHash, now)
		return err
	})
	return plan, err
}

func buildFinanceRechargeRecovery(ctx context.Context, tx *gorm.DB, original financeRechargeGuardPlan, originalHash string, now int64) (financeRechargeRecoveryPlan, error) {
	var empty financeRechargeRecoveryPlan
	hash, err := financeRechargeGuardHash(original)
	if err != nil || hash != originalHash || original.Version != 1 || len(original.Rows) == 0 || original.CreatedAt <= 0 || now < original.CreatedAt {
		return empty, errors.New("invalid original repair evidence")
	}
	if _, err := validateFinanceRechargeGuardRequest(original.Request, now); err != nil {
		return empty, err
	}
	c := original.Request.Corrections[0]
	history, err := financeRechargeRecoveryHistory(ctx, tx, c.Domain)
	if err != nil {
		return empty, err
	}
	firstVersion, lastVersion := original.Rows[0].Version, original.Rows[len(original.Rows)-1].Version
	var before, applied []ChannelFinanceVersion
	for _, row := range history {
		if row.Version < firstVersion {
			before = append(before, row)
		}
		if row.UpdatedBy == "guarded-recharge:"+originalHash {
			applied = append(applied, row)
		}
		// A later edit affecting the restored window requires independent review.
		// Later current settings and later-period updates are retained untouched.
		if row.Version > lastVersion && row.EffectiveAt < c.ToTs {
			return empty, errors.New("later historical edit conflicts with recovery; do not overwrite it")
		}
	}
	beforeHash, err := financeRechargeGuardHash(before)
	if err != nil || beforeHash != original.OriginalHistorySHA256 {
		return empty, errors.New("original history no longer matches reviewed repair")
	}
	want := append([]ChannelFinanceVersion(nil), original.Rows...)
	for i := range want {
		want[i].ID = 0
		want[i].UpdatedBy = "guarded-recharge:" + originalHash
	}
	sort.Slice(applied, func(i, j int) bool { return applied[i].Version < applied[j].Version })
	if !reflect.DeepEqual(applied, want) {
		return empty, errors.New("original repair is not fully committed or its receipt changed")
	}
	derived, err := buildFinanceRechargeCorrection(before, c, original.CreatedAt)
	if err != nil || !reflect.DeepEqual(derived, original.Rows) {
		return empty, errors.New("original repair rows cannot be reproduced")
	}
	rows, err := buildFinanceRechargeCompensation(history, before, c, now)
	if err != nil {
		return empty, err
	}
	if len(history)+len(rows) > financeRechargeCorrectionVersionLimit {
		return empty, errors.New("recovery would exceed history budget; independent review required")
	}
	preview, err := projectFinanceRechargeRecovery(ctx, tx, original.Request, rows, now)
	if err != nil {
		return empty, err
	}
	guard, err := financeRechargeRecipientHash(ctx, tx, original.Request, "")
	if err != nil {
		return empty, err
	}
	return financeRechargeRecoveryPlan{Original: original, OriginalSHA256: originalHash, CreatedAt: now, ExpiresAt: now + 900, RecipientSHA256: guard, Rows: rows, Preview: preview}, nil
}

// Reserve enough history capacity for immediate compensation before committing
// a repair. Subsequent independent edits still require a fresh recovery review.
func validateFinanceRechargeRecoveryReserve(history, added []ChannelFinanceVersion, c FinanceRechargeCorrection, now int64) error {
	combined := append(append([]ChannelFinanceVersion(nil), history...), added...)
	sort.Slice(combined, func(i, j int) bool {
		if combined[i].EffectiveAt == combined[j].EffectiveAt {
			return combined[i].Version < combined[j].Version
		}
		return combined[i].EffectiveAt < combined[j].EffectiveAt
	})
	if len(combined) == 0 {
		return errors.New("empty recovery history")
	}
	recovery, err := buildFinanceRechargeCompensation(combined, history, c, now)
	if err != nil {
		return err
	}
	if len(combined)+len(recovery) > financeRechargeCorrectionVersionLimit {
		return errors.New("repair must reserve history capacity for recovery")
	}
	return nil
}

func buildFinanceRechargeCompensation(current, before []ChannelFinanceVersion, c FinanceRechargeCorrection, now int64) ([]ChannelFinanceVersion, error) {
	last := current[len(current)-1]
	maxVersion := int64(0)
	for _, row := range current {
		if row.Version > maxVersion {
			maxVersion = row.Version
		}
	}
	if last.Version != maxVersion || c.ToTs > last.EffectiveAt || maxVersion > math.MaxInt64-int64(len(before))-3 {
		return nil, errors.New("recovery has ambiguous current anchor or version overflow")
	}
	active := func(rows []ChannelFinanceVersion, at int64) string {
		snapshot := `{"domain":` + string(mustRechargeJSON(c.Domain)) + `}`
		for _, row := range rows {
			if row.EffectiveAt > at {
				break
			}
			snapshot = row.SnapshotJSON
		}
		return snapshot
	}
	points := []int64{c.FromTs}
	for i, row := range before {
		if row.EffectiveAt > c.FromTs && row.EffectiveAt < c.ToTs && (i+1 == len(before) || before[i+1].EffectiveAt != row.EffectiveAt) {
			points = append(points, row.EffectiveAt)
		}
	}
	var out []ChannelFinanceVersion
	appendRow := func(at int64, snapshot string) {
		maxVersion++
		out = append(out, ChannelFinanceVersion{Domain: c.Domain, Version: maxVersion, SnapshotJSON: snapshot, EffectiveAt: at, CreatedAt: now, UpdatedBy: "local-recharge-recovery"})
	}
	for _, at := range points {
		appendRow(at, active(before, at))
	}
	appendRow(c.ToTs, active(current, c.ToTs))
	if c.ToTs != last.EffectiveAt {
		appendRow(last.EffectiveAt, last.SnapshotJSON)
	}
	return out, nil
}

func projectFinanceRechargeRecovery(ctx context.Context, tx *gorm.DB, request FinanceRechargeRehearsalRequest, added []ChannelFinanceVersion, now int64) (FinanceRechargeRehearsalResult, error) {
	result := FinanceRechargeRehearsalResult{Mode: "unconfirmed_recovery_preview"}
	scope, err := validateFinanceRechargeGuardRequest(request, now)
	if err != nil {
		return result, err
	}
	c := request.Corrections[0]
	var directory ChannelUpstreamAccount
	if err := tx.WithContext(ctx).Select("domain", "provider", "usage_adapter", "usage_sync_enabled").Where("domain=?", c.Domain).First(&directory).Error; err != nil {
		return result, err
	}
	if !directory.UsageSyncEnabled || directory.Provider == upstreamProviderOpenOx {
		return result, errors.New("recovery requires an enabled original-bill account")
	}
	m := &Monitor{storeDB: tx}
	accounts := map[string]ChannelUpstreamAccountView{c.Domain: {Configured: true, UsageSyncEnabled: true, Provider: directory.Provider, UsageAdapter: directory.UsageAdapter, UsageGranularity: upstreamUsageGranularity(directory.Provider, directory.UsageAdapter)}}
	accounts, err = m.financeRelevantUpstreamAccounts(ctx, scope, accounts)
	if err != nil {
		return result, err
	}
	terms, err := m.loadChannelRechargeVersions(ctx, accounts, channelFinanceSnapshot{})
	if err != nil {
		return result, err
	}
	next := append([]channelRechargeVersion(nil), terms[c.Domain]...)
	for _, row := range added {
		var s channelFinanceVersionSnapshot
		if err := json.Unmarshal([]byte(row.SnapshotJSON), &s); err != nil {
			return result, err
		}
		next = append(next, channelRechargeVersion{Version: row.Version, EffectiveAt: row.EffectiveAt, Paid: s.UpstreamRechargePaid, Credit: s.UpstreamRechargeCredit, Valid: validChannelFinanceNumber(s.UpstreamRechargePaid) && validChannelFinanceNumber(s.UpstreamRechargeCredit)})
	}
	var bills []ChannelUpstreamUsageHour
	if err := tx.WithContext(ctx).Where("domain=? AND hour_ts>=? AND hour_ts<?", c.Domain, scope.FromTs, scope.ToTs).Order("hour_ts").Limit(financeRechargeGuardHours + 1).Find(&bills).Error; err != nil {
		return result, err
	}
	if len(bills) > financeRechargeGuardHours {
		return result, errors.New("recovery bill budget exceeded")
	}
	detail, err := projectFinanceRechargeRehearsal(ctx, bills, scope, now, accounts, terms[c.Domain], next, c)
	if err != nil {
		return result, err
	}
	result.Domains = []FinanceRechargeRehearsalDomain{detail}
	return result, nil
}

func executeFinanceRechargeRecovery(ctx context.Context, db *gorm.DB, plan financeRechargeRecoveryPlan, approvedHash string, now int64) (financeRechargeGuardReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var receipt financeRechargeGuardReceipt
	hash, err := financeRechargeGuardHash(plan)
	if err != nil || hash != approvedHash || plan.CreatedAt <= 0 || now < plan.CreatedAt || plan.ExpiresAt != plan.CreatedAt+900 || now > plan.ExpiresAt {
		return receipt, errors.New("invalid, unapproved or expired recovery plan")
	}
	if _, err := validateFinanceRechargeGuardRequest(plan.Original.Request, now); err != nil {
		return receipt, err
	}
	marker := "guarded-recovery:" + hash
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var applied []ChannelFinanceVersion
		if err := tx.Where("domain=? AND updated_by=?", plan.Original.Request.Corrections[0].Domain, marker).Order("version").Limit(financeRechargeCorrectionVersionLimit + 1).Find(&applied).Error; err != nil {
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
				return errors.New("recovery receipt changed")
			}
			guard, err := financeRechargeRecipientHash(ctx, tx, plan.Original.Request, marker)
			if err != nil {
				return err
			}
			if guard != plan.RecipientSHA256 {
				return errors.New("recovery committed but recipient changed; inspect without replay")
			}
			receipt = financeRechargeGuardReceipt{PlanSHA256: hash, Status: "already_applied"}
			return nil
		}
		fresh, err := buildFinanceRechargeRecovery(ctx, tx, plan.Original, plan.OriginalSHA256, plan.CreatedAt)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(fresh, plan) {
			return errors.New("recovery recipient or reviewed money changed")
		}
		rows := append([]ChannelFinanceVersion(nil), plan.Rows...)
		for i := range rows {
			rows[i].UpdatedBy = marker
		}
		if err := tx.CreateInBatches(&rows, 100).Error; err != nil {
			return err
		}
		after, err := projectFinanceRechargeRecovery(ctx, tx, plan.Original.Request, nil, plan.CreatedAt)
		if err != nil {
			return err
		}
		if err := verifyFinanceRechargeRehearsalBalances(plan.Preview, after); err != nil {
			return err
		}
		economics, err := rebuildFinanceRechargeEconomics(ctx, tx, plan.Original.Request.Corrections, plan.CreatedAt)
		if err != nil {
			return err
		}
		receipt = financeRechargeGuardReceipt{PlanSHA256: hash, Status: "applied", AddedVersions: len(rows), Economics: economics}
		return nil
	})
	if err != nil {
		return financeRechargeGuardReceipt{}, err
	}
	return receipt, nil
}
