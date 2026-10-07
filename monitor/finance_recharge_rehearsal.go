//go:build unix

package monitor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gorm.io/gorm"
)

type FinanceRechargeRehearsalRequest struct {
	SourceSHA256     string                      `json:"source_sha256"`
	FromDate         string                      `json:"from_date"`
	ToDate           string                      `json:"to_date_exclusive"`
	Corrections      []FinanceRechargeCorrection `json:"corrections"`
	RebuildEconomics bool                        `json:"rebuild_economics,omitempty"`
}

// This entry point can only create a NEW local rehearsal directory. It cannot
// target an existing/live database or promote the proposal to production.
// Its receipt explicitly remains unconfirmed even after technical checks pass.
func RehearseFinanceRechargeCorrections(parent context.Context, source, output string, request FinanceRechargeRehearsalRequest) (FinanceRechargeRehearsalResult, error) {
	var empty FinanceRechargeRehearsalResult
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	from, err := financeStartHour(request.FromDate)
	if err != nil {
		return empty, err
	}
	to, err := financeStartHour(request.ToDate)
	now := time.Now().Unix()
	digest, hashErr := hex.DecodeString(request.SourceSHA256)
	if err != nil || from < 0 || to <= from || to > now-now%3600 || to-from > financeReportMaxDays*86400 ||
		hashErr != nil || len(digest) != 32 || len(request.Corrections) == 0 || len(request.Corrections) > 8 ||
		!filepath.IsAbs(source) || !filepath.IsAbs(output) {
		return empty, errors.New("rehearsal requires an exact snapshot hash, absolute paths, closed dates and 1 to 8 explicit corrections")
	}
	seen := map[string]bool{}
	for _, correction := range request.Corrections {
		if seen[correction.Domain] || correction.FromTs < from || correction.ToTs > to {
			return empty, errors.New("rehearsal corrections must have distinct domains within the query range")
		}
		seen[correction.Domain] = true
	}
	backup, err := openGiftRelevantBackup(source)
	if err != nil {
		return empty, err
	}
	defer backup.close()
	before, err := financeRechargeBackupHash(ctx, backup)
	if err != nil {
		return empty, err
	}
	if before != request.SourceSHA256 {
		return empty, errors.New("rehearsal source hash changed; rebuild the proposal")
	}
	// Validate and project the complete finite proposal before creating files.
	result, added, err := prepareFinanceRechargeRehearsal(ctx, backup.db, request, stabilityScope{FromTs: from, ToTs: to}, now)
	if err != nil {
		return empty, err
	}
	if err := backup.unchanged(); err != nil {
		return empty, err
	}
	parentInfo, err := os.Lstat(filepath.Dir(output))
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return empty, errors.New("rehearsal output requires an existing regular parent directory")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		return empty, err
	}
	proposal, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		return empty, err
	}
	if err := giftLocalWriteNew(filepath.Join(output, "unconfirmed-proposal.json"), proposal); err != nil {
		return empty, err
	}
	destination := filepath.Join(output, "monitor-rehearsal.db")
	copiedHash, err := giftLocalCopyBackup(source, destination)
	if err != nil {
		return empty, err
	}
	if copiedHash != before {
		return empty, errors.New("source changed while copying; discard rehearsal directory")
	}
	if err := backup.unchanged(); err != nil {
		return empty, err
	}
	db, closeDB, err := giftLocalDatabase(destination)
	if err != nil {
		return empty, err
	}
	defer closeDB()
	if err := db.WithContext(ctx).Exec("PRAGMA journal_mode=DELETE").Error; err != nil {
		return empty, err
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if len(added) > 0 {
			if err := tx.CreateInBatches(&added, 100).Error; err != nil {
				return err
			}
		}
		// Re-read SQLite through the production loader, then run the proposal
		// again. A complete correction must produce zero additional rows.
		reloaded, duplicate, err := prepareFinanceRechargeRehearsal(ctx, tx, request, stabilityScope{FromTs: from, ToTs: to}, now)
		if err != nil {
			return err
		}
		if len(duplicate) != 0 {
			return errors.New("rehearsal retry would append more history")
		}
		if err := verifyFinanceRechargeRehearsalBalances(result, reloaded); err != nil {
			return err
		}
		if request.RebuildEconomics {
			result.Economics, err = rebuildFinanceRechargeEconomics(ctx, tx, request.Corrections, now)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	var integrity string
	if err := db.WithContext(ctx).Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		return empty, errors.New("rehearsal SQLite integrity check failed")
	}
	closeDB()
	if err := backup.unchanged(); err != nil {
		return empty, err
	}
	afterSource, err := financeRechargeBackupHash(ctx, backup)
	if err != nil || afterSource != before {
		return empty, errors.New("original snapshot changed during rehearsal")
	}
	copy, err := openGiftRelevantBackup(destination)
	if err != nil {
		return empty, err
	}
	defer copy.close()
	result.OutputSHA256, err = financeRechargeBackupHash(ctx, copy)
	if err != nil {
		return empty, err
	}
	result.SourceSHA256, result.AddedVersions = before, len(added)
	receipt, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return empty, err
	}
	if err := giftLocalWriteNew(filepath.Join(output, "rehearsal-result.json"), receipt); err != nil {
		return empty, err
	}
	return result, ctx.Err()
}

func prepareFinanceRechargeRehearsal(ctx context.Context, db *gorm.DB, request FinanceRechargeRehearsalRequest, scope stabilityScope, now int64) (FinanceRechargeRehearsalResult, []ChannelFinanceVersion, error) {
	result := FinanceRechargeRehearsalResult{Mode: "unconfirmed_local_rehearsal", Domains: []FinanceRechargeRehearsalDomain{}}
	var added []ChannelFinanceVersion
	for _, correction := range request.Corrections {
		if err := ctx.Err(); err != nil {
			return result, nil, err
		}
		var history []ChannelFinanceVersion
		if err := db.WithContext(ctx).Where("domain=?", correction.Domain).Order("effective_at,version").
			Limit(financeRechargeCorrectionVersionLimit + 1).Find(&history).Error; err != nil {
			return result, nil, err
		}
		rows, err := buildFinanceRechargeCorrection(history, correction, now)
		if err != nil {
			return result, nil, fmt.Errorf("%s: %w", correction.Domain, err)
		}
		m := &Monitor{storeDB: db}
		var directory ChannelUpstreamAccount
		if err := db.WithContext(ctx).Select("domain", "provider", "usage_adapter", "usage_sync_enabled").
			Where("domain=?", correction.Domain).First(&directory).Error; err != nil {
			return result, nil, err
		}
		if !directory.UsageSyncEnabled || directory.Provider == upstreamProviderOpenOx {
			return result, nil, errors.New("recharge rehearsal requires an enabled original-bill account")
		}
		accounts := map[string]ChannelUpstreamAccountView{correction.Domain: {Configured: true, UsageSyncEnabled: true,
			Provider: directory.Provider, UsageAdapter: directory.UsageAdapter, UsageGranularity: upstreamUsageGranularity(directory.Provider, directory.UsageAdapter)}}
		accounts, err = m.financeRelevantUpstreamAccounts(ctx, scope, accounts)
		if err != nil {
			return result, nil, err
		}
		var bills []ChannelUpstreamUsageHour
		if err := db.WithContext(ctx).Where("domain=? AND hour_ts>=? AND hour_ts<?", correction.Domain, scope.FromTs, scope.ToTs).
			Order("hour_ts").Limit(financeRechargeInspectBucketLimit + 1).Find(&bills).Error; err != nil {
			return result, nil, err
		}
		if len(bills) > financeRechargeInspectBucketLimit {
			return result, nil, errors.New("rehearsal bill budget exceeded")
		}
		oldTerms, err := m.loadChannelRechargeVersions(ctx, accounts, channelFinanceSnapshot{})
		if err != nil {
			return result, nil, err
		}
		newTerms := append([]channelRechargeVersion(nil), oldTerms[correction.Domain]...)
		for _, row := range rows {
			var snapshot channelFinanceVersionSnapshot
			if err := json.Unmarshal([]byte(row.SnapshotJSON), &snapshot); err != nil {
				return result, nil, err
			}
			newTerms = append(newTerms, channelRechargeVersion{Version: row.Version, EffectiveAt: row.EffectiveAt,
				Paid: snapshot.UpstreamRechargePaid, Credit: snapshot.UpstreamRechargeCredit,
				Valid: validChannelFinanceNumber(snapshot.UpstreamRechargePaid) && validChannelFinanceNumber(snapshot.UpstreamRechargeCredit)})
		}
		detail, err := projectFinanceRechargeRehearsal(ctx, bills, scope, now, accounts, oldTerms[correction.Domain], newTerms, correction)
		if err != nil {
			return result, nil, err
		}
		result.Domains = append(result.Domains, detail)
		added = append(added, rows...)
	}
	return result, added, nil
}
