//go:build unix

package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

type FinanceRechargeInspectionOptions struct {
	// Explicit local planning exclusions, never an instruction to zero cost or
	// change the production report's source scope.
	ExcludedDomains []string
}

// InspectFinanceRechargeBackup is a local, read-only plan builder. It does not
// construct a running Monitor, migrate SQLite, read credentials, contact an
// upstream, or update current channel finance settings. Reuse the existing
// guarded closed-backup reader rather than adding a second SQLite protocol.
func InspectFinanceRechargeBackup(parent context.Context, mainPath, startDate, endDate string, options FinanceRechargeInspectionOptions) (FinanceRechargeGapInspection, error) {
	var empty FinanceRechargeGapInspection
	excluded := map[string]bool{}
	if len(options.ExcludedDomains) > maxChannelFinanceRows {
		return empty, errors.New("too many local planning exclusions")
	}
	for _, domain := range options.ExcludedDomains {
		if domain == "" || len(domain) > 253 || strings.TrimSpace(domain) != domain || strings.ToLower(domain) != domain {
			return empty, errors.New("planning exclusions require canonical domain names")
		}
		excluded[domain] = true
	}
	from, err := financeStartHour(startDate)
	if err != nil {
		return empty, err
	}
	to, err := financeStartHour(endDate)
	if err != nil {
		return empty, err
	}
	now := time.Now().Unix()
	if from < 0 || to <= from || to > now-now%3600 || to-from > financeReportMaxDays*86400 {
		return empty, errors.New("inspection requires explicit, bounded closed dates")
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	backup, err := openGiftRelevantBackup(mainPath)
	if err != nil {
		return empty, err
	}
	defer backup.close()
	before, err := financeRechargeBackupHash(ctx, backup)
	if err != nil {
		return empty, err
	}
	var count int64
	if err := backup.db.WithContext(ctx).Model(&ChannelUpstreamUsageHour{}).Where("hour_ts>=? AND hour_ts<?", cstDayStart(from), to).Count(&count).Error; err != nil {
		return empty, err
	}
	if count > financeRechargeInspectBucketLimit {
		return empty, errors.New("closed snapshot exceeds recharge inspection bucket budget; use a smaller period")
	}
	var directory []struct {
		Domain, Provider, UsageAdapter string
		UsageSyncEnabled               bool
	}
	if err := backup.db.WithContext(ctx).Raw(`SELECT domain,provider,COALESCE(usage_adapter,'') usage_adapter,
		COALESCE(usage_sync_enabled,0) usage_sync_enabled FROM channel_upstream_accounts ORDER BY domain`).Scan(&directory).Error; err != nil {
		return empty, err
	}
	accounts := make(map[string]ChannelUpstreamAccountView, len(directory))
	for _, entry := range directory {
		if excluded[entry.Domain] {
			continue
		}
		accounts[entry.Domain] = ChannelUpstreamAccountView{Configured: true, Provider: entry.Provider, UsageAdapter: entry.UsageAdapter,
			UsageSyncEnabled: entry.UsageSyncEnabled, UsageGranularity: upstreamUsageGranularity(entry.Provider, entry.UsageAdapter)}
	}
	m := &Monitor{storeDB: backup.db}
	scope := stabilityScope{FromTs: from, ToTs: to}
	accounts, err = m.financeRelevantUpstreamAccounts(ctx, scope, accounts)
	if err != nil {
		return empty, err
	}
	inputs, err := m.loadFinanceBillInputs(ctx, scope, accounts, channelFinanceSnapshot{})
	if err != nil {
		return empty, err
	}
	result, err := inspectFinanceRechargeGaps(ctx, inputs, now)
	if err != nil {
		return empty, err
	}
	if err := backup.unchanged(); err != nil {
		return empty, err
	}
	after, err := financeRechargeBackupHash(ctx, backup)
	if err != nil || before != after {
		return empty, errors.New("source snapshot changed while preparing the recharge plan")
	}
	result.SourceSnapshotSHA256 = before
	result.ExcludedDomains = []string{}
	for domain := range excluded {
		result.ExcludedDomains = append(result.ExcludedDomains, domain)
	}
	sort.Strings(result.ExcludedDomains)
	if err := backup.unchanged(); err != nil {
		return empty, err
	}
	return result, ctx.Err()
}

func financeRechargeBackupHash(ctx context.Context, backup *giftRelevantBackup) (string, error) {
	f, err := giftLocalOpenRegular(backup.path, os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !os.SameFile(backup.info, info) || info.Size() > 2<<30 {
		return "", errors.New("snapshot identity or size is outside the inspection budget")
	}
	hash := sha256.New()
	buf := make([]byte, 256<<10)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buf)
		if n > 0 {
			if _, writeErr := hash.Write(buf[:n]); writeErr != nil {
				return "", writeErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
