package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	financePeriodCacheMaxEntries = 36
	financePeriodCacheMaxBytes   = 32 << 20
	financePeriodCacheTTL        = 7 * 24 * time.Hour
	financePeriodSnapshotDirName = "finance-period-cache"
	financePeriodCacheSchema     = 4
)

// financePeriodComponent is the expensive, independently reproducible part of
// one month. JSON storage gives every caller an isolated deep copy: later gift
// allocation and CUR projection can safely enrich the returned statements
// without mutating the cached base facts.
type financePeriodComponent struct {
	Statement        financeStatementView        `json:"statement"`
	UserCoverage     StabilityDataCoverage       `json:"user_coverage"`
	UpstreamCoverage financeUpstreamCoverageView `json:"upstream_coverage"`
	CostDetails      []financeCostDetailView     `json:"cost_details"`
	Days             []financeDailyView          `json:"days"`
}

type financePeriodAccountInput struct {
	Domain              string  `json:"domain"`
	Configured          bool    `json:"configured"`
	Enabled             bool    `json:"enabled"`
	Provider            string  `json:"provider"`
	UsageSyncEnabled    bool    `json:"usage_sync_enabled"`
	UsageAdapter        string  `json:"usage_adapter"`
	UsageGranularity    string  `json:"usage_granularity"`
	UnitPerUSD          float64 `json:"unit_per_usd"`
	FinanceRequiredFrom int64   `json:"finance_required_from"`
}

func financePeriodInputFingerprint(
	accounts map[string]ChannelUpstreamAccountView,
	finance channelFinanceSnapshot,
	internalAccounts financeConfiguredInternalEvidence,
	internalCost financeInternalTestCostEvidence,
	businessGroups map[string]bool,
) (string, error) {
	accountRows := make([]financePeriodAccountInput, 0, len(accounts))
	for domain, account := range accounts {
		accountRows = append(accountRows, financePeriodAccountInput{
			Domain: domain, Configured: account.Configured, Enabled: account.Enabled,
			Provider: account.Provider, UsageSyncEnabled: account.UsageSyncEnabled,
			UsageAdapter: account.UsageAdapter, UsageGranularity: account.UsageGranularity,
			UnitPerUSD: account.UnitPerUSD, FinanceRequiredFrom: account.FinanceRequiredFrom,
		})
	}
	sort.Slice(accountRows, func(i, j int) bool { return accountRows[i].Domain < accountRows[j].Domain })
	input := struct {
		Accounts           []financePeriodAccountInput                  `json:"accounts"`
		Settings           ChannelFinanceSetting                        `json:"settings"`
		HasSettings        bool                                         `json:"has_settings"`
		SiteGroups         map[string]ChannelSaleGroupRate              `json:"site_groups"`
		DomainCosts        map[string]ChannelDomainCost                 `json:"domain_costs"`
		DomainGroupCosts   map[string]map[string]ChannelDomainGroupCost `json:"domain_group_costs"`
		ChannelGroupCosts  map[int]map[string]ChannelFinanceChannelCost `json:"channel_group_costs"`
		ChannelCanonical   map[int]ChannelFinanceChannelCost            `json:"channel_canonical"`
		ChannelConflicts   map[int]bool                                 `json:"channel_conflicts"`
		DomainVersions     map[string]ChannelFinanceVersion             `json:"domain_versions"`
		InternalAccountIDs []int64                                      `json:"internal_account_ids"`
		InternalComplete   bool                                         `json:"internal_complete"`
		InternalScope      stabilityScope                               `json:"internal_scope"`
		InternalRows       []FinanceInternalAccountHourFact             `json:"internal_rows"`
		InternalCost       financeInternalTestCostEvidence              `json:"internal_cost"`
		BusinessGroups     map[string]bool                              `json:"business_groups"`
	}{
		Accounts: accountRows, Settings: finance.settings, HasSettings: finance.hasSettings,
		SiteGroups: finance.siteGroups, DomainCosts: finance.domainCosts,
		DomainGroupCosts: finance.domainGroupCost, ChannelGroupCosts: finance.channelGroupCost,
		ChannelCanonical: finance.channelCanonicalCost, ChannelConflicts: finance.channelCostConflict,
		DomainVersions: finance.domainVersions, InternalAccountIDs: append([]int64(nil), internalAccounts.AccountIDs...),
		InternalComplete: internalAccounts.Complete, InternalRows: internalAccounts.Rows, InternalCost: internalCost,
		InternalScope:  internalAccounts.VerifiedScope,
		BusinessGroups: businessGroups,
	}
	sort.Slice(input.InternalAccountIDs, func(i, j int) bool { return input.InternalAccountIDs[i] < input.InternalAccountIDs[j] })
	payload, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("序列化月度核算输入版本: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (m *Monitor) getFinancePeriodCache() *boundedByteCache {
	m.financePeriodCacheOnce.Do(func() {
		m.financePeriodCache = newBoundedByteCache(financePeriodCacheMaxEntries, financePeriodCacheMaxBytes)
	})
	return m.financePeriodCache
}

func financePeriodCacheKey(scope stabilityScope, configurationHash, sourceFingerprint string) string {
	return fmt.Sprintf("v%d:%d:%d:%s:%s", financePeriodCacheSchema, scope.FromTs, scope.ToTs, configurationHash, sourceFingerprint)
}

func financePeriodLogicalKey(scope stabilityScope, configurationHash string) string {
	return fmt.Sprintf("v%d:%d:%d:%s", financePeriodCacheSchema, scope.FromTs, scope.ToTs, configurationHash)
}

func (m *Monitor) buildFinancePeriodComponent(
	ctx context.Context,
	scope stabilityScope,
	now int64,
	configurationHash string,
	accounts map[string]ChannelUpstreamAccountView,
	finance channelFinanceSnapshot,
	internalTestCost financeInternalTestCostEvidence,
	internalAccounts financeConfiguredInternalEvidence,
	businessGroups map[string]bool,
) (financePeriodComponent, bool, error) {
	build := func() (financePeriodComponent, error) {
		statement, userCoverage, upstreamCoverage, details, err := m.buildFinancePeriod(
			ctx, scope, now, accounts, finance, internalTestCost, internalAccounts, businessGroups,
		)
		if err != nil {
			return financePeriodComponent{}, err
		}
		days, err := m.buildFinanceDailyViews(ctx, scope, now, internalTestCost, internalAccounts, businessGroups)
		if err != nil {
			return financePeriodComponent{}, err
		}
		return financePeriodComponent{
			Statement: statement, UserCoverage: userCoverage, UpstreamCoverage: upstreamCoverage,
			CostDetails: details, Days: days,
		}, nil
	}

	fingerprint, err := m.financeReportPeriodSourceFingerprint(ctx, scope.FromTs, scope.ToTs)
	if err != nil {
		// Fingerprinting is an optimization only. A direct build preserves the
		// existing fail-closed financial result when the cache guard is unavailable.
		component, buildErr := build()
		return component, false, buildErr
	}
	inputFingerprint, err := financePeriodInputFingerprint(accounts, finance, internalAccounts, internalTestCost, businessGroups)
	if err != nil {
		component, buildErr := build()
		return component, false, buildErr
	}
	configurationKey := configurationHash + ":" + inputFingerprint
	key := financePeriodCacheKey(scope, configurationKey, fingerprint)
	cache := m.getFinancePeriodCache()
	force, _ := ctx.Value(financeForceRebuildKey{}).(bool)
	if !force {
		if payload, ok := cache.Get(key, time.Now()); ok {
			var component financePeriodComponent
			if json.Unmarshal(payload, &component) == nil {
				return component, true, nil
			}
		}
		if payload, ok, loadErr := m.loadFinancePeriodSnapshot(
			financePeriodLogicalKey(scope, configurationKey), fingerprint,
		); loadErr == nil && ok {
			var component financePeriodComponent
			if json.Unmarshal(payload, &component) == nil {
				cache.Put(key, payload, financePeriodCacheTTL, time.Now())
				return component, true, nil
			}
		}
	}
	component, err := build()
	if err != nil {
		return financePeriodComponent{}, false, err
	}
	currentFingerprint, err := m.financeReportPeriodSourceFingerprint(ctx, scope.FromTs, scope.ToTs)
	if err != nil {
		return financePeriodComponent{}, false, err
	}
	if currentFingerprint != fingerprint {
		return financePeriodComponent{}, false, errFinanceFactsChanged
	}
	payload, err := json.Marshal(component)
	if err != nil {
		return financePeriodComponent{}, false, fmt.Errorf("序列化月度经营核算组件: %w", err)
	}
	cache.Put(key, payload, financePeriodCacheTTL, time.Now())
	m.persistFinancePeriodSnapshotAsync(financePeriodLogicalKey(scope, configurationKey), fingerprint, payload)
	return component, false, nil
}

func financePeriodSnapshotPath(storePath, logicalKey string) (string, error) {
	storePath = strings.TrimSpace(storePath)
	if storePath == "" || !storeUsesFile(storePath) {
		return "", errors.New("月度经营核算缓存需要文件型本地库路径")
	}
	digest := sha256.Sum256([]byte(logicalKey))
	return filepath.Join(filepath.Dir(storePath), financePeriodSnapshotDirName,
		hex.EncodeToString(digest[:])+financeReportSnapshotFileSuffix), nil
}

func (m *Monitor) loadFinancePeriodSnapshot(logicalKey, fingerprint string) ([]byte, bool, error) {
	if !m.cfg.FinanceReportSnapshotReadEnabled {
		return nil, false, nil
	}
	path, err := financePeriodSnapshotPath(m.cfg.StorePath, logicalKey)
	if err != nil {
		return nil, false, err
	}
	m.financeSnapshotWriteMu.RLock()
	defer m.financeSnapshotWriteMu.RUnlock()
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if info.Size() <= 0 || info.Size() > int64(financeReportCacheMaxBytes+64*1024) {
		return nil, false, errors.New("月度经营核算缓存大小异常")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	var envelope financeReportSnapshotEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, false, err
	}
	if envelope.FormatVersion != financeReportSnapshotFormatVersion ||
		envelope.CacheKey != logicalKey || envelope.SourceFingerprint != fingerprint ||
		envelope.StoredAt <= 0 || time.Since(time.Unix(envelope.StoredAt, 0)) > financePeriodCacheTTL ||
		time.Unix(envelope.StoredAt, 0).After(time.Now().Add(time.Minute)) ||
		len(envelope.Payload) == 0 || len(envelope.Payload) > financeReportCacheMaxBytes || !json.Valid(envelope.Payload) {
		return nil, false, nil
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.PayloadSHA256 != hex.EncodeToString(digest[:]) {
		return nil, false, errors.New("月度经营核算缓存哈希不一致")
	}
	return append([]byte(nil), envelope.Payload...), true, nil
}

func (m *Monitor) persistFinancePeriodSnapshotAsync(logicalKey, fingerprint string, payload []byte) {
	if !m.cfg.FinanceReportSnapshotShadowEnabled {
		return
	}
	payload = append([]byte(nil), payload...)
	go func() {
		m.financeSnapshotWriteMu.Lock()
		defer m.financeSnapshotWriteMu.Unlock()
		if err := m.persistFinancePeriodSnapshot(logicalKey, fingerprint, payload); err != nil {
			// This cache is always rebuildable. A failed shadow write must never
			// change report availability or financial semantics.
			slog.Warn("月度经营核算缓存影子写入失败", "err", err)
			return
		}
	}()
}

func (m *Monitor) persistFinancePeriodSnapshot(logicalKey, fingerprint string, payload []byte) error {
	if !m.cfg.FinanceReportSnapshotShadowEnabled {
		return nil
	}
	if strings.TrimSpace(logicalKey) == "" || len(payload) == 0 ||
		len(payload) > financeReportCacheMaxBytes || !json.Valid(payload) {
		return errors.New("月度经营核算缓存输入无效")
	}
	path, err := financePeriodSnapshotPath(m.cfg.StorePath, logicalKey)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	envelope, err := json.Marshal(financeReportSnapshotEnvelope{
		FormatVersion: financeReportSnapshotFormatVersion, CacheKey: logicalKey,
		SourceFingerprint: fingerprint, StoredAt: time.Now().Unix(),
		PayloadSHA256: hex.EncodeToString(digest[:]), Payload: append(json.RawMessage(nil), payload...),
	})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".finance-period-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(envelope); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return pruneFinanceReportSnapshots(dir, financePeriodCacheMaxEntries)
}
