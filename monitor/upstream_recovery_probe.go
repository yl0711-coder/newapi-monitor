package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

var errUpstreamProbeRefreshRequired = errors.New("访问令牌需要续期，请使用账户配置的保存并验证连接完成续期；检测本身不会刷新令牌")
var errUpstreamProbeBudget = errors.New("恢复检测已达到单轮请求上限，未恢复任务")

type upstreamRecoveryProbeTransport struct {
	base  http.RoundTripper
	calls atomic.Int32
}

func (t *upstreamRecoveryProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet || t.calls.Add(1) > 4 {
		return nil, errUpstreamProbeBudget
	}
	return t.base.RoundTrip(r)
}

// Probe only the requested capability, never a different successful endpoint.
// No returned data is published and no credential is rotated. A successful
// probe is permission to enqueue one normal bounded sync, not proof of coverage.
func (m *Monitor) probeUpstreamRecovery(ctx context.Context, row ChannelUpstreamAccount, task string) error {
	credential, err := m.credentialForAccount(row)
	if err != nil {
		return err
	}
	sharedClient := m.channelUpstreamHTTPClient()
	clientCopy := *sharedClient
	clientCopy.Transport = &upstreamRecoveryProbeTransport{base: sharedClient.Transport}
	client := &clientCopy
	now := time.Now().Unix()
	to, day := now-now%3600, cstDayStart(now)-86400
	from := to - 60
	pacer := newUpstreamUsageRequestPacer(4, upstreamUsageRequestInterval)
	switch cred := credential.(type) {
	case newAPICredential:
		if cred.AccessToken == "" || (cred.ExpiresAt > 0 && cred.ExpiresAt <= now+30) {
			return errUpstreamProbeRefreshRequired
		}
		switch task {
		case "balance":
			_, err = fetchNewAPIBalance(ctx, client, row, cred)
		case "usage", "usage_history":
			_, err = fetchNewAPIUsagePage(ctx, client, row, cred, from, to, 1, pacer)
		case "pricing":
			_, err = fetchNewAPILogPageWithType(ctx, client, row, cred, from, to, 1, pacer, 2, decodeNewAPIUsageItem)
		case "error_logs":
			_, err = fetchUpstreamErrorLogPage(ctx, client, row, cred, from, to, 1, pacer)
		case "funds":
			for _, logType := range []int{1, 3, 4, 6} {
				decode := func(raw json.RawMessage) (upstreamFundItem, error) { return decodeUpstreamFundItem(row, logType, raw) }
				if _, err = fetchNewAPILogPageWithType(ctx, client, row, cred, from, to, 1, pacer, logType, decode); err != nil {
					break
				}
			}
		default:
			return errDiagnosticShape
		}
	case sub2APICredential:
		if cred.AccessToken == "" || cred.ExpiresAt <= now+30 {
			return errUpstreamProbeRefreshRequired
		}
		switch task {
		case "balance":
			_, err = sub2APIProfile(ctx, client, row, cred)
		case "usage", "usage_history":
			_, err = fetchSub2APIUsageWindow(ctx, client, row, cred, day, day+86400, pacer, row.UsageAdapter)
		case "pricing":
			_, err = fetchSub2PricingPage(ctx, client, row, cred, day, 1, pacer)
		case "funds":
			_, _, err = fetchSub2APIFundEvents(ctx, client, row, cred, now)
		default:
			return errDiagnosticShape
		}
	case tokenForceCredential:
		if cred.AccessToken == "" || cred.ExpiresAt <= now+30 {
			return errUpstreamProbeRefreshRequired
		}
		switch task {
		case "balance":
			_, err = tokenForceBalance(ctx, client, row, cred)
		case "usage", "usage_history":
			_, err = fetchTokenForceUsagePage(ctx, client, row, cred, from, to, 0, pacer)
		default:
			return errDiagnosticShape
		}
	case openOxCredential:
		switch task {
		case "balance":
			_, _, err = readOpenOxProfile(ctx, client, row, cred)
		case "usage", "usage_history":
			page, pageErr := fetchOpenOxUsagePage(ctx, client, row, cred, day, day+86400, 1, pacer)
			if pageErr != nil {
				return pageErr
			}
			_, err = aggregateOpenOxUsage(row, page.Items, day, day+86400)
		default:
			return errDiagnosticShape
		}
	case aiCodeWithCredential:
		if task == "balance" {
			_, _, err = syncAICodeWithBalanceSnapshot(ctx, client, row, cred)
			return err
		}
		// Usage recovery is per Key: never authorize every key based on a
		// successful first key. The caller supplies exactly one selected slot.
		keys, keyErr := aiCodeWithCredentialKeys(cred)
		if keyErr != nil {
			return keyErr
		}
		if len(keys) != 1 && task == "pricing" {
			normalized, normalizeErr := normalizeAICodeWithCredential(cred)
			if normalizeErr != nil {
				return normalizeErr
			}
			version, versionErr := aiCodeWithCredentialSetVersion(normalized)
			if versionErr != nil {
				return versionErr
			}
			var checkpoint AICodeWithPricingCheckpoint
			readErr := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND credential_set_version = ? AND semantics_version = ? AND next_credential < total_credentials", row.Domain, newAPIUpstreamAccountEpoch(row), version, upstreamPricingSemanticsVersion).Order("updated_at DESC").First(&checkpoint).Error
			if readErr != nil && !errors.Is(readErr, gorm.ErrRecordNotFound) {
				return readErr
			}
			if checkpoint.NextCredential < 0 || checkpoint.NextCredential >= len(normalized.Slots) {
				return errDiagnosticShape
			}
			keys = []string{normalized.Slots[checkpoint.NextCredential].Secret}
		}
		if len(keys) != 1 {
			return errors.New("春秋多 Key 需要逐 Key 验证恢复")
		}
		switch task {
		case "pricing":
			_, err = fetchAICodeWithPricingDay(ctx, client, row, keys[0], day, pacer)
		case "usage", "usage_history":
			recordMode := m.cfg.UpstreamAICodeWithRecordsEnabled
			kind := "tail"
			if task == "usage_history" {
				kind = "backfill"
			}
			var round AICodeWithUsageRound
			readErr := m.storeDB.WithContext(ctx).Where("domain = ? AND kind = ? AND status = ?", row.Domain, kind, upstreamStatusPending).First(&round).Error
			if readErr != nil && !errors.Is(readErr, gorm.ErrRecordNotFound) {
				return readErr
			}
			if readErr == nil {
				recordMode = round.RecordMode // The unfinished round freezes its protocol.
			}
			if recordMode {
				_, err = fetchAICodeWithRecordPage(ctx, client, row, keys[0], day, "", pacer)
			} else {
				_, err = fetchAICodeWithUsageWindow(ctx, client, row, keys[0], day, day+86400, pacer)
			}
		default:
			return errDiagnosticShape
		}
	default:
		return errDiagnosticShape
	}
	return err
}

// Retained for task-level checks and tests; capability rejection must happen
// before issuing any request, not after a guessed endpoint returns 404.
func upstreamRecoveryTaskAllowed(s Settings, row ChannelUpstreamAccount, task string) bool {
	if !row.Enabled || s.LocalSnapshotOnly {
		return false
	}
	supported := row.Provider == upstreamProviderNewAPI || row.Provider == upstreamProviderSub2API || row.Provider == upstreamProviderTokenForce || row.Provider == upstreamProviderAICodeWith || row.Provider == upstreamProviderOpenOx
	if !supported {
		return false
	}
	if task == "balance" {
		return s.UpstreamSyncEnabled
	}
	if !row.UsageSyncEnabled {
		return false
	}
	switch task {
	case "usage", "usage_history":
		return s.UpstreamUsageSyncEnabled
	case "funds":
		return s.UpstreamFundsSyncEnabled && upstreamFundProviderSupported(row.Provider) && pricingLedgerDomainAllowed(s.UpstreamFundsDomains, row.Domain)
	case "pricing":
		return s.UpstreamPricingLedgerEnabled && pricingLedgerAccountSupported(row) && pricingLedgerDomainAllowed(s.UpstreamPricingLedgerDomains, row.Domain)
	case "error_logs":
		return s.UpstreamErrorLogSyncEnabled && row.Provider == upstreamProviderNewAPI && pricingLedgerDomainAllowed(s.UpstreamErrorLogDomains, row.Domain)
	}
	return false
}
