package monitor

// Sub2API pricing evidence adapter.
//
// It downloads only the authenticated user's redacted usage DTO. Prompts,
// responses, API keys, IP addresses and request IDs are neither decoded nor
// persisted. A complete natural day is paged once and split into truthful
// hourly evidence locally, avoiding 24 repeated scans of the same upstream day.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

type sub2PricingUsageItem struct {
	ID                 int64
	CreatedAt          int64
	ModelName          string
	SourceGroup        string
	GroupRatio         pricingRatioValue
	EffectiveRatio     pricingRatioValue
	BillingMode        string
	PromptTokens       int64
	CompletionTokens   int64
	ActualCostMicros   int64
	ActualCostKnown    bool
	TokensExactKnown   bool
	EvidenceCapability string
}

type sub2PricingPage struct {
	Items       []sub2PricingUsageItem
	Total       int64
	Page        int
	PageSize    int
	Pages       int
	Fingerprint [32]byte
}

func exactDecimalMicros(raw json.RawMessage) (int64, error) {
	value, err := rawJSONNumber(raw)
	if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) || value > float64(math.MaxInt64)/1_000_000 {
		return 0, fmt.Errorf("金额不是有效非负数")
	}
	return int64(math.Round(value * 1_000_000)), nil
}

func decodeSub2PricingUsageItem(rawItem json.RawMessage) (sub2PricingUsageItem, error) {
	var raw struct {
		ID           json.RawMessage `json:"id"`
		CreatedAt    string          `json:"created_at"`
		Model        string          `json:"model"`
		InputTokens  json.RawMessage `json:"input_tokens"`
		OutputTokens json.RawMessage `json:"output_tokens"`
		CacheCreate  json.RawMessage `json:"cache_creation_tokens"`
		CacheRead    json.RawMessage `json:"cache_read_tokens"`
		ActualCost   json.RawMessage `json:"actual_cost"`
		Rate         json.RawMessage `json:"rate_multiplier"`
		BillingMode  *string         `json:"billing_mode"`
		Group        *struct {
			Name string          `json:"name"`
			Rate json.RawMessage `json:"rate_multiplier"`
		} `json:"group"`
	}
	if err := json.Unmarshal(rawItem, &raw); err != nil {
		return sub2PricingUsageItem{}, fmt.Errorf("Sub2API 使用日志条目无效: %w", err)
	}
	id, err := rawJSONInt64Exact(raw.ID)
	if err != nil || id <= 0 {
		return sub2PricingUsageItem{}, fmt.Errorf("Sub2API 使用日志缺少有效 id")
	}
	created, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw.CreatedAt))
	if err != nil {
		return sub2PricingUsageItem{}, fmt.Errorf("Sub2API 使用日志缺少有效 created_at")
	}
	prompt, promptOK := optionalRawJSONInt64Exact(raw.InputTokens)
	completion, completionOK := optionalRawJSONInt64Exact(raw.OutputTokens)
	cacheCreate, cacheCreateOK := optionalRawJSONInt64Exact(raw.CacheCreate)
	cacheRead, cacheReadOK := optionalRawJSONInt64Exact(raw.CacheRead)
	if !promptOK || !completionOK || !cacheCreateOK || !cacheReadOK || prompt < 0 || completion < 0 || cacheCreate < 0 || cacheRead < 0 ||
		prompt > math.MaxInt64-cacheCreate || prompt+cacheCreate > math.MaxInt64-cacheRead {
		return sub2PricingUsageItem{}, fmt.Errorf("Sub2API 使用日志 token 无效")
	}
	prompt += cacheCreate + cacheRead
	costMicros, err := exactDecimalMicros(raw.ActualCost)
	if err != nil {
		return sub2PricingUsageItem{}, fmt.Errorf("Sub2API actual_cost 无效: %w", err)
	}
	effective := canonicalPricingRatio(raw.Rate, len(raw.Rate) > 0)
	group := canonicalPricingRatio(nil, false)
	groupName := ""
	if raw.Group != nil {
		groupName = boundedPricingDimension(raw.Group.Name)
		group = canonicalPricingRatio(raw.Group.Rate, len(raw.Group.Rate) > 0)
	}
	capability := "cost_only"
	if effective.State == pricingRatioValid {
		// UsageLog.RateMultiplier is persisted with the request and is
		// historical evidence. The nested Group object is a live association;
		// its current multiplier must not be attached to an old hour as though
		// it were a historical snapshot.
		capability = "effective_rate"
	}
	billingMode := ""
	if raw.BillingMode != nil {
		billingMode = boundedPricingDimension(*raw.BillingMode)
	}
	return sub2PricingUsageItem{
		ID: id, CreatedAt: created.Unix(), ModelName: boundedPricingDimension(raw.Model), SourceGroup: groupName,
		GroupRatio: group, EffectiveRatio: effective, BillingMode: billingMode,
		PromptTokens: prompt, CompletionTokens: completion, ActualCostMicros: costMicros,
		ActualCostKnown: true, TokensExactKnown: true, EvidenceCapability: capability,
	}, nil
}

func sub2PricingFingerprint(items []sub2PricingUsageItem, total int64) [32]byte {
	rows := make([]string, 0, len(items)+1)
	rows = append(rows, strconv.FormatInt(total, 10))
	for _, item := range items {
		rows = append(rows, strings.Join([]string{
			strconv.FormatInt(item.ID, 10), strconv.FormatInt(item.CreatedAt, 10),
			strconv.FormatInt(item.ActualCostMicros, 10), item.GroupRatio.Canonical,
			item.EffectiveRatio.Canonical, item.SourceGroup, item.ModelName,
		}, "|"))
	}
	return sha256.Sum256([]byte(strings.Join(rows, "\n")))
}

func fetchSub2PricingPage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred sub2APICredential, dayTs int64, page int, pacer *upstreamUsageRequestPacer) (sub2PricingPage, error) {
	if page < 1 {
		return sub2PricingPage{}, fmt.Errorf("Sub2API 计价分页无效")
	}
	if err := pacer.beforeRequest(ctx); err != nil {
		return sub2PricingPage{}, err
	}
	day := time.Unix(dayTs, 0).In(cstLocation).Format("2006-01-02")
	query := url.Values{}
	query.Set("page", strconv.Itoa(page))
	query.Set("page_size", strconv.Itoa(upstreamUsagePageSize))
	query.Set("sort_by", "created_at")
	query.Set("sort_order", "asc")
	query.Set("start_date", day)
	query.Set("end_date", day)
	query.Set("timezone", "Asia/Shanghai")
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, "/api/v1/usage")+"?"+query.Encode(), sub2APIUsageHeaders(cred), nil)
	if err != nil {
		return sub2PricingPage{}, wrapSub2APIUsageHTTPError(err)
	}
	var envelope struct {
		Items    []json.RawMessage `json:"items"`
		Total    int64             `json:"total"`
		Page     int               `json:"page"`
		PageSize int               `json:"page_size"`
		Pages    int               `json:"pages"`
	}
	if err := decodeSub2APIData(body, &envelope); err != nil {
		return sub2PricingPage{}, fmt.Errorf("Sub2API 计价日志响应无效: %w", err)
	}
	if envelope.Total < 0 || envelope.Page != page || envelope.PageSize <= 0 || envelope.PageSize > upstreamUsagePageSize || envelope.Pages < 0 {
		return sub2PricingPage{}, fmt.Errorf("Sub2API 计价分页元数据无效")
	}
	items := make([]sub2PricingUsageItem, 0, len(envelope.Items))
	for _, rawItem := range envelope.Items {
		item, err := decodeSub2PricingUsageItem(rawItem)
		if err != nil {
			return sub2PricingPage{}, err
		}
		if item.CreatedAt < dayTs || item.CreatedAt >= dayTs+86400 {
			return sub2PricingPage{}, fmt.Errorf("Sub2API 计价日志返回了窗口外条目")
		}
		items = append(items, item)
	}
	return sub2PricingPage{Items: items, Total: envelope.Total, Page: page, PageSize: envelope.PageSize, Pages: envelope.Pages, Fingerprint: sub2PricingFingerprint(items, envelope.Total)}, nil
}

func buildSub2PricingEvidence(account ChannelUpstreamAccount, items []sub2PricingUsageItem, now int64) ([]ChannelUpstreamPricingHourEvidence, error) {
	epoch := newAPIUpstreamAccountEpoch(account)
	byDimension := make(map[string]*ChannelUpstreamPricingHourEvidence)
	for _, item := range items {
		hourTs := item.CreatedAt - item.CreatedAt%3600
		attributes := newAPIPricingAttributes{
			GroupName: item.SourceGroup, ModelName: item.ModelName,
			GroupRatioState:     pricingRatioMissing,
			UserGroupRatioState: pricingRatioMissing,
			EffectiveRatio:      item.EffectiveRatio.Text, EffectiveRatioSource: "rate_multiplier",
			DiscountRatioState: pricingRatioMissing, EvidenceCapability: item.EvidenceCapability,
			BillingMode: item.BillingMode, OtherValid: true,
		}
		dimensionHash := pricingDimensionHash(attributes)
		mapKey := strconv.FormatInt(hourTs, 10) + ":" + dimensionHash
		evidence := byDimension[mapKey]
		if evidence == nil {
			evidence = &ChannelUpstreamPricingHourEvidence{
				Domain: account.Domain, AccountEpoch: epoch, HourTs: hourTs,
				SemanticsVersion: upstreamPricingSemanticsVersion, DimensionHash: dimensionHash,
				Provider: upstreamProviderSub2API, SourceGroup: item.SourceGroup, ModelName: item.ModelName,
				GroupRatioState:     pricingRatioMissing,
				UserGroupRatioState: pricingRatioMissing,
				EffectiveRatio:      item.EffectiveRatio.Text, EffectiveRatioSource: "rate_multiplier",
				DiscountRatioState: pricingRatioMissing, EvidenceCapability: item.EvidenceCapability,
				BillingMode: item.BillingMode, OtherValid: true,
				FirstSourceAt: item.CreatedAt, LastSourceAt: item.CreatedAt, FetchedAt: now,
			}
			byDimension[mapKey] = evidence
		}
		evidence.SourceRows++
		if item.CreatedAt < evidence.FirstSourceAt {
			evidence.FirstSourceAt = item.CreatedAt
		}
		if item.CreatedAt > evidence.LastSourceAt {
			evidence.LastSourceAt = item.CreatedAt
		}
		if item.ActualCostMicros <= 0 {
			evidence.NonPositiveQuotaRows++
		}
		if err := addPricingCounter(&evidence.EligibleRequests, 1); err != nil {
			return nil, err
		}
		if err := addPricingCounter(&evidence.PromptTokens, item.PromptTokens); err != nil {
			return nil, err
		}
		if err := addPricingCounter(&evidence.CompletionTokens, item.CompletionTokens); err != nil {
			return nil, err
		}
		if item.ActualCostMicros > 0 {
			if err := addPricingCounter(&evidence.FinalQuota, item.ActualCostMicros); err != nil {
				return nil, err
			}
		}
	}
	return pricingEvidenceMapToSlice(byDimension), nil
}

func validateSub2PricingPage(page sub2PricingPage, total int64, number, pages int) error {
	if page.Total != total || page.Page != number || page.Pages != pages {
		return fmt.Errorf("Sub2API 计价分页在扫描期间变化")
	}
	expected := upstreamUsagePageSize
	if number == pages {
		expected = int(total) - (number-1)*upstreamUsagePageSize
	}
	if total == 0 {
		expected = 0
	}
	if len(page.Items) != expected {
		return fmt.Errorf("Sub2API 计价分页数量异常（got=%d want=%d）", len(page.Items), expected)
	}
	return nil
}

func sub2PricingCheckpointProgress(checkpoint ChannelUpstreamPricingPageCheckpoint) string {
	return fmt.Sprintf("%s 已安全保存 %d/%d 条（下一页 %d）",
		time.Unix(checkpoint.HourTs, 0).In(cstLocation).Format("2006-01-02"), checkpoint.SourceRows, checkpoint.Total, checkpoint.NextPage)
}

// fetchSub2PricingDay pages one natural day once and returns all 24 truthful
// hour buckets. A budget yield leaves only a redacted aggregate checkpoint.
func (m *Monitor) fetchSub2PricingDay(ctx context.Context, account ChannelUpstreamAccount, cred sub2APICredential, dayTs int64, pacer *upstreamUsageRequestPacer, now int64) (map[int64][]ChannelUpstreamPricingHourEvidence, string, bool, error) {
	if dayTs != cstDayStart(dayTs) {
		return nil, "", false, fmt.Errorf("Sub2API 计价必须按中国自然日同步")
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	var checkpoint ChannelUpstreamPricingPageCheckpoint
	loadErr := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND hour_ts = ?", account.Domain, epoch, upstreamPricingSemanticsVersion, dayTs).First(&checkpoint).Error
	if loadErr != nil && !errors.Is(loadErr, gorm.ErrRecordNotFound) {
		return nil, "", false, loadErr
	}
	hasCheckpoint := loadErr == nil && checkpoint.Provider == upstreamProviderSub2API && checkpoint.WindowSeconds == 86400 && checkpoint.NextPage >= 2 && checkpoint.Total >= 0
	if loadErr == nil && !hasCheckpoint {
		if err := m.deletePricingPageCheckpoint(ctx, account.Domain, epoch, dayTs); err != nil {
			return nil, "", false, err
		}
	}
	client := m.channelUpstreamHTTPClient()
	var first sub2PricingPage
	checkpointVerified := false
	if hasCheckpoint {
		page, err := fetchSub2PricingPage(ctx, client, account, cred, dayTs, 1, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return nil, sub2PricingCheckpointProgress(checkpoint), false, nil
			}
			return nil, "", false, err
		}
		first = page
		if page.Total != checkpoint.Total || hex.EncodeToString(page.Fingerprint[:]) != checkpoint.FirstPageFingerprint {
			if err := m.deletePricingPageCheckpoint(ctx, account.Domain, epoch, dayTs); err != nil {
				return nil, "", false, err
			}
			return nil, "", false, fmt.Errorf("Sub2API 计价日志断点期间首页已变化，窗口将重试")
		}
		checkpointVerified = true
	}
	evidenceMap := make(map[string]*ChannelUpstreamPricingHourEvidence)
	if !hasCheckpoint {
		page, err := fetchSub2PricingPage(ctx, client, account, cred, dayTs, 1, pacer)
		if err != nil {
			return nil, "", false, err
		}
		first = page
		pages := first.Pages
		if first.Total == 0 {
			pages = 0
		}
		if pages == 0 && first.Total > 0 {
			pages = int((first.Total + upstreamUsagePageSize - 1) / upstreamUsagePageSize)
			first.Pages = pages
		}
		if err := validateSub2PricingPage(first, first.Total, 1, maxInt(1, pages)); err != nil && first.Total > 0 {
			return nil, "", false, err
		}
		rows, err := buildSub2PricingEvidence(account, first.Items, now)
		if err != nil {
			return nil, "", false, err
		}
		if err := mergePricingEvidenceRows(evidenceMap, rows); err != nil {
			return nil, "", false, err
		}
		checkpoint = ChannelUpstreamPricingPageCheckpoint{
			Domain: account.Domain, AccountEpoch: epoch, SemanticsVersion: upstreamPricingSemanticsVersion,
			HourTs: dayTs, Provider: upstreamProviderSub2API, WindowSeconds: 86400,
			NextPage: 2, Total: first.Total, SourceRows: int64(len(first.Items)),
			FirstPageFingerprint: hex.EncodeToString(first.Fingerprint[:]),
		}
		if err := m.savePricingPageCheckpoint(ctx, &checkpoint, evidenceMap); err != nil {
			return nil, "", false, err
		}
	} else {
		var err error
		evidenceMap, err = decodePricingCheckpointEvidence(checkpoint)
		if err != nil {
			_ = m.deletePricingPageCheckpoint(ctx, account.Domain, epoch, dayTs)
			return nil, "", false, err
		}
	}
	pages := first.Pages
	if checkpoint.Total == 0 {
		pages = 0
	}
	if pages == 0 && checkpoint.Total > 0 {
		pages = int((checkpoint.Total + upstreamUsagePageSize - 1) / upstreamUsagePageSize)
	}
	for checkpoint.NextPage <= pages {
		pageNumber := checkpoint.NextPage
		page, err := fetchSub2PricingPage(ctx, client, account, cred, dayTs, pageNumber, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return nil, sub2PricingCheckpointProgress(checkpoint), false, nil
			}
			return nil, "", false, err
		}
		if err := validateSub2PricingPage(page, checkpoint.Total, pageNumber, pages); err != nil {
			return nil, "", false, err
		}
		rows, err := buildSub2PricingEvidence(account, page.Items, now)
		if err != nil {
			return nil, "", false, err
		}
		if err := mergePricingEvidenceRows(evidenceMap, rows); err != nil {
			return nil, "", false, err
		}
		checkpoint.SourceRows += int64(len(page.Items))
		checkpoint.NextPage++
		if err := m.savePricingPageCheckpoint(ctx, &checkpoint, evidenceMap); err != nil {
			return nil, "", false, err
		}
	}
	if checkpoint.SourceRows != checkpoint.Total {
		return nil, "", false, fmt.Errorf("Sub2API 计价日志分页数量不完整（got=%d want=%d）", checkpoint.SourceRows, checkpoint.Total)
	}
	if pages > 1 && !checkpointVerified {
		probe, err := fetchSub2PricingPage(ctx, client, account, cred, dayTs, 1, pacer)
		if err != nil {
			var exhausted *upstreamUsageRunBudgetExhausted
			if errors.As(err, &exhausted) {
				return nil, sub2PricingCheckpointProgress(checkpoint), false, nil
			}
			return nil, "", false, err
		}
		if probe.Total != checkpoint.Total || hex.EncodeToString(probe.Fingerprint[:]) != checkpoint.FirstPageFingerprint {
			_ = m.deletePricingPageCheckpoint(ctx, account.Domain, epoch, dayTs)
			return nil, "", false, fmt.Errorf("Sub2API 计价日志扫描期间首页已变化，窗口将重试")
		}
	}
	byHour := make(map[int64][]ChannelUpstreamPricingHourEvidence, 24)
	for hour := dayTs; hour < dayTs+86400; hour += 3600 {
		byHour[hour] = nil
	}
	for _, row := range pricingEvidenceMapToSlice(evidenceMap) {
		byHour[row.HourTs] = append(byHour[row.HourTs], row)
	}
	for hour := range byHour {
		sort.Slice(byHour[hour], func(i, j int) bool { return byHour[hour][i].DimensionHash < byHour[hour][j].DimensionHash })
	}
	return byHour, "", true, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (m *Monitor) syncStoredSub2Pricing(ctx context.Context, domain string) (ChannelUpstreamPricingSyncState, error) {
	m.upstreamPricingMu.Lock()
	defer m.upstreamPricingMu.Unlock()
	now := time.Now().Unix()
	var account ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&account, "domain = ?", domain).Error; err != nil {
		return ChannelUpstreamPricingSyncState{}, err
	}
	if !account.Enabled || !account.UsageSyncEnabled || account.Provider != upstreamProviderSub2API {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("该账户未启用 Sub2API 使用日志同步")
	}
	if account.UsageAdapter == upstreamUsageAdapterSub2Stats {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("Sub2API 旧站点只提供单日汇总，无法构建逐请求倍率证据")
	}
	if !m.cfg.UpstreamPricingLedgerEnabled || !pricingLedgerDomainAllowed(m.cfg.UpstreamPricingLedgerDomains, account.Domain) {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("该域名未进入计价账本灰度名单")
	}
	credentialAny, err := m.credentialForAccount(account)
	if err != nil {
		return ChannelUpstreamPricingSyncState{}, err
	}
	credential, ok := credentialAny.(sub2APICredential)
	if !ok {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("Sub2API 凭据格式无效")
	}
	if credential.AccessToken == "" || credential.ExpiresAt <= time.Now().Add(2*time.Minute).Unix() {
		credential, err = refreshSub2API(ctx, m.channelUpstreamHTTPClient(), account, credential)
		if err != nil {
			return ChannelUpstreamPricingSyncState{}, err
		}
		if err := m.persistSyncedUpstreamAccount(ctx, &account, credential); err != nil {
			return ChannelUpstreamPricingSyncState{}, err
		}
	}
	state, err := m.loadOrCreatePricingSyncState(ctx, account, now)
	if err != nil {
		return state, err
	}
	today := cstDayStart(now)
	state.BackfillStartHour = cstDayStart(state.BackfillStartHour)
	state.BackfillNextHour = cstDayStart(state.BackfillNextHour)
	state.BackfillTargetHour = today
	state.BackfillDone = state.BackfillNextHour >= today
	state.TailNextSyncAt = today + 86400 + 300
	state.LastAttemptAt = now
	previousFailures := state.ConsecutiveFailures
	if !state.BackfillDone && (state.BackfillNextSyncAt == 0 || state.BackfillNextSyncAt <= now) {
		pacer := newUpstreamUsageRequestPacer(upstreamPricingMaxRequestsPerRun, upstreamUsageRequestInterval)
		var byHour map[int64][]ChannelUpstreamPricingHourEvidence
		var progress string
		var complete bool
		byHour, progress, complete, err = m.fetchSub2PricingDay(ctx, account, credential, state.BackfillNextHour, pacer, now)
		if err == nil && !complete {
			state.Status, state.Progress, state.LastError = "paging", progress, ""
			state.BackfillNextSyncAt = now + 60
		} else if err == nil {
			allVerified := true
			state.LastError = ""
			for hour := state.BackfillNextHour; hour < state.BackfillNextHour+86400; hour += 3600 {
				evidence := byHour[hour]
				hourState, buildErr := pricingHourStateFromEvidence(account, hour, evidence, now)
				if buildErr != nil {
					err = buildErr
					break
				}
				if persistErr := m.persistNewAPIPricingHour(ctx, account, hour, evidence, hourState, now); persistErr != nil {
					err = persistErr
					break
				}
				var published ChannelUpstreamPricingHourState
				if queryErr := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND hour_ts = ? AND semantics_version = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hour, upstreamPricingSemanticsVersion).First(&published).Error; queryErr != nil {
					err = queryErr
					break
				}
				if published.Status != "verified" || !pricingReconcileAccepted(account.Provider, published.ReconcileStatus) {
					allVerified = false
					if state.LastError == "" {
						if published.Status == "verified" {
							state.Status, state.LastError = "reconcile_blocked", pricingReconcileBlockMessage(published)
						} else {
							state.Status, state.LastError = "pending_verification", "已完整读取整日日志，等待下一轮一致性复核"
						}
					}
				}
			}
			if err == nil {
				state.LastSuccessAt = now
				state.Progress = ""
				if allVerified {
					state.BackfillNextHour += 86400
					state.BackfillDone = state.BackfillNextHour >= today
					state.Status, state.LastError = "backfilling", ""
					if state.BackfillDone {
						state.Status = "ok"
						state.BackfillNextSyncAt = 0
					} else {
						state.BackfillNextSyncAt = now + 60
					}
				} else {
					state.BackfillNextSyncAt = now + 60
				}
			}
		}
	}
	if err != nil {
		state.Status = "error"
		state.LastError = sanitizeUpstreamErrorWithSecrets(err, upstreamCredentialSecrets(credential)...)
		state.ConsecutiveFailures = previousFailures + 1
		state.BackfillNextSyncAt = pricingSyncRetryAt(now, state.ConsecutiveFailures)
		if upstreamAt := upstreamRetryAt(err); upstreamAt > state.BackfillNextSyncAt {
			state.BackfillNextSyncAt = upstreamAt
		}
	} else {
		state.ConsecutiveFailures = 0
	}
	if countErr := m.refreshPricingSyncCounts(ctx, &state); countErr != nil && err == nil {
		err = countErr
		state.Status, state.LastError = "error", sanitizeUpstreamError(countErr)
	}
	if saveErr := m.savePricingSyncState(ctx, &state); saveErr != nil {
		return state, saveErr
	}
	return state, err
}
