package monitor

// AICodeWith pricing evidence adapter.
//
// The documented key endpoint is rate-limited per key and exposes actual
// charges. Its detail contract exposes the applied channel discount, but does
// not guarantee a base group multiplier. The adapter therefore publishes
// `discount_only` evidence and never pretends that it is a complete rate.

import (
	"context"
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
	"gorm.io/gorm/clause"
)

const aiCodeWithPricingDetailsLimit = 1000

type AICodeWithPricingCheckpoint struct {
	Domain               string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch         string `gorm:"primaryKey;size:64;column:account_epoch"`
	SemanticsVersion     int    `gorm:"primaryKey;column:semantics_version"`
	DayTs                int64  `gorm:"primaryKey;column:day_ts"`
	CredentialSetVersion string `gorm:"size:64;column:credential_set_version"`
	NextCredential       int    `gorm:"column:next_credential"`
	TotalCredentials     int    `gorm:"column:total_credentials"`
	SourceRows           int64  `gorm:"column:source_rows"`
	AggregatesJSON       string `gorm:"type:text;column:aggregates_json"`
	UpdatedAt            int64  `gorm:"column:updated_at;index"`
}

func (AICodeWithPricingCheckpoint) TableName() string {
	return "aicodewith_pricing_checkpoints"
}

type aiCodeWithPricingItem struct {
	CreatedAt        int64
	ModelName        string
	SourceGroup      string
	Discount         pricingRatioValue
	PromptTokens     int64
	CompletionTokens int64
	CostMicros       int64
}

func firstRawField(fields map[string]json.RawMessage, names ...string) (json.RawMessage, bool) {
	for _, name := range names {
		if raw, ok := fields[name]; ok {
			return raw, true
		}
	}
	return nil, false
}

func rawStringField(fields map[string]json.RawMessage, names ...string) string {
	raw, ok := firstRawField(fields, names...)
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func parseAICodeWithTimestamp(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("缺少 timestamp")
	}
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(text))
		if err != nil {
			return 0, fmt.Errorf("timestamp 不是 RFC3339")
		}
		return parsed.Unix(), nil
	}
	value, err := rawJSONNumber(raw)
	if err != nil || value < 0 || value != math.Trunc(value) {
		return 0, fmt.Errorf("timestamp 无效")
	}
	if value > 1e12 {
		value /= 1000
	}
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("timestamp 溢出")
	}
	return int64(value), nil
}

func decodeAICodeWithPricingItem(rawItem json.RawMessage) (aiCodeWithPricingItem, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(rawItem, &fields); err != nil || fields == nil {
		return aiCodeWithPricingItem{}, fmt.Errorf("AICodeWith 使用明细条目无效")
	}
	timestampRaw, _ := firstRawField(fields, "timestamp", "created_at", "createdAt")
	createdAt, err := parseAICodeWithTimestamp(timestampRaw)
	if err != nil {
		return aiCodeWithPricingItem{}, fmt.Errorf("AICodeWith 使用明细时间无效: %w", err)
	}
	discountRaw, discountPresent := firstRawField(fields, "channel_discount", "channelDiscount", "discount")
	discount := canonicalPricingRatio(discountRaw, discountPresent)
	if discount.State != pricingRatioValid {
		return aiCodeWithPricingItem{}, fmt.Errorf("AICodeWith 使用明细未返回有效折扣")
	}
	costRaw, costPresent := firstRawField(fields, "total_cost_cny", "totalCostCNY", "cost", "actual_cost")
	if !costPresent {
		return aiCodeWithPricingItem{}, fmt.Errorf("AICodeWith 使用明细未返回实际扣费")
	}
	costMicros, err := exactDecimalMicros(costRaw)
	if err != nil {
		return aiCodeWithPricingItem{}, fmt.Errorf("AICodeWith 实际扣费无效: %w", err)
	}
	parseTokens := func(names ...string) (int64, error) {
		raw, present := firstRawField(fields, names...)
		if !present {
			return 0, nil
		}
		value, ok := optionalRawJSONInt64Exact(raw)
		if !ok || value < 0 {
			return 0, fmt.Errorf("token 数无效")
		}
		return value, nil
	}
	prompt, err := parseTokens("input_tokens", "inputTokens")
	if err != nil {
		return aiCodeWithPricingItem{}, err
	}
	completion, err := parseTokens("output_tokens", "outputTokens")
	if err != nil {
		return aiCodeWithPricingItem{}, err
	}
	cacheRead, err := parseTokens("cache_read_tokens", "cacheReadTokens")
	if err != nil || prompt > math.MaxInt64-cacheRead {
		return aiCodeWithPricingItem{}, fmt.Errorf("AICodeWith 缓存 token 无效")
	}
	cacheWrite, err := parseTokens("cache_creation_tokens", "cache_write_tokens", "cacheWriteTokens")
	if err != nil || prompt+cacheRead > math.MaxInt64-cacheWrite {
		return aiCodeWithPricingItem{}, fmt.Errorf("AICodeWith 缓存 token 无效")
	}
	prompt += cacheRead + cacheWrite
	return aiCodeWithPricingItem{
		CreatedAt:   createdAt,
		ModelName:   boundedPricingDimension(rawStringField(fields, "model_name", "modelName", "model", "service_name", "serviceName")),
		SourceGroup: boundedPricingDimension(rawStringField(fields, "channel_name", "channelName", "group_name", "groupName")),
		Discount:    discount, PromptTokens: prompt, CompletionTokens: completion, CostMicros: costMicros,
	}, nil
}

func decodeAICodeWithPricingDetails(body []byte) ([]aiCodeWithPricingItem, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil || root == nil {
		return nil, fmt.Errorf("AICodeWith 使用明细响应无效")
	}
	container := root
	if dataRaw, ok := root["data"]; ok {
		var dataMap map[string]json.RawMessage
		if json.Unmarshal(dataRaw, &dataMap) == nil && dataMap != nil {
			container = dataMap
		} else {
			var direct []json.RawMessage
			if json.Unmarshal(dataRaw, &direct) == nil {
				return decodeAICodeWithPricingItems(direct)
			}
		}
	}
	var rawItems []json.RawMessage
	for _, name := range []string{"details", "records", "items", "usage_records"} {
		if raw, ok := container[name]; ok && json.Unmarshal(raw, &rawItems) == nil {
			break
		}
	}
	if rawItems == nil {
		return nil, fmt.Errorf("AICodeWith 使用明细缺少 details/records/items")
	}
	for _, name := range []string{"total", "total_count", "totalCount"} {
		if raw, ok := container[name]; ok {
			total, err := rawJSONInt64Exact(raw)
			if err != nil || total < int64(len(rawItems)) {
				return nil, fmt.Errorf("AICodeWith 使用明细 total 无效")
			}
			if total > int64(len(rawItems)) {
				return nil, fmt.Errorf("AICodeWith 使用明细需要分页，但上游未提供已验证的分页契约")
			}
			break
		}
	}
	if len(rawItems) >= aiCodeWithPricingDetailsLimit {
		return nil, fmt.Errorf("AICodeWith 使用明细达到单次上限，为防止漏账拒绝发布")
	}
	return decodeAICodeWithPricingItems(rawItems)
}

func decodeAICodeWithPricingItems(rawItems []json.RawMessage) ([]aiCodeWithPricingItem, error) {
	items := make([]aiCodeWithPricingItem, 0, len(rawItems))
	for _, rawItem := range rawItems {
		item, err := decodeAICodeWithPricingItem(rawItem)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func fetchAICodeWithPricingDay(ctx context.Context, client *http.Client, account ChannelUpstreamAccount, apiKey string, dayTs int64, pacer *upstreamUsageRequestPacer) ([]aiCodeWithPricingItem, error) {
	if err := pacer.beforeRequest(ctx); err != nil {
		return nil, err
	}
	day := time.Unix(dayTs, 0).In(cstLocation).Format("2006-01-02")
	query := url.Values{}
	query.Set("start", day)
	query.Set("end", day)
	query.Set("limit", strconv.Itoa(aiCodeWithPricingDetailsLimit))
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(account.BaseURL, "/api/v1/usage/details")+"?"+query.Encode(), map[string]string{"Authorization": "Bearer " + apiKey}, nil)
	if err != nil {
		var statusErr *upstreamHTTPError
		if errors.As(err, &statusErr) && (statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden) {
			return nil, &upstreamAuthError{err: err}
		}
		return nil, err
	}
	items, err := decodeAICodeWithPricingDetails(body)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.CreatedAt < dayTs || item.CreatedAt >= dayTs+86400 {
			return nil, fmt.Errorf("AICodeWith 使用明细返回了窗口外条目")
		}
	}
	return items, nil
}

func buildAICodeWithPricingEvidence(account ChannelUpstreamAccount, items []aiCodeWithPricingItem, now int64) ([]ChannelUpstreamPricingHourEvidence, error) {
	epoch := newAPIUpstreamAccountEpoch(account)
	byDimension := make(map[string]*ChannelUpstreamPricingHourEvidence)
	for _, item := range items {
		hourTs := item.CreatedAt - item.CreatedAt%3600
		attributes := newAPIPricingAttributes{
			GroupName: item.SourceGroup, ModelName: item.ModelName,
			GroupRatioState: pricingRatioMissing, UserGroupRatioState: pricingRatioMissing,
			EffectiveRatioSource: "unknown", DiscountRatio: item.Discount.Text,
			DiscountCanonical: item.Discount.Canonical, DiscountRatioState: item.Discount.State,
			EvidenceCapability: "discount_only", OtherValid: true,
		}
		dimensionHash := pricingDimensionHash(attributes)
		mapKey := strconv.FormatInt(hourTs, 10) + ":" + dimensionHash
		evidence := byDimension[mapKey]
		if evidence == nil {
			evidence = &ChannelUpstreamPricingHourEvidence{
				Domain: account.Domain, AccountEpoch: epoch, HourTs: hourTs,
				SemanticsVersion: upstreamPricingSemanticsVersion, DimensionHash: dimensionHash,
				Provider: upstreamProviderAICodeWith, SourceGroup: item.SourceGroup, ModelName: item.ModelName,
				GroupRatioState: pricingRatioMissing, UserGroupRatioState: pricingRatioMissing,
				EffectiveRatioSource: "unknown", DiscountRatio: item.Discount.Text,
				DiscountRatioState: item.Discount.State, EvidenceCapability: "discount_only", OtherValid: true,
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
		if item.CostMicros <= 0 {
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
		if item.CostMicros > 0 {
			if err := addPricingCounter(&evidence.FinalQuota, item.CostMicros); err != nil {
				return nil, err
			}
		}
	}
	return pricingEvidenceMapToSlice(byDimension), nil
}

func (m *Monitor) loadAICodeWithPricingCheckpoint(ctx context.Context, account ChannelUpstreamAccount, version string, dayTs int64) (AICodeWithPricingCheckpoint, map[string]*ChannelUpstreamPricingHourEvidence, error) {
	epoch := newAPIUpstreamAccountEpoch(account)
	var checkpoint AICodeWithPricingCheckpoint
	err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND day_ts = ?", account.Domain, epoch, upstreamPricingSemanticsVersion, dayTs).First(&checkpoint).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return AICodeWithPricingCheckpoint{Domain: account.Domain, AccountEpoch: epoch, SemanticsVersion: upstreamPricingSemanticsVersion, DayTs: dayTs, CredentialSetVersion: version}, make(map[string]*ChannelUpstreamPricingHourEvidence), nil
	}
	if err != nil {
		return checkpoint, nil, err
	}
	if checkpoint.CredentialSetVersion != version || checkpoint.NextCredential < 0 || checkpoint.TotalCredentials < 0 || len(checkpoint.AggregatesJSON) > upstreamPricingMaxCheckpointBytes {
		if deleteErr := m.storeDB.WithContext(ctx).Delete(&checkpoint).Error; deleteErr != nil {
			return checkpoint, nil, deleteErr
		}
		return AICodeWithPricingCheckpoint{Domain: account.Domain, AccountEpoch: epoch, SemanticsVersion: upstreamPricingSemanticsVersion, DayTs: dayTs, CredentialSetVersion: version}, make(map[string]*ChannelUpstreamPricingHourEvidence), nil
	}
	var rows []ChannelUpstreamPricingHourEvidence
	if checkpoint.AggregatesJSON != "" {
		if err := json.Unmarshal([]byte(checkpoint.AggregatesJSON), &rows); err != nil {
			return checkpoint, nil, fmt.Errorf("AICodeWith 计价断点损坏: %w", err)
		}
	}
	evidence := make(map[string]*ChannelUpstreamPricingHourEvidence, len(rows))
	for _, row := range rows {
		if row.Provider != upstreamProviderAICodeWith || row.Domain != account.Domain || row.AccountEpoch != epoch || row.HourTs < dayTs || row.HourTs >= dayTs+86400 {
			return checkpoint, nil, fmt.Errorf("AICodeWith 计价断点边界无效")
		}
		copyRow := row
		evidence[pricingEvidenceMapKey(row)] = &copyRow
	}
	return checkpoint, evidence, nil
}

func (m *Monitor) saveAICodeWithPricingCheckpoint(ctx context.Context, checkpoint *AICodeWithPricingCheckpoint, evidence map[string]*ChannelUpstreamPricingHourEvidence) error {
	if len(evidence) > upstreamPricingMaxCheckpointDimensions {
		return fmt.Errorf("AICodeWith 计价断点维度超过安全上限")
	}
	encoded, err := json.Marshal(pricingEvidenceMapToSlice(evidence))
	if err != nil {
		return err
	}
	if len(encoded) > upstreamPricingMaxCheckpointBytes {
		return fmt.Errorf("AICodeWith 计价断点超过安全大小")
	}
	checkpoint.AggregatesJSON = string(encoded)
	checkpoint.UpdatedAt = time.Now().Unix()
	return m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(checkpoint).Error
}

func (m *Monitor) fetchAICodeWithPricingDay(ctx context.Context, account ChannelUpstreamAccount, credential aiCodeWithCredential, dayTs int64, now int64) (map[int64][]ChannelUpstreamPricingHourEvidence, string, bool, error) {
	normalized, err := normalizeAICodeWithCredential(credential)
	if err != nil {
		return nil, "", false, err
	}
	version, err := aiCodeWithCredentialSetVersion(normalized)
	if err != nil {
		return nil, "", false, err
	}
	checkpoint, evidenceMap, err := m.loadAICodeWithPricingCheckpoint(ctx, account, version, dayTs)
	if err != nil {
		return nil, "", false, err
	}
	checkpoint.TotalCredentials = len(normalized.Slots)
	budget := aiCodeWithKeysPerTurn
	if budget < 1 {
		budget = 1
	}
	processed := 0
	for checkpoint.NextCredential < len(normalized.Slots) && processed < budget {
		slot := normalized.Slots[checkpoint.NextCredential]
		items, fetchErr := fetchAICodeWithPricingDay(ctx, m.channelUpstreamHTTPClient(), account, slot.Secret, dayTs, newUpstreamUsageRequestPacer(1, 0))
		if fetchErr != nil {
			return nil, "", false, fetchErr
		}
		rows, buildErr := buildAICodeWithPricingEvidence(account, items, now)
		if buildErr != nil {
			return nil, "", false, buildErr
		}
		if mergeErr := mergePricingEvidenceRows(evidenceMap, rows); mergeErr != nil {
			return nil, "", false, mergeErr
		}
		checkpoint.SourceRows += int64(len(items))
		checkpoint.NextCredential++
		processed++
		if saveErr := m.saveAICodeWithPricingCheckpoint(ctx, &checkpoint, evidenceMap); saveErr != nil {
			return nil, "", false, saveErr
		}
	}
	if checkpoint.NextCredential < len(normalized.Slots) {
		return nil, fmt.Sprintf("%s 已安全处理 %d/%d 把 Key", time.Unix(dayTs, 0).In(cstLocation).Format("2006-01-02"), checkpoint.NextCredential, len(normalized.Slots)), false, nil
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

func (m *Monitor) syncStoredAICodeWithPricing(ctx context.Context, domain string) (ChannelUpstreamPricingSyncState, error) {
	m.upstreamPricingMu.Lock()
	defer m.upstreamPricingMu.Unlock()
	now := time.Now().Unix()
	var account ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&account, "domain = ?", domain).Error; err != nil {
		return ChannelUpstreamPricingSyncState{}, err
	}
	if !account.Enabled || !account.UsageSyncEnabled || account.Provider != upstreamProviderAICodeWith {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("该账户未启用 AICodeWith 使用日志同步")
	}
	if !m.cfg.UpstreamPricingLedgerEnabled || !pricingLedgerDomainAllowed(m.cfg.UpstreamPricingLedgerDomains, account.Domain) {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("该域名未进入计价账本灰度名单")
	}
	credentialAny, err := m.credentialForAccount(account)
	if err != nil {
		return ChannelUpstreamPricingSyncState{}, err
	}
	credential, ok := credentialAny.(aiCodeWithCredential)
	if !ok {
		return ChannelUpstreamPricingSyncState{}, fmt.Errorf("AICodeWith 凭据格式无效")
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
	state.TailNextSyncAt = today + 86400 + 600
	state.LastAttemptAt = now
	previousFailures := state.ConsecutiveFailures
	if !state.BackfillDone && (state.BackfillNextSyncAt == 0 || state.BackfillNextSyncAt <= now) {
		var byHour map[int64][]ChannelUpstreamPricingHourEvidence
		var progress string
		var complete bool
		byHour, progress, complete, err = m.fetchAICodeWithPricingDay(ctx, account, credential, state.BackfillNextHour, now)
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
						state.Status, state.LastError = "pending_verification", "已完整读取整日折扣明细，等待下一轮一致性复核"
					}
				}
			}
			// The next verification pass must contact every key again; never
			// verify a day by replaying the local staging checkpoint.
			if deleteErr := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND day_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), upstreamPricingSemanticsVersion, state.BackfillNextHour).Delete(&AICodeWithPricingCheckpoint{}).Error; deleteErr != nil && err == nil {
				err = deleteErr
			}
			if err == nil {
				state.LastSuccessAt, state.Progress = now, ""
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
