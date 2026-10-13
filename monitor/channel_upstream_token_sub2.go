package monitor

// Sub2 token evidence is a shadow of the account ledger. These pure functions
// do not fetch, save, activate bindings, or publish money. The independent
// checkpoint must never be resumed from a legacy pricing checkpoint, which
// deliberately has no token identity. Complete scan != independently verified.

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
)

const (
	sub2TokenCheckpointFormat = "sub2-token-shadow-v1"
	sub2TokenSourceKind       = "sub2_api_key_id"
	sub2TokenChargeUnit       = "sub2_actual_cost_micros"
	sub2TokenMaxScanRows      = 20000
)

func validateSub2TokenContext(account ChannelUpstreamAccount, day int64, key []byte, keyID string) error {
	if account.Provider != upstreamProviderSub2API || strings.TrimSpace(account.Domain) == "" ||
		strings.TrimSpace(account.BaseURL) == "" || (account.UserID <= 0 && strings.TrimSpace(account.Account) == "") ||
		day < 0 || day > math.MaxInt64-86400 || day != cstDayStart(day) || len(key) < 32 ||
		strings.TrimSpace(keyID) != keyID || keyID == "" || len(keyID) > 64 {
		return errors.New("Sub2 令牌证据账户或扫描上下文无效")
	}
	return nil
}

// Amounts use actual_cost's existing six-decimal ledger unit. Neither the
// upstream group multiplier nor our recharge correction is applied here.
// A missing identity blocks only this shadow, never the legacy account total.
func buildSub2TokenEvidence(account ChannelUpstreamAccount, items []sub2PricingUsageItem, day, now int64, key []byte, keyID string) ([]ChannelUpstreamCostHourEvidence, error) {
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return nil, err
	}
	if len(items) > sub2TokenMaxScanRows {
		return nil, errors.New("Sub2 令牌证据超出单次安全容量")
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	seen := make(map[int64]bool)
	rows := make([]ChannelUpstreamCostHourEvidence, 0, len(items))
	for _, item := range items {
		id, err := canonicalUpstreamTokenID(item.UpstreamTokenID)
		if err != nil || id == "" || id != item.UpstreamTokenID || item.UpstreamTokenState != "valid" {
			return nil, errors.New("Sub2 日志缺少可靠令牌 ID，暂不拆分渠道成本")
		}
		if item.ID <= 0 || seen[item.ID] || item.CreatedAt < day || item.CreatedAt >= day+86400 ||
			!item.ActualCostKnown || !item.TokensExactKnown || item.ActualCostMicros < 0 || item.PromptTokens < 0 || item.CompletionTokens < 0 {
			return nil, errors.New("Sub2 令牌日志重复、越界或计费字段无效")
		}
		seen[item.ID] = true
		source, err := channelCostSourceRef(key, account.Provider, epoch, sub2TokenSourceKind, id)
		if err != nil {
			return nil, err
		}
		// Reuse the account ledger's pricing dimensions, including its treatment
		// of historical rate_multiplier vs the mutable nested group object.
		pricing, err := buildSub2PricingEvidence(account, []sub2PricingUsageItem{item}, now)
		if err != nil {
			return nil, err
		}
		p := pricing[0]
		row := ChannelUpstreamCostHourEvidence{
			Domain: account.Domain, AccountEpoch: epoch, HourTs: p.HourTs,
			SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: account.Provider,
			SourceRef: source, SourceRefKind: sub2TokenSourceKind, HMACKeyID: keyID,
			PricingDimensionHash: p.DimensionHash, SourceGroup: p.SourceGroup, UpstreamModel: p.ModelName,
			BillingMode: p.BillingMode, ChargeUnit: sub2TokenChargeUnit, ChargeUnitsPerUSD: "1000000",
			ChargeUnits: item.ActualCostMicros, Requests: 1, PromptTokens: item.PromptTokens, CompletionTokens: item.CompletionTokens,
			FirstSourceAt: item.CreatedAt, LastSourceAt: item.CreatedAt, FetchedAt: now,
		}
		row.DimensionHash = channelCostDimensionHash(source, p.DimensionHash, p.SourceGroup, p.ModelName, p.BillingMode, sub2TokenChargeUnit)
		rows = append(rows, row)
	}
	return mergeSub2TokenDayRows(rows)
}

func mergeSub2TokenDayRows(rows []ChannelUpstreamCostHourEvidence) ([]ChannelUpstreamCostHourEvidence, error) {
	byHour := make(map[int64]map[string]*ChannelUpstreamCostHourEvidence)
	var total int64
	for _, row := range rows {
		if err := addPricingCounter(&total, row.ChargeUnits); err != nil {
			return nil, err
		}
		if byHour[row.HourTs] == nil {
			byHour[row.HourTs] = make(map[string]*ChannelUpstreamCostHourEvidence)
		}
		if err := mergeChannelCostEvidenceRows(byHour[row.HourTs], []ChannelUpstreamCostHourEvidence{row}); err != nil {
			return nil, err
		}
	}
	out := make([]ChannelUpstreamCostHourEvidence, 0)
	for _, group := range byHour {
		out = append(out, channelCostEvidenceMapToSlice(group)...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].HourTs != out[j].HourTs {
			return out[i].HourTs < out[j].HourTs
		}
		return out[i].DimensionHash < out[j].DimensionHash
	})
	return out, nil
}

type sub2TokenCheckpoint struct {
	Format       string
	Domain       string
	AccountEpoch string
	Day          int64
	KeyID        string
	Total        int64
	PageSize     int
	Pages        int
	NextPage     int
	PageHashes   map[int]string
	// Anonymous upstream row identities detect overlapping pages. They are NOT
	// user request IDs and are never reused for NewAPI (whose log IDs can move).
	SeenRows map[string]bool
	Rows     []ChannelUpstreamCostHourEvidence
}

type sub2TokenCheckpointEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	MAC     string          `json:"mac"`
}

func sub2TokenCheckpointMAC(key, payload []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(sub2TokenCheckpointFormat + "\x00"))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func decodeSub2TokenCheckpoint(encoded []byte, account ChannelUpstreamAccount, day int64, key []byte, keyID string) (sub2TokenCheckpoint, error) {
	var cp sub2TokenCheckpoint
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return cp, err
	}
	if len(encoded) == 0 || len(encoded) > upstreamPricingMaxCheckpointBytes {
		return cp, errors.New("Sub2 令牌断点大小无效")
	}
	var envelope sub2TokenCheckpointEnvelope
	if json.Unmarshal(encoded, &envelope) != nil || !hmac.Equal([]byte(envelope.MAC), []byte(sub2TokenCheckpointMAC(key, envelope.Payload))) ||
		json.Unmarshal(envelope.Payload, &cp) != nil {
		return sub2TokenCheckpoint{}, errors.New("Sub2 令牌断点校验失败，禁止沿用旧计价断点")
	}
	if cp.Format != sub2TokenCheckpointFormat || cp.Domain != account.Domain || cp.AccountEpoch != newAPIUpstreamAccountEpoch(account) ||
		cp.Day != day || cp.KeyID != keyID || cp.Total < 0 || cp.Total > sub2TokenMaxScanRows || cp.PageSize <= 0 || cp.PageSize > upstreamUsagePageSize ||
		cp.NextPage < 2 || cp.NextPage > cp.Pages+1 || cp.Pages != sub2TokenPageCount(cp.Total, cp.PageSize) ||
		len(cp.PageHashes) != cp.NextPage-1 || len(cp.SeenRows) > sub2TokenMaxScanRows {
		return sub2TokenCheckpoint{}, errors.New("Sub2 令牌断点身份或分页状态不匹配")
	}
	if err := validateSub2TokenCheckpointRows(cp); err != nil {
		return sub2TokenCheckpoint{}, err
	}
	return cp, nil
}

// Check logical invariants as well as the MAC. This catches malformed state
// produced by a future writer bug, not only changed bytes on disk.
func validateSub2TokenCheckpointRows(cp sub2TokenCheckpoint) error {
	invalid := errors.New("Sub2 令牌断点计数或证据不一致")
	expected := int64(cp.NextPage-1) * int64(cp.PageSize)
	if expected > cp.Total {
		expected = cp.Total
	}
	if int64(len(cp.SeenRows)) != expected {
		return invalid
	}
	for i := 1; i < cp.NextPage; i++ {
		if !validSHA256Hex(cp.PageHashes[i]) {
			return invalid
		}
	}
	for id, seen := range cp.SeenRows {
		if !seen || !validSHA256Hex(id) {
			return invalid
		}
	}
	var requests, charge int64
	dimensions := make(map[string]bool)
	for _, row := range cp.Rows {
		dimension := strconv.FormatInt(row.HourTs, 10) + ":" + row.DimensionHash
		if dimensions[dimension] || row.Domain != cp.Domain || row.AccountEpoch != cp.AccountEpoch || row.Provider != upstreamProviderSub2API ||
			row.HourTs < cp.Day || row.HourTs >= cp.Day+86400 || row.HourTs%3600 != 0 ||
			row.SemanticsVersion != channelCostEvidenceSemanticsVersion || row.HMACKeyID != cp.KeyID || row.SourceRefKind != sub2TokenSourceKind ||
			!validSHA256Hex(row.SourceRef) || !validSHA256Hex(row.PricingDimensionHash) || row.Requests <= 0 || row.PromptTokens < 0 || row.CompletionTokens < 0 ||
			row.ChargeUnit != sub2TokenChargeUnit || row.ChargeUnitsPerUSD != "1000000" ||
			row.FirstSourceAt < row.HourTs || row.LastSourceAt >= row.HourTs+3600 || row.LastSourceAt < row.FirstSourceAt ||
			row.DimensionHash != channelCostDimensionHash(row.SourceRef, row.PricingDimensionHash, row.SourceGroup, row.UpstreamModel, row.BillingMode, row.ChargeUnit) ||
			row.ContentHash != channelCostEvidenceRowHash(row) {
			return invalid
		}
		dimensions[dimension] = true
		if addPricingCounter(&requests, row.Requests) != nil || addPricingCounter(&charge, row.ChargeUnits) != nil {
			return invalid
		}
	}
	if requests != expected {
		return invalid
	}
	return nil
}

func sub2TokenPageCount(total int64, size int) int {
	if total == 0 {
		return 1 // retain a validated empty first page
	}
	return int((total + int64(size) - 1) / int64(size))
}

// advanceSub2TokenCheckpoint returns a new encoded value, never mutates the
// caller's old bytes, and accepts exact page retries without adding money.
// The separate cursor store persists the whole envelope atomically with CAS.
// The collector uses this as a shadow only; completion is not verification.
func advanceSub2TokenCheckpoint(previous []byte, account ChannelUpstreamAccount, page sub2PricingPage, day, now int64, key []byte, keyID string) ([]byte, error) {
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return nil, err
	}
	if page.Total < 0 || page.Total > sub2TokenMaxScanRows || page.PageSize <= 0 || page.PageSize > upstreamUsagePageSize || page.Page < 1 {
		return nil, errors.New("Sub2 令牌分页超出安全范围")
	}
	pages := sub2TokenPageCount(page.Total, page.PageSize)
	if page.Page > pages || (page.Pages != pages && !(page.Total == 0 && page.Pages == 0)) {
		return nil, errors.New("Sub2 令牌分页总数不一致")
	}
	want := page.PageSize
	if page.Page == pages {
		want = int(page.Total) - (pages-1)*page.PageSize
	}
	if len(page.Items) != want {
		return nil, errors.New("Sub2 令牌分页条数不一致")
	}
	cp := sub2TokenCheckpoint{Format: sub2TokenCheckpointFormat, Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account),
		Day: day, KeyID: keyID, Total: page.Total, PageSize: page.PageSize, Pages: pages, NextPage: 1,
		PageHashes: make(map[int]string), SeenRows: make(map[string]bool)}
	if len(previous) != 0 {
		var err error
		cp, err = decodeSub2TokenCheckpoint(previous, account, day, key, keyID)
		if err != nil {
			return nil, err
		}
	}
	if cp.Total != page.Total || cp.PageSize != page.PageSize || cp.Pages != pages {
		return nil, errors.New("Sub2 令牌分页扫描期间发生变化")
	}
	// Unlike the legacy fingerprint, this covers token ID/state and all
	// consumed numeric fields. Only a digest, never the DTO, is persisted.
	payload, err := json.Marshal(page.Items)
	if err != nil {
		return nil, errors.New("Sub2 令牌页摘要生成失败")
	}
	digest := sha256.Sum256(payload)
	fingerprint := hex.EncodeToString(digest[:])
	if old, ok := cp.PageHashes[page.Page]; ok {
		if old != fingerprint {
			return nil, errors.New("Sub2 已采集页内容或令牌身份变化，需要重新扫描")
		}
		return bytes.Clone(previous), nil
	}
	if page.Page != cp.NextPage {
		return nil, errors.New("Sub2 令牌断点不允许跳页")
	}
	rows, err := buildSub2TokenEvidence(account, page.Items, day, now, key, keyID)
	if err != nil {
		return nil, err
	}
	for _, item := range page.Items {
		ref, err := channelCostSourceRef(key, account.Provider, cp.AccountEpoch, "sub2_usage_row_id", strconv.FormatInt(item.ID, 10))
		if err != nil {
			return nil, err
		}
		if cp.SeenRows[ref] {
			return nil, errors.New("Sub2 不同分页出现重复日志，暂不发布令牌成本")
		}
		cp.SeenRows[ref] = true
	}
	cp.Rows, err = mergeSub2TokenDayRows(append(cp.Rows, rows...))
	if err != nil {
		return nil, err
	}
	cp.PageHashes[page.Page] = fingerprint
	cp.NextPage++
	if err := validateSub2TokenCheckpointRows(cp); err != nil {
		return nil, err
	}
	payload, err = json.Marshal(cp)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(sub2TokenCheckpointEnvelope{Payload: payload, MAC: sub2TokenCheckpointMAC(key, payload)})
	if err != nil || len(encoded) > upstreamPricingMaxCheckpointBytes {
		return nil, errors.New("Sub2 令牌断点超过安全容量，旧账户账本不受影响")
	}
	return encoded, nil
}
