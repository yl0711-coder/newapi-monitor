package monthlysource

import (
	"encoding/json"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// This adapter describes the reviewed RC4/RC26 producers, not every NewAPI
// fork. The caller must establish the source's historical unit before using it.
const FundingProducerRC4RC26 = "newapi-rc4-4f3272fe-rc26-8f6961c6"

type FundingRules struct {
	Producer     string
	QuotaPerUnit int64
}

type FundingLog struct {
	ID        int64  `json:"id"`
	UserID    int64  `json:"user_id"`
	CreatedAt int64  `json:"created_at"`
	Type      int    `json:"type"`
	Content   string `json:"content"`
	Other     string `json:"other"`
}

const (
	FundingMovement  = "wallet_movement"
	FundingNonWallet = "non_wallet_event"
	FundingSeparate  = "separate_ledger"
	FundingReview    = "needs_review"
)

// A parsed LOG is not proof that the corresponding wallet write succeeded,
// nor proof of complete coverage. No publication/complete flag is provided.
// DeltaQuota is nil for unknown or non-wallet events, except a source-proven
// unchanged override explicitly retains zero. It is never an invented zero.
// No raw text, admin metadata, names or credentials reach this result.
type FundingResult struct {
	LogID        int64  `json:"log_id"`
	At           int64  `json:"at"`
	SourceDigest string `json:"source_digest"`
	Status       string `json:"status"`
	Reason       string `json:"reason"`
	TargetUserID int64  `json:"target_user_id"`
	DeltaQuota   *int64 `json:"delta_quota"`
	Kind         string `json:"kind"`
	Method       string `json:"method"`
}

var (
	fundingDecimal  = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]{1,6})?$`)
	fundingInteger  = regexp.MustCompile(`^[+-]?[0-9]+$`)
	fundingPayment  = regexp.MustCompile(`^[0-9]+(?:\.[0-9]{1,6})?$`)
	fundingRedeem   = regexp.MustCompile(`^通过兑换码充值 (.+)，兑换码ID ([0-9]+)$`)
	fundingOverride = regexp.MustCompile(`^管理员覆盖用户额度从 (.+) 为 (.+)$`)
)

// NormalizeFundingLog is a bounded, pure classifier. Unknown monetary shapes
// stay reviewable; there is no fallback to quota=0, payment amount, today's FX,
// admin user, or inferred transfers. It does not query, persist or publish.
func NormalizeFundingLog(row FundingLog, rules FundingRules) FundingResult {
	r := FundingResult{LogID: row.ID, At: row.CreatedAt, Status: FundingReview, Reason: "invalid_source_row"}
	if row.ID <= 0 || row.UserID <= 0 || row.CreatedAt <= 0 || len(row.Content) > 4096 || len(row.Other) > 65536 || !utf8.ValidString(row.Content) || !utf8.ValidString(row.Other) {
		return r
	}
	raw, _ := json.Marshal(row)
	r.SourceDigest = digest(raw)
	if rules.Producer != FundingProducerRC4RC26 || rules.QuotaPerUnit != 500000 {
		r.Reason = "unverified_producer_or_unit"
		return r
	}
	envelope, ok := fundingObject(row.Other, true)
	if !ok {
		r.Reason = "invalid_or_ambiguous_metadata"
		return r
	}
	r.Reason = "unrecognized_funding_format"
	switch row.Type {
	case 1:
		return fundingTopup(row, r)
	case 3:
		return fundingManagement(row, envelope, r)
	case 4:
		return fundingSystem(row, r)
	case 2, 6:
		r.Status, r.Reason = FundingSeparate, "consumption_or_refund_not_funding"
	case 5, 7:
		r.Status, r.Reason = FundingNonWallet, "error_or_login_not_funding"
	}
	return r
}

func fundingAmount(raw string, formatted bool) (int64, bool) {
	value := strings.TrimSpace(raw)
	if strings.HasSuffix(value, " 点额度") {
		return fundingPoints(strings.TrimSuffix(value, " 点额度"))
	}
	if strings.HasSuffix(value, " 额度") {
		value = strings.TrimSuffix(value, " 额度")
	} else if !formatted {
		return 0, false
	}
	if strings.HasPrefix(value, "$") || strings.HasPrefix(value, "＄") {
		if strings.HasPrefix(value, "$") {
			value = strings.TrimPrefix(value, "$")
		} else {
			value = strings.TrimPrefix(value, "＄")
		}
		if !fundingDecimal.MatchString(value) {
			return 0, false
		}
		amount, ok := new(big.Rat).SetString(value)
		if !ok {
			return 0, false
		}
		amount.Mul(amount, big.NewRat(500000, 1))
		// These producers render dollar quotas through float64. Do not claim
		// exact recovery at magnitudes that can already lose integer points.
		if !amount.IsInt() || !amount.Num().IsInt64() || new(big.Int).Abs(amount.Num()).Cmp(big.NewInt(1<<50-1)) > 0 {
			return 0, false
		}
		return amount.Num().Int64(), true
	}
	// FormatQuota in TOKENS mode writes a bare integer. LogQuota always has
	// a unit suffix. CNY/custom currencies require historical rate evidence.
	if formatted && value == strings.TrimSpace(raw) {
		return fundingPoints(value)
	}
	return 0, false
}

func fundingPoints(raw string) (int64, bool) {
	if !fundingInteger.MatchString(raw) {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	return v, err == nil
}

func fundingMove(r FundingResult, userID, delta int64, kind, method string) FundingResult {
	r.Status, r.Reason, r.TargetUserID = FundingMovement, "", userID
	r.DeltaQuota, r.Kind, r.Method = &delta, kind, method
	return r
}

func fundingPositive(r FundingResult, userID int64, raw string, formatted bool, kind, method string) FundingResult {
	amount, ok := fundingAmount(raw, formatted)
	if !ok || amount <= 0 {
		r.Reason = "invalid_or_unresolved_funding_amount"
		return r
	}
	return fundingMove(r, userID, amount, kind, method)
}

func fundingTopup(row FundingLog, r FundingResult) FundingResult {
	if match := fundingRedeem.FindStringSubmatch(row.Content); match != nil {
		id, err := strconv.ParseInt(match[2], 10, 64)
		if err != nil || id <= 0 {
			return r
		}
		return fundingPositive(r, row.UserID, match[1], false, "recharge", "兑换码充值")
	}
	for _, format := range []struct {
		prefix, separator, method string
		points                    bool
	}{
		{"使用在线充值成功，充值金额: ", "，支付金额：", "在线充值", false},
		{"管理员补单成功，充值金额: ", "，支付金额：", "后台充值", false},
		{"使用Creem充值成功，充值额度: ", "，支付金额：", "在线充值", true},
		{"Waffo充值成功，充值额度: ", "，支付金额: ", "在线充值", false},
		{"Waffo Pancake充值成功，充值额度: ", "，支付金额: ", "在线充值", false},
	} {
		if !strings.HasPrefix(row.Content, format.prefix) {
			continue
		}
		amount, payment, found := strings.Cut(strings.TrimPrefix(row.Content, format.prefix), format.separator)
		if !found || !fundingPayment.MatchString(payment) {
			return r
		}
		if format.points {
			// Creem records the raw integer quota, unlike the other providers.
			value, ok := fundingPoints(amount)
			if !ok || value <= 0 {
				return r
			}
			return fundingMove(r, row.UserID, value, "recharge", format.method)
		}
		return fundingPositive(r, row.UserID, amount, true, "recharge", format.method)
	}
	if strings.HasPrefix(row.Content, "使用余额购买订阅成功，套餐: ") {
		r.Reason = "subscription_wallet_debit_requires_separate_evidence"
	} else if strings.HasPrefix(row.Content, "订阅购买成功，套餐: ") {
		r.Status, r.Reason = FundingSeparate, "external_subscription_not_wallet_topup"
	}
	return r
}

func fundingSystem(row FundingLog, r FundingResult) FundingResult {
	for _, gift := range []struct{ prefix, method string }{
		{"新用户注册赠送 ", "注册赠送"},
		{"使用邀请码赠送 ", "邀请赠送"},
		{"用户签到，获得额度 ", "签到赠送"},
	} {
		if strings.HasPrefix(row.Content, gift.prefix) {
			return fundingPositive(r, row.UserID, strings.TrimPrefix(row.Content, gift.prefix), false, "gift", gift.method)
		}
	}
	if strings.HasPrefix(row.Content, "邀请用户赠送 ") {
		// inviteUser increments aff_quota, NOT users.quota. Its later wallet
		// transfer has no money log in these revisions and needs other evidence.
		r.Status, r.Reason = FundingSeparate, "affiliate_reward_not_wallet"
		return r
	}
	switch row.Content {
	case "开始设置两步验证", "成功启用两步验证", "禁用两步验证", "重新生成两步验证备用码", "通用安全验证成功 (验证方式: 2FA)":
		r.Status, r.Reason = FundingNonWallet, "security_event_not_funding"
	}
	return r
}

func fundingManagement(row FundingLog, envelope map[string]json.RawMessage, r FundingResult) FundingResult {
	for key := range envelope {
		if strings.EqualFold(key, "op") && key != "op" {
			return r
		}
	}
	if op, exists := envelope["op"]; exists {
		return fundingStructuredManagement(row, envelope, op, r)
	}
	if len(envelope) == 0 && fundingLegacyBindingClear(row.Content) {
		r.Status, r.Reason = FundingNonWallet, "legacy_binding_clear_not_funding"
		return r
	}
	// RC4 owner is the beneficiary. Never take admin_info.admin_id as target.
	for _, direction := range []struct {
		prefix   string
		subtract bool
	}{
		{"管理员增加用户额度 ", false}, {"管理员减少用户额度 ", true},
	} {
		if !strings.HasPrefix(row.Content, direction.prefix) {
			continue
		}
		amount, ok := fundingAmount(strings.TrimPrefix(row.Content, direction.prefix), false)
		if !ok || amount <= 0 {
			return r
		}
		if direction.subtract {
			return fundingMove(r, row.UserID, -amount, "adjustment", "余额扣减")
		}
		return fundingMove(r, row.UserID, amount, "recharge", "后台充值")
	}
	if match := fundingOverride.FindStringSubmatch(row.Content); match != nil {
		return fundingOverrideMove(r, row.UserID, match[1], match[2])
	}
	return r
}

// RC4 controller/user.go AdminClearUserBinding logs this exact message after
// model/user.go ClearBinding updates one of these seven binding columns only.
// Keep the reviewed producer's complete shape; no broad "admin"/"binding"
// substring exemption and no fallback over structured/unknown money metadata.
func fundingLegacyBindingClear(content string) bool {
	const prefix = "admin cleared "
	if !strings.HasPrefix(content, prefix) {
		return false
	}
	binding, username, ok := strings.Cut(strings.TrimPrefix(content, prefix), " binding for user ")
	if !ok || username == "" || strings.TrimSpace(username) != username || utf8.RuneCountInString(username) > 20 || strings.ContainsAny(username, "\x00\r\n\t") {
		return false
	}
	switch binding {
	case "email", "github", "discord", "oidc", "wechat", "telegram", "linuxdo":
		return true
	}
	return false
}

func fundingStructuredManagement(row FundingLog, envelope map[string]json.RawMessage, raw json.RawMessage, r FundingResult) FundingResult {
	op, ok := fundingObject(string(raw), false)
	if !ok {
		return r
	}
	for key := range op {
		if key != "action" && key != "params" {
			return r
		}
	}
	action, ok := fundingString(op["action"])
	if !ok {
		return r
	}
	switch action {
	case "user.quota_add", "user.quota_subtract", "user.quota_override":
	case "user.update":
		return fundingProfileUpdate(row, envelope, op["params"], r)
	case "user.create":
		// Creation can initialize a wallet. Keep the amount unresolved, but a
		// reviewed explicit beneficiary must not block unrelated accounts.
		if target, ok := fundingCreatedUserTarget(op["params"]); ok {
			r.TargetUserID, r.Reason = target, "created_user_wallet_requires_evidence"
		}
		return r
	default:
		// Only source-reviewed, wallet-unrelated actions are exempted. Unknown
		// admin actions, including future quota operations, are not discarded.
		switch action {
		case "channel.key_view", "user.binding_clear", "user.2fa_disable", "user.passkey_register", "user.passkey_delete", "user.reset_passkey":
			r.Status, r.Reason = FundingNonWallet, "security_action_not_funding"
		}
		if fundingResourceAudit(row, envelope, action, op["params"]) {
			r.Status, r.Reason = FundingNonWallet, "reviewed_resource_operation_not_funding"
		}
		return r
	}
	params, ok := fundingObject(string(op["params"]), false)
	if !ok {
		return r
	}
	for key := range params {
		if key != "target_user_id" && !((action == "user.quota_override" && (key == "from" || key == "to")) || (action != "user.quota_override" && key == "quota")) {
			return r
		}
	}
	target, ok := fundingTarget(row.UserID, envelope, params)
	if !ok {
		r.Reason = "invalid_or_missing_target_user"
		return r
	}
	if action == "user.quota_override" {
		before, beforeOK := fundingString(params["from"])
		after, afterOK := fundingString(params["to"])
		if !beforeOK || !afterOK {
			return r
		}
		return fundingOverrideMove(r, target, before, after)
	}
	text, ok := fundingString(params["quota"])
	if !ok {
		return r
	}
	amount, ok := fundingAmount(text, false)
	if !ok || amount <= 0 {
		return r
	}
	if action == "user.quota_subtract" {
		return fundingMove(r, target, -amount, "adjustment", "余额扣减")
	}
	return fundingMove(r, target, amount, "recharge", "后台充值")
}

// Reviewed producer: rc26 controller.UpdateUser -> User.EditWithTx updates
// username/display_name/group/remark/password/auth, never quota/used_quota.
// Its audit params contain only id, username and an optional target_user_id.
// Do not generalize this to user.create or future fields/actions: those may
// have money effects that the current source contract does not establish.
func fundingProfileUpdate(row FundingLog, envelope map[string]json.RawMessage, raw json.RawMessage, r FundingResult) FundingResult {
	params, ok := fundingObject(string(raw), false)
	if !ok {
		return r
	}
	for key := range params {
		if key != "id" && key != "username" && key != "target_user_id" {
			return r
		}
	}
	id, idOK := fundingID(params["id"])
	name, nameOK := fundingString(params["username"])
	target, targetOK := fundingTarget(row.UserID, envelope, params)
	if !idOK || !nameOK || strings.TrimSpace(name) == "" || !targetOK || target != id {
		return r
	}
	r.Status, r.Reason, r.TargetUserID = FundingNonWallet, "profile_update_not_funding", target
	return r
}

func fundingTarget(owner int64, envelope, params map[string]json.RawMessage) (int64, bool) {
	if raw, present := params["target_user_id"]; present {
		return fundingID(raw)
	}
	// RC26 recordManageAuditFor deliberately omits target when target==actor.
	// Accept that exact self-operation only with matching operator metadata.
	// Present-but-invalid/null target never falls back to the actor.
	admin, ok := fundingObject(string(envelope["admin_info"]), false)
	if !ok {
		return 0, false
	}
	id, ok := fundingID(admin["admin_id"])
	return owner, ok && id == owner
}

func fundingID(raw json.RawMessage) (int64, bool) {
	value := string(raw)
	if len(value) > 0 && value[0] == '"' {
		var ok bool
		value, ok = fundingString(raw)
		if !ok {
			return 0, false
		}
	}
	id, err := strconv.ParseInt(value, 10, 64)
	return id, err == nil && id > 0 && strconv.FormatInt(id, 10) == value
}

func fundingString(raw json.RawMessage) (string, bool) {
	var result string
	err := json.Unmarshal(raw, &result)
	return result, err == nil && string(raw) != "null"
}

func fundingOverrideMove(r FundingResult, target int64, from, to string) FundingResult {
	before, beforeOK := fundingAmount(from, false)
	after, afterOK := fundingAmount(to, false)
	if !beforeOK || !afterOK {
		return r
	}
	difference := new(big.Int).Sub(big.NewInt(after), big.NewInt(before))
	if !difference.IsInt64() {
		r.Reason = "funding_delta_overflow"
		return r
	}
	delta := difference.Int64()
	if delta < 0 {
		return fundingMove(r, target, delta, "adjustment", "余额扣减")
	}
	if delta == 0 {
		r = fundingMove(r, target, delta, "", "")
		r.Status, r.Reason = FundingNonWallet, "unchanged_quota_override"
		return r
	}
	return fundingMove(r, target, delta, "recharge", "后台充值")
}
