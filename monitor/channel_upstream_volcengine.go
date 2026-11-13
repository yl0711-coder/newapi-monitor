package monitor

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Volcengine (火山引擎/方舟) is a cloud vendor, not a relay panel. Balance and
// cost come from the billing OpenAPI (AK/SK, HMAC-SHA256 signature). The bill
// detail API only exposes daily amounts per product/config, so usage is stored
// as China calendar-day buckets, never split into synthetic hours.
const (
	upstreamProviderVolcengine     = "volcengine"
	upstreamUsageAdapterVolcengine = "volcengine_bill_day"
	volcengineBillingService       = "billing"
	volcengineBillingRegion        = "cn-beijing"
	volcengineBillingVersion       = "2022-01-01"
	volcengineBillPageSize         = 100
	volcengineBillMaxPages         = 20
	// Bills for recent days are still being rated; re-read today and the two
	// previous days and keep a day provisional until 24 hours after it ends.
	volcengineTailDays        = 2
	volcengineSettleSeconds   = 86400
	volcengineMaxWindowDays   = volcengineTailDays + 1
	volcengineCurrencyCNY     = "CNY"
	volcengineAccessKeyMaxLen = 256
)

type volcengineCredential struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

// Variables only so tests can point at a local server and pin the clock.
var (
	volcengineBillingEndpoint = "https://open.volcengineapi.com/"
	volcengineNow             = func() time.Time { return time.Now().UTC() }
)

func volcengineHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func volcengineHMAC(key []byte, content string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(content))
	return mac.Sum(nil)
}

// signVolcengineRequest implements the documented Volcengine signature (the
// same algorithm as the official SDK's Sign4): signed headers content-type,
// host, x-content-sha256 and x-date; key = HMAC chain over date/region/service
// and the literal "request".
func signVolcengineRequest(req *http.Request, body []byte, cred volcengineCredential, region, service string, now time.Time) {
	xDate := now.UTC().Format("20060102T150405Z")
	shortDate := xDate[:8]
	payloadHash := volcengineHash(body)
	req.Header.Set("X-Date", xDate)
	req.Header.Set("X-Content-Sha256", payloadHash)
	host := req.URL.Host
	if h, port, found := strings.Cut(host, ":"); found && (port == "80" || port == "443") {
		host = h
	}
	req.Host = host
	headers := map[string]string{
		"content-type":     strings.TrimSpace(req.Header.Get("Content-Type")),
		"host":             host,
		"x-content-sha256": payloadHash,
		"x-date":           xDate,
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name + ":" + headers[name] + "\n")
	}
	signedHeaders := strings.Join(names, ";")
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonicalRequest := strings.Join([]string{req.Method, path, volcengineCanonicalQuery(req.URL.Query()), canonicalHeaders.String(), signedHeaders, payloadHash}, "\n")
	scope := strings.Join([]string{shortDate, region, service, "request"}, "/")
	stringToSign := strings.Join([]string{"HMAC-SHA256", xDate, scope, volcengineHash([]byte(canonicalRequest))}, "\n")
	key := volcengineHMAC([]byte(cred.SecretAccessKey), shortDate)
	key = volcengineHMAC(key, region)
	key = volcengineHMAC(key, service)
	key = volcengineHMAC(key, "request")
	signature := hex.EncodeToString(volcengineHMAC(key, stringToSign))
	req.Header.Set("Authorization", "HMAC-SHA256 Credential="+cred.AccessKeyID+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

// volcengineCanonicalQuery matches the SDK: keys sorted, values sorted, RFC 3986
// escaping with spaces as %20.
func volcengineCanonicalQuery(values url.Values) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		vals := append([]string(nil), values[key]...)
		sort.Strings(vals)
		for _, value := range vals {
			parts = append(parts, volcengineEscape(key)+"="+volcengineEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func volcengineEscape(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

type volcengineResponseMetadata struct {
	RequestID string `json:"RequestId"`
	Error     *struct {
		Code    string `json:"Code"`
		Message string `json:"Message"`
	} `json:"Error"`
}

// volcengineAuthCodes are documented signature/credential failures. They are
// reported as reconnect-required, never retried as transient errors.
var volcengineAuthCodes = map[string]bool{
	"InvalidAccessKey": true, "InvalidCredential": true, "SignatureDoesNotMatch": true,
	"InvalidAuthorization": true, "AccessDenied": true, "Forbidden": true,
	"InvalidSecretToken": true, "MissingAuthenticationToken": true,
}

func volcengineCall(ctx context.Context, client *http.Client, cred volcengineCredential, action string, body any, target any) error {
	if strings.TrimSpace(cred.AccessKeyID) == "" || strings.TrimSpace(cred.SecretAccessKey) == "" {
		return &upstreamAuthError{err: fmt.Errorf("火山引擎 AK/SK 为空，请重新填写")}
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := volcengineBillingEndpoint + "?" + url.Values{"Action": {action}, "Version": {volcengineBillingVersion}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "NexusAPI-Monitor/1.0")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	signVolcengineRequest(req, encoded, cred, volcengineBillingRegion, volcengineBillingService, volcengineNow())
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("连接上游失败: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponseBody+1))
	if err != nil {
		return fmt.Errorf("读取上游响应失败: %w", err)
	}
	if len(data) > maxUpstreamResponseBody {
		return fmt.Errorf("上游响应超过安全上限")
	}
	var envelope struct {
		ResponseMetadata volcengineResponseMetadata `json:"ResponseMetadata"`
		Result           json.RawMessage            `json:"Result"`
	}
	decodeErr := json.Unmarshal(data, &envelope)
	if apiErr := envelope.ResponseMetadata.Error; decodeErr == nil && apiErr != nil && apiErr.Code != "" {
		// Only the documented code is retained: messages may echo request data.
		err := fmt.Errorf("火山引擎 %s 返回错误 %s", action, sanitizeVolcengineCode(apiErr.Code))
		// Documented credential/permission codes need the user; a bare 403
		// does not, and goes through the shared 403 policy.
		if volcengineAuthCodes[apiErr.Code] || resp.StatusCode == http.StatusUnauthorized {
			return &upstreamAuthError{err: err}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			retryAt := int64(0)
			if upstreamResponseHasRetryAfter(resp.StatusCode, resp.Header.Get("Retry-After")) {
				retryAt = parseUpstreamRetryAfter(resp.Header.Get("Retry-After"), time.Now()).Unix()
			}
			return &upstreamHTTPError{Status: resp.StatusCode, Message: err.Error(), RetryAt: retryAt}
		}
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		safeErr := &upstreamHTTPError{Status: resp.StatusCode}
		if upstreamResponseHasRetryAfter(resp.StatusCode, resp.Header.Get("Retry-After")) {
			safeErr.RetryAt = parseUpstreamRetryAfter(resp.Header.Get("Retry-After"), time.Now()).Unix()
		}
		if resp.StatusCode == http.StatusUnauthorized {
			return &upstreamAuthError{err: safeErr}
		}
		return safeErr
	}
	if decodeErr != nil || len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return fmt.Errorf("火山引擎 %s 响应结构无效", action)
	}
	if json.Unmarshal(envelope.Result, target) != nil {
		return fmt.Errorf("火山引擎 %s 响应字段无法解析", action)
	}
	return nil
}

func sanitizeVolcengineCode(code string) string {
	if len(code) > 64 {
		code = code[:64]
	}
	for _, r := range code {
		if !(r == '.' || r == '_' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return "未知错误码"
		}
	}
	return code
}

func volcengineAmount(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("金额为空")
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("金额无效")
	}
	return value, nil
}

// readVolcengineBalance reads the account's available balance (CNY face value,
// unit 1, the same convention as TokenForce's CNY ledger).
func readVolcengineBalance(ctx context.Context, client *http.Client, cred volcengineCredential) (upstreamBalanceResult, int64, error) {
	var result struct {
		AccountID        *int64 `json:"AccountID"`
		AvailableBalance string `json:"AvailableBalance"`
		Currency         string `json:"Currency"`
	}
	if err := volcengineCall(ctx, client, cred, "QueryBalanceAcct", struct{}{}, &result); err != nil {
		return upstreamBalanceResult{}, 0, err
	}
	if result.Currency != "" && result.Currency != volcengineCurrencyCNY {
		return upstreamBalanceResult{}, 0, fmt.Errorf("火山引擎账户币种不是人民币，未写入余额")
	}
	amount, err := volcengineAmount(result.AvailableBalance)
	if err != nil {
		return upstreamBalanceResult{}, 0, fmt.Errorf("火山引擎可用余额%s", err.Error())
	}
	accountID := int64(0)
	if result.AccountID != nil {
		accountID = *result.AccountID
	}
	// Available balance may be negative in arrears; keep the real number.
	return upstreamBalanceResult{BalanceUSD: amount, BalanceRaw: amount, BalanceUnit: tokenForceLedgerUnit}, accountID, nil
}

type volcengineBillItem struct {
	BillDetailID  string `json:"BillDetailId"`
	ExpenseDate   string `json:"ExpenseDate"`
	Product       string `json:"Product"`
	PayableAmount string `json:"PayableAmount"`
	Currency      string `json:"Currency"`
	Count         string `json:"Count"`
	Unit          string `json:"Unit"`
}

type volcengineBillPage struct {
	List   []volcengineBillItem `json:"List"`
	Total  *int                 `json:"Total"`
	Offset *int                 `json:"Offset"` // optional echo; checked only when present
	Limit  int                  `json:"Limit"`
}

func fetchVolcengineBillPage(ctx context.Context, client *http.Client, cred volcengineCredential, day time.Time, offset int, pacer *upstreamUsageRequestPacer) (volcengineBillPage, error) {
	page := volcengineBillPage{}
	if err := pacer.beforeRequest(ctx); err != nil {
		return page, err
	}
	body := map[string]any{
		"BillPeriod":    day.Format("2006-01"),
		"ExpenseDate":   day.Format("2006-01-02"),
		"GroupPeriod":   1, // Aggregate by day.
		"Limit":         volcengineBillPageSize,
		"Offset":        offset,
		"NeedRecordNum": 1,
	}
	if err := volcengineCall(ctx, client, cred, "ListBillDetail", body, &page); err != nil {
		return page, err
	}
	if page.Total == nil || *page.Total < 0 || page.List == nil || (page.Offset != nil && *page.Offset != offset) || len(page.List) > volcengineBillPageSize {
		return page, fmt.Errorf("火山引擎账单分页字段缺失或不匹配，未发布部分账单")
	}
	return page, nil
}

// volcengineTokenMultiplier recognizes token-measured usage only; other units
// (instances, hours, storage) contribute cost but never fabricated tokens.
func volcengineTokenMultiplier(unit string) int64 {
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case "token", "tokens", "个token", "个tokens":
		return 1
	case "千token", "千tokens", "千个token", "千个tokens", "ktoken", "ktokens", "k tokens":
		return 1000
	case "百万token", "百万tokens", "mtoken", "mtokens", "m tokens":
		return 1000000
	}
	return 0
}

func fetchVolcengineBillDay(ctx context.Context, client *http.Client, cred volcengineCredential, day time.Time, pacer *upstreamUsageRequestPacer) (cost float64, tokens int64, err error) {
	first, err := fetchVolcengineBillPage(ctx, client, cred, day, 0, pacer)
	if err != nil {
		return 0, 0, err
	}
	if *first.Total > volcengineBillPageSize*volcengineBillMaxPages {
		return 0, 0, fmt.Errorf("火山引擎 %s 账单行数超出单轮安全上限", day.Format("2006-01-02"))
	}
	items := append([]volcengineBillItem(nil), first.List...)
	for offset := volcengineBillPageSize; len(items) < *first.Total; offset += volcengineBillPageSize {
		if len(items) != offset {
			return 0, 0, fmt.Errorf("火山引擎账单分页提前结束，未发布部分账单")
		}
		next, fetchErr := fetchVolcengineBillPage(ctx, client, cred, day, offset, pacer)
		if fetchErr != nil {
			return 0, 0, fetchErr
		}
		if *next.Total != *first.Total {
			return 0, 0, fmt.Errorf("火山引擎账单分页期间行数变化，稍后重试")
		}
		if len(next.List) == 0 {
			return 0, 0, fmt.Errorf("火山引擎账单分页提前结束，未发布部分账单")
		}
		items = append(items, next.List...)
	}
	if len(items) != *first.Total {
		return 0, 0, fmt.Errorf("火山引擎实际账单行数与 Total 不符")
	}
	wantDate := day.Format("2006-01-02")
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		if item.ExpenseDate != "" && !strings.HasPrefix(item.ExpenseDate, wantDate) {
			return 0, 0, fmt.Errorf("火山引擎账单行日期与查询日期不一致")
		}
		if item.Currency != "" && item.Currency != volcengineCurrencyCNY {
			return 0, 0, fmt.Errorf("火山引擎账单币种不是人民币，未发布账单")
		}
		if item.BillDetailID != "" {
			if seen[item.BillDetailID] {
				return 0, 0, fmt.Errorf("火山引擎账单行重复，稍后重试")
			}
			seen[item.BillDetailID] = true
		}
		amount, amountErr := volcengineAmount(item.PayableAmount)
		if amountErr != nil {
			return 0, 0, fmt.Errorf("火山引擎账单应付金额%s", amountErr.Error())
		}
		cost += amount
		if multiplier := volcengineTokenMultiplier(item.Unit); multiplier > 0 && strings.TrimSpace(item.Count) != "" {
			count, countErr := strconv.ParseFloat(strings.TrimSpace(item.Count), 64)
			if countErr != nil || count < 0 || math.IsNaN(count) || math.IsInf(count, 0) {
				return 0, 0, fmt.Errorf("火山引擎账单用量无效")
			}
			add := math.Round(count * float64(multiplier))
			if add > float64(math.MaxInt64-tokens) {
				return 0, 0, fmt.Errorf("火山引擎账单用量溢出")
			}
			tokens += int64(add)
		}
	}
	if math.IsNaN(cost) || math.IsInf(cost, 0) {
		return 0, 0, fmt.Errorf("火山引擎账单金额溢出")
	}
	return math.Round(cost*1e6) / 1e6, tokens, nil
}

// fetchVolcengineUsageWindow reads whole China calendar days in [from, to).
// from must be a day start; the window may cover at most volcengineMaxWindowDays.
// Each day becomes one bucket; recent days are provisional because Volcengine
// keeps rating usage after the fact.
func fetchVolcengineUsageWindow(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred volcengineCredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	if from <= 0 || from != cstDayStart(from) || to <= from || to > from+volcengineMaxWindowDays*86400 {
		return upstreamUsageResult{}, fmt.Errorf("火山引擎账单必须按中国自然日同步")
	}
	now := volcengineNow().Unix()
	hours := make([]ChannelUpstreamUsageHour, 0, volcengineMaxWindowDays)
	for dayStart := from; dayStart < to; dayStart += 86400 {
		day := time.Unix(dayStart, 0).In(cstLocation)
		cost, tokens, err := fetchVolcengineBillDay(ctx, client, cred, day, pacer)
		if err != nil {
			return upstreamUsageResult{}, err
		}
		end := dayStart + 86400
		if end > to {
			end = to
		}
		hours = append(hours, ChannelUpstreamUsageHour{
			Domain: row.Domain, Provider: upstreamProviderVolcengine, SourceKind: upstreamUsageAdapterVolcengine,
			HourTs: dayStart, BucketSeconds: end - dayStart, Tokens: tokens,
			Quota: cost, CostUSD: cost, UnitPerUSD: tokenForceLedgerUnit,
			Provisional: dayStart+86400+volcengineSettleSeconds > now,
		})
	}
	return upstreamUsageResult{Hours: hours, DataUntil: to, Adapter: upstreamUsageAdapterVolcengine}, nil
}

func validateVolcengineInput(in *channelUpstreamSaveInput) error {
	if in.AccessKeyID != "" || in.SecretAccessKey != "" {
		if in.AccessKeyID == "" || in.SecretAccessKey == "" {
			return fmt.Errorf("火山引擎 AccessKey ID 和 Secret Access Key 需要一起填写")
		}
		for _, value := range []string{in.AccessKeyID, in.SecretAccessKey} {
			if len(value) > volcengineAccessKeyMaxLen || strings.ContainsAny(value, "\r\n\t ") {
				return fmt.Errorf("火山引擎 AK/SK 格式无效")
			}
		}
	}
	if in.AccessToken != "" || in.Password != "" || in.RefreshToken != "" || in.APIKey != "" || len(in.APIKeys) != 0 || in.SessionID != "" {
		return fmt.Errorf("火山引擎只使用费用中心只读子用户的 AK/SK，不使用模型 API Key、密码或令牌")
	}
	return nil
}

var errVolcengineCredentialMismatch = errors.New("火山引擎凭据与供应商不匹配")

// volcengineSettlementLookbackDays bounds how far back a day left provisional
// is still repaired.
const volcengineSettlementLookbackDays = 35

// prioritizeVolcengineSettlement re-reads a day left provisional. A day is
// final only when read at least volcengineSettleSeconds after it ends, which
// the tail does only while the day is still in its window; if every tail read
// on that last day failed, the day would stay provisional forever. The history
// lane re-reads the oldest such day without moving its own cursor.
func (m *Monitor) prioritizeVolcengineSettlement(ctx context.Context, row ChannelUpstreamAccount, now int64, plan upstreamUsageSyncPlan) (upstreamUsageSyncPlan, error) {
	if row.Provider != upstreamProviderVolcengine || plan.backfillTo > plan.backfillFrom ||
		(row.UsageBackfillNextSyncAt > 0 && row.UsageBackfillNextSyncAt > now) {
		return plan, nil
	}
	today := cstDayStart(now)
	var hours []ChannelUpstreamUsageHour
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND provisional = ? AND hour_ts >= ? AND hour_ts < ?",
		row.Domain, true, today-volcengineSettlementLookbackDays*86400, today-volcengineTailDays*86400).
		Order("hour_ts ASC").Limit(1).Find(&hours).Error; err != nil {
		return plan, err
	}
	if len(hours) == 0 {
		return plan, nil
	}
	day := cstDayStart(hours[0].HourTs)
	plan.backfillFrom, plan.backfillTo, plan.recentOnly = day, day+86400, true
	return plan, nil
}
