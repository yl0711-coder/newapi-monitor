package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Orion is the console panel behind modelrouter.best (title "Orion"). Its API
// lives under /api/v1/console; it is not Sub2API or NewAPI. A management key
// (smk_..., created on the panel's "Management keys" page) acts as the user
// and never rotates, so no login or refresh is ever performed here.
const (
	upstreamProviderOrion     = "orion"
	upstreamUsageAdapterOrion = "orion_requests"
	// The live panel's own pager offers 10/20/50/100 rows per page, and it
	// answered limit=200 (with an outcome filter it never sends) with HTTP 400.
	orionPageSize         = 100
	orionMaxPages         = 50
	orionMaxWindowSeconds = 48 * 3600
	// Requests are billed when answered, so late rows are rare: re-read one
	// closed hour instead of the generic three. The live tail window is that
	// overlap plus two new hours, which keeps a busy account (about 1,600
	// requests per hour) inside one run's 50-page budget at 100 rows per page.
	orionTailOverlapSeconds = int64(3600)
	orionTailMaxSeconds     = orionTailOverlapSeconds + 2*3600
	// Error text kept from a rejected request, so a parameter change on the
	// panel is visible in status instead of a bare status code.
	orionErrorMessageMaxRunes = 120
)

type orionCredential struct {
	AccessToken string `json:"access_token"`
}

func orionGET(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred orionCredential, path string, target any) error {
	if strings.TrimSpace(cred.AccessToken) == "" {
		return &upstreamAuthError{err: fmt.Errorf("Orion 管理密钥为空，请重新填写")}
	}
	raw, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(row.BaseURL, path), map[string]string{"Authorization": "Bearer " + cred.AccessToken}, nil)
	if err != nil {
		var httpErr *upstreamHTTPError
		if errors.As(err, &httpErr) {
			// Keep the status for the shared retry policy. Drop the body of an
			// authentication failure, which may echo the key; for other client
			// errors keep a short message so the rejected parameter is visible.
			safeErr := &upstreamHTTPError{Status: httpErr.Status, RetryAt: httpErr.RetryAt}
			if httpErr.Status != http.StatusUnauthorized && httpErr.Status != http.StatusForbidden {
				safeErr.Message = orionSafeErrorMessage(httpErr.Message, cred.AccessToken)
			}
			// Only 401 proves the key is invalid. A 403 (role, firewall,
			// temporary block) goes through the shared 403 policy: cooldown
			// and recovery probes instead of immediate isolation.
			if httpErr.Status == http.StatusUnauthorized {
				return &upstreamAuthError{err: safeErr}
			}
			return safeErr
		}
		return err
	}
	if json.Unmarshal(raw, target) != nil {
		return fmt.Errorf("Orion 响应字段无法解析")
	}
	return nil
}

// readOrionProfile reads GET /api/v1/console/me. The console renders the
// balance from the signed-in user's "balance"; accept the user object either
// at the top level or under "user".
func readOrionProfile(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred orionCredential) (upstreamBalanceResult, int64, error) {
	type orionUser struct {
		ID      json.RawMessage `json:"id"`
		Balance json.RawMessage `json:"balance"`
	}
	var profile struct {
		orionUser
		User *orionUser `json:"user"`
	}
	if err := orionGET(ctx, client, row, cred, "/api/v1/console/me", &profile); err != nil {
		return upstreamBalanceResult{}, 0, err
	}
	user := profile.orionUser
	if profile.User != nil && len(profile.User.Balance) > 0 {
		user = *profile.User
	}
	amount, err := rawJSONNumber(user.Balance)
	if err != nil {
		return upstreamBalanceResult{}, 0, fmt.Errorf("Orion 账户信息缺少有效余额")
	}
	id, idErr := rawJSONNumber(user.ID)
	if idErr != nil || id <= 0 || id != math.Trunc(id) || id > math.MaxInt64/2 {
		return upstreamBalanceResult{}, 0, fmt.Errorf("Orion 账户信息缺少有效用户 ID")
	}
	userID := int64(id)
	if row.UserID > 0 && row.UserID != userID {
		return upstreamBalanceResult{}, 0, fmt.Errorf("Orion 密钥账户与已绑定用户不一致，请核对账户配置")
	}
	return upstreamBalanceResult{BalanceUSD: amount, BalanceRaw: amount, BalanceUnit: 1}, userID, nil
}

// orionSafeErrorMessage keeps a short, single-line upstream error message
// without the credential, or nothing if it cannot be shown safely.
func orionSafeErrorMessage(message, credential string) string {
	message = strings.Join(strings.Fields(message), " ")
	lower := strings.ToLower(message)
	if message == "" || (credential != "" && strings.Contains(message, credential)) ||
		strings.Contains(lower, "bearer") || strings.Contains(lower, "smk_") || strings.Contains(message, "eyJ") {
		return ""
	}
	if runes := []rune(message); len(runes) > orionErrorMessageMaxRunes {
		message = string(runes[:orionErrorMessageMaxRunes]) + "…"
	}
	return message
}

type orionRequestItem struct {
	RequestID      string          `json:"request_id"`
	At             string          `json:"at"`
	InputTokens    *int64          `json:"input_tokens"`
	OutputTokens   *int64          `json:"output_tokens"`
	CacheCreate5m  *int64          `json:"cache_create_5m"`
	CacheCreate1h  *int64          `json:"cache_create_1h"`
	CacheRead      *int64          `json:"cache_read"`
	CostBilled     json.RawMessage `json:"cost_billed"`
	Status         *int            `json:"status"`
	ModelRequested string          `json:"model_requested"`
}

type orionRequestPage struct {
	Items      []orionRequestItem `json:"items"`
	NextCursor *string            `json:"next_cursor"`
}

// parseOrionTime reads a request time. The panel answers in UTC: a request
// listed for the window 09:00-10:00Z (sent as UTC) carried 09:59:49, newest
// first (live check, 2026-11-13). An explicit offset is honoured; a time
// without one is UTC. The window range check after parsing still rejects a
// whole window if any time falls outside it, so a misread zone cannot be
// stored under the wrong hour.
func parseOrionTime(raw string) (time.Time, error) {
	if at, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return at, nil
	}
	return time.ParseInLocation("2006-01-02T15:04:05.999999999", raw, time.UTC)
}

func orionTime(ts int64) string {
	return time.Unix(ts, 0).UTC().Format("2006-01-02T15:04:05.000Z")
}

func fetchOrionRequestPage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred orionCredential, from, to int64, cursor string, pacer *upstreamUsageRequestPacer) (orionRequestPage, error) {
	page := orionRequestPage{}
	if err := pacer.beforeRequest(ctx); err != nil {
		return page, err
	}
	// Same query as the console's usage "records" tab: time range, limit and
	// cursor only. That tab lists answered requests; failed ones are a separate
	// tab (failed=true). Parameters the panel never sends are not added.
	query := url.Values{"from": {orionTime(from)}, "to": {orionTime(to)}, "limit": {strconv.Itoa(orionPageSize)}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	if err := orionGET(ctx, client, row, cred, "/api/v1/console/me/requests?"+query.Encode(), &page); err != nil {
		return page, err
	}
	if page.Items == nil || len(page.Items) > orionPageSize {
		return page, fmt.Errorf("Orion 请求明细分页字段缺失或不匹配，未发布部分账单")
	}
	return page, nil
}

func orionTokens(item orionRequestItem) (int64, bool) {
	var total int64
	for _, part := range []*int64{item.InputTokens, item.OutputTokens, item.CacheCreate5m, item.CacheCreate1h, item.CacheRead} {
		if part == nil {
			continue
		}
		if *part < 0 || *part > math.MaxInt64-total {
			return 0, false
		}
		total += *part
	}
	return total, item.InputTokens != nil && item.OutputTokens != nil
}

// fetchOrionUsageWindow reads every request in the half-open window [from, to)
// by cursor, then aggregates hourly buckets. Partial reads are never published.
func fetchOrionUsageWindow(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred orionCredential, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
	if from <= 0 || from%3600 != 0 || to <= from || to-from > orionMaxWindowSeconds {
		return upstreamUsageResult{}, fmt.Errorf("Orion 用量窗口无效")
	}
	items := []orionRequestItem{}
	cursor := ""
	seenCursor := map[string]bool{}
	for page := 1; ; page++ {
		if page > orionMaxPages {
			return upstreamUsageResult{}, fmt.Errorf("Orion 区间请求数超出单轮安全上限，未发布部分账单")
		}
		next, err := fetchOrionRequestPage(ctx, client, row, cred, from, to, cursor, pacer)
		if err != nil {
			return upstreamUsageResult{}, err
		}
		items = append(items, next.Items...)
		if next.NextCursor == nil || *next.NextCursor == "" {
			break
		}
		if seenCursor[*next.NextCursor] || len(next.Items) == 0 {
			return upstreamUsageResult{}, fmt.Errorf("Orion 分页游标未前进，未发布部分账单")
		}
		seenCursor[*next.NextCursor] = true
		cursor = *next.NextCursor
	}
	return aggregateOrionUsage(row, items, from, to)
}

func aggregateOrionUsage(row ChannelUpstreamAccount, items []orionRequestItem, from, to int64) (upstreamUsageResult, error) {
	hours := make([]ChannelUpstreamUsageHour, (to-from+3599)/3600)
	for i := range hours {
		hours[i] = ChannelUpstreamUsageHour{Domain: row.Domain, Provider: upstreamProviderOrion, SourceKind: upstreamUsageAdapterOrion, HourTs: from + int64(i)*3600, BucketSeconds: 3600, UnitPerUSD: 1}
		if end := hours[i].HourTs + 3600; end > to {
			hours[i].BucketSeconds = to - hours[i].HourTs
		}
	}
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		at, err := parseOrionTime(item.At)
		if err != nil {
			return upstreamUsageResult{}, fmt.Errorf("Orion 请求时间无效")
		}
		if item.RequestID == "" || seen[item.RequestID] {
			return upstreamUsageResult{}, fmt.Errorf("Orion 请求明细缺少 request_id 或重复，未发布部分账单")
		}
		seen[item.RequestID] = true
		if at.Unix() == to {
			// The panel may treat "to" as inclusive. A record in the second
			// that starts the next window belongs to that window, which reads
			// it (from is inclusive); counting it here would count it twice.
			continue
		}
		if at.Unix() < from || at.Unix() > to {
			return upstreamUsageResult{}, fmt.Errorf("Orion 请求明细不在查询区间 %s ~ %s 内", orionTime(from), orionTime(to))
		}
		cost, costErr := rawJSONNumber(item.CostBilled)
		if item.Status != nil && *item.Status >= 400 && (costErr != nil || cost == 0) {
			// A failed request that was not billed (zero or no billed amount)
			// is not usage. The records list should not contain one; skip it
			// if the panel ever does.
			continue
		}
		tokens, tokensOK := orionTokens(item)
		if costErr != nil || cost < 0 || !tokensOK {
			return upstreamUsageResult{}, fmt.Errorf("Orion 请求明细金额或 token 字段无效")
		}
		bucket := &hours[(at.Unix()-from)/3600]
		if tokens > math.MaxInt64-bucket.Tokens || math.IsInf(bucket.CostUSD+cost, 0) {
			return upstreamUsageResult{}, fmt.Errorf("Orion 聚合数值溢出")
		}
		bucket.Requests++
		bucket.Tokens += tokens
		bucket.CostUSD += cost // Billed amount in the panel's balance unit.
		bucket.Quota += cost
	}
	return upstreamUsageResult{Hours: hours, DataUntil: to, Adapter: upstreamUsageAdapterOrion}, nil
}

func validateOrionInput(in *channelUpstreamSaveInput) error {
	if len(in.AccessToken) > 16<<10 || strings.ContainsAny(in.AccessToken, "\r\n\t ") || strings.HasPrefix(strings.ToLower(in.AccessToken), "bearer") || strings.HasPrefix(in.AccessToken, "sk-") {
		return fmt.Errorf("请填写 Orion 后台“管理密钥”（smk_ 开头），不是模型 API Key 或 Authorization 整行")
	}
	if in.Password != "" || in.RefreshToken != "" || in.APIKey != "" || len(in.APIKeys) != 0 || in.AccessKeyID != "" || in.SecretAccessKey != "" {
		return fmt.Errorf("Orion 只支持导入管理密钥，不使用密码或 Refresh Token")
	}
	return nil
}
