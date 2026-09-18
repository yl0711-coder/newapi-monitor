package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const channelCostOwnershipMaxTokens = 1000

type channelCostOwnershipChannel struct {
	ChannelID int    `json:"channel_id"`
	Name      string `json:"name"`
	Status    int    `json:"status"`
}

type channelCostOwnershipMatch struct {
	SourceRef  string                        `json:"source_ref"`
	State      string                        `json:"state"`
	Candidates []channelCostOwnershipChannel `json:"candidates"`
}

type channelCostOwnershipReport struct {
	Domain           string                      `json:"domain"`
	AccountEpoch     string                      `json:"account_epoch"`
	Sources          int                         `json:"sources"`
	ExactUnique      int                         `json:"exact_unique"`
	ExactShared      int                         `json:"exact_shared"`
	Unmatched        int                         `json:"unmatched"`
	TokenUnavailable int                         `json:"token_unavailable"`
	Matches          []channelCostOwnershipMatch `json:"matches"`
}

type channelCostUpstreamToken struct {
	ID int `json:"id"`
}

func channelCostKeyFingerprint(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "sk-")
	if raw == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(digest[:])
}

func channelCostChannelKeys(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var values []string
	if strings.HasPrefix(raw, "[") {
		if json.Unmarshal([]byte(raw), &values) != nil {
			return nil
		}
		// NewAPI multi-key JSON arrays contain strings. Other JSON credentials
		// are deliberately not interpreted as API keys.
	} else {
		values = strings.Split(strings.Trim(raw, "\n"), "\n")
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		fingerprint := channelCostKeyFingerprint(value)
		if fingerprint == "" || seen[fingerprint] {
			continue
		}
		seen[fingerprint] = true
		result = append(result, fingerprint)
	}
	return result
}

func newAPITokenHeaders(account ChannelUpstreamAccount, credential newAPICredential) map[string]string {
	return map[string]string{
		"Authorization": "Bearer " + credential.AccessToken,
		"New-Api-User":  strconv.FormatInt(account.UserID, 10),
	}
}

func fetchNewAPIChannelCostTokens(ctx context.Context, client *http.Client, account ChannelUpstreamAccount, credential newAPICredential) ([]channelCostUpstreamToken, error) {
	const pageSize = 100
	headers := newAPITokenHeaders(account, credential)
	result := make([]channelCostUpstreamToken, 0)
	seen := map[int]bool{}
	for page := 1; ; page++ {
		query := url.Values{"p": {strconv.Itoa(page)}, "size": {strconv.Itoa(pageSize)}}
		body, err := doUpstreamJSON(ctx, client, http.MethodGet, upstreamEndpoint(account.BaseURL, "/api/token/")+"?"+query.Encode(), headers, nil)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Items []channelCostUpstreamToken `json:"items"`
			Total int                        `json:"total"`
		}
		if err := decodeNewAPIData(body, &payload); err != nil {
			return nil, err
		}
		if payload.Total < 0 || payload.Total > channelCostOwnershipMaxTokens || len(result)+len(payload.Items) > channelCostOwnershipMaxTokens {
			return nil, errors.New("上游令牌数量超过单次安全核对上限")
		}
		for _, token := range payload.Items {
			if token.ID <= 0 || seen[token.ID] {
				return nil, errors.New("上游令牌列表包含无效或重复 ID")
			}
			seen[token.ID] = true
			result = append(result, token)
		}
		if len(result) >= payload.Total || len(payload.Items) == 0 {
			if len(result) != payload.Total {
				return nil, fmt.Errorf("上游令牌分页不完整（got=%d want=%d）", len(result), payload.Total)
			}
			return result, nil
		}
	}
}

func fetchNewAPIChannelCostTokenKeys(ctx context.Context, client *http.Client, account ChannelUpstreamAccount, credential newAPICredential, ids []int) (map[int]string, error) {
	result := make(map[int]string, len(ids))
	headers := newAPITokenHeaders(account, credential)
	for start := 0; start < len(ids); start += 100 {
		end := start + 100
		if end > len(ids) {
			end = len(ids)
		}
		body, err := doUpstreamJSON(ctx, client, http.MethodPost, upstreamEndpoint(account.BaseURL, "/api/token/batch/keys"), headers, map[string]any{"ids": ids[start:end]})
		if err != nil {
			var statusErr *upstreamHTTPError
			if errors.As(err, &statusErr) && (statusErr.Status == http.StatusNotFound || statusErr.Status == http.StatusMethodNotAllowed) {
				return fetchNewAPIChannelCostTokenKeysIndividually(ctx, client, account, credential, ids)
			}
			return nil, err
		}
		var payload struct {
			Keys map[string]string `json:"keys"`
		}
		if err := decodeNewAPIData(body, &payload); err != nil {
			return nil, err
		}
		for _, id := range ids[start:end] {
			key := strings.TrimSpace(payload.Keys[strconv.Itoa(id)])
			if key == "" {
				return nil, fmt.Errorf("上游未返回令牌 %d 的完整 Key", id)
			}
			result[id] = key
		}
	}
	return result, nil
}

func fetchNewAPIChannelCostTokenKeysIndividually(ctx context.Context, client *http.Client, account ChannelUpstreamAccount, credential newAPICredential, ids []int) (map[int]string, error) {
	result := make(map[int]string, len(ids))
	headers := newAPITokenHeaders(account, credential)
	for _, id := range ids {
		body, err := doUpstreamJSON(ctx, client, http.MethodPost, upstreamEndpoint(account.BaseURL, "/api/token/"+strconv.Itoa(id)+"/key"), headers, nil)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Key string `json:"key"`
		}
		if err := decodeNewAPIData(body, &payload); err != nil || strings.TrimSpace(payload.Key) == "" {
			if err == nil {
				err = fmt.Errorf("上游未返回令牌 %d 的完整 Key", id)
			}
			return nil, err
		}
		result[id] = payload.Key
	}
	return result, nil
}

type channelCostLocalKeyOwner struct {
	ChannelID int
	Name      string
	Status    int
	Key       string
	BaseURL   sql.NullString
}

func (m *Monitor) loadChannelCostLocalKeyOwners(ctx context.Context, domain string) (map[string][]channelCostOwnershipChannel, error) {
	if m.prodDB == nil {
		return nil, errors.New("生产只读数据库未连接")
	}
	rows, err := m.prodDB.QueryContext(ctx, "SELECT id,name,status,`key`,base_url FROM channels")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	owners := map[string][]channelCostOwnershipChannel{}
	for rows.Next() {
		var row channelCostLocalKeyOwner
		if err := rows.Scan(&row.ChannelID, &row.Name, &row.Status, &row.Key, &row.BaseURL); err != nil {
			return nil, err
		}
		if !row.BaseURL.Valid || normalizeChannelBaseDomain(row.BaseURL.String) != domain {
			continue
		}
		owner := channelCostOwnershipChannel{ChannelID: row.ChannelID, Name: row.Name, Status: row.Status}
		for _, fingerprint := range channelCostChannelKeys(row.Key) {
			owners[fingerprint] = append(owners[fingerprint], owner)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for key := range owners {
		sort.Slice(owners[key], func(i, j int) bool { return owners[key][i].ChannelID < owners[key][j].ChannelID })
	}
	return owners, nil
}

func (m *Monitor) inspectChannelCostOwnership(ctx context.Context, account ChannelUpstreamAccount, credential newAPICredential) (channelCostOwnershipReport, error) {
	report := channelCostOwnershipReport{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), Matches: []channelCostOwnershipMatch{}}
	var sourceRefs []string
	if err := m.storeDB.WithContext(ctx).Model(&ChannelUpstreamCostHourEvidence{}).
		Distinct("source_ref").Where("domain = ? AND account_epoch = ? AND semantics_version = ?", account.Domain, report.AccountEpoch, channelCostEvidenceSemanticsVersion).
		Order("source_ref").Pluck("source_ref", &sourceRefs).Error; err != nil {
		return report, err
	}
	report.Sources = len(sourceRefs)
	if len(sourceRefs) == 0 {
		return report, nil
	}
	wanted := make(map[string]bool, len(sourceRefs))
	for _, sourceRef := range sourceRefs {
		wanted[sourceRef] = true
	}
	tokens, err := fetchNewAPIChannelCostTokens(ctx, m.channelUpstreamHTTPClient(), account, credential)
	if err != nil {
		return report, err
	}
	ids := make([]int, 0)
	refByID := map[int]string{}
	for _, token := range tokens {
		sourceRef, err := channelCostSourceRef([]byte(m.cfg.ChannelCostHMACKey), account.Provider, report.AccountEpoch, channelCostSourceKindNewAPIToken, strconv.Itoa(token.ID))
		if err != nil {
			return report, err
		}
		if wanted[sourceRef] {
			ids = append(ids, token.ID)
			refByID[token.ID] = sourceRef
		}
	}
	keys, err := fetchNewAPIChannelCostTokenKeys(ctx, m.channelUpstreamHTTPClient(), account, credential, ids)
	if err != nil {
		return report, err
	}
	localOwners, err := m.loadChannelCostLocalKeyOwners(ctx, account.Domain)
	if err != nil {
		return report, err
	}
	matchByRef := make(map[string]channelCostOwnershipMatch, len(sourceRefs))
	for _, sourceRef := range sourceRefs {
		matchByRef[sourceRef] = channelCostOwnershipMatch{SourceRef: sourceRef, State: "token_unavailable", Candidates: []channelCostOwnershipChannel{}}
	}
	for id, sourceRef := range refByID {
		candidates := localOwners[channelCostKeyFingerprint(keys[id])]
		match := channelCostOwnershipMatch{SourceRef: sourceRef, Candidates: append([]channelCostOwnershipChannel(nil), candidates...)}
		switch len(candidates) {
		case 0:
			match.State = "unmatched"
		case 1:
			match.State = "exact_unique"
		default:
			match.State = "exact_shared"
		}
		matchByRef[sourceRef] = match
	}
	for _, sourceRef := range sourceRefs {
		match := matchByRef[sourceRef]
		report.Matches = append(report.Matches, match)
		switch match.State {
		case "exact_unique":
			report.ExactUnique++
		case "exact_shared":
			report.ExactShared++
		case "unmatched":
			report.Unmatched++
		default:
			report.TokenUnavailable++
		}
	}
	return report, nil
}

func (m *Monitor) inspectChannelCostOwnershipHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if m.cfg.LocalSnapshotOnly {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地快照不包含生产渠道密钥，无法执行精确归属核对"})
		return
	}
	domain := strings.ToLower(strings.TrimSpace(c.Query("domain")))
	if !m.channelCostAPIAllowed(domain) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "该域名的渠道成本闭环未进入灰度"})
		return
	}
	var account ChannelUpstreamAccount
	if err := m.storeDB.WithContext(c.Request.Context()).Where("domain = ?", domain).First(&account).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "上游账户不存在"})
		return
	}
	if account.Provider != upstreamProviderNewAPI {
		c.JSON(http.StatusBadRequest, gin.H{"error": "当前只支持 NewAPI 上游的令牌精确核对"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	// Reading full upstream token keys must not race balance, billing or
	// credential operations for the same account. The existing account gate
	// gives this explicit admin action priority over background workers while
	// keeping unrelated upstream domains independent.
	release, err := m.acquireUpstreamAccountAdmin(ctx, domain)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "上游账户正在执行其他操作，请稍后重试"})
		return
	}
	defer release()
	var credential newAPICredential
	if err := m.openUpstreamCredential(account, &credential); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	report, err := m.inspectChannelCostOwnership(ctx, account, credential)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": sanitizeUpstreamError(err)})
		return
	}
	c.JSON(http.StatusOK, report)
}
