package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	upstreamUsageAdapterAICodeWithRecord = "aicodewith_record"
	aiCodeWithRecordPageSize             = 100
	aiCodeWithRecordMaxPages             = 10000
	aiCodeWithRecordResumeSeconds        = int64(60)
	aiCodeWithRecordMigrationDays        = int64(7)
	aiCodeWithRecordScale                = int64(1_000_000_000) // exact nanounits; reject rather than round unsupported precision
)

// Only hashes of source IDs/record identity are persisted, not raw logs,
// models, content or credentials. Completed rounds remove these checkpoints.
type AICodeWithRecordSeen struct {
	Domain      string `gorm:"primaryKey"`
	RoundID     string `gorm:"primaryKey"`
	SlotID      string `gorm:"primaryKey"`
	RecordHash  string `gorm:"primaryKey"`
	ContentHash string
}

type AICodeWithRecordCheckpoint struct {
	Domain           string `gorm:"primaryKey"`
	RoundID          string `gorm:"primaryKey"`
	SlotID           string `gorm:"primaryKey"`
	SourceKeyID      int64
	Cursor           string
	CursorHashesJSON string
	Pages            int
	RecordsDone      bool
	Requests         int64
	Tokens           int64
	CostUnits        int64
	Unit             float64
	HoursJSON        string
	UpdatedAt        int64
}

type aiCodeWithRecordHour struct{ Requests, Tokens, CostUnits int64 }
type aiCodeWithRecord struct {
	ID                           string
	CreatedAt, Tokens, CostUnits int64
}
type aiCodeWithRecordPage struct {
	KeyID   int64
	Records []aiCodeWithRecord
	HasMore bool
	Cursor  string
}

var aiCodeWithDecimalPattern = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]{1,2})?$`)

func aiCodeWithExactCost(raw json.RawMessage) (int64, error) {
	text := strings.TrimSpace(string(raw))
	if strings.HasPrefix(text, "\"") {
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, fmt.Errorf("AICodeWith 金额格式无效")
		}
	}
	if len(text) == 0 || len(text) > 64 || !aiCodeWithDecimalPattern.MatchString(text) {
		return 0, fmt.Errorf("AICodeWith 金额无效")
	}
	r, ok := new(big.Rat).SetString(text)
	if !ok || r.Sign() < 0 {
		return 0, fmt.Errorf("AICodeWith 金额无效")
	}
	r.Mul(r, new(big.Rat).SetInt64(aiCodeWithRecordScale))
	if !r.IsInt() || !r.Num().IsInt64() {
		return 0, fmt.Errorf("AICodeWith 金额精度或范围超限，拒绝舍入")
	}
	return r.Num().Int64(), nil
}

func decodeAICodeWithRecordPage(body []byte, day int64) (aiCodeWithRecordPage, error) {
	var envelope struct {
		Data struct {
			KeyID   json.RawMessage             `json:"api_key_id"`
			Period  struct{ Start, End string } `json:"period"`
			GroupBy string                      `json:"group_by"`
			Records *[]struct {
				ID        json.RawMessage `json:"id"`
				CreatedAt string          `json:"created_at"`
				Cost      json.RawMessage `json:"cost"`
				Tokens    json.RawMessage `json:"total_tokens"`
			} `json:"records"`
			HasMore *bool  `json:"has_more"`
			Cursor  string `json:"next_cursor"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return aiCodeWithRecordPage{}, fmt.Errorf("AICodeWith record 响应无效")
	}
	d := envelope.Data
	date := time.Unix(day, 0).In(cstLocation).Format("2006-01-02")
	id, err := rawJSONInt64Exact(d.KeyID)
	if err != nil || id <= 0 || d.GroupBy != "record" || d.Period.Start != date || d.Period.End != date || d.Records == nil || d.HasMore == nil {
		return aiCodeWithRecordPage{}, fmt.Errorf("AICodeWith record 身份、范围或分页字段无效")
	}
	if len(*d.Records) > aiCodeWithRecordPageSize || len(d.Cursor) > 4096 || (*d.HasMore && (d.Cursor == "" || len(*d.Records) == 0)) || (!*d.HasMore && d.Cursor != "") {
		return aiCodeWithRecordPage{}, fmt.Errorf("AICodeWith record 分页边界无效")
	}
	page := aiCodeWithRecordPage{KeyID: id, HasMore: *d.HasMore, Cursor: d.Cursor}
	for _, raw := range *d.Records {
		var recordID string
		if len(raw.ID) > 0 && raw.ID[0] == '"' {
			_ = json.Unmarshal(raw.ID, &recordID)
		} else {
			n, e := rawJSONInt64Exact(raw.ID)
			if e == nil && n > 0 {
				recordID = strconv.FormatInt(n, 10)
			}
		}
		stamp, e := time.Parse(time.RFC3339Nano, raw.CreatedAt)
		tokens, te := rawJSONInt64Exact(raw.Tokens)
		cost, ce := aiCodeWithExactCost(raw.Cost)
		if recordID == "" || len(recordID) > 256 || e != nil || stamp.Unix() < day || stamp.Unix() >= day+86400 || te != nil || tokens < 0 || ce != nil {
			return aiCodeWithRecordPage{}, fmt.Errorf("AICodeWith record ID、时区、费用或 Token 无效")
		}
		page.Records = append(page.Records, aiCodeWithRecord{ID: recordID, CreatedAt: stamp.Unix(), Tokens: tokens, CostUnits: cost})
	}
	return page, nil
}

func fetchAICodeWithRecordPage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, secret string, day int64, cursor string, pacer *upstreamUsageRequestPacer) (aiCodeWithRecordPage, error) {
	if err := pacer.beforeRequest(ctx); err != nil {
		return aiCodeWithRecordPage{}, err
	}
	date := time.Unix(day, 0).In(cstLocation).Format("2006-01-02")
	q := url.Values{"start": {date}, "end": {date}, "group_by": {"record"}, "limit": {strconv.Itoa(aiCodeWithRecordPageSize)}}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	body, err := doUpstreamJSON(ctx, client, http.MethodGet, aicodeWithEndpoint(row.BaseURL, "/api/v1/api-keys/usage")+"?"+q.Encode(), map[string]string{"Authorization": "Bearer " + secret}, nil)
	if err != nil {
		return aiCodeWithRecordPage{}, aiCodeWithRecordAuthError(err)
	}
	return decodeAICodeWithRecordPage(body, day)
}
