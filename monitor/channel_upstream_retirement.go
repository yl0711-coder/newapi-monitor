package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// ChannelUpstreamRetirement is an operator decision, not a routing state.
// A retiring supplier may still carry low-priority traffic to use its
// remaining balance; only replenishment reminders are suppressed.
type ChannelUpstreamRetirement struct {
	Domain    string `gorm:"primaryKey;size:253;column:domain"`
	Retiring  bool   `gorm:"column:retiring"`
	UpdatedAt int64  `gorm:"column:updated_at"`
	UpdatedBy string `gorm:"size:128;column:updated_by"`
	// Reasons is a JSON array of retirementReasonCodes chosen when marking;
	// ReasonNote is the free text for "other". Both are cleared on unmark.
	// Rows marked before reasons existed keep them empty.
	Reasons    string `gorm:"size:256;column:reasons"`
	ReasonNote string `gorm:"size:400;column:reason_note"`
}

// retirementReasonCodes are the selectable 停止使用 reasons, in display order.
var retirementReasonCodes = []string{"no_recharge", "degraded", "unstable", "slow", "token_inflation", "other"}

const retirementReasonNoteMaxRunes = 100

// normalizeRetirementReasons validates the selection and returns the codes in
// canonical order plus the note (kept only with "other").
func normalizeRetirementReasons(codes []string, note string) ([]string, string, error) {
	selected := map[string]bool{}
	for _, code := range codes {
		code = strings.TrimSpace(code)
		known := false
		for _, allowed := range retirementReasonCodes {
			if code == allowed {
				known = true
				break
			}
		}
		if !known {
			return nil, "", fmt.Errorf("未知的停止使用原因：%s", code)
		}
		selected[code] = true
	}
	var ordered []string
	for _, code := range retirementReasonCodes {
		if selected[code] {
			ordered = append(ordered, code)
		}
	}
	if len(ordered) == 0 {
		return nil, "", errors.New("请至少选择一个停止使用原因")
	}
	note = strings.Join(strings.Fields(note), " ")
	if !selected["other"] {
		return ordered, "", nil
	}
	if note == "" {
		return nil, "", errors.New("选择“其他”时请填写原因")
	}
	if utf8.RuneCountInString(note) > retirementReasonNoteMaxRunes {
		return nil, "", fmt.Errorf("其他原因不能超过 %d 个字", retirementReasonNoteMaxRunes)
	}
	return ordered, note, nil
}

// retirementReasonList decodes the stored codes; unknown or malformed values
// are dropped so a bad row never breaks the report.
func retirementReasonList(stored string) []string {
	if strings.TrimSpace(stored) == "" {
		return nil
	}
	var codes []string
	if json.Unmarshal([]byte(stored), &codes) != nil {
		return nil
	}
	var out []string
	for _, code := range retirementReasonCodes {
		for _, got := range codes {
			if got == code {
				out = append(out, code)
				break
			}
		}
	}
	return out
}

func (ChannelUpstreamRetirement) TableName() string { return "channel_upstream_retirements" }

func (m *Monitor) loadChannelUpstreamRetirements(ctx context.Context) (map[string]ChannelUpstreamRetirement, error) {
	result := map[string]ChannelUpstreamRetirement{}
	if m == nil || m.storeDB == nil {
		return nil, errors.New("渠道本地库不可用")
	}
	// Old closed SQLite snapshots remain readable without mutating them. A
	// running Monitor migrates this small table before serving requests.
	if !m.storeDB.Migrator().HasTable(&ChannelUpstreamRetirement{}) {
		return result, nil
	}
	var rows []ChannelUpstreamRetirement
	if err := m.storeDB.WithContext(ctx).Where("retiring = ?", true).Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		result[row.Domain] = row
	}
	return result, nil
}

type channelUpstreamRetirementInput struct {
	Domain           string   `json:"domain"`
	Retiring         *bool    `json:"retiring"`
	ExpectedRetiring *bool    `json:"expected_retiring"`
	Reasons          []string `json:"reasons"`
	ReasonNote       string   `json:"reason_note"`
}

var errChannelUpstreamRetirementChanged = errors.New("停止使用标记已被其他管理员修改，请刷新后重试")

func (m *Monitor) saveChannelUpstreamRetirementHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4<<10)
	var input channelUpstreamRetirementInput
	if err := c.ShouldBindJSON(&input); err != nil || input.Retiring == nil || input.ExpectedRetiring == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "停止使用标记请求格式无效"})
		return
	}
	domain := strings.ToLower(strings.TrimSpace(input.Domain))
	if domain == "" || len(domain) > 253 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "主域名无效"})
		return
	}
	var reasons []string
	var note string
	if *input.Retiring {
		var err error
		if reasons, note, err = normalizeRetirementReasons(input.Reasons, input.ReasonNote); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	encodedReasons := ""
	if len(reasons) > 0 {
		raw, _ := json.Marshal(reasons)
		encodedReasons = string(raw)
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	exists, err := m.channelDomainExists(ctx, domain)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "核对渠道主域名失败"})
		return
	}
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "渠道主域名不存在"})
		return
	}
	now := time.Now().Unix()
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ChannelUpstreamRetirement
		err := tx.First(&row, "domain = ?", domain).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		current := err == nil && row.Retiring
		if current != *input.ExpectedRetiring {
			return errChannelUpstreamRetirementChanged
		}
		if current == *input.Retiring && (!current || (row.Reasons == encodedReasons && row.ReasonNote == note)) {
			return nil
		}
		// Marking (or re-saving the reasons of a marked upstream) stores the
		// reasons; unmarking clears them.
		row.Domain, row.Retiring, row.UpdatedAt, row.UpdatedBy = domain, *input.Retiring, now, c.GetString("uname")
		row.Reasons, row.ReasonNote = encodedReasons, note
		return tx.Save(&row).Error
	})
	if errors.Is(err, errChannelUpstreamRetirementChanged) {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "保存停止使用标记失败"})
		return
	}
	if reasons == nil {
		reasons = []string{}
	}
	c.JSON(http.StatusOK, gin.H{"domain": domain, "retiring": *input.Retiring, "updated_at": now,
		"reasons": reasons, "reason_note": note})
}

// Automatic upstream sync pauses for a domain only while BOTH hold: an operator
// marked it 停止使用 and none of its current NewAPI channels is enabled
// (status 1). Either condition alone keeps syncing: a retiring supplier may
// still carry traffic, and a disabled channel may be enabled again. The pause
// is derived on every scheduler pass, so enabling a channel or clearing the
// mark resumes from the stored watermarks; stored history is never removed.
// Operator-triggered syncs are not affected.
//
// The pause starts only after the domain has had no recorded traffic for
// upstreamSyncPauseGraceSeconds, so the usage up to the moment its last
// channel was disabled, and the next day's history re-read of that final day,
// are still imported before syncing stops.
//
// upstreamSyncNotPausedSQL is a WHERE fragment for channel_upstream_accounts.
const upstreamSyncPauseGraceSeconds = 48 * 3600

// Keep the recent-traffic lookup channel-first. Otherwise a missing/stale
// SQLite statistic can cause every retiring domain to scan all 48h traffic.
// CROSS JOIN retains the same live normalized-domain and grace predicates;
// the existing (channel_id,hour_ts) index bounds the inner history read.
var upstreamSyncPausedDomainSQL = `SELECT 1 FROM channel_upstream_retirements r
	WHERE r.retiring = 1 AND r.domain = LOWER(TRIM(%s))
	AND NOT EXISTS (SELECT 1 FROM channel_snaps s
		WHERE s.deleted_at = 0 AND s.status = 1 AND LOWER(TRIM(s.base_domain)) = r.domain)
	AND NOT EXISTS (SELECT 1 FROM channel_snaps s CROSS JOIN stability_hour_samples h ON h.channel_id = s.id
		WHERE LOWER(TRIM(s.base_domain)) = r.domain
		AND h.hour_ts >= CAST(strftime('%%s','now') AS INTEGER) - ` + strconv.Itoa(upstreamSyncPauseGraceSeconds) + `
		AND h.success + h.anomaly + h.failed > 0)`

var upstreamSyncNotPausedSQL = "NOT EXISTS (" + fmt.Sprintf(upstreamSyncPausedDomainSQL, "channel_upstream_accounts.domain") + ")"

// upstreamSyncPausedDomains lists the configured domains whose automatic sync is
// paused, for display. Old closed snapshots without the table pause nothing.
func (m *Monitor) upstreamSyncPausedDomains(ctx context.Context) (map[string]bool, error) {
	result := map[string]bool{}
	if m == nil || m.storeDB == nil {
		return nil, errors.New("渠道本地库不可用")
	}
	if !m.storeDB.Migrator().HasTable(&ChannelUpstreamRetirement{}) {
		return result, nil
	}
	var domains []string
	err := m.storeDB.WithContext(ctx).Raw(`SELECT a.domain FROM channel_upstream_accounts a
		WHERE EXISTS (` + fmt.Sprintf(upstreamSyncPausedDomainSQL, "a.domain") + `)`).Scan(&domains).Error
	if err != nil {
		return nil, err
	}
	for _, domain := range domains {
		result[domain] = true
	}
	return result, nil
}
