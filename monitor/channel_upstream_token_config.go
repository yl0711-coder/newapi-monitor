package monitor

// Optional token configuration is an operator's attribution intent, NOT a
// confirmed cost binding. Saving here must never publish money, change finance
// versions, enqueue historical repairs, or contact the upstream. Verification
// and time-bounded activation are separate from this append-only configuration.

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type ChannelUpstreamTokenConfig struct {
	Domain       string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch string `gorm:"primaryKey;size:64;column:account_epoch"`
	ChannelID    int    `gorm:"primaryKey;column:channel_id"`
	Revision     int64  `gorm:"primaryKey;column:revision"`
	Provider     string `gorm:"size:24;column:provider"`
	TokenID      string `gorm:"size:19;column:token_id"`
	CreatedAt    int64  `gorm:"column:created_at"`
	CreatedBy    string `gorm:"size:128;column:created_by"`
}

func (ChannelUpstreamTokenConfig) TableName() string { return "channel_upstream_token_configs" }

type upstreamTokenConfigInput struct {
	Domain           string  `json:"domain"`
	ChannelID        int     `json:"channel_id"`
	AccountEpoch     string  `json:"account_epoch"`
	TokenID          *string `json:"upstream_token_id"` // nil: preserve; "": explicitly clear
	ExpectedRevision int64   `json:"expected_revision"`
}

type upstreamTokenConfigView struct {
	Domain              string                       `json:"domain"`
	ChannelID           int                          `json:"channel_id"`
	AccountEpoch        string                       `json:"account_epoch"`
	Provider            string                       `json:"provider"`
	TokenID             string                       `json:"upstream_token_id"`
	Revision            int64                        `json:"revision"`
	UpdatedAt           int64                        `json:"updated_at"`
	State               string                       `json:"state"`
	AttributionVerified bool                         `json:"attribution_verified"`
	Unchanged           bool                         `json:"unchanged"`
	SharedChannelIDs    []int                        `json:"shared_channel_ids,omitempty"`
	Processing          *upstreamTokenProcessingView `json:"processing,omitempty"`
}

var (
	errUpstreamTokenScope    = errors.New("上游账户或渠道归属无效，请重新打开配置")
	errUpstreamTokenConflict = errors.New("上游账户或令牌配置已变更，请重新打开后再保存")
	errUpstreamTokenReadOnly = errors.New("只读验收环境不允许保存令牌配置")
)

func normalizeUpstreamTokenScope(domain string, channelID int) (string, error) {
	domain = strings.ToLower(strings.TrimSpace(domain))
	if domain == "" || len(domain) > 253 || normalizeChannelBaseDomain(domain) != domain || channelID <= 0 {
		return "", errUpstreamTokenScope
	}
	return domain, nil
}

// Select only non-secret identity columns. Neither credentials nor balances
// are required for editing the optional configuration.
func readUpstreamTokenScope(tx *gorm.DB, domain string, channelID int) (ChannelUpstreamAccount, error) {
	var account ChannelUpstreamAccount
	if err := tx.Select("domain", "provider", "base_url", "user_id", "account", "usage_adapter").Where("domain = ?", domain).Take(&account).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return account, errUpstreamTokenScope
		}
		return account, err
	}
	var channel ChannelSnap
	if err := tx.Select("id", "base_domain").Where("id = ?", channelID).Take(&channel).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return account, errUpstreamTokenScope
		}
		return account, err
	}
	if channel.BaseDomain != domain || account.Provider == "" || account.BaseURL == "" {
		return account, errUpstreamTokenScope
	}
	return account, nil
}

func readUpstreamTokenConfig(tx *gorm.DB, account ChannelUpstreamAccount, channelID int) (upstreamTokenConfigView, error) {
	view := upstreamTokenConfigView{Domain: account.Domain, ChannelID: channelID,
		AccountEpoch: newAPIUpstreamAccountEpoch(account), Provider: account.Provider, State: "unconfigured"}
	var row ChannelUpstreamTokenConfig
	// No optional setting is the normal default, not a database error. Find
	// avoids emitting "record not found" for every unconfigured channel.
	result := tx.Where("domain = ? AND account_epoch = ? AND channel_id = ?", view.Domain, view.AccountEpoch, channelID).
		Order("revision DESC").Limit(1).Find(&row)
	if result.Error != nil {
		return view, result.Error
	}
	if result.RowsAffected == 0 {
		return view, nil
	}
	view.TokenID, view.Revision, view.UpdatedAt = row.TokenID, row.Revision, row.CreatedAt
	if view.TokenID != "" {
		view.State = "pending_verification"
		if (account.Provider != upstreamProviderNewAPI && account.Provider != upstreamProviderSub2API) || account.UsageAdapter == upstreamUsageAdapterSub2Stats {
			view.State = "unsupported"
		}
		// Advisory only: one source listed on multiple physical channels must
		// not be treated as several independently chargeable sources.
		if err := tx.Raw(`SELECT c.channel_id FROM channel_upstream_token_configs c
			WHERE c.domain=? AND c.account_epoch=? AND c.token_id=?
			AND NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n WHERE n.domain=c.domain
			 AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision)
			ORDER BY c.channel_id`, view.Domain, view.AccountEpoch, view.TokenID).Scan(&view.SharedChannelIDs).Error; err != nil {
			return view, err
		}
		if len(view.SharedChannelIDs) < 2 {
			view.SharedChannelIDs = nil
		}
	}
	return view, nil
}

func (m *Monitor) loadUpstreamTokenConfig(ctx context.Context, domain string, channelID int) (upstreamTokenConfigView, error) {
	domain, err := normalizeUpstreamTokenScope(domain, channelID)
	if err != nil {
		return upstreamTokenConfigView{}, err
	}
	var view upstreamTokenConfigView
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		account, err := readUpstreamTokenScope(tx, domain, channelID)
		if err != nil {
			return err
		}
		view, err = readUpstreamTokenConfig(tx, account, channelID)
		return err
	})
	return view, err
}

func (m *Monitor) saveUpstreamTokenConfig(ctx context.Context, in upstreamTokenConfigInput, actor string, now int64) (upstreamTokenConfigView, error) {
	if m.cfg.LocalSnapshotOnly {
		return upstreamTokenConfigView{}, errUpstreamTokenReadOnly
	}
	domain, err := normalizeUpstreamTokenScope(in.Domain, in.ChannelID)
	if err != nil {
		return upstreamTokenConfigView{}, err
	}
	if in.ExpectedRevision < 0 || len(in.AccountEpoch) != 64 || now <= 0 {
		return upstreamTokenConfigView{}, errUpstreamTokenConflict
	}
	tokenID := ""
	if in.TokenID != nil {
		tokenID, err = canonicalUpstreamTokenID(*in.TokenID)
		if err != nil {
			return upstreamTokenConfigView{}, err
		}
	}
	actor = strings.TrimSpace(actor)
	if actor == "" || len([]rune(actor)) > 128 || strings.ContainsAny(actor, "\r\n\x00") {
		return upstreamTokenConfigView{}, errors.New("配置操作人无效")
	}
	// Reuse the existing local attribution write lock; the DB transaction and
	// composite PK also protect against competing Monitor processes.
	if err := m.lockUpstreamTokenWrite(ctx); err != nil {
		return upstreamTokenConfigView{}, err
	}
	defer m.channelCostBindingMu.Unlock()
	var view upstreamTokenConfigView
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		account, err := readUpstreamTokenScope(tx, domain, in.ChannelID)
		if err != nil {
			return err
		}
		view, err = readUpstreamTokenConfig(tx, account, in.ChannelID)
		if err != nil {
			return err
		}
		if view.AccountEpoch != in.AccountEpoch || view.Revision != in.ExpectedRevision {
			return errUpstreamTokenConflict
		}
		if in.TokenID == nil || tokenID == view.TokenID {
			view.Unchanged = true
			return nil
		}
		if view.Revision == math.MaxInt64 {
			return errUpstreamTokenConflict
		}
		row := ChannelUpstreamTokenConfig{Domain: domain, AccountEpoch: view.AccountEpoch, ChannelID: in.ChannelID,
			Revision: view.Revision + 1, Provider: account.Provider, TokenID: tokenID, CreatedAt: now, CreatedBy: actor}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		view, err = readUpstreamTokenConfig(tx, account, in.ChannelID)
		return err
	})
	return view, err
}

func upstreamTokenConfigHTTPError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errUpstreamTokenReadOnly):
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
	case errors.Is(err, errUpstreamTokenConflict):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "version_conflict": true})
	case errors.Is(err, errUpstreamTokenScope):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		// DB errors can contain SQL and values. Never reflect them to clients.
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "令牌配置暂不可用，请稍后重试"})
	}
}

func (m *Monitor) getUpstreamTokenConfigHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	channelID, err := strconv.Atoi(c.Query("channel_id"))
	if err != nil {
		upstreamTokenConfigHTTPError(c, errUpstreamTokenScope)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	view, err := m.loadUpstreamTokenConfigWithStatus(ctx, c.Query("domain"), channelID, time.Now().Unix())
	if err != nil {
		upstreamTokenConfigHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, view)
}

func (m *Monitor) saveUpstreamTokenConfigHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if m.cfg.LocalSnapshotOnly {
		upstreamTokenConfigHTTPError(c, errUpstreamTokenReadOnly)
		return
	}
	const maxBody = 4096
	if c.Request.ContentLength > maxBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "配置内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBody)
	var in upstreamTokenConfigInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "配置格式无效；令牌 ID 请使用字符串"})
		return
	}
	if in.TokenID != nil {
		if _, err := canonicalUpstreamTokenID(*in.TokenID); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	view, err := m.saveUpstreamTokenConfig(ctx, in, c.GetString("uname"), time.Now().Unix())
	if err != nil {
		upstreamTokenConfigHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, view)
}
