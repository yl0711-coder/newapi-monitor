package monitor

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

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
	Domain           string `json:"domain"`
	Retiring         *bool  `json:"retiring"`
	ExpectedRetiring *bool  `json:"expected_retiring"`
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
		if current == *input.Retiring {
			return nil
		}
		row.Domain, row.Retiring, row.UpdatedAt, row.UpdatedBy = domain, *input.Retiring, now, c.GetString("uname")
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
	c.JSON(http.StatusOK, gin.H{"domain": domain, "retiring": *input.Retiring, "updated_at": now})
}
