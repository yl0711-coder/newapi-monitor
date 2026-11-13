package monitor

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// ChannelDomainAlias attributes channels whose NewAPI base_url uses another
// address of an already configured supplier (for example a bare origin IP
// that redirects to the supplier's console) to that supplier's domain. It is a
// Monitor-side mapping only: NewAPI routing is unchanged, the channel keeps its
// real host for display, and the channel directory refresh applies it. Finance
// attribution follows the existing domain-change rule (forward only, the
// change hour unattributed), so no historical money moves.
type ChannelDomainAlias struct {
	Alias     string `gorm:"primaryKey;size:253;column:alias"`
	Domain    string `gorm:"size:253;column:domain;index"`
	UpdatedAt int64  `gorm:"column:updated_at"`
	UpdatedBy string `gorm:"size:128;column:updated_by"`
}

func (ChannelDomainAlias) TableName() string { return "channel_domain_aliases" }

// loadChannelDomainAliases returns alias -> domain. Old closed snapshots
// without the table have no aliases.
func loadChannelDomainAliases(ctx context.Context, db *gorm.DB) (map[string]string, error) {
	result := map[string]string{}
	if db == nil {
		return nil, errors.New("渠道本地库不可用")
	}
	if !db.Migrator().HasTable(&ChannelDomainAlias{}) {
		return result, nil
	}
	var rows []ChannelDomainAlias
	if err := db.WithContext(ctx).Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		result[row.Alias] = row.Domain
	}
	return result, nil
}

func channelDomainWithAlias(aliases map[string]string, domain string) string {
	if target, ok := aliases[domain]; ok && target != "" {
		return target
	}
	return domain
}

// channelDomainAliasesByTarget lists, per target domain, the aliases mapped to it.
func channelDomainAliasesByTarget(aliases map[string]string) map[string][]string {
	out := map[string][]string{}
	for alias, domain := range aliases {
		out[domain] = append(out[domain], alias)
	}
	for domain := range out {
		sort.Strings(out[domain])
	}
	return out
}

type channelDomainAliasInput struct {
	Alias  string `json:"alias"`
	Domain string `json:"domain"` // empty removes the alias
}

var errChannelDomainAliasInvalid = errors.New("域名归并参数无效")

func (m *Monitor) saveChannelDomainAliasHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4<<10)
	var in channelDomainAliasInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errChannelDomainAliasInvalid.Error()})
		return
	}
	in.Alias = strings.ToLower(strings.TrimSpace(in.Alias))
	in.Domain = strings.ToLower(strings.TrimSpace(in.Domain))
	if in.Alias == "" || len(in.Alias) > 253 || normalizeChannelBaseDomain(in.Alias) != in.Alias ||
		(in.Domain != "" && (len(in.Domain) > 253 || normalizeChannelBaseDomain(in.Domain) != in.Domain || in.Domain == in.Alias)) {
		c.JSON(http.StatusBadRequest, gin.H{"error": errChannelDomainAliasInvalid.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	now := time.Now().Unix()
	err := m.queuedWriteStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if in.Domain == "" {
			return tx.Where("alias = ?", in.Alias).Delete(&ChannelDomainAlias{}).Error
		}
		// The alias must be a real channel address, by its own host: its
		// base_domain may already show an earlier alias target.
		var hosts []string
		if err := tx.Model(&ChannelSnap{}).Where("deleted_at = 0").Distinct().Pluck("base_host", &hosts).Error; err != nil {
			return err
		}
		known := false
		for _, host := range hosts {
			if normalizeChannelBaseDomain(host) == in.Alias {
				known = true
				break
			}
		}
		if !known {
			return errors.New("该地址不是当前任何渠道的站点地址")
		}
		var count int64
		if err := tx.Model(&ChannelUpstreamAccount{}).Where("domain = ?", in.Domain).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			return errors.New("目标域名尚未配置上游账户，不能归并")
		}
		// An alias with its own account would book the same upstream twice.
		if err := tx.Model(&ChannelUpstreamAccount{}).Where("domain = ?", in.Alias).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return errors.New("该地址已单独配置上游账户；请先删除其账户配置再归并，避免成本重复计算")
		}
		if err := tx.Model(&ChannelDomainAlias{}).Where("alias = ? OR domain = ?", in.Domain, in.Alias).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return errors.New("不支持多级归并")
		}
		row := ChannelDomainAlias{Alias: in.Alias, Domain: in.Domain, UpdatedAt: now, UpdatedBy: c.GetString("uname")}
		return tx.Save(&row).Error
	})
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, context.DeadlineExceeded) || isUpstreamUsageLocalStoreBusy(err) {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"alias": in.Alias, "domain": in.Domain, "updated_at": now,
		"note": "下一轮渠道目录同步（约 1 分钟内）生效；经营核算只从生效时刻起按新域名归属，历史金额不迁移"})
}
