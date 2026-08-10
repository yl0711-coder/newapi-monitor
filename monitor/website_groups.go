package monitor

// 网站分组目录只记录 NewAPI 分组管理中“用户实际可能使用”的分组。
// 它与日志中出现过的分组集合故意分离：日志可以包含历史、测试或前置拒绝，
// 不能拿来作为网站计价配置的候选列表。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	websiteGroupSpecialOption = "group_ratio_setting.group_special_usable_group"
	maxWebsiteGroupSources    = 500
	maxWebsiteGroupPayload    = 2 << 20
)

// WebsiteGroupCatalog 是 NewAPI 当前可用分组的本地目录快照。
// Active=false 的记录不删除，以便历史倍率版本仍能解释旧数据。
type WebsiteGroupCatalog struct {
	Grp              string  `gorm:"primaryKey;size:64;column:grp"`
	Source           string  `gorm:"size:32;index;column:source"`
	SourceMultiplier float64 `gorm:"column:source_multiplier"`
	Active           bool    `gorm:"index;column:active"`
	SyncedAt         int64   `gorm:"index;column:synced_at"`
}

// ChannelWebsiteGroupRateView 是网站计价弹窗使用的当前分组目录。
type ChannelWebsiteGroupRateView struct {
	Name             string  `json:"name"`
	Source           string  `json:"source"`
	SourceMultiplier float64 `json:"source_multiplier"`
	Active           bool    `json:"active"`
	SyncedAt         int64   `json:"synced_at"`
	SiteConfigured   bool    `json:"site_configured"`
	SiteMultiplier   float64 `json:"site_multiplier"`
}

type websiteGroupSource struct {
	Name       string
	Multiplier float64
}

// collectWebsiteGroupSources 将 /api/pricing 的“用户可选”与 options 中的
// “分组特殊可用分组”合并。删除标记和缺少合法 GroupRatio 的特殊键不会进入目录。
func collectWebsiteGroupSources(usable []string, ratios map[string]float64, special map[string]map[string]string) ([]websiteGroupSource, int) {
	names := make(map[string]struct{}, len(usable))
	for _, name := range usable {
		name = strings.TrimSpace(name)
		if name != "" {
			names[name] = struct{}{}
		}
	}
	for _, groups := range special {
		for key, description := range groups {
			name := normalizeSpecialWebsiteGroupName(key, description, ratios)
			if name == "" {
				continue
			}
			names[name] = struct{}{}
		}
	}
	out := make([]websiteGroupSource, 0, len(names))
	skipped := 0
	for name := range names {
		value, ok := ratios[name]
		if !ok || !isPositiveFiniteWebsiteGroupRatio(value) {
			skipped++
			continue
		}
		out = append(out, websiteGroupSource{Name: name, Multiplier: value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, skipped
}

// normalizeSpecialWebsiteGroupName handles both the current NewAPI syntax and
// the legacy append_N form found in older deployments.  A special-group map
// uses:
//   - "+:name" to add name (the value is only a description), and
//   - "-:name" to remove name from that user's available groups.
//
// Older deployments used append_N as the key and stored the added group name
// in the value.  Prefer that value when it is a known group ratio; otherwise
// leave the key in place so an ordinary group named append_N is not lost.
func normalizeSpecialWebsiteGroupName(key, description string, ratios map[string]float64) string {
	key = strings.TrimSpace(key)
	if key == "" || strings.HasPrefix(key, "-:") || strings.EqualFold(strings.TrimSpace(description), "remove") {
		return ""
	}
	if strings.HasPrefix(key, "+:") {
		return strings.TrimSpace(strings.TrimPrefix(key, "+:"))
	}
	if strings.HasPrefix(key, "append_") {
		legacyName := strings.TrimSpace(description)
		if legacyName != "" {
			if _, ok := ratios[legacyName]; ok {
				return legacyName
			}
		}
	}
	return key
}

func isPositiveFiniteWebsiteGroupRatio(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func parseWebsiteGroupRatio(raw json.RawMessage) (float64, error) {
	var number float64
	if err := json.Unmarshal(raw, &number); err == nil {
		return number, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return 0, err
	}
	var value float64
	if _, err := fmt.Sscanf(strings.TrimSpace(text), "%f", &value); err != nil {
		return 0, err
	}
	return value, nil
}

type websiteGroupPricingResponse struct {
	UsableGroup map[string]json.RawMessage `json:"usable_group"`
	GroupRatio  map[string]json.RawMessage `json:"group_ratio"`
}

func (m *Monitor) fetchWebsiteGroupSources(ctx context.Context) ([]websiteGroupSource, int, error) {
	if m.prodDB == nil {
		return nil, 0, errors.New("生产库只读连接未配置")
	}
	base := strings.TrimRight(strings.TrimSpace(m.cfg.NewAPIBaseURL), "/")
	if base == "" {
		return nil, 0, errors.New("NewAPI 地址未配置")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/pricing", nil)
	if err != nil {
		return nil, 0, fmt.Errorf("构造分组目录请求失败: %w", err)
	}
	client := &http.Client{Timeout: 8 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return nil, 0, fmt.Errorf("读取 NewAPI 可选分组失败: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("读取 NewAPI 可选分组失败: HTTP %d", response.StatusCode)
	}
	var pricing websiteGroupPricingResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, maxWebsiteGroupPayload)).Decode(&pricing); err != nil {
		return nil, 0, fmt.Errorf("解析 NewAPI 分组目录失败: %w", err)
	}
	usable := make([]string, 0, len(pricing.UsableGroup))
	for name := range pricing.UsableGroup {
		usable = append(usable, name)
	}
	ratios := make(map[string]float64, len(pricing.GroupRatio))
	for name, raw := range pricing.GroupRatio {
		value, err := parseWebsiteGroupRatio(raw)
		if err != nil {
			continue
		}
		ratios[name] = value
	}
	special := map[string]map[string]string{}
	rows, err := m.prodDB.QueryContext(ctx, "SELECT `value` FROM options WHERE `key` = ? LIMIT 1", websiteGroupSpecialOption)
	if err != nil {
		return nil, 0, fmt.Errorf("读取分组特殊可用配置失败: %w", err)
	}
	if rows.Next() {
		var raw sql.NullString
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, 0, fmt.Errorf("读取分组特殊可用配置失败: %w", err)
		}
		if raw.Valid && strings.TrimSpace(raw.String) != "" {
			if err := json.Unmarshal([]byte(raw.String), &special); err != nil {
				rows.Close()
				return nil, 0, fmt.Errorf("解析分组特殊可用配置失败: %w", err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, 0, fmt.Errorf("读取分组特殊可用配置失败: %w", err)
	}
	rows.Close()
	sources, skipped := collectWebsiteGroupSources(usable, ratios, special)
	if len(sources) == 0 {
		return nil, skipped, errors.New("NewAPI 没有返回可用于计价的服务分组")
	}
	if len(sources) > maxWebsiteGroupSources {
		return nil, skipped, fmt.Errorf("网站分组超过安全上限 %d", maxWebsiteGroupSources)
	}
	return sources, skipped, nil
}

func (m *Monitor) loadWebsiteGroupRates(ctx context.Context, finance channelFinanceSnapshot) ([]ChannelWebsiteGroupRateView, int64, error) {
	var rows []WebsiteGroupCatalog
	if err := m.storeDB.WithContext(ctx).Where("active = ?", true).Order("grp ASC").Find(&rows).Error; err != nil {
		return nil, 0, fmt.Errorf("读取网站分组目录: %w", err)
	}
	views := make([]ChannelWebsiteGroupRateView, 0, len(rows))
	var syncedAt int64
	for _, row := range rows {
		view := ChannelWebsiteGroupRateView{Name: row.Grp, Source: row.Source, SourceMultiplier: row.SourceMultiplier, Active: row.Active, SyncedAt: row.SyncedAt}
		if row.SyncedAt > syncedAt {
			syncedAt = row.SyncedAt
		}
		if rate, ok := finance.siteGroups[row.Grp]; ok {
			view.SiteConfigured = true
			view.SiteMultiplier = rate.Multiplier
		}
		views = append(views, view)
	}
	return views, syncedAt, nil
}

type websiteGroupSyncInput struct {
	ConfirmUpdate    bool   `json:"confirm_update,omitempty"`
	ExpectedRevision string `json:"expected_global_revision,omitempty"`
}

func websiteGroupCatalogChanged(catalog []WebsiteGroupCatalog, site []ChannelSaleGroupRate, sources []websiteGroupSource) bool {
	want := make(map[string]float64, len(sources))
	for _, source := range sources {
		want[source.Name] = source.Multiplier
	}
	active := make(map[string]WebsiteGroupCatalog, len(catalog))
	for _, row := range catalog {
		if row.Active {
			active[row.Grp] = row
		}
	}
	if len(active) != len(want) {
		return true
	}
	for name, value := range want {
		row, ok := active[name]
		if !ok || math.Abs(row.SourceMultiplier-value) > 1e-9 {
			return true
		}
	}
	current := make(map[string]float64, len(site))
	for _, row := range site {
		current[row.Grp] = row.Multiplier
	}
	if len(current) != len(want) {
		return true
	}
	for name, value := range want {
		if math.Abs(current[name]-value) > 1e-9 {
			return true
		}
	}
	return false
}

func (m *Monitor) syncWebsiteGroupCatalogHandler(c *gin.Context) {
	if c.Request.ContentLength > maxChannelFinanceBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "配置内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelFinanceBody)
	var in websiteGroupSyncInput
	if err := c.ShouldBindJSON(&in); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "配置格式无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 12*time.Second)
	defer cancel()
	sources, skipped, err := m.fetchWebsiteGroupSources(ctx)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	now, updatedBy := time.Now().Unix(), c.GetString("uname")
	var currentRevision string
	var affectedDomains, updatedDomains int
	var unchanged bool
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		currentRevision, err = channelFinanceGlobalRevision(tx)
		if err != nil {
			return err
		}
		var catalog []WebsiteGroupCatalog
		if err := tx.Find(&catalog).Error; err != nil {
			return err
		}
		var site []ChannelSaleGroupRate
		if err := tx.Find(&site).Error; err != nil {
			return err
		}
		changed := websiteGroupCatalogChanged(catalog, site, sources)
		if !changed {
			unchanged = true
			return tx.Model(&WebsiteGroupCatalog{}).Where("active = ?", true).Updates(map[string]any{"source": "newapi", "synced_at": now}).Error
		}
		if (len(catalog) > 0 || len(site) > 0) && !in.ConfirmUpdate {
			return errChannelFinanceConfirmationRequired
		}
		if in.ConfirmUpdate && in.ExpectedRevision != currentRevision {
			return errChannelFinanceGlobalConflict
		}
		if err := ensureChannelFinanceSettingTx(tx, now, updatedBy); err != nil {
			return err
		}
		if err := tx.Model(&WebsiteGroupCatalog{}).Where("active = ?", true).Update("active", false).Error; err != nil {
			return err
		}
		// ChannelSaleGroupRate is the current website-wide configuration.  Groups
		// removed from NewAPI must leave this current table; their previous values
		// remain available in ChannelFinanceVersion snapshots for history.
		groupNames := make([]string, 0, len(sources))
		for _, source := range sources {
			groupNames = append(groupNames, source.Name)
		}
		if len(groupNames) == 0 {
			if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&ChannelSaleGroupRate{}).Error; err != nil {
				return err
			}
		} else if err := tx.Where("grp NOT IN ?", groupNames).Delete(&ChannelSaleGroupRate{}).Error; err != nil {
			return err
		}
		for _, source := range sources {
			row := WebsiteGroupCatalog{Grp: source.Name, Source: "newapi", SourceMultiplier: source.Multiplier, Active: true, SyncedAt: now}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "grp"}}, UpdateAll: true}).Create(&row).Error; err != nil {
				return err
			}
			rowRate := ChannelSaleGroupRate{Grp: source.Name, Multiplier: source.Multiplier, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "grp"}}, UpdateAll: true}).Create(&rowRate).Error; err != nil {
				return err
			}
		}
		domains, err := channelFinanceAllDomains(tx, "")
		if err != nil {
			return err
		}
		affectedDomains = len(domains)
		for _, domain := range domains {
			snapshot, err := currentChannelFinanceVersionJSON(tx, domain)
			if err != nil {
				return fmt.Errorf("生成 %s 倍率版本: %w", domain, err)
			}
			if _, appended, err := appendChannelFinanceVersion(tx, domain, snapshot, now, updatedBy); err != nil {
				return fmt.Errorf("保存 %s 倍率版本: %w", domain, err)
			} else if appended {
				updatedDomains++
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errChannelFinanceConfirmationRequired) {
			c.JSON(http.StatusConflict, gin.H{"error": "确认后将以 NewAPI 当前用户可用分组和倍率更新网站计价基准，并为受影响主域名追加版本", "confirmation_required": true, "current_global_revision": currentRevision, "affected_domains": affectedDomains, "group_count": len(sources)})
			return
		}
		if errors.Is(err, errChannelFinanceGlobalConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "网站计价基准已被其他会话更新，请重新打开后再同步", "version_conflict": true, "current_global_revision": currentRevision})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "同步网站分组失败"})
		return
	}
	newRevision, _ := channelFinanceGlobalRevision(m.storeDB.WithContext(ctx))
	c.JSON(http.StatusOK, gin.H{"ok": true, "unchanged": unchanged, "group_count": len(sources), "skipped": skipped, "updated_domains": updatedDomains, "affected_domains": affectedDomains, "current_global_revision": newRevision, "synced_at": now})
}
