package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
	defaultChannelFinanceFX = 7.0
	maxChannelFinanceGroups = 500
	maxChannelFinanceRows   = 2000
	maxChannelFinanceBody   = 128 << 10
)

// ChannelFinanceSetting 是渠道毛利率计算使用的我方公共计价基准。
// 该配置只保存在 Monitor 本地 SQLite，不读取或改写 NewAPI 的计价配置。
type ChannelFinanceSetting struct {
	ID                 int     `gorm:"primaryKey;autoIncrement:false"`
	FXBenchmark        float64 `gorm:"column:fx_benchmark"`
	SiteRechargePaid   float64 `gorm:"column:site_recharge_paid"`
	SiteRechargeCredit float64 `gorm:"column:site_recharge_credit"`
	EffectiveAt        int64   `gorm:"column:effective_at"`
	UpdatedAt          int64   `gorm:"column:updated_at;index"`
	UpdatedBy          string  `gorm:"column:updated_by;size:128"`
}

// ChannelSaleGroupRate 是我方某服务分组对用户采用的计价倍率。
// 同一服务分组在所有上游主域名下共用该倍率。
type ChannelSaleGroupRate struct {
	Grp         string  `gorm:"primaryKey;size:64;column:grp"`
	Multiplier  float64 `gorm:"column:multiplier"`
	EffectiveAt int64   `gorm:"column:effective_at"`
	UpdatedAt   int64   `gorm:"column:updated_at;index"`
	UpdatedBy   string  `gorm:"column:updated_by;size:128"`
}

// ChannelDomainCost 是一个上游主域名的充值兑换关系。
type ChannelDomainCost struct {
	Domain         string  `gorm:"primaryKey;size:253;column:domain"`
	RechargePaid   float64 `gorm:"column:recharge_paid"`
	RechargeCredit float64 `gorm:"column:recharge_credit"`
	EffectiveAt    int64   `gorm:"column:effective_at"`
	UpdatedAt      int64   `gorm:"column:updated_at;index"`
	UpdatedBy      string  `gorm:"column:updated_by;size:128"`
}

// ChannelDomainGroupCost 是上游主域名在某服务分组下采用的倍率。
type ChannelDomainGroupCost struct {
	Domain         string  `gorm:"primaryKey;size:253;column:domain"`
	Grp            string  `gorm:"primaryKey;size:64;column:grp"`
	Multiplier     float64 `gorm:"column:multiplier"` // 上游公布的基础倍率
	DiscountFactor float64 `gorm:"column:discount_factor;not null;default:1"`
	EffectiveAt    int64   `gorm:"column:effective_at"`
	UpdatedAt      int64   `gorm:"column:updated_at;index"`
	UpdatedBy      string  `gorm:"column:updated_by;size:128"`
}

// ChannelFinanceChannelCost 保存某个具体渠道在某服务分组上的上游成本口径。
// 产品层级的倍率属于渠道而不是服务分组；主域名批量保存时会将同一组
// 基础倍率和折扣系数镜像到该渠道关联的每个服务分组，以兼容现有按渠道+分组
// 计算和历史快照结构，避免同一渠道出现不同倍率。
// 渠道 ID 使用本地 ChannelSnap 的快照 ID；即使 NewAPI 后续删除渠道，
// 这条配置仍保留，历史版本仍可按原渠道解释。旧版只按主域名+分组保存的
// ChannelDomainGroupCost 仍会作为没有渠道级配置时的兼容回退。
type ChannelFinanceChannelCost struct {
	ChannelID         int     `gorm:"primaryKey;autoIncrement:false;column:channel_id"`
	Grp               string  `gorm:"primaryKey;size:64;column:grp"`
	UpstreamGroupName string  `gorm:"size:128;column:upstream_group_name"`
	Multiplier        float64 `gorm:"column:multiplier"`
	DiscountFactor    float64 `gorm:"column:discount_factor;not null;default:1"`
	EffectiveAt       int64   `gorm:"column:effective_at"`
	UpdatedAt         int64   `gorm:"column:updated_at;index"`
	UpdatedBy         string  `gorm:"column:updated_by;size:128"`
}

// ChannelFinanceAudit 是早期未发布实现的兼容读取模型。
// 新版启动时会将其中的快照迁移为正式倍率版本，之后不再写入该表。
type ChannelFinanceAudit struct {
	ID           uint64 `gorm:"primaryKey;autoIncrement"`
	Domain       string `gorm:"size:253;column:domain;index"`
	SnapshotJSON string `gorm:"type:text;column:snapshot_json"`
	CreatedAt    int64  `gorm:"column:created_at;index"`
	UpdatedBy    string `gorm:"column:updated_by;size:128"`
}

// ChannelFinanceVersion 是某个归并主域名的追加式倍率版本。
// SnapshotJSON 保存该版本完整的双方倍率与兑换口径，不包含 API Key、完整 URL 或其他凭据。
// 旧版本永不更新；修改配置只会为同一 Domain 追加下一个 Version。
type ChannelFinanceVersion struct {
	ID           uint64 `gorm:"primaryKey;autoIncrement"`
	Domain       string `gorm:"size:253;column:domain;uniqueIndex:idx_channel_finance_domain_version,priority:1"`
	Version      int64  `gorm:"column:version;uniqueIndex:idx_channel_finance_domain_version,priority:2"`
	SnapshotJSON string `gorm:"type:text;column:snapshot_json"`
	EffectiveAt  int64  `gorm:"column:effective_at;index"`
	CreatedAt    int64  `gorm:"column:created_at;index"`
	UpdatedBy    string `gorm:"column:updated_by;size:128"`
}

type ChannelFinanceSettingsView struct {
	Configured         bool    `json:"configured"`
	CanEdit            bool    `json:"can_edit"`
	FXBenchmark        float64 `json:"fx_benchmark"`
	SiteRechargePaid   float64 `json:"site_recharge_paid"`
	SiteRechargeCredit float64 `json:"site_recharge_credit"`
	EffectiveAt        int64   `json:"effective_at"`
	UpdatedAt          int64   `json:"updated_at"`
	UpdatedBy          string  `json:"updated_by"`
}

type ChannelDomainFinanceView struct {
	Configured     bool    `json:"configured"`
	RechargePaid   float64 `json:"recharge_paid"`
	RechargeCredit float64 `json:"recharge_credit"`
	Version        int64   `json:"version"`
	EffectiveAt    int64   `json:"effective_at"`
	UpdatedAt      int64   `json:"updated_at"`
	UpdatedBy      string  `json:"updated_by"`
}

// ChannelGroupFinanceView 在双方口径完整时给出上游实际倍率、倍率差、计价折扣和毛利率。
// UpstreamEffectiveMultiplier = 上游基础倍率 × 折扣系数 ÷ 充值比例(到账÷支付)。
// MultiplierGap = 我方倍率 - 上游实际倍率；它不是毛利率。
// Discount 为相对官方美元标价的比例，例如 1.3/7 = 0.185714（18.57%）。
// EstimatedMargin 是预估毛利率，不包含支付手续费、汇损、税费或退款等费用。
type ChannelGroupFinanceView struct {
	Complete                    bool    `json:"complete"`
	SiteConfigured              bool    `json:"site_configured"`
	UpstreamConfigured          bool    `json:"upstream_configured"`
	SiteMultiplier              float64 `json:"site_multiplier"`
	UpstreamMultiplier          float64 `json:"upstream_multiplier"` // 基础倍率
	UpstreamDiscountFactor      float64 `json:"upstream_discount_factor"`
	UpstreamGroupName           string  `json:"upstream_group_name"`
	UpstreamEffectiveMultiplier float64 `json:"upstream_effective_multiplier"`
	MultiplierGap               float64 `json:"multiplier_gap"`
	SiteDiscount                float64 `json:"site_discount"`
	UpstreamDiscount            float64 `json:"upstream_discount"`
	EstimatedMargin             float64 `json:"estimated_margin"`
}

type channelFinanceSnapshot struct {
	settings         ChannelFinanceSetting
	hasSettings      bool
	siteGroups       map[string]ChannelSaleGroupRate
	domainCosts      map[string]ChannelDomainCost
	domainGroupCost  map[string]map[string]ChannelDomainGroupCost
	channelGroupCost map[int]map[string]ChannelFinanceChannelCost
	domainVersions   map[string]ChannelFinanceVersion
}

func defaultChannelFinanceSettingsView() ChannelFinanceSettingsView {
	return ChannelFinanceSettingsView{
		FXBenchmark: defaultChannelFinanceFX, SiteRechargePaid: 1, SiteRechargeCredit: 1,
	}
}

func (s channelFinanceSnapshot) settingsView() ChannelFinanceSettingsView {
	view := defaultChannelFinanceSettingsView()
	if !s.hasSettings {
		return view
	}
	view.Configured = true
	view.FXBenchmark = s.settings.FXBenchmark
	view.SiteRechargePaid = s.settings.SiteRechargePaid
	view.SiteRechargeCredit = s.settings.SiteRechargeCredit
	view.EffectiveAt = s.settings.EffectiveAt
	view.UpdatedAt = s.settings.UpdatedAt
	view.UpdatedBy = s.settings.UpdatedBy
	return view
}

func (s channelFinanceSnapshot) domainView(domain string) ChannelDomainFinanceView {
	cost, ok := s.domainCosts[domain]
	if !ok {
		return ChannelDomainFinanceView{}
	}
	view := ChannelDomainFinanceView{
		Configured: true, RechargePaid: cost.RechargePaid, RechargeCredit: cost.RechargeCredit,
		EffectiveAt: cost.EffectiveAt, UpdatedAt: cost.UpdatedAt, UpdatedBy: cost.UpdatedBy,
	}
	if version, exists := s.domainVersions[domain]; exists {
		view.Version = version.Version
		view.EffectiveAt = version.EffectiveAt
		view.UpdatedAt = version.CreatedAt
		view.UpdatedBy = version.UpdatedBy
	}
	return view
}

func (s channelFinanceSnapshot) groupView(domain, group string) ChannelGroupFinanceView {
	return s.groupViewForChannel(domain, 0, group)
}

func (s channelFinanceSnapshot) groupViewForChannel(domain string, channelID int, group string) ChannelGroupFinanceView {
	view := ChannelGroupFinanceView{}
	site, siteOK := s.siteGroups[group]
	upstreamByGroup := s.domainGroupCost[domain]
	upstream, upstreamOK := upstreamByGroup[group]
	upstreamGroupName := ""
	if channelID > 0 {
		if channelRates := s.channelGroupCost[channelID]; channelRates != nil {
			if channelRate, exists := channelRates[group]; exists {
				upstream = ChannelDomainGroupCost{Domain: domain, Grp: group, Multiplier: channelRate.Multiplier, DiscountFactor: channelRate.DiscountFactor}
				upstreamOK = true
				upstreamGroupName = strings.TrimSpace(channelRate.UpstreamGroupName)
			}
		}
	}
	view.SiteConfigured = siteOK
	view.UpstreamConfigured = upstreamOK
	if siteOK {
		view.SiteMultiplier = site.Multiplier
	}
	if upstreamOK {
		view.UpstreamMultiplier = upstream.Multiplier
		view.UpstreamDiscountFactor = normalizedUpstreamDiscountFactor(upstream.DiscountFactor)
		view.UpstreamGroupName = upstreamGroupName
	}
	domainCost, domainOK := s.domainCosts[domain]
	if domainOK && upstreamOK && validChannelFinanceNumber(upstream.Multiplier) && validChannelFinanceNumber(view.UpstreamDiscountFactor) &&
		validChannelFinanceNumber(domainCost.RechargePaid) && validChannelFinanceNumber(domainCost.RechargeCredit) {
		// 充值比例定义为“到账 ÷ 支付”，故实际倍率 = 基础倍率 × 折扣系数 ÷ 充值比例。
		view.UpstreamEffectiveMultiplier = upstream.Multiplier * view.UpstreamDiscountFactor * domainCost.RechargePaid / domainCost.RechargeCredit
		if siteOK {
			view.MultiplierGap = site.Multiplier - view.UpstreamEffectiveMultiplier
		}
	}
	if !s.hasSettings || !siteOK || !domainOK || !upstreamOK {
		return view
	}
	if !validChannelFinanceNumber(s.settings.FXBenchmark) ||
		!validChannelFinanceNumber(s.settings.SiteRechargePaid) ||
		!validChannelFinanceNumber(s.settings.SiteRechargeCredit) ||
		!validChannelFinanceNumber(site.Multiplier) ||
		!validChannelFinanceNumber(domainCost.RechargePaid) ||
		!validChannelFinanceNumber(domainCost.RechargeCredit) ||
		!validChannelFinanceNumber(upstream.Multiplier) ||
		!validChannelFinanceNumber(view.UpstreamDiscountFactor) ||
		!validChannelFinanceNumber(view.UpstreamEffectiveMultiplier) {
		return view
	}
	view.SiteDiscount = site.Multiplier * (s.settings.SiteRechargePaid / s.settings.SiteRechargeCredit) / s.settings.FXBenchmark
	view.UpstreamDiscount = view.UpstreamEffectiveMultiplier / s.settings.FXBenchmark
	if view.SiteDiscount <= 0 || math.IsNaN(view.SiteDiscount) || math.IsInf(view.SiteDiscount, 0) ||
		math.IsNaN(view.UpstreamDiscount) || math.IsInf(view.UpstreamDiscount, 0) {
		return view
	}
	view.EstimatedMargin = (view.SiteDiscount - view.UpstreamDiscount) / view.SiteDiscount
	view.Complete = true
	return view
}

// 旧版表与版本快照没有折扣系数；零值按 1 解释，保证升级前后的计算结果不变。
func normalizedUpstreamDiscountFactor(v float64) float64 {
	if v == 0 {
		return 1
	}
	return v
}

func (m *Monitor) loadChannelFinanceSnapshot(ctx context.Context) (channelFinanceSnapshot, error) {
	s := channelFinanceSnapshot{
		siteGroups: map[string]ChannelSaleGroupRate{}, domainCosts: map[string]ChannelDomainCost{},
		domainGroupCost: map[string]map[string]ChannelDomainGroupCost{}, channelGroupCost: map[int]map[string]ChannelFinanceChannelCost{},
		domainVersions: map[string]ChannelFinanceVersion{},
	}
	var setting ChannelFinanceSetting
	tx := m.storeDB.WithContext(ctx).First(&setting, "id = ?", 1)
	if tx.Error == nil {
		s.settings, s.hasSettings = setting, true
	} else if !errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return s, fmt.Errorf("读取我方计价配置: %w", tx.Error)
	}
	var siteRates []ChannelSaleGroupRate
	if tx := m.storeDB.WithContext(ctx).Find(&siteRates); tx.Error != nil {
		return s, fmt.Errorf("读取我方分组倍率: %w", tx.Error)
	}
	for _, rate := range siteRates {
		s.siteGroups[rate.Grp] = rate
	}
	var domainCosts []ChannelDomainCost
	if tx := m.storeDB.WithContext(ctx).Find(&domainCosts); tx.Error != nil {
		return s, fmt.Errorf("读取上游充值配置: %w", tx.Error)
	}
	for _, cost := range domainCosts {
		s.domainCosts[cost.Domain] = cost
	}
	var groupCosts []ChannelDomainGroupCost
	if tx := m.storeDB.WithContext(ctx).Find(&groupCosts); tx.Error != nil {
		return s, fmt.Errorf("读取上游分组倍率: %w", tx.Error)
	}
	for _, cost := range groupCosts {
		if s.domainGroupCost[cost.Domain] == nil {
			s.domainGroupCost[cost.Domain] = map[string]ChannelDomainGroupCost{}
		}
		s.domainGroupCost[cost.Domain][cost.Grp] = cost
	}
	var channelCosts []ChannelFinanceChannelCost
	if tx := m.storeDB.WithContext(ctx).Find(&channelCosts); tx.Error != nil {
		return s, fmt.Errorf("读取渠道级上游倍率: %w", tx.Error)
	}
	for _, cost := range channelCosts {
		if s.channelGroupCost[cost.ChannelID] == nil {
			s.channelGroupCost[cost.ChannelID] = map[string]ChannelFinanceChannelCost{}
		}
		s.channelGroupCost[cost.ChannelID][cost.Grp] = cost
	}
	var versions []ChannelFinanceVersion
	if tx := m.storeDB.WithContext(ctx).Order("domain ASC, version DESC").Find(&versions); tx.Error != nil {
		return s, fmt.Errorf("读取渠道倍率版本: %w", tx.Error)
	}
	for _, version := range versions {
		if _, exists := s.domainVersions[version.Domain]; !exists {
			s.domainVersions[version.Domain] = version
		}
	}
	return s, nil
}

type channelFinanceGroupInput struct {
	Group                  string   `json:"group"`
	SiteMultiplier         *float64 `json:"site_multiplier"`
	UpstreamMultiplier     *float64 `json:"upstream_multiplier"`
	UpstreamDiscountFactor *float64 `json:"upstream_discount_factor"`
}

type channelFinanceSaveInput struct {
	Domain                 string                     `json:"domain"`
	FXBenchmark            float64                    `json:"fx_benchmark"`
	SiteRechargePaid       float64                    `json:"site_recharge_paid"`
	SiteRechargeCredit     float64                    `json:"site_recharge_credit"`
	UpstreamRechargePaid   float64                    `json:"upstream_recharge_paid"`
	UpstreamRechargeCredit float64                    `json:"upstream_recharge_credit"`
	Groups                 []channelFinanceGroupInput `json:"groups"`
	ConfirmUpdate          bool                       `json:"confirm_update,omitempty"`
	ExpectedVersion        int64                      `json:"expected_version,omitempty"`
	ExpectedGlobalRevision string                     `json:"expected_global_revision,omitempty"`
}

type channelFinanceVersionGroup struct {
	Group                  string  `json:"group"`
	SiteMultiplier         float64 `json:"site_multiplier"`
	UpstreamMultiplier     float64 `json:"upstream_multiplier"`
	UpstreamDiscountFactor float64 `json:"upstream_discount_factor"`
}

type channelFinanceVersionChannel struct {
	ChannelID              int     `json:"channel_id"`
	Group                  string  `json:"group"`
	UpstreamGroupName      string  `json:"upstream_group_name,omitempty"`
	UpstreamMultiplier     float64 `json:"upstream_multiplier"`
	UpstreamDiscountFactor float64 `json:"upstream_discount_factor"`
}

// channelFinanceVersionSnapshot 是版本表中的稳定序列化结构。
// 确认标记等请求控制字段不进入版本数据。
type channelFinanceVersionSnapshot struct {
	Domain                 string                         `json:"domain"`
	FXBenchmark            float64                        `json:"fx_benchmark"`
	SiteRechargePaid       float64                        `json:"site_recharge_paid"`
	SiteRechargeCredit     float64                        `json:"site_recharge_credit"`
	UpstreamRechargePaid   float64                        `json:"upstream_recharge_paid"`
	UpstreamRechargeCredit float64                        `json:"upstream_recharge_credit"`
	Groups                 []channelFinanceVersionGroup   `json:"groups"`
	ChannelRates           []channelFinanceVersionChannel `json:"channel_rates,omitempty"`
}

func channelFinanceVersionSnapshotOf(in channelFinanceSaveInput) channelFinanceVersionSnapshot {
	snapshot := channelFinanceVersionSnapshot{
		Domain: in.Domain, FXBenchmark: in.FXBenchmark,
		SiteRechargePaid: in.SiteRechargePaid, SiteRechargeCredit: in.SiteRechargeCredit,
		UpstreamRechargePaid: in.UpstreamRechargePaid, UpstreamRechargeCredit: in.UpstreamRechargeCredit,
		Groups: make([]channelFinanceVersionGroup, 0, len(in.Groups)),
	}
	for _, group := range in.Groups {
		if group.SiteMultiplier == nil || group.UpstreamMultiplier == nil || group.UpstreamDiscountFactor == nil {
			continue
		}
		snapshot.Groups = append(snapshot.Groups, channelFinanceVersionGroup{
			Group: group.Group, SiteMultiplier: *group.SiteMultiplier, UpstreamMultiplier: *group.UpstreamMultiplier,
			UpstreamDiscountFactor: *group.UpstreamDiscountFactor,
		})
	}
	sort.Slice(snapshot.Groups, func(i, j int) bool { return snapshot.Groups[i].Group < snapshot.Groups[j].Group })
	return snapshot
}

func marshalChannelFinanceVersion(in channelFinanceSaveInput) (string, error) {
	raw, err := json.Marshal(channelFinanceVersionSnapshotOf(in))
	return string(raw), err
}

func normalizeChannelFinanceVersionJSON(raw string) (string, error) {
	var snapshot channelFinanceVersionSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return "", err
	}
	for i := range snapshot.Groups {
		snapshot.Groups[i].UpstreamDiscountFactor = normalizedUpstreamDiscountFactor(snapshot.Groups[i].UpstreamDiscountFactor)
	}
	for i := range snapshot.ChannelRates {
		snapshot.ChannelRates[i].UpstreamDiscountFactor = normalizedUpstreamDiscountFactor(snapshot.ChannelRates[i].UpstreamDiscountFactor)
	}
	sort.Slice(snapshot.Groups, func(i, j int) bool { return snapshot.Groups[i].Group < snapshot.Groups[j].Group })
	sort.Slice(snapshot.ChannelRates, func(i, j int) bool {
		if snapshot.ChannelRates[i].ChannelID != snapshot.ChannelRates[j].ChannelID {
			return snapshot.ChannelRates[i].ChannelID < snapshot.ChannelRates[j].ChannelID
		}
		return snapshot.ChannelRates[i].Group < snapshot.ChannelRates[j].Group
	})
	normalized, err := json.Marshal(snapshot)
	return string(normalized), err
}

var (
	errChannelFinanceConfirmationRequired = errors.New("倍率版本更新需要确认")
	errChannelFinanceVersionConflict      = errors.New("倍率版本已变更")
	errChannelFinanceGlobalConflict       = errors.New("全局计价基准已变更")
)

func channelFinanceGlobalChanged(tx *gorm.DB, in channelFinanceSaveInput) (bool, int64, error) {
	var setting ChannelFinanceSetting
	err := tx.First(&setting, "id = ?", 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return true, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	changed := setting.FXBenchmark != in.FXBenchmark ||
		setting.SiteRechargePaid != in.SiteRechargePaid ||
		setting.SiteRechargeCredit != in.SiteRechargeCredit
	groups := make([]string, 0, len(in.Groups))
	want := make(map[string]float64, len(in.Groups))
	for _, group := range in.Groups {
		groups = append(groups, group.Group)
		want[group.Group] = *group.SiteMultiplier
	}
	if len(groups) > 0 {
		var rows []ChannelSaleGroupRate
		if err := tx.Where("grp IN ?", groups).Find(&rows).Error; err != nil {
			return false, 0, err
		}
		seen := make(map[string]bool, len(rows))
		for _, row := range rows {
			seen[row.Grp] = true
			if want[row.Grp] != row.Multiplier {
				changed = true
			}
		}
		for _, group := range groups {
			if !seen[group] {
				changed = true
			}
		}
	}
	return changed, setting.UpdatedAt, nil
}

func channelFinanceGlobalRevision(tx *gorm.DB) (string, error) {
	var setting ChannelFinanceSetting
	err := tx.First(&setting, "id = ?", 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		setting = ChannelFinanceSetting{}
	} else if err != nil {
		return "", err
	}
	var groups []ChannelSaleGroupRate
	if err := tx.Order("grp ASC").Find(&groups).Error; err != nil {
		return "", err
	}
	raw, err := json.Marshal(struct {
		FXBenchmark, SiteRechargePaid, SiteRechargeCredit float64
		Groups                                            []ChannelSaleGroupRate
	}{setting.FXBenchmark, setting.SiteRechargePaid, setting.SiteRechargeCredit, groups})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:]), nil
}

func channelFinanceDomains(tx *gorm.DB, include string) ([]string, error) {
	var rows []struct{ Domain string }
	if err := tx.Model(&ChannelDomainCost{}).Select("domain").Scan(&rows).Error; err != nil {
		return nil, err
	}
	set := map[string]bool{}
	if include != "" {
		set[include] = true
	}
	for _, row := range rows {
		if domain := strings.TrimSpace(row.Domain); domain != "" {
			set[domain] = true
		}
	}
	out := make([]string, 0, len(set))
	for domain := range set {
		out = append(out, domain)
	}
	sort.Strings(out)
	return out, nil
}

// channelFinanceAllDomains 在网站级口径变更时使用：除已有充值配置外，
// 还要覆盖渠道快照中已经归并、但尚未填写充值比例的主域名。
func channelFinanceAllDomains(tx *gorm.DB, include string) ([]string, error) {
	domains, err := channelFinanceDomains(tx, include)
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(domains))
	for _, domain := range domains {
		set[domain] = true
	}
	var rows []struct{ Domain string }
	if err := tx.Model(&ChannelSnap{}).Select("base_domain AS domain").Where("base_domain <> ''").Distinct().Scan(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		if domain := strings.TrimSpace(row.Domain); domain != "" {
			set[domain] = true
		}
	}
	out := make([]string, 0, len(set))
	for domain := range set {
		out = append(out, domain)
	}
	sort.Strings(out)
	return out, nil
}

// currentChannelFinanceVersionJSON 从当前状态表生成某主域名的完整版本快照。
// 全局口径变化时可为所有主域名各追加一版，从而保证任何渠道的计算变化都有版本依据。
func currentChannelFinanceVersionJSON(tx *gorm.DB, domain string) (string, error) {
	var setting ChannelFinanceSetting
	if err := tx.First(&setting, "id = ?", 1).Error; err != nil {
		return "", err
	}
	var domainCost ChannelDomainCost
	domainCostErr := tx.First(&domainCost, "domain = ?", domain).Error
	if domainCostErr != nil && !errors.Is(domainCostErr, gorm.ErrRecordNotFound) {
		return "", domainCostErr
	}
	if errors.Is(domainCostErr, gorm.ErrRecordNotFound) {
		// 渠道级成本可以先于主域名充值比例配置；版本先记录默认 1:1，
		// 后续保存主域名充值比例时会追加一版，不阻塞渠道口径维护。
		domainCost = ChannelDomainCost{Domain: domain, RechargePaid: 1, RechargeCredit: 1}
	}
	var upstream []ChannelDomainGroupCost
	if err := tx.Where("domain = ?", domain).Order("grp ASC").Find(&upstream).Error; err != nil {
		return "", err
	}
	var site []ChannelSaleGroupRate
	if err := tx.Order("grp ASC").Find(&site).Error; err != nil {
		return "", err
	}
	siteByGroup := map[string]ChannelSaleGroupRate{}
	groupSet := map[string]bool{}
	for _, row := range site {
		siteByGroup[row.Grp] = row
		groupSet[row.Grp] = true
	}
	upstreamByGroup := map[string]ChannelDomainGroupCost{}
	for _, row := range upstream {
		upstreamByGroup[row.Grp] = row
		groupSet[row.Grp] = true
	}
	groupNames := make([]string, 0, len(groupSet))
	for name := range groupSet {
		if strings.TrimSpace(name) != "" {
			groupNames = append(groupNames, name)
		}
	}
	sort.Strings(groupNames)
	snapshot := channelFinanceVersionSnapshot{
		Domain: domain, FXBenchmark: setting.FXBenchmark,
		SiteRechargePaid: setting.SiteRechargePaid, SiteRechargeCredit: setting.SiteRechargeCredit,
		UpstreamRechargePaid: domainCost.RechargePaid, UpstreamRechargeCredit: domainCost.RechargeCredit,
	}
	for _, name := range groupNames {
		site, exists := siteByGroup[name]
		if !exists {
			continue
		}
		upstream := upstreamByGroup[name]
		snapshot.Groups = append(snapshot.Groups, channelFinanceVersionGroup{
			Group: name, SiteMultiplier: site.Multiplier, UpstreamMultiplier: upstream.Multiplier,
			UpstreamDiscountFactor: normalizedUpstreamDiscountFactor(upstream.DiscountFactor),
		})
	}
	var snaps []ChannelSnap
	if err := tx.Where("base_domain = ?", domain).Find(&snaps).Error; err != nil {
		return "", err
	}
	channelIDs := make([]int, 0, len(snaps))
	for _, snap := range snaps {
		channelIDs = append(channelIDs, snap.ID)
	}
	if len(channelIDs) > 0 {
		var rates []ChannelFinanceChannelCost
		if err := tx.Where("channel_id IN ?", channelIDs).Order("channel_id ASC, grp ASC").Find(&rates).Error; err != nil {
			return "", err
		}
		for _, rate := range rates {
			snapshot.ChannelRates = append(snapshot.ChannelRates, channelFinanceVersionChannel{
				ChannelID: rate.ChannelID, Group: rate.Grp, UpstreamGroupName: strings.TrimSpace(rate.UpstreamGroupName), UpstreamMultiplier: rate.Multiplier,
				UpstreamDiscountFactor: normalizedUpstreamDiscountFactor(rate.DiscountFactor),
			})
		}
	}
	raw, err := json.Marshal(snapshot)
	return string(raw), err
}

func appendChannelFinanceVersion(tx *gorm.DB, domain, snapshot string, now int64, updatedBy string) (int64, bool, error) {
	var latest ChannelFinanceVersion
	err := tx.Where("domain = ?", domain).Order("version DESC").First(&latest).Error
	if err == nil {
		previous, normalizeErr := normalizeChannelFinanceVersionJSON(latest.SnapshotJSON)
		if normalizeErr != nil {
			return 0, false, normalizeErr
		}
		normalized, normalizeErr := normalizeChannelFinanceVersionJSON(snapshot)
		if normalizeErr != nil {
			return 0, false, normalizeErr
		}
		if previous == normalized {
			return latest.Version, false, nil
		}
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, false, err
	}
	version := latest.Version + 1
	return version, true, tx.Create(&ChannelFinanceVersion{
		Domain: domain, Version: version, SnapshotJSON: snapshot,
		EffectiveAt: now, CreatedAt: now, UpdatedBy: updatedBy,
	}).Error
}

// migrateLegacyChannelFinanceVersions 将早期快照一次性转为每个主域名独立编号的倍率版本。
// 迁移只追加新表，不删除或改写旧快照。
func migrateLegacyChannelFinanceVersions(db *gorm.DB) error {
	if !db.Migrator().HasTable(&ChannelFinanceAudit{}) {
		return nil
	}
	var existing []struct {
		Domain string
		Count  int64
	}
	if err := db.Model(&ChannelFinanceVersion{}).Select("domain, COUNT(*) count").Group("domain").Scan(&existing).Error; err != nil {
		return err
	}
	hasVersions := make(map[string]bool, len(existing))
	for _, row := range existing {
		if row.Count > 0 {
			hasVersions[row.Domain] = true
		}
	}
	var legacy []ChannelFinanceAudit
	if err := db.Order("domain ASC, created_at ASC, id ASC").Find(&legacy).Error; err != nil {
		return err
	}
	if len(legacy) == 0 {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		versions := map[string]int64{}
		for _, row := range legacy {
			// 按主域名幂等迁移：某个域已有新版本时不重复导入，
			// 但不能因其中一个域已迁移，就漏掉其他域的旧快照。
			if hasVersions[row.Domain] {
				continue
			}
			versions[row.Domain]++
			effectiveAt := row.CreatedAt
			if effectiveAt <= 0 {
				effectiveAt = time.Now().Unix()
			}
			version := ChannelFinanceVersion{
				Domain: row.Domain, Version: versions[row.Domain], SnapshotJSON: row.SnapshotJSON,
				EffectiveAt: effectiveAt, CreatedAt: effectiveAt, UpdatedBy: row.UpdatedBy,
			}
			if err := tx.Create(&version).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func validChannelFinanceNumber(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0.000000001 && v <= 1_000_000_000
}

func validateChannelFinanceInput(in *channelFinanceSaveInput) error {
	in.Domain = strings.TrimSpace(strings.ToLower(in.Domain))
	if in.Domain == "" || normalizeChannelBaseDomain(in.Domain) != in.Domain {
		return fmt.Errorf("主域名无效")
	}
	for label, value := range map[string]float64{
		"折扣基准": in.FXBenchmark, "我方充值支付": in.SiteRechargePaid,
		"我方充值到账": in.SiteRechargeCredit, "上游充值支付": in.UpstreamRechargePaid,
		"上游充值到账": in.UpstreamRechargeCredit,
	} {
		if !validChannelFinanceNumber(value) {
			return fmt.Errorf("%s必须是大于 0 的有限数字", label)
		}
	}
	if len(in.Groups) > maxChannelFinanceGroups {
		return fmt.Errorf("服务分组超过安全上限 %d", maxChannelFinanceGroups)
	}
	if in.ExpectedVersion < 0 {
		return fmt.Errorf("期望版本无效")
	}
	if len(in.ExpectedGlobalRevision) > 64 || strings.ContainsAny(in.ExpectedGlobalRevision, "\r\n\x00") {
		return fmt.Errorf("期望全局修订无效")
	}
	seen := map[string]bool{}
	for i := range in.Groups {
		group := strings.TrimSpace(in.Groups[i].Group)
		in.Groups[i].Group = group
		if group == "" || len([]rune(group)) > 64 || strings.ContainsAny(group, "\r\n\x00") {
			return fmt.Errorf("服务分组名称无效")
		}
		if seen[group] {
			return fmt.Errorf("服务分组 %s 重复", group)
		}
		seen[group] = true
		if in.Groups[i].SiteMultiplier == nil || in.Groups[i].UpstreamMultiplier == nil || in.Groups[i].UpstreamDiscountFactor == nil {
			return fmt.Errorf("服务分组 %s 必须同时填写我方倍率、上游基础倍率和上游折扣系数", group)
		}
		if !validChannelFinanceNumber(*in.Groups[i].SiteMultiplier) || !validChannelFinanceNumber(*in.Groups[i].UpstreamMultiplier) ||
			!validChannelFinanceNumber(*in.Groups[i].UpstreamDiscountFactor) {
			return fmt.Errorf("服务分组 %s 的倍率和折扣系数必须是大于 0 的有限数字", group)
		}
	}
	return nil
}

func (m *Monitor) allowedChannelFinanceGroups(ctx context.Context, domain string) (map[string]bool, error) {
	var rows []struct{ Groups string }
	if tx := m.storeDB.WithContext(ctx).Model(&ChannelSnap{}).Select("groups").Where("base_domain = ?", domain).Scan(&rows); tx.Error != nil {
		return nil, fmt.Errorf("核对主域名配置: %w", tx.Error)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("该主域名不属于当前渠道配置")
	}
	allowed := map[string]bool{}
	for _, row := range rows {
		for _, group := range splitList(row.Groups) {
			allowed[group] = true
		}
	}
	var history []struct{ Grp string }
	if tx := m.storeDB.WithContext(ctx).Raw(`SELECT DISTINCT s.grp FROM stability_hour_samples s
		JOIN channel_snaps c ON c.id=s.channel_id WHERE c.base_domain=? AND s.grp<>'' LIMIT ?`, domain, maxChannelFinanceGroups+1).Scan(&history); tx.Error != nil {
		return nil, fmt.Errorf("核对历史服务分组: %w", tx.Error)
	}
	if len(history) > maxChannelFinanceGroups {
		return nil, fmt.Errorf("历史服务分组超过安全上限 %d", maxChannelFinanceGroups)
	}
	for _, row := range history {
		allowed[strings.TrimSpace(row.Grp)] = true
	}
	return allowed, nil
}

func (m *Monitor) saveChannelFinanceHandler(c *gin.Context) {
	if c.Request.ContentLength > maxChannelFinanceBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "配置内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelFinanceBody)
	var in channelFinanceSaveInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(400, gin.H{"error": "配置格式无效"})
		return
	}
	if err := validateChannelFinanceInput(&in); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	allowed, err := m.allowedChannelFinanceGroups(ctx, in.Domain)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	for _, group := range in.Groups {
		if !allowed[group.Group] {
			c.JSON(400, gin.H{"error": fmt.Sprintf("服务分组 %s 不属于该主域名", group.Group)})
			return
		}
	}
	now, updatedBy := time.Now().Unix(), c.GetString("uname")
	incomingSnapshot, err := marshalChannelFinanceVersion(in)
	if err != nil {
		c.JSON(500, gin.H{"error": "生成倍率版本失败"})
		return
	}
	var version, effectiveAt, currentGlobalUpdatedAt int64
	var currentGlobalRevision string
	var unchanged, globalChanged bool
	var affectedDomains, appendedDomains int
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var latest ChannelFinanceVersion
		latestErr := tx.Where("domain = ?", in.Domain).Order("version DESC").First(&latest).Error
		latestExists := latestErr == nil
		if latestExists {
			version, effectiveAt = latest.Version, latest.EffectiveAt
			previous, normalizeErr := normalizeChannelFinanceVersionJSON(latest.SnapshotJSON)
			if normalizeErr != nil {
				return fmt.Errorf("读取当前倍率版本: %w", normalizeErr)
			}
			unchanged = previous == incomingSnapshot
		} else if errors.Is(latestErr, gorm.ErrRecordNotFound) {
			version = 0
		} else {
			return latestErr
		}
		var globalErr error
		globalChanged, currentGlobalUpdatedAt, globalErr = channelFinanceGlobalChanged(tx, in)
		if globalErr != nil {
			return globalErr
		}
		currentGlobalRevision, globalErr = channelFinanceGlobalRevision(tx)
		if globalErr != nil {
			return globalErr
		}
		unchanged = unchanged && !globalChanged
		if unchanged {
			return nil
		}
		domains, err := channelFinanceDomains(tx, in.Domain)
		if err != nil {
			return err
		}
		affectedDomains = 1
		if globalChanged {
			affectedDomains = len(domains)
		}
		// 首次建立全站计价且只有当前一个域名时可直接保存；其余变更必须二次确认。
		confirmationRequired := latestExists || globalChanged && len(domains) > 1
		if confirmationRequired && !in.ConfirmUpdate {
			return errChannelFinanceConfirmationRequired
		}
		if in.ConfirmUpdate {
			if in.ExpectedVersion != version {
				return errChannelFinanceVersionConflict
			}
			if globalChanged && in.ExpectedGlobalRevision != currentGlobalRevision {
				return errChannelFinanceGlobalConflict
			}
		}

		setting := ChannelFinanceSetting{
			ID: 1, FXBenchmark: in.FXBenchmark, SiteRechargePaid: in.SiteRechargePaid,
			SiteRechargeCredit: in.SiteRechargeCredit, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy,
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, UpdateAll: true}).Create(&setting).Error; err != nil {
			return err
		}
		domainCost := ChannelDomainCost{
			Domain: in.Domain, RechargePaid: in.UpstreamRechargePaid, RechargeCredit: in.UpstreamRechargeCredit,
			EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy,
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "domain"}}, UpdateAll: true}).Create(&domainCost).Error; err != nil {
			return err
		}
		groupNames := make([]string, 0, len(in.Groups))
		for _, group := range in.Groups {
			groupNames = append(groupNames, group.Group)
		}
		obsolete := tx.Where("domain = ?", in.Domain)
		if len(groupNames) > 0 {
			obsolete = obsolete.Where("grp NOT IN ?", groupNames)
		}
		if err := obsolete.Delete(&ChannelDomainGroupCost{}).Error; err != nil {
			return err
		}
		for _, group := range in.Groups {
			siteRate := ChannelSaleGroupRate{Grp: group.Group, Multiplier: *group.SiteMultiplier, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "grp"}}, UpdateAll: true}).Create(&siteRate).Error; err != nil {
				return err
			}
			upstreamRate := ChannelDomainGroupCost{
				Domain: in.Domain, Grp: group.Group, Multiplier: *group.UpstreamMultiplier,
				DiscountFactor: *group.UpstreamDiscountFactor, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy,
			}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "domain"}, {Name: "grp"}}, UpdateAll: true}).Create(&upstreamRate).Error; err != nil {
				return err
			}
		}
		versionDomains := []string{in.Domain}
		if globalChanged {
			versionDomains = domains
		}
		for _, domain := range versionDomains {
			snapshot, snapshotErr := currentChannelFinanceVersionJSON(tx, domain)
			if snapshotErr != nil {
				return fmt.Errorf("生成 %s 倍率版本: %w", domain, snapshotErr)
			}
			nextVersion, appended, appendErr := appendChannelFinanceVersion(tx, domain, snapshot, now, updatedBy)
			if appendErr != nil {
				return fmt.Errorf("保存 %s 倍率版本: %w", domain, appendErr)
			}
			if appended {
				appendedDomains++
			}
			if domain == in.Domain {
				version, effectiveAt = nextVersion, now
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errChannelFinanceConfirmationRequired) {
			c.JSON(http.StatusConflict, gin.H{
				"error":                 "确认后将创建新倍率版本，历史版本会保留",
				"confirmation_required": true, "current_version": version, "next_version": version + 1,
				"global_changed": globalChanged, "affected_domains": affectedDomains,
				"current_global_updated_at": currentGlobalUpdatedAt, "current_global_revision": currentGlobalRevision,
			})
			return
		}
		if errors.Is(err, errChannelFinanceVersionConflict) || errors.Is(err, errChannelFinanceGlobalConflict) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "配置已被其他会话更新，请重新打开后再修改", "version_conflict": true,
				"current_version": version, "current_global_updated_at": currentGlobalUpdatedAt,
			})
			return
		}
		c.JSON(500, gin.H{"error": "保存本地财务配置失败"})
		return
	}
	if unchanged {
		c.JSON(200, gin.H{"ok": true, "unchanged": true, "version": version, "effective_at": effectiveAt})
		return
	}
	c.JSON(200, gin.H{"ok": true, "version": version, "effective_at": effectiveAt, "updated_domains": appendedDomains})
}
