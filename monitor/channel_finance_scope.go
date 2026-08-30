package monitor

// 本文件提供新的分层财务配置接口：
//   - /channels/finance/site：全站唯一的计价基准和服务分组倍率；
//   - /channels/finance/domain-rates：某归并主域名的充值比例及全部渠道倍率；
//   - /channels/finance/domain：兼容旧客户端的单独充值比例接口；
//   - /channels/finance/channel：兼容旧客户端的单渠道接口。
// 旧的 /channels/finance 接口保留给已有客户端兼容使用，新版页面不再调用它。

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type channelFinanceSiteGroupInput struct {
	Group          string   `json:"group"`
	SiteMultiplier *float64 `json:"site_multiplier"`
}

type channelFinanceSiteSaveInput struct {
	FXBenchmark        float64                        `json:"fx_benchmark"`
	SiteRechargePaid   float64                        `json:"site_recharge_paid"`
	SiteRechargeCredit float64                        `json:"site_recharge_credit"`
	Groups             []channelFinanceSiteGroupInput `json:"groups"`
	ConfirmUpdate      bool                           `json:"confirm_update,omitempty"`
	ExpectedRevision   string                         `json:"expected_global_revision,omitempty"`
}

type channelFinanceDomainSaveInput struct {
	Domain                 string  `json:"domain"`
	UpstreamRechargePaid   float64 `json:"upstream_recharge_paid"`
	UpstreamRechargeCredit float64 `json:"upstream_recharge_credit"`
	ConfirmUpdate          bool    `json:"confirm_update,omitempty"`
	ExpectedVersion        int64   `json:"expected_version,omitempty"`
}

type channelFinanceChannelSaveInput struct {
	ChannelID              int     `json:"channel_id"`
	Group                  string  `json:"group"`
	UpstreamGroupName      string  `json:"upstream_group_name"`
	UpstreamMultiplier     float64 `json:"upstream_multiplier"`
	UpstreamDiscountFactor float64 `json:"upstream_discount_factor"`
	ConfirmUpdate          bool    `json:"confirm_update,omitempty"`
	ExpectedVersion        int64   `json:"expected_version,omitempty"`
}

// channelFinanceChannelRateInput 是主域名批量配置接口的渠道级输入。
// 一个渠道只有一组上游倍率；其关联的多个服务分组会在事务中镜像保存相同口径。
type channelFinanceChannelRateInput struct {
	ChannelID              int     `json:"channel_id"`
	UpstreamGroupName      string  `json:"upstream_group_name"`
	UpstreamMultiplier     float64 `json:"upstream_multiplier"`
	UpstreamDiscountFactor float64 `json:"upstream_discount_factor"`
}

// channelFinanceDomainRatesSaveInput 批量保存某个主域名下的渠道倍率。
// 页面按主域名集中维护，避免同一个渠道的倍率散落在多个渠道详情弹窗里。
type channelFinanceDomainRatesSaveInput struct {
	Domain                 string                           `json:"domain"`
	UpstreamRechargePaid   float64                          `json:"upstream_recharge_paid"`
	UpstreamRechargeCredit float64                          `json:"upstream_recharge_credit"`
	Rates                  []channelFinanceChannelRateInput `json:"rates"`
	ConfirmUpdate          bool                             `json:"confirm_update,omitempty"`
	ExpectedVersion        int64                            `json:"expected_version,omitempty"`
}

func validateFinancePositive(label string, value float64) error {
	if !validChannelFinanceNumber(value) {
		return fmt.Errorf("%s必须是大于 0 的有限数字", label)
	}
	return nil
}

func validateUpstreamGroupName(value string) error {
	if len([]rune(value)) > 128 || strings.ContainsAny(value, "\r\n\x00") {
		return errors.New("上游分组名无效")
	}
	return nil
}

func validateChannelFinanceSiteInput(in *channelFinanceSiteSaveInput) error {
	if err := validateFinancePositive("折扣基准", in.FXBenchmark); err != nil {
		return err
	}
	if err := validateFinancePositive("我方充值支付", in.SiteRechargePaid); err != nil {
		return err
	}
	if err := validateFinancePositive("我方充值到账", in.SiteRechargeCredit); err != nil {
		return err
	}
	if len(in.Groups) > maxChannelFinanceGroups {
		return fmt.Errorf("服务分组超过安全上限 %d", maxChannelFinanceGroups)
	}
	seen := map[string]bool{}
	for i := range in.Groups {
		in.Groups[i].Group = strings.TrimSpace(in.Groups[i].Group)
		if in.Groups[i].Group == "" || len([]rune(in.Groups[i].Group)) > 64 || strings.ContainsAny(in.Groups[i].Group, "\r\n\x00") {
			return errors.New("服务分组名称无效")
		}
		if seen[in.Groups[i].Group] {
			return fmt.Errorf("服务分组 %s 重复", in.Groups[i].Group)
		}
		seen[in.Groups[i].Group] = true
		if in.Groups[i].SiteMultiplier == nil {
			return fmt.Errorf("服务分组 %s 必须填写我方倍率", in.Groups[i].Group)
		}
		if err := validateFinancePositive("我方倍率", *in.Groups[i].SiteMultiplier); err != nil {
			return fmt.Errorf("服务分组 %s：%w", in.Groups[i].Group, err)
		}
	}
	if len(in.ExpectedRevision) > 64 || strings.ContainsAny(in.ExpectedRevision, "\r\n\x00") {
		return errors.New("期望全局修订无效")
	}
	return nil
}

func ensureChannelFinanceSettingTx(tx *gorm.DB, now int64, updatedBy string) error {
	var setting ChannelFinanceSetting
	err := tx.First(&setting, "id = ?", 1).Error
	if err == nil {
		return nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return tx.Create(&ChannelFinanceSetting{
		ID: 1, FXBenchmark: defaultChannelFinanceFX, SiteRechargePaid: 1, SiteRechargeCredit: 1,
		EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy,
	}).Error
}

func (m *Monitor) saveChannelFinanceSiteHandler(c *gin.Context) {
	if c.Request.ContentLength > maxChannelFinanceBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "配置内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelFinanceBody)
	var in channelFinanceSiteSaveInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "配置格式无效"})
		return
	}
	if err := validateChannelFinanceSiteInput(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	now, updatedBy := time.Now().Unix(), c.GetString("uname")
	var unchanged, changed bool
	var currentRevision string
	var affectedDomains, updatedDomains int
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var setting ChannelFinanceSetting
		settingErr := tx.First(&setting, "id = ?", 1).Error
		hasSetting := settingErr == nil
		if settingErr != nil && !errors.Is(settingErr, gorm.ErrRecordNotFound) {
			return settingErr
		}
		currentRevision, settingErr = channelFinanceGlobalRevision(tx)
		if settingErr != nil {
			return settingErr
		}
		changed = !hasSetting || setting.FXBenchmark != in.FXBenchmark || setting.SiteRechargePaid != in.SiteRechargePaid || setting.SiteRechargeCredit != in.SiteRechargeCredit
		var oldGroups []ChannelSaleGroupRate
		if err := tx.Find(&oldGroups).Error; err != nil {
			return err
		}
		old := make(map[string]float64, len(oldGroups))
		for _, row := range oldGroups {
			old[row.Grp] = row.Multiplier
		}
		for _, group := range in.Groups {
			if value, exists := old[group.Group]; !exists || value != *group.SiteMultiplier {
				changed = true
			}
		}
		if !changed {
			unchanged = true
			return nil
		}
		confirmationRequired := hasSetting || len(oldGroups) > 0
		if confirmationRequired && !in.ConfirmUpdate {
			return errChannelFinanceConfirmationRequired
		}
		if in.ConfirmUpdate && in.ExpectedRevision != currentRevision {
			return errChannelFinanceGlobalConflict
		}
		setting = ChannelFinanceSetting{ID: 1, FXBenchmark: in.FXBenchmark, SiteRechargePaid: in.SiteRechargePaid, SiteRechargeCredit: in.SiteRechargeCredit, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, UpdateAll: true}).Create(&setting).Error; err != nil {
			return err
		}
		for _, group := range in.Groups {
			row := ChannelSaleGroupRate{Grp: group.Group, Multiplier: *group.SiteMultiplier, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "grp"}}, UpdateAll: true}).Create(&row).Error; err != nil {
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
		if errors.Is(err, errFinanceActivationConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "受影响上游存在待整点生效的倍率变更，请等待生效或先取消"})
			return
		}
		if errors.Is(err, errChannelFinanceConfirmationRequired) {
			c.JSON(http.StatusConflict, gin.H{"error": "确认后将更新全站计价基准并为受影响主域名追加版本", "confirmation_required": true, "current_global_revision": currentRevision, "affected_domains": affectedDomains})
			return
		}
		if errors.Is(err, errChannelFinanceGlobalConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "全站计价基准已被其他会话更新，请重新打开后再修改", "version_conflict": true, "current_global_revision": currentRevision})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存网站计价配置失败"})
		return
	}
	if unchanged {
		c.JSON(http.StatusOK, gin.H{"ok": true, "unchanged": true, "current_global_revision": currentRevision})
		return
	}
	newRevision, _ := channelFinanceGlobalRevision(m.storeDB.WithContext(ctx))
	c.JSON(http.StatusOK, gin.H{"ok": true, "updated_domains": updatedDomains, "affected_domains": affectedDomains, "current_global_revision": newRevision})
}

func (m *Monitor) saveChannelFinanceDomainHandler(c *gin.Context) {
	if c.Request.ContentLength > maxChannelFinanceBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "配置内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelFinanceBody)
	var in channelFinanceDomainSaveInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "配置格式无效"})
		return
	}
	in.Domain = strings.TrimSpace(strings.ToLower(in.Domain))
	if in.Domain == "" || normalizeChannelBaseDomain(in.Domain) != in.Domain {
		c.JSON(http.StatusBadRequest, gin.H{"error": "主域名无效"})
		return
	}
	if err := validateFinancePositive("上游充值支付", in.UpstreamRechargePaid); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateFinancePositive("上游充值到账", in.UpstreamRechargeCredit); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if in.ExpectedVersion < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "期望版本无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	now, updatedBy := time.Now().Unix(), c.GetString("uname")
	var version int64
	var unchanged bool
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current ChannelDomainCost
		err := tx.First(&current, "domain = ?", in.Domain).Error
		hasCurrent := err == nil
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if hasCurrent && current.RechargePaid == in.UpstreamRechargePaid && current.RechargeCredit == in.UpstreamRechargeCredit {
			unchanged = true
			return nil
		}
		var latest ChannelFinanceVersion
		latestErr := tx.Where("domain = ?", in.Domain).Order("version DESC").First(&latest).Error
		if latestErr != nil && !errors.Is(latestErr, gorm.ErrRecordNotFound) {
			return latestErr
		}
		version = latest.Version
		if !errors.Is(latestErr, gorm.ErrRecordNotFound) && !in.ConfirmUpdate {
			return errChannelFinanceConfirmationRequired
		}
		if in.ConfirmUpdate && in.ExpectedVersion != version {
			return errChannelFinanceVersionConflict
		}
		if err := ensureChannelFinanceSettingTx(tx, now, updatedBy); err != nil {
			return err
		}
		row := ChannelDomainCost{Domain: in.Domain, RechargePaid: in.UpstreamRechargePaid, RechargeCredit: in.UpstreamRechargeCredit, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "domain"}}, UpdateAll: true}).Create(&row).Error; err != nil {
			return err
		}
		snapshot, err := currentChannelFinanceVersionJSON(tx, in.Domain)
		if err != nil {
			return err
		}
		next, _, err := appendChannelFinanceVersion(tx, in.Domain, snapshot, now, updatedBy)
		version = next
		return err
	})
	if err != nil {
		if errors.Is(err, errFinanceActivationConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "该上游存在待整点生效的倍率变更，请等待生效或先取消"})
			return
		}
		if errors.Is(err, errChannelFinanceConfirmationRequired) {
			c.JSON(http.StatusConflict, gin.H{"error": "确认后将创建新的主域名充值比例版本，历史版本会保留", "confirmation_required": true, "current_version": version, "next_version": version + 1})
			return
		}
		if errors.Is(err, errChannelFinanceVersionConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "主域名倍率版本已变更，请重新打开后再修改", "version_conflict": true, "current_version": version})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存主域名充值比例失败"})
		return
	}
	if unchanged {
		c.JSON(http.StatusOK, gin.H{"ok": true, "unchanged": true, "version": version})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "version": version, "effective_at": now})
}

func (m *Monitor) saveChannelFinanceChannelHandler(c *gin.Context) {
	if c.Request.ContentLength > maxChannelFinanceBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "配置内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelFinanceBody)
	var in channelFinanceChannelSaveInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "配置格式无效"})
		return
	}
	in.Group = strings.TrimSpace(in.Group)
	in.UpstreamGroupName = strings.TrimSpace(in.UpstreamGroupName)
	if in.ChannelID <= 0 || in.Group == "" || len([]rune(in.Group)) > 64 || strings.ContainsAny(in.Group, "\r\n\x00") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "渠道或服务分组无效"})
		return
	}
	if err := validateUpstreamGroupName(in.UpstreamGroupName); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateFinancePositive("上游基础倍率", in.UpstreamMultiplier); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateFinancePositive("上游折扣系数", in.UpstreamDiscountFactor); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	now, updatedBy := time.Now().Unix(), c.GetString("uname")
	var version int64
	var unchanged bool
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var snap ChannelSnap
		if err := tx.First(&snap, "id = ?", in.ChannelID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("渠道快照不存在，无法保存历史成本口径")
			}
			return err
		}
		if strings.TrimSpace(snap.BaseDomain) == "" {
			return errors.New("该渠道没有归并主域名，无法保存渠道成本口径")
		}
		if !containsString(splitList(snap.Groups), in.Group) {
			return errors.New("服务分组不属于该渠道快照")
		}
		var current ChannelFinanceChannelCost
		currentErr := tx.Where("channel_id = ? AND grp = ?", in.ChannelID, in.Group).First(&current).Error
		if currentErr == nil && strings.TrimSpace(current.UpstreamGroupName) == in.UpstreamGroupName && current.Multiplier == in.UpstreamMultiplier && normalizedUpstreamDiscountFactor(current.DiscountFactor) == in.UpstreamDiscountFactor {
			unchanged = true
			var latest ChannelFinanceVersion
			if err := tx.Where("domain = ?", snap.BaseDomain).Order("version DESC").First(&latest).Error; err == nil {
				version = latest.Version
			}
			return nil
		}
		if currentErr != nil && !errors.Is(currentErr, gorm.ErrRecordNotFound) {
			return currentErr
		}
		var latest ChannelFinanceVersion
		latestErr := tx.Where("domain = ?", snap.BaseDomain).Order("version DESC").First(&latest).Error
		if latestErr != nil && !errors.Is(latestErr, gorm.ErrRecordNotFound) {
			return latestErr
		}
		version = latest.Version
		if !errors.Is(latestErr, gorm.ErrRecordNotFound) && !in.ConfirmUpdate {
			return errChannelFinanceConfirmationRequired
		}
		if in.ConfirmUpdate && in.ExpectedVersion != version {
			return errChannelFinanceVersionConflict
		}
		if err := ensureChannelFinanceSettingTx(tx, now, updatedBy); err != nil {
			return err
		}
		row := ChannelFinanceChannelCost{ChannelID: in.ChannelID, Grp: in.Group, UpstreamGroupName: in.UpstreamGroupName, Multiplier: in.UpstreamMultiplier, DiscountFactor: in.UpstreamDiscountFactor, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "channel_id"}, {Name: "grp"}}, UpdateAll: true}).Create(&row).Error; err != nil {
			return err
		}
		snapshot, err := currentChannelFinanceVersionJSON(tx, snap.BaseDomain)
		if err != nil {
			return err
		}
		next, _, err := appendChannelFinanceVersion(tx, snap.BaseDomain, snapshot, now, updatedBy)
		version = next
		return err
	})
	if err != nil {
		if errors.Is(err, errFinanceActivationConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "该上游存在待整点生效的倍率变更，请等待生效或先取消"})
			return
		}
		if errors.Is(err, errChannelFinanceConfirmationRequired) {
			c.JSON(http.StatusConflict, gin.H{"error": "确认后将创建新的渠道成本版本，历史版本会保留", "confirmation_required": true, "current_version": version, "next_version": version + 1})
			return
		}
		if errors.Is(err, errChannelFinanceVersionConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "渠道成本版本已变更，请重新打开后再修改", "version_conflict": true, "current_version": version})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if unchanged {
		c.JSON(http.StatusOK, gin.H{"ok": true, "unchanged": true, "version": version})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "version": version, "effective_at": now})
}

// saveChannelFinanceDomainRatesHandler 在一个事务中保存主域名下所有渠道倍率。
// 每次提交最多追加一个版本，避免批量修改时产生大量无意义的版本号。
func (m *Monitor) saveChannelFinanceDomainRatesHandler(c *gin.Context) {
	if c.Request.ContentLength > maxChannelFinanceBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "配置内容过大"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChannelFinanceBody)
	var in channelFinanceDomainRatesSaveInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "配置格式无效"})
		return
	}
	in.Domain = strings.TrimSpace(strings.ToLower(in.Domain))
	if in.Domain == "" || normalizeChannelBaseDomain(in.Domain) != in.Domain {
		c.JSON(http.StatusBadRequest, gin.H{"error": "主域名无效"})
		return
	}
	if err := validateFinancePositive("上游充值支付", in.UpstreamRechargePaid); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateFinancePositive("上游充值到账", in.UpstreamRechargeCredit); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(in.Rates) > maxChannelFinanceRows {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("渠道倍率配置超过安全上限 %d", maxChannelFinanceRows)})
		return
	}
	seen := make(map[int]bool, len(in.Rates))
	for i := range in.Rates {
		row := &in.Rates[i]
		row.UpstreamGroupName = strings.TrimSpace(row.UpstreamGroupName)
		if row.ChannelID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "渠道无效"})
			return
		}
		if err := validateUpstreamGroupName(row.UpstreamGroupName); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if seen[row.ChannelID] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "渠道重复"})
			return
		}
		seen[row.ChannelID] = true
		if err := validateFinancePositive("上游基础倍率", row.UpstreamMultiplier); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := validateFinancePositive("上游折扣系数", row.UpstreamDiscountFactor); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	if in.ExpectedVersion < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "期望版本无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
	defer cancel()
	now, updatedBy := time.Now().Unix(), c.GetString("uname")
	var version int64
	var unchanged bool
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var currentDomain ChannelDomainCost
		domainErr := tx.Where("domain = ?", in.Domain).First(&currentDomain).Error
		if domainErr != nil && !errors.Is(domainErr, gorm.ErrRecordNotFound) {
			return domainErr
		}
		domainSame := domainErr == nil && currentDomain.RechargePaid == in.UpstreamRechargePaid && currentDomain.RechargeCredit == in.UpstreamRechargeCredit
		var latest ChannelFinanceVersion
		latestErr := tx.Where("domain = ?", in.Domain).Order("version DESC").First(&latest).Error
		if latestErr != nil && !errors.Is(latestErr, gorm.ErrRecordNotFound) {
			return latestErr
		}
		version = latest.Version
		if !errors.Is(latestErr, gorm.ErrRecordNotFound) && !in.ConfirmUpdate {
			// 只有内容发生变化时才需要确认；先读取现有值判断，避免打开后无修改也弹确认。
			allSame, err := channelFinanceDomainRatesEqual(tx, in.Domain, in.Rates)
			if err != nil {
				return err
			}
			if allSame && domainSame {
				unchanged = true
				return nil
			}
			return errChannelFinanceConfirmationRequired
		}
		if in.ConfirmUpdate && in.ExpectedVersion != version {
			return errChannelFinanceVersionConflict
		}
		allSame, err := channelFinanceDomainRatesEqual(tx, in.Domain, in.Rates)
		if err != nil {
			return err
		}
		if allSame && domainSame {
			unchanged = true
			return nil
		}
		if err := ensureChannelFinanceSettingTx(tx, now, updatedBy); err != nil {
			return err
		}
		domainCost := ChannelDomainCost{Domain: in.Domain, RechargePaid: in.UpstreamRechargePaid, RechargeCredit: in.UpstreamRechargeCredit, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "domain"}}, UpdateAll: true}).Create(&domainCost).Error; err != nil {
			return err
		}
		for _, row := range in.Rates {
			var snap ChannelSnap
			if err := tx.First(&snap, "id = ?", row.ChannelID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("渠道 #%d 快照不存在，无法保存历史成本口径", row.ChannelID)
				}
				return err
			}
			if strings.TrimSpace(snap.BaseDomain) != in.Domain {
				return fmt.Errorf("渠道 #%d 不属于主域名 %s", row.ChannelID, in.Domain)
			}
			groups := sortedUnique(splitList(snap.Groups))
			if len(groups) == 0 {
				return fmt.Errorf("渠道 #%d 没有关联服务分组，无法保存倍率", row.ChannelID)
			}
			// 一个物理渠道只有一份当前上游成本口径。先删除旧版遗留的
			// 已移除分组行，再为当前分组完整重建；历史值已经保存在追加式
			// ChannelFinanceVersion 中，不应继续污染当前配置的一致性判断。
			if err := tx.Where("channel_id = ?", row.ChannelID).Delete(&ChannelFinanceChannelCost{}).Error; err != nil {
				return err
			}
			for _, group := range groups {
				cost := ChannelFinanceChannelCost{ChannelID: row.ChannelID, Grp: group, UpstreamGroupName: row.UpstreamGroupName, Multiplier: row.UpstreamMultiplier, DiscountFactor: row.UpstreamDiscountFactor, EffectiveAt: now, UpdatedAt: now, UpdatedBy: updatedBy}
				if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "channel_id"}, {Name: "grp"}}, UpdateAll: true}).Create(&cost).Error; err != nil {
					return err
				}
			}
		}
		snapshot, err := currentChannelFinanceVersionJSON(tx, in.Domain)
		if err != nil {
			return err
		}
		next, _, err := appendChannelFinanceVersion(tx, in.Domain, snapshot, now, updatedBy)
		version = next
		return err
	})
	if err != nil {
		if errors.Is(err, errFinanceActivationConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "该上游存在待整点生效的倍率变更，请等待生效或先取消"})
			return
		}
		if errors.Is(err, errChannelFinanceConfirmationRequired) {
			c.JSON(http.StatusConflict, gin.H{"error": "确认后将创建新的渠道倍率版本，历史版本会保留", "confirmation_required": true, "current_version": version, "next_version": version + 1})
			return
		}
		if errors.Is(err, errChannelFinanceVersionConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "主域名倍率版本已变更，请重新打开后再修改", "version_conflict": true, "current_version": version})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if unchanged {
		c.JSON(http.StatusOK, gin.H{"ok": true, "unchanged": true, "version": version})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "version": version, "effective_at": now})
}

func channelFinanceDomainRatesEqual(tx *gorm.DB, domain string, rows []channelFinanceChannelRateInput) (bool, error) {
	for _, row := range rows {
		var snap ChannelSnap
		if err := tx.First(&snap, "id = ?", row.ChannelID).Error; err != nil {
			return false, err
		}
		if strings.TrimSpace(snap.BaseDomain) != domain {
			return false, fmt.Errorf("渠道 #%d 与主域名不匹配", row.ChannelID)
		}
		groups := sortedUnique(splitList(snap.Groups))
		if len(groups) == 0 {
			return false, fmt.Errorf("渠道 #%d 没有关联服务分组", row.ChannelID)
		}
		var current []ChannelFinanceChannelCost
		if err := tx.Where("channel_id = ?", row.ChannelID).Find(&current).Error; err != nil {
			return false, err
		}
		byGroup := make(map[string]ChannelFinanceChannelCost, len(current))
		for _, cost := range current {
			byGroup[cost.Grp] = cost
		}
		if len(byGroup) != len(groups) {
			return false, nil
		}
		for _, group := range groups {
			cost, ok := byGroup[group]
			if !ok || strings.TrimSpace(cost.UpstreamGroupName) != row.UpstreamGroupName || cost.Multiplier != row.UpstreamMultiplier || normalizedUpstreamDiscountFactor(cost.DiscountFactor) != row.UpstreamDiscountFactor {
				return false, nil
			}
		}
	}
	return true, nil
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
