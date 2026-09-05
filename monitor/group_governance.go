package monitor

// group_governance.go 是 NewAPI 分组的只读治理投影。
//
// 运行边界：
//   - 只有持有现有 NewAPI 来源 lease 的后台 worker 才会读生产库；
//   - 生产查询只选取分组、状态与用户展示字段，不读任何 Key/密码/邮箱；
//   - 发布为一次 SQLite 事务，Web/CSV/用户展开只读这份本地快照；
//   - 同步失败保留上一份成功结果，不把未知重置为 0。

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	groupGovernanceStateID       = 1
	groupGovernanceMaxGroups     = 2000
	groupGovernanceMaxUsers      = 200000
	groupGovernanceQueryTimeout  = 35 * time.Second
	groupGovernanceHistoryDays   = 30
	groupGovernanceUserPageLimit = 100
)

const groupGovernanceTokenStatsSQL = "SELECT /*+ MAX_EXECUTION_TIME(8000) */ " +
	"BINARY TRIM(COALESCE(`group`, '')) AS grp, status, " +
	"CASE WHEN expired_time > 0 AND expired_time < ? THEN 1 ELSE 0 END AS expired, COUNT(*) " +
	"FROM tokens WHERE deleted_at IS NULL AND TRIM(COALESCE(`group`, '')) <> '' " +
	"GROUP BY BINARY TRIM(COALESCE(`group`, '')), status, expired"

var groupNameRatioPattern = regexp.MustCompile(`(?i)(?:^|[-_])(\d+(?:\.\d+)?)x(?:$|[-_])`)

// GroupGovernanceState 是当前已发布快照的头部。ID 固定为 1。
type GroupGovernanceState struct {
	ID                     uint   `gorm:"primaryKey;autoIncrement:false" json:"-"`
	Revision               string `gorm:"size:32" json:"revision"`
	LastAttemptAt          int64  `gorm:"column:last_attempt_at" json:"last_attempt_at"`
	LastSuccessAt          int64  `gorm:"column:last_success_at;index" json:"last_success_at"`
	Complete               bool   `gorm:"column:complete" json:"complete"`
	LastError              string `gorm:"size:1024;column:last_error" json:"last_error,omitempty"`
	SourceErrorsJSON       string `gorm:"type:text;column:source_errors_json" json:"-"`
	CoverageStartAt        int64  `gorm:"column:coverage_start_at" json:"coverage_start_at"`
	CoverageEndAt          int64  `gorm:"column:coverage_end_at" json:"coverage_end_at"`
	HistoryComplete        bool   `gorm:"column:history_complete" json:"history_complete"`
	SubscriptionVerified   bool   `gorm:"column:subscription_verified" json:"subscription_verified"`
	CurrentGroupCount      int    `gorm:"column:current_group_count" json:"current_group_count"`
	HistoricalGroupCount   int    `gorm:"column:historical_group_count" json:"historical_group_count"`
	HighRiskCount          int    `gorm:"column:high_risk_count" json:"high_risk_count"`
	NoEnabledChannelCount  int    `gorm:"column:no_enabled_channel_count" json:"no_enabled_channel_count"`
	CleanupCandidateCount  int    `gorm:"column:cleanup_candidate_count" json:"cleanup_candidate_count"`
	UnattributedRejects7d  int64  `gorm:"column:unattributed_rejects_7d" json:"unattributed_rejections_7d"`
	UnattributedRejects30d int64  `gorm:"column:unattributed_rejects_30d" json:"unattributed_rejections_30d"`
}

// GroupGovernanceGroup 是一个分组的当前脱敏投影。可展开的数组以 JSON
// 存在 Monitor 本地库，这些数组只含名称/ID/状态，不含凭据。
type GroupGovernanceGroup struct {
	Grp                       string  `gorm:"primaryKey;size:64;column:grp" json:"group"`
	Current                   bool    `gorm:"index;column:current" json:"current"`
	HistoricalOnly            bool    `gorm:"index;column:historical_only" json:"historical_only"`
	DisplayName               string  `gorm:"size:255;column:display_name" json:"display_name"`
	RatioConfigured           bool    `gorm:"column:ratio_configured" json:"ratio_configured"`
	Ratio                     float64 `gorm:"column:ratio" json:"ratio"`
	UserSelectable            bool    `gorm:"column:user_selectable" json:"user_selectable"`
	AutoConfigured            bool    `gorm:"column:auto_configured" json:"auto_configured"`
	EnabledChannels           int     `gorm:"column:enabled_channels" json:"enabled_channels"`
	DisabledChannels          int     `gorm:"column:disabled_channels" json:"disabled_channels"`
	UserCount                 int     `gorm:"column:user_count" json:"user_count"`
	EnabledUserCount          int     `gorm:"column:enabled_user_count" json:"enabled_user_count"`
	DisabledUserCount         int     `gorm:"column:disabled_user_count" json:"disabled_user_count"`
	ExplicitTokenCount        int64   `gorm:"column:explicit_token_count" json:"explicit_token_count"`
	EnabledTokenCount         int64   `gorm:"column:enabled_token_count" json:"enabled_token_count"`
	DisabledTokenCount        int64   `gorm:"column:disabled_token_count" json:"disabled_token_count"`
	ExpiredTokenCount         int64   `gorm:"column:expired_token_count" json:"expired_token_count"`
	EnabledSubscriptionPlans  int     `gorm:"column:enabled_subscription_plans" json:"enabled_subscription_plans"`
	DisabledSubscriptionPlans int     `gorm:"column:disabled_subscription_plans" json:"disabled_subscription_plans"`
	Requests7d                int64   `gorm:"column:requests_7d" json:"requests_7d"`
	Requests30d               int64   `gorm:"column:requests_30d" json:"requests_30d"`
	PreRouteRejections7d      int64   `gorm:"column:pre_route_rejections_7d" json:"pre_route_rejections_7d"`
	PreRouteRejections30d     int64   `gorm:"column:pre_route_rejections_30d" json:"pre_route_rejections_30d"`
	LastObservedAt            int64   `gorm:"column:last_observed_at" json:"last_observed_at"`
	Status                    string  `gorm:"size:16;index;column:status" json:"status"`
	CleanupCandidate          bool    `gorm:"index;column:cleanup_candidate" json:"cleanup_candidate"`
	NameHasRatio              bool    `gorm:"index;column:name_has_ratio" json:"name_has_ratio"`
	IssuesJSON                string  `gorm:"type:text;column:issues_json" json:"-"`
	ConfigSourcesJSON         string  `gorm:"type:text;column:config_sources_json" json:"-"`
	ChannelsJSON              string  `gorm:"type:text;column:channels_json" json:"-"`
	SubscriptionsJSON         string  `gorm:"type:text;column:subscriptions_json" json:"-"`
	SyncedAt                  int64   `gorm:"index;column:synced_at" json:"synced_at"`
}

// GroupGovernanceUser 是 users.group 的实际关联，用于管理员展开核对。
// 不保存 Email、Quota、AccessToken 等与分组治理无关字段。
type GroupGovernanceUser struct {
	Grp         string `gorm:"primaryKey;size:64;column:grp;index:idx_group_governance_user_group_status,priority:1" json:"group"`
	UserID      int    `gorm:"primaryKey;autoIncrement:false;column:user_id" json:"user_id"`
	Username    string `gorm:"size:64;index;column:username" json:"username"`
	DisplayName string `gorm:"size:64;column:display_name" json:"display_name"`
	Status      int    `gorm:"index:idx_group_governance_user_group_status,priority:2;column:status" json:"status"`
	Role        int    `gorm:"column:role" json:"role"`
}

type groupGovernanceChannel struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Status int    `json:"status"`
}

type groupGovernanceSubscription struct {
	ID      int    `json:"id"`
	Title   string `json:"title"`
	Enabled bool   `json:"enabled"`
}

type groupGovernanceActivity struct {
	Delivered7d  int64
	Delivered30d int64
	Rejected7d   int64
	Rejected30d  int64
	LastAt       int64
}

type groupGovernanceTokenStats struct {
	Total    int64
	Enabled  int64
	Disabled int64
	Expired  int64
}

type groupGovernanceInput struct {
	Ratios                 map[string]float64
	Displays               map[string]string
	Selectable             map[string]bool
	Auto                   map[string]bool
	ConfigSources          map[string]map[string]struct{}
	Channels               []ChannelSnap
	Users                  []GroupGovernanceUser
	Tokens                 map[string]groupGovernanceTokenStats
	Subscriptions          map[string][]groupGovernanceSubscription
	Activity               map[string]groupGovernanceActivity
	SourceErrors           []string
	HistoryComplete        bool
	SubscriptionChecked    bool
	CoverageStartAt        int64
	CoverageEndAt          int64
	UnattributedRejects7d  int64
	UnattributedRejects30d int64
}

type groupGovernanceOptionResult struct {
	Ratios        map[string]float64
	Displays      map[string]string
	Selectable    map[string]bool
	Auto          map[string]bool
	ConfigSources map[string]map[string]struct{}
	ParseErrors   []string
}

type groupGovernanceGroupView struct {
	GroupGovernanceGroup
	Issues        []string                      `json:"issues"`
	ConfigSources []string                      `json:"config_sources"`
	Channels      []groupGovernanceChannel      `json:"channels"`
	Subscriptions []groupGovernanceSubscription `json:"subscriptions"`
}

type groupGovernanceReport struct {
	Enabled      bool                       `json:"enabled"`
	State        GroupGovernanceState       `json:"state"`
	SourceErrors []string                   `json:"source_errors"`
	Groups       []groupGovernanceGroupView `json:"groups"`
}

func groupGovernanceInterval(minutes int) time.Duration {
	if minutes < 5 {
		minutes = 5
	}
	if minutes > 1440 {
		minutes = 1440
	}
	return time.Duration(minutes) * time.Minute
}

func (m *Monitor) runGroupGovernanceLoop(ctx context.Context) {
	run := func() {
		if err := m.syncGroupGovernance(ctx); err != nil && ctx.Err() == nil {
			// 错误只记录类别化摘要；SQL/DSN/数据内容不输出。
			m.markGroupGovernanceFailure(err)
		}
	}
	run()
	ticker := time.NewTicker(groupGovernanceInterval(m.cfg.GroupGovernanceSyncMinutes))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (m *Monitor) syncGroupGovernance(parent context.Context) error {
	if !m.cfg.GroupGovernanceEnabled {
		return nil
	}
	if m.prodDB == nil {
		return errSourceNotReady
	}
	ctx, cancel := context.WithTimeout(parent, groupGovernanceQueryTimeout)
	defer cancel()
	release, err := m.acquireBackgroundSourceLow(ctx)
	if err != nil {
		return err
	}
	released := false
	defer func() {
		if !released {
			release()
		}
	}()

	options, err := m.fetchGroupGovernanceOptions(ctx)
	if err != nil {
		m.reportSourceQueryError(err)
		return fmt.Errorf("分组配置来源不可用: %w", err)
	}
	users, err := m.fetchGroupGovernanceUsers(ctx)
	if err != nil {
		m.reportSourceQueryError(err)
		return fmt.Errorf("用户分组来源不可用: %w", err)
	}
	tokens, err := m.fetchGroupGovernanceTokens(ctx, time.Now().Unix())
	if err != nil {
		m.reportSourceQueryError(err)
		return fmt.Errorf("令牌分组来源不可用: %w", err)
	}

	subscriptions, subscriptionChecked, subscriptionErr := m.fetchGroupGovernanceSubscriptions(ctx)
	// 生产库读取已结束，立即释放共享来源泳道；后续 SQLite 聚合与
	// 快照发布不得继续阻塞实时采样。
	release()
	released = true
	channels, channelUpdatedAt, channelErr := m.loadGroupGovernanceChannels(ctx)
	now := time.Now()
	activity, coverageStart, coverageEnd, historyComplete, unattributed7d, unattributed30d, historyErr := m.loadGroupGovernanceActivity(ctx, now)
	sourceErrors := append([]string(nil), options.ParseErrors...)
	if subscriptionErr != nil {
		sourceErrors = append(sourceErrors, "订阅套餐引用未核验")
	}
	channelMaxAge := int64(groupGovernanceInterval(m.cfg.GroupGovernanceSyncMinutes).Seconds() * 2)
	if samplerAge := int64(m.cfg.SampleSeconds * 3); samplerAge > channelMaxAge {
		channelMaxAge = samplerAge
	}
	if channelMaxAge < 15*60 {
		channelMaxAge = 15 * 60
	}
	if channelErr != nil || channelUpdatedAt == 0 {
		sourceErrors = append(sourceErrors, "渠道快照尚不可用")
	} else if now.Unix()-channelUpdatedAt > channelMaxAge {
		sourceErrors = append(sourceErrors, "渠道快照已过期")
	}
	if historyErr != nil {
		sourceErrors = append(sourceErrors, "本地历史使用量未核验")
	} else if !historyComplete {
		sourceErrors = append(sourceErrors, "本地历史覆盖不足 30 天")
	}

	rows, userRows, state, err := buildGroupGovernanceSnapshot(groupGovernanceInput{
		Ratios: options.Ratios, Displays: options.Displays, Selectable: options.Selectable,
		Auto: options.Auto, ConfigSources: options.ConfigSources, Channels: channels, Users: users,
		Tokens: tokens, Subscriptions: subscriptions, Activity: activity, SourceErrors: sourceErrors,
		HistoryComplete: historyComplete, SubscriptionChecked: subscriptionChecked,
		CoverageStartAt: coverageStart, CoverageEndAt: coverageEnd,
		UnattributedRejects7d: unattributed7d, UnattributedRejects30d: unattributed30d,
	}, now)
	if err != nil {
		return err
	}
	return m.publishGroupGovernanceSnapshot(ctx, rows, userRows, state)
}

func (m *Monitor) fetchGroupGovernanceOptions(ctx context.Context) (groupGovernanceOptionResult, error) {
	result := groupGovernanceOptionResult{
		Ratios: map[string]float64{}, Displays: map[string]string{}, Selectable: map[string]bool{},
		Auto: map[string]bool{}, ConfigSources: map[string]map[string]struct{}{},
	}
	keys := []string{
		"GroupRatio", "UserUsableGroups", "AutoGroups", "GroupGroupRatio",
		websiteGroupSpecialOption, "TopupGroupRatio", "ModelRequestRateLimitGroup",
	}
	args := make([]any, len(keys))
	for i := range keys {
		args[i] = keys[i]
	}
	rows, err := m.prodDB.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(5000) */ `key`, `value`\n"+
		"FROM options WHERE `key` IN (?, ?, ?, ?, ?, ?, ?)", args...)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	values := map[string]string{}
	for rows.Next() {
		var key string
		var value sql.NullString
		if err := rows.Scan(&key, &value); err != nil {
			return result, err
		}
		if value.Valid {
			values[key] = value.String
		}
	}
	if err := rows.Err(); err != nil {
		return result, err
	}

	add := func(group, source string) {
		group = strings.TrimSpace(group)
		if group == "" {
			return
		}
		if len([]byte(group)) > 64 {
			result.ParseErrors = append(result.ParseErrors, source+"含超长分组名")
			return
		}
		if result.ConfigSources[group] == nil {
			result.ConfigSources[group] = map[string]struct{}{}
		}
		result.ConfigSources[group][source] = struct{}{}
	}
	parseObject := func(key string) map[string]json.RawMessage {
		raw, exists := values[key]
		if !exists || strings.TrimSpace(raw) == "" {
			result.ParseErrors = append(result.ParseErrors, key+"未配置")
			return nil
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &object); err != nil {
			result.ParseErrors = append(result.ParseErrors, key+" JSON 无法解析")
			return nil
		}
		return object
	}

	for group, raw := range parseObject("GroupRatio") {
		add(group, "GroupRatio")
		value, err := parseWebsiteGroupRatio(raw)
		if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			result.ParseErrors = append(result.ParseErrors, "GroupRatio["+group+"]值无效")
			continue
		}
		result.Ratios[strings.TrimSpace(group)] = value // 0 是 NewAPI rc4 允许的有效配置。
	}
	if raw, ok := values["UserUsableGroups"]; ok && strings.TrimSpace(raw) != "" {
		var display map[string]string
		if err := json.Unmarshal([]byte(raw), &display); err != nil {
			result.ParseErrors = append(result.ParseErrors, "UserUsableGroups JSON 无法解析")
		} else {
			for group, description := range display {
				group = strings.TrimSpace(group)
				add(group, "UserUsableGroups")
				result.Displays[group], result.Selectable[group] = strings.TrimSpace(description), true
			}
		}
	}
	if raw, ok := values["AutoGroups"]; ok && strings.TrimSpace(raw) != "" {
		var groups []string
		if err := json.Unmarshal([]byte(raw), &groups); err != nil {
			result.ParseErrors = append(result.ParseErrors, "AutoGroups JSON 无法解析")
		} else {
			for _, group := range groups {
				group = strings.TrimSpace(group)
				add(group, "AutoGroups")
				result.Auto[group] = true
			}
		}
	}
	if raw, ok := values["GroupGroupRatio"]; ok && strings.TrimSpace(raw) != "" {
		var nested map[string]map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &nested); err != nil {
			result.ParseErrors = append(result.ParseErrors, "GroupGroupRatio JSON 无法解析")
		} else {
			for userGroup, usingGroups := range nested {
				add(userGroup, "GroupGroupRatio:user")
				for usingGroup := range usingGroups {
					add(usingGroup, "GroupGroupRatio:using")
				}
			}
		}
	}
	if raw, ok := values[websiteGroupSpecialOption]; ok && strings.TrimSpace(raw) != "" {
		var special map[string]map[string]string
		if err := json.Unmarshal([]byte(raw), &special); err != nil {
			result.ParseErrors = append(result.ParseErrors, websiteGroupSpecialOption+" JSON 无法解析")
		} else {
			for userGroup, usable := range special {
				add(userGroup, websiteGroupSpecialOption+":user")
				for key, description := range usable {
					name := strings.TrimSpace(key)
					if strings.HasPrefix(name, "+:") || strings.HasPrefix(name, "-:") {
						name = strings.TrimSpace(name[2:])
					} else if strings.HasPrefix(name, "append_") {
						// rc4 的旧格式只在 value 确实是已知分组时才把 value
						// 当作分组名，否则保留 append_N，与 NewAPI 兼容逻辑一致。
						candidate := strings.TrimSpace(description)
						if _, known := result.Ratios[candidate]; known {
							name = candidate
						}
					}
					add(name, websiteGroupSpecialOption)
				}
			}
		}
	}
	for _, key := range []string{"TopupGroupRatio", "ModelRequestRateLimitGroup"} {
		if raw, ok := values[key]; ok && strings.TrimSpace(raw) != "" {
			var object map[string]json.RawMessage
			if err := json.Unmarshal([]byte(raw), &object); err != nil {
				result.ParseErrors = append(result.ParseErrors, key+" JSON 无法解析")
			} else {
				for group := range object {
					add(group, key)
				}
			}
		}
	}
	return result, nil
}

func (m *Monitor) fetchGroupGovernanceUsers(ctx context.Context) ([]GroupGovernanceUser, error) {
	rows, err := m.prodDB.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(8000) */ id, username, "+
		"COALESCE(display_name, ''), status, role, TRIM(COALESCE(`group`, '')) "+
		"FROM users WHERE deleted_at IS NULL ORDER BY id LIMIT ?", groupGovernanceMaxUsers+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := make([]GroupGovernanceUser, 0)
	scanned := 0
	for rows.Next() {
		scanned++
		var user GroupGovernanceUser
		if err := rows.Scan(&user.UserID, &user.Username, &user.DisplayName, &user.Status, &user.Role, &user.Grp); err != nil {
			return nil, err
		}
		user.Grp = strings.TrimSpace(user.Grp)
		if user.Grp != "" {
			users = append(users, user)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if scanned > groupGovernanceMaxUsers {
		return nil, fmt.Errorf("用户数超过安全上限 %d", groupGovernanceMaxUsers)
	}
	return users, nil
}

func (m *Monitor) fetchGroupGovernanceTokens(ctx context.Context, now int64) (map[string]groupGovernanceTokenStats, error) {
	rows, err := m.prodDB.QueryContext(ctx, groupGovernanceTokenStatsSQL, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]groupGovernanceTokenStats{}
	for rows.Next() {
		var group string
		var status, expired int
		var count int64
		if err := rows.Scan(&group, &status, &expired, &count); err != nil {
			return nil, err
		}
		stats := result[group]
		stats.Total += count
		if status == 1 && expired == 0 {
			stats.Enabled += count
		} else {
			stats.Disabled += count
		}
		if expired == 1 {
			stats.Expired += count
		}
		result[group] = stats
	}
	return result, rows.Err()
}

func (m *Monitor) fetchGroupGovernanceSubscriptions(ctx context.Context) (map[string][]groupGovernanceSubscription, bool, error) {
	rows, err := m.prodDB.QueryContext(ctx, `SELECT /*+ MAX_EXECUTION_TIME(5000) */ id, title, enabled,
TRIM(COALESCE(upgrade_group, '')) FROM subscription_plans WHERE TRIM(COALESCE(upgrade_group, '')) <> ''`)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	result := map[string][]groupGovernanceSubscription{}
	for rows.Next() {
		var group string
		var plan groupGovernanceSubscription
		if err := rows.Scan(&plan.ID, &plan.Title, &plan.Enabled, &group); err != nil {
			return nil, false, err
		}
		result[group] = append(result[group], plan)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return result, true, nil
}

func (m *Monitor) loadGroupGovernanceChannels(ctx context.Context) ([]ChannelSnap, int64, error) {
	var rows []ChannelSnap
	if err := m.storeDB.WithContext(ctx).Where("deleted_at = 0").Order("id ASC").Find(&rows).Error; err != nil {
		return nil, 0, err
	}
	var updated int64
	for _, row := range rows {
		if row.UpdatedAt > updated {
			updated = row.UpdatedAt
		}
	}
	return rows, updated, nil
}

func (m *Monitor) loadGroupGovernanceActivity(ctx context.Context, now time.Time) (map[string]groupGovernanceActivity, int64, int64, bool, int64, int64, error) {
	result := map[string]groupGovernanceActivity{}
	cutoff30 := now.Unix() - groupGovernanceHistoryDays*86400
	cutoff7 := now.Unix() - 7*86400
	type activityRow struct {
		Grp    string
		Count  int64
		LastAt int64
	}
	var delivered30, delivered7, rejected30, rejected7 []activityRow
	effective := stabilityEffectiveSampleSQL("sh")
	queries := []struct {
		dst    *[]activityRow
		query  string
		cutoff int64
	}{
		{&delivered30, `SELECT sh.grp, SUM(sh.success + sh.anomaly + sh.failed) AS count, MAX(sh.hour_ts) AS last_at FROM stability_hour_samples sh WHERE sh.hour_ts >= ? AND sh.grp <> '' AND ` + effective + ` GROUP BY sh.grp`, cutoff30},
		{&delivered7, `SELECT sh.grp, SUM(sh.success + sh.anomaly + sh.failed) AS count, MAX(sh.hour_ts) AS last_at FROM stability_hour_samples sh WHERE sh.hour_ts >= ? AND sh.grp <> '' AND ` + effective + ` GROUP BY sh.grp`, cutoff7},
		{&rejected30, `SELECT grp, SUM(count) AS count, MAX(hour_ts) AS last_at FROM stability_reject_hours WHERE hour_ts >= ? AND grp <> '' GROUP BY grp`, cutoff30},
		{&rejected7, `SELECT grp, SUM(count) AS count, MAX(hour_ts) AS last_at FROM stability_reject_hours WHERE hour_ts >= ? AND grp <> '' GROUP BY grp`, cutoff7},
	}
	for _, query := range queries {
		if err := m.storeDB.WithContext(ctx).Raw(query.query, query.cutoff).Scan(query.dst).Error; err != nil {
			return nil, 0, 0, false, 0, 0, err
		}
	}
	merge := func(rows []activityRow, delivered, recent bool) {
		for _, row := range rows {
			group := strings.TrimSpace(row.Grp)
			if group == "" {
				continue
			}
			activity := result[group]
			if delivered && recent {
				activity.Delivered7d = row.Count
			} else if delivered {
				activity.Delivered30d = row.Count
			} else if recent {
				activity.Rejected7d = row.Count
			} else {
				activity.Rejected30d = row.Count
			}
			if row.LastAt > activity.LastAt {
				activity.LastAt = row.LastAt
			}
			result[group] = activity
		}
	}
	merge(delivered30, true, false)
	merge(delivered7, true, true)
	merge(rejected30, false, false)
	merge(rejected7, false, true)
	var unattributed struct {
		Count7d  int64
		Count30d int64
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT
COALESCE(SUM(CASE WHEN hour_ts >= ? THEN count ELSE 0 END),0) AS count7d,
COALESCE(SUM(count),0) AS count30d
FROM stability_reject_hours WHERE hour_ts >= ? AND TRIM(COALESCE(grp,'')) = ''`, cutoff7, cutoff30).Scan(&unattributed).Error; err != nil {
		return nil, 0, 0, false, 0, 0, err
	}

	// 完整性必须读“空小时也签收”的台账，不能用事实表 MIN/MAX 猜测。
	// 后者会把真实零流量误判为未采集，进而让清理候选永远无法判定。
	coverage := m.stabilityDataCoverage(ctx, cutoff30, now.Unix(), now.Unix())
	return result, coverage.FromTs, coverage.ToTs, coverage.EffectiveComplete, unattributed.Count7d, unattributed.Count30d, nil
}

type groupGovernanceBuildRow struct {
	row           GroupGovernanceGroup
	issues        []string
	configSources []string
	channels      []groupGovernanceChannel
	subscriptions []groupGovernanceSubscription
}

func buildGroupGovernanceSnapshot(input groupGovernanceInput, now time.Time) ([]GroupGovernanceGroup, []GroupGovernanceUser, GroupGovernanceState, error) {
	builders := map[string]*groupGovernanceBuildRow{}
	ensure := func(group string) *groupGovernanceBuildRow {
		group = strings.TrimSpace(group)
		if group == "" || len([]byte(group)) > 64 {
			return nil
		}
		if builders[group] == nil {
			builders[group] = &groupGovernanceBuildRow{row: GroupGovernanceGroup{Grp: group}}
		}
		return builders[group]
	}
	for group, ratio := range input.Ratios {
		if b := ensure(group); b != nil {
			b.row.RatioConfigured, b.row.Ratio = true, ratio
			b.row.Current = true
		}
	}
	for group, description := range input.Displays {
		if b := ensure(group); b != nil {
			b.row.DisplayName, b.row.UserSelectable, b.row.Current = description, input.Selectable[group], true
		}
	}
	for group := range input.Auto {
		if b := ensure(group); b != nil {
			b.row.AutoConfigured, b.row.Current = true, true
		}
	}
	for group, sources := range input.ConfigSources {
		if b := ensure(group); b != nil {
			b.row.Current = true
			for source := range sources {
				b.configSources = append(b.configSources, source)
			}
		}
	}
	for _, channel := range input.Channels {
		seen := map[string]struct{}{}
		for _, group := range splitList(channel.Groups) {
			if _, duplicate := seen[group]; duplicate {
				continue
			}
			seen[group] = struct{}{}
			if b := ensure(group); b != nil {
				b.row.Current = true
				if channel.Status == 1 {
					b.row.EnabledChannels++
				} else {
					b.row.DisabledChannels++
				}
				b.channels = append(b.channels, groupGovernanceChannel{ID: channel.ID, Name: channel.Name, Status: channel.Status})
			}
		}
	}
	userRows := make([]GroupGovernanceUser, 0, len(input.Users))
	for _, user := range input.Users {
		if b := ensure(user.Grp); b != nil {
			b.row.Current = true
			b.row.UserCount++
			if user.Status == 1 {
				b.row.EnabledUserCount++
			} else {
				b.row.DisabledUserCount++
			}
			userRows = append(userRows, user)
		}
	}
	for group, stats := range input.Tokens {
		if b := ensure(group); b != nil {
			b.row.Current = true
			b.row.ExplicitTokenCount, b.row.EnabledTokenCount = stats.Total, stats.Enabled
			b.row.DisabledTokenCount, b.row.ExpiredTokenCount = stats.Disabled, stats.Expired
		}
	}
	for group, plans := range input.Subscriptions {
		if b := ensure(group); b != nil {
			b.row.Current = true
			for _, plan := range plans {
				if plan.Enabled {
					b.row.EnabledSubscriptionPlans++
				} else {
					b.row.DisabledSubscriptionPlans++
				}
			}
			b.subscriptions = append(b.subscriptions, plans...)
		}
	}
	for group, activity := range input.Activity {
		if b := ensure(group); b != nil {
			b.row.Requests7d = activity.Delivered7d + activity.Rejected7d
			b.row.Requests30d = activity.Delivered30d + activity.Rejected30d
			b.row.PreRouteRejections7d, b.row.PreRouteRejections30d = activity.Rejected7d, activity.Rejected30d
			b.row.LastObservedAt = activity.LastAt
		}
	}
	if len(builders) > groupGovernanceMaxGroups {
		return nil, nil, GroupGovernanceState{}, fmt.Errorf("分组数超过安全上限 %d", groupGovernanceMaxGroups)
	}

	canonical := map[string][]string{}
	for group := range builders {
		key := canonicalGroupName(group)
		canonical[key] = append(canonical[key], group)
	}
	for key := range canonical {
		sort.Strings(canonical[key])
	}
	criticalConfigError := false
	for _, sourceErr := range input.SourceErrors {
		if strings.Contains(sourceErr, "JSON") || strings.Contains(sourceErr, "GroupRatio") || strings.Contains(sourceErr, "UserUsableGroups") || strings.Contains(sourceErr, "渠道快照") {
			criticalConfigError = true
			break
		}
	}

	groups := make([]GroupGovernanceGroup, 0, len(builders))
	state := GroupGovernanceState{
		ID: groupGovernanceStateID, Revision: strconv.FormatInt(now.UnixNano(), 10),
		LastAttemptAt: now.Unix(), LastSuccessAt: now.Unix(), Complete: len(input.SourceErrors) == 0,
		CoverageStartAt: input.CoverageStartAt, CoverageEndAt: input.CoverageEndAt,
		HistoryComplete: input.HistoryComplete, SubscriptionVerified: input.SubscriptionChecked,
		UnattributedRejects7d: input.UnattributedRejects7d, UnattributedRejects30d: input.UnattributedRejects30d,
	}
	state.SourceErrorsJSON = marshalGroupGovernanceJSON(input.SourceErrors)
	for _, b := range builders {
		row := &b.row
		row.SyncedAt = now.Unix()
		row.HistoricalOnly = !row.Current
		if row.Current {
			state.CurrentGroupCount++
		} else {
			state.HistoricalGroupCount++
		}
		if row.UserCount > 0 {
			b.configSources = append(b.configSources, "users.group")
		}
		if row.ExplicitTokenCount > 0 {
			b.configSources = append(b.configSources, "tokens.group")
		}
		if len(b.channels) > 0 {
			b.configSources = append(b.configSources, "channels.group")
		}
		if len(b.subscriptions) > 0 {
			b.configSources = append(b.configSources, "subscription_plans.upgrade_group")
		}

		high := false
		specialAuto := row.Grp == "auto"
		ratioRiskReference := row.EnabledChannels > 0 || row.UserCount > 0 || row.ExplicitTokenCount > 0 || row.EnabledSubscriptionPlans > 0
		if row.Current && ratioRiskReference && !row.RatioConfigured && !specialAuto {
			b.issues = append(b.issues, "当前引用但缺少倍率")
			high = true
		}
		if row.UserSelectable && row.EnabledChannels == 0 {
			b.issues = append(b.issues, "用户可选但无启用渠道")
			high = true
		}
		if row.EnabledUserCount > 0 && row.EnabledChannels == 0 {
			b.issues = append(b.issues, "有启用用户但无启用渠道")
			high = true
		}
		if criticalConfigError && row.Current {
			b.issues = append(b.issues, "关键配置来源不完整")
			high = true
		}

		pending := false
		if !row.RatioConfigured && row.DisabledChannels > 0 && !ratioRiskReference && !specialAuto {
			b.issues = append(b.issues, "禁用渠道引用但缺少倍率")
			pending = true
		}
		if match := groupNameRatioPattern.FindStringSubmatch(row.Grp); len(match) == 2 {
			row.NameHasRatio = true
			b.issues = append(b.issues, "名称包含倍率")
			pending = true
			if named, err := strconv.ParseFloat(match[1], 64); err == nil && row.RatioConfigured && math.Abs(named-row.Ratio) > 0.01 {
				b.issues = append(b.issues, "名称倍率与当前倍率不一致")
			}
		}
		if row.RatioConfigured && strings.TrimSpace(row.DisplayName) == "" {
			b.issues = append(b.issues, "已配置倍率但缺少展示说明")
			pending = true
		}
		if row.UserSelectable && !row.RatioConfigured && !specialAuto {
			b.issues = append(b.issues, "只有展示配置")
			pending = true
		}
		if row.AutoConfigured && row.EnabledChannels == 0 {
			b.issues = append(b.issues, "自动分组无启用渠道")
			pending = true
		}
		if variants := canonical[canonicalGroupName(row.Grp)]; len(variants) > 1 {
			b.issues = append(b.issues, "疑似重复: "+strings.Join(variants, " / "))
			pending = true
		}

		special := row.Grp == "default" || row.Grp == "auto"
		blockingConfigReference := false
		for _, source := range b.configSources {
			switch source {
			case "GroupRatio", "channels.group", "users.group", "tokens.group":
				// 清理规则对这些来源有独立、更精确的数量条件。
			default:
				blockingConfigReference = true
			}
		}
		row.CleanupCandidate = row.Current && row.RatioConfigured && row.EnabledChannels == 0 &&
			row.UserCount == 0 && row.ExplicitTokenCount == 0 && len(b.subscriptions) == 0 && !blockingConfigReference &&
			row.Requests30d == 0 && !special && input.HistoryComplete && input.SubscriptionChecked
		if row.CleanupCandidate {
			b.issues = append(b.issues, "清理候选（仍需人工核对历史任务与长期令牌）")
		}
		if row.HistoricalOnly {
			b.issues = append(b.issues, "近期历史遗留")
		}
		if high {
			row.Status = "high"
			state.HighRiskCount++
		} else if pending {
			row.Status = "pending"
		} else if row.CleanupCandidate || row.HistoricalOnly || (row.Current && row.EnabledChannels == 0) {
			row.Status = "observe"
		} else {
			row.Status = "normal"
		}
		if row.Current && row.EnabledChannels == 0 {
			state.NoEnabledChannelCount++
		}
		if row.CleanupCandidate {
			state.CleanupCandidateCount++
		}
		sort.Strings(b.issues)
		sort.Strings(b.configSources)
		sort.Slice(b.channels, func(i, j int) bool { return b.channels[i].ID < b.channels[j].ID })
		sort.Slice(b.subscriptions, func(i, j int) bool { return b.subscriptions[i].ID < b.subscriptions[j].ID })
		row.IssuesJSON = marshalGroupGovernanceJSON(b.issues)
		row.ConfigSourcesJSON = marshalGroupGovernanceJSON(uniqueStrings(b.configSources))
		row.ChannelsJSON = marshalGroupGovernanceJSON(b.channels)
		row.SubscriptionsJSON = marshalGroupGovernanceJSON(b.subscriptions)
		groups = append(groups, *row)
	}
	sort.Slice(groups, func(i, j int) bool {
		order := map[string]int{"high": 0, "pending": 1, "observe": 2, "normal": 3}
		if order[groups[i].Status] != order[groups[j].Status] {
			return order[groups[i].Status] < order[groups[j].Status]
		}
		return groups[i].Grp < groups[j].Grp
	})
	return groups, userRows, state, nil
}

func canonicalGroupName(name string) string {
	return strings.Map(func(r rune) rune {
		if r == '-' || r == '_' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, strings.ToLower(strings.TrimSpace(name)))
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func marshalGroupGovernanceJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "[]"
	}
	return string(raw)
}

func (m *Monitor) publishGroupGovernanceSnapshot(ctx context.Context, groups []GroupGovernanceGroup, users []GroupGovernanceUser, state GroupGovernanceState) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("1 = 1").Delete(&GroupGovernanceUser{}).Error; err != nil {
			return err
		}
		if err := tx.Where("1 = 1").Delete(&GroupGovernanceGroup{}).Error; err != nil {
			return err
		}
		if len(groups) > 0 {
			if err := tx.CreateInBatches(groups, 100).Error; err != nil {
				return err
			}
		}
		if len(users) > 0 {
			if err := tx.CreateInBatches(users, 250).Error; err != nil {
				return err
			}
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, UpdateAll: true}).Create(&state).Error
	})
}

func (m *Monitor) markGroupGovernanceFailure(syncErr error) {
	if m.storeDB == nil || syncErr == nil {
		return
	}
	now := time.Now().Unix()
	message := groupGovernanceErrorSummary(syncErr)
	var state GroupGovernanceState
	err := m.storeDB.First(&state, groupGovernanceStateID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		state = GroupGovernanceState{ID: groupGovernanceStateID}
	} else if err != nil {
		return
	}
	state.LastAttemptAt, state.Complete, state.LastError = now, false, message
	state.SourceErrorsJSON = marshalGroupGovernanceJSON([]string{message})
	_ = m.storeDB.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, UpdateAll: true}).Create(&state).Error
}

func groupGovernanceErrorSummary(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "后台同步超时，已保留上次快照"
	case errors.Is(err, errSourceNotReady):
		return "NewAPI 只读来源尚未就绪，已保留上次快照"
	default:
		return "后台同步失败，已保留上次快照"
	}
}

func (m *Monitor) loadGroupGovernanceReport(ctx context.Context) (groupGovernanceReport, error) {
	report := groupGovernanceReport{Enabled: m.cfg.GroupGovernanceEnabled, Groups: []groupGovernanceGroupView{}, SourceErrors: []string{}}
	if !m.cfg.GroupGovernanceEnabled {
		return report, nil
	}
	var rows []GroupGovernanceGroup
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&report.State, groupGovernanceStateID).Error; err != nil {
			return err
		}
		return tx.Order("CASE status WHEN 'high' THEN 0 WHEN 'pending' THEN 1 WHEN 'observe' THEN 2 ELSE 3 END, grp ASC").Find(&rows).Error
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return report, nil
		}
		return report, err
	}
	_ = json.Unmarshal([]byte(report.State.SourceErrorsJSON), &report.SourceErrors)
	for _, row := range rows {
		view := groupGovernanceGroupView{GroupGovernanceGroup: row}
		_ = json.Unmarshal([]byte(row.IssuesJSON), &view.Issues)
		_ = json.Unmarshal([]byte(row.ConfigSourcesJSON), &view.ConfigSources)
		_ = json.Unmarshal([]byte(row.ChannelsJSON), &view.Channels)
		_ = json.Unmarshal([]byte(row.SubscriptionsJSON), &view.Subscriptions)
		if view.Issues == nil {
			view.Issues = []string{}
		}
		if view.ConfigSources == nil {
			view.ConfigSources = []string{}
		}
		if view.Channels == nil {
			view.Channels = []groupGovernanceChannel{}
		}
		if view.Subscriptions == nil {
			view.Subscriptions = []groupGovernanceSubscription{}
		}
		report.Groups = append(report.Groups, view)
	}
	return report, nil
}

func (m *Monitor) serveGroupGovernanceReport(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	report, err := m.loadGroupGovernanceReport(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取分组治理本地快照失败"})
		return
	}
	c.JSON(http.StatusOK, report)
}

func (m *Monitor) serveGroupGovernanceUsers(c *gin.Context) {
	if !m.cfg.GroupGovernanceEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "分组治理功能未启用"})
		return
	}
	group := strings.TrimSpace(c.Query("group"))
	if group == "" || len([]byte(group)) > 64 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "分组参数无效"})
		return
	}
	query := strings.TrimSpace(c.Query("q"))
	if len([]rune(query)) > 64 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "搜索关键词过长"})
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit < 1 {
		limit = 20
	}
	if limit > groupGovernanceUserPageLimit {
		limit = groupGovernanceUserPageLimit
	}
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if offset < 0 || offset > groupGovernanceMaxUsers {
		offset = 0
	}
	db := m.storeDB.WithContext(c.Request.Context()).Model(&GroupGovernanceUser{}).Where("grp = ?", group)
	if query != "" {
		db = db.Where("user_id = ? OR instr(lower(username), lower(?)) > 0 OR instr(lower(display_name), lower(?)) > 0", query, query, query)
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取关联用户失败"})
		return
	}
	var users []GroupGovernanceUser
	if err := db.Order("status ASC, user_id ASC").Limit(limit).Offset(offset).Find(&users).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取关联用户失败"})
		return
	}
	if users == nil {
		users = []GroupGovernanceUser{}
	}
	c.JSON(http.StatusOK, gin.H{"group": group, "total": total, "offset": offset, "limit": limit, "users": users})
}

func (m *Monitor) exportGroupGovernanceCSV(c *gin.Context) {
	if !m.cfg.GroupGovernanceEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "分组治理功能未启用"})
		return
	}
	report, err := m.loadGroupGovernanceReport(c.Request.Context())
	if err != nil || report.State.LastSuccessAt == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "尚无可导出的可靠分组快照"})
		return
	}
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Disposition", `attachment; filename="newapi-group-governance.csv"`)
	c.Status(http.StatusOK)
	// Excel 在无 BOM 时容易误判中文编码。
	_, _ = c.Writer.Write([]byte{0xEF, 0xBB, 0xBF})
	w := csv.NewWriter(c.Writer)
	_ = w.Write([]string{
		"old_group", "display_name", "current_ratio", "status", "enabled_channels", "disabled_channels",
		"user_count", "enabled_user_count", "disabled_user_count", "explicit_token_count",
		"requests_7d", "requests_30d", "pre_route_rejections_7d", "pre_route_rejections_30d",
		"last_observed_at", "issues", "suggested_new_group", "notes",
	})
	for _, group := range report.Groups {
		ratio := ""
		if group.RatioConfigured {
			ratio = strconv.FormatFloat(group.Ratio, 'f', -1, 64)
		}
		_ = w.Write([]string{
			groupGovernanceCSVSafe(group.Grp), groupGovernanceCSVSafe(group.DisplayName), ratio, group.Status,
			strconv.Itoa(group.EnabledChannels), strconv.Itoa(group.DisabledChannels), strconv.Itoa(group.UserCount),
			strconv.Itoa(group.EnabledUserCount), strconv.Itoa(group.DisabledUserCount), strconv.FormatInt(group.ExplicitTokenCount, 10),
			strconv.FormatInt(group.Requests7d, 10), strconv.FormatInt(group.Requests30d, 10),
			strconv.FormatInt(group.PreRouteRejections7d, 10), strconv.FormatInt(group.PreRouteRejections30d, 10),
			formatGroupGovernanceCSVTime(group.LastObservedAt), groupGovernanceCSVSafe(strings.Join(group.Issues, "; ")), "", "",
		})
	}
	w.Flush()
}

func groupGovernanceCSVSafe(value string) string {
	value = strings.ReplaceAll(value, "\x00", "")
	trimmed := strings.TrimLeft(value, " \t\r\n")
	if trimmed != "" && strings.ContainsRune("=+-@", rune(trimmed[0])) {
		return "'" + value
	}
	return value
}

func formatGroupGovernanceCSVTime(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.Unix(ts, 0).In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05")
}
