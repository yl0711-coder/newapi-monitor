package usagelogs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// User describes a user resolved from the replica for the administrator log view.
type User struct {
	ID           int64  `json:"id"`
	Username     string `json:"username"`
	DisplayName  string `json:"display_name"`
	Quota        int64  `json:"quota"`
	UsedQuota    int64  `json:"used_quota"`
	RequestCount int64  `json:"request_count"`
	Group        string `json:"group"`
	AffCode      string `json:"aff_code"`
	AffCount     int64  `json:"aff_count"`
	AffQuota     int64  `json:"aff_quota"`
	Remark       string `json:"remark"`
}

// Options holds the display settings read from the
// replica's options table, with new-api's defaults.
type Options struct {
	QuotaPerUnit        float64  `json:"quota_per_unit"`
	QuotaDisplayType    string   `json:"quota_display_type"`
	USDExchangeRate     float64  `json:"usd_exchange_rate"`
	CustomSymbol        string   `json:"custom_currency_symbol"`
	CustomExchangeRate  float64  `json:"custom_currency_exchange_rate"`
	Groups              []string `json:"groups"`
	HasEnabledSubscribe bool     `json:"has_enabled_subscription_plans"`
}

func loadUsageLogsOptions(ctx context.Context, db usageLogsQuerier) (Options, error) {
	o := Options{QuotaPerUnit: 500000, QuotaDisplayType: "USD", USDExchangeRate: 7.3, CustomSymbol: "¤",
		CustomExchangeRate: 1, Groups: []string{}}
	keys := []any{"QuotaPerUnit", "USDExchangeRate", "GroupRatio", "general_setting.quota_display_type",
		"general_setting.custom_currency_symbol", "general_setting.custom_currency_exchange_rate"}
	rows, err := db.QueryContext(ctx, usageLogsSelect+"`key`, COALESCE(`value`,'') FROM options WHERE `key` IN (?,?,?,?,?,?)", keys...)
	if err != nil {
		return o, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return o, err
		}
		v = strings.TrimSpace(v)
		num, numErr := strconv.ParseFloat(v, 64)
		finite := numErr == nil && !math.IsNaN(num) && !math.IsInf(num, 0)
		switch k {
		case "QuotaPerUnit":
			if finite && num > 0 {
				o.QuotaPerUnit = num
			}
		case "USDExchangeRate":
			if finite && num > 0 {
				o.USDExchangeRate = num
			}
		case "general_setting.quota_display_type":
			if v != "" {
				o.QuotaDisplayType = strings.ToUpper(v)
			}
		case "general_setting.custom_currency_symbol":
			if v != "" {
				o.CustomSymbol = v
			}
		case "general_setting.custom_currency_exchange_rate":
			if finite && num > 0 {
				o.CustomExchangeRate = num
			}
		case "GroupRatio":
			var ratios map[string]json.RawMessage
			if json.Unmarshal([]byte(v), &ratios) == nil {
				for name := range ratios {
					if name != "auto" {
						o.Groups = append(o.Groups, name)
					}
				}
				sort.Strings(o.Groups)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return o, err
	}
	var enabled int64
	// The wallet icon on cost cells only shows when a plan is enabled. A
	// missing table (older schema) just means no plans.
	if err := db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM subscription_plans WHERE enabled = 1").Scan(&enabled); err == nil {
		o.HasEnabledSubscribe = enabled > 0
	}
	return o, nil
}

// Meta reads the log view's display settings.
func (r *Request) Meta() (Options, error) {
	db, session, err := r.open(false)
	if err != nil {
		return Options{}, err
	}
	ctx, cancel := context.WithTimeout(session, usageLogsQueryTimeout)
	defer cancel()
	o, err := loadUsageLogsOptions(ctx, db)
	if err != nil {
		return Options{}, fmt.Errorf("读取站点设置失败: %w", usageLogsQueryErr(ctx, err))
	}
	return o, nil
}

// User looks up the log view's user details by ID.
func (r *Request) User(id int64) (User, error) {
	if id <= 0 {
		return User{}, errors.New("用户 ID 无效")
	}
	db, session, err := r.open(false)
	if err != nil {
		return User{}, err
	}
	ctx, cancel := context.WithTimeout(session, usageLogsQueryTimeout)
	defer cancel()
	var u User
	err = db.QueryRowContext(ctx, usageLogsSelect+"id, COALESCE(username,''), COALESCE(display_name,''), COALESCE(quota,0), "+
		"COALESCE(used_quota,0), COALESCE(request_count,0), COALESCE(`group`,''), COALESCE(aff_code,''), COALESCE(aff_count,0), "+
		"COALESCE(aff_quota,0), COALESCE(remark,'') FROM users WHERE id = ? AND deleted_at IS NULL", id).
		Scan(&u.ID, &u.Username, &u.DisplayName, &u.Quota, &u.UsedQuota, &u.RequestCount, &u.Group, &u.AffCode, &u.AffCount, &u.AffQuota, &u.Remark)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, errors.New("用户不存在")
	}
	if err != nil {
		return User{}, fmt.Errorf("获取用户信息失败: %w", usageLogsQueryErr(ctx, err))
	}
	return u, nil
}
