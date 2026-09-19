package monitor

import (
	"context"
	"strings"

	"gorm.io/gorm"
)

// ChannelBusinessGroupPolicy 保存网站分组是否进入渠道管理的业务统计口径。
//
// 该配置只影响 Monitor 的展示与汇总，不修改 NewAPI 分组、原始日志或
// 历史小时汇总。老数据库中没有记录的分组按“计入”处理，以保证升级前后口径不变。
type ChannelBusinessGroupPolicy struct {
	Grp       string `gorm:"primaryKey;size:64;column:grp"`
	Included  bool   `gorm:"not null;column:included"`
	UpdatedAt int64  `gorm:"column:updated_at;index"`
	UpdatedBy string `gorm:"column:updated_by;size:128"`
}

func loadChannelBusinessGroupPolicies(ctx context.Context, db *gorm.DB) (map[string]bool, error) {
	var rows []ChannelBusinessGroupPolicy
	if err := db.WithContext(ctx).Order("grp ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	result := make(map[string]bool, len(rows))
	for _, row := range rows {
		if group := strings.TrimSpace(row.Grp); group != "" {
			result[group] = row.Included
		}
	}
	return result, nil
}

func channelBusinessGroupIncluded(policies map[string]bool, group string) bool {
	included, configured := policies[strings.TrimSpace(group)]
	return !configured || included
}
