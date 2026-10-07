package monitor

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const channelCostHistoricalChannelLimit = 500

// Historical attribution must not depend on the business report's date/group
// filters. This local, domain-scoped directory contains no credentials and does
// not change which channels are eligible for next-hour bindings.
type channelCostHistoricalChannelView struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Current bool   `json:"current"`
}

func loadChannelCostHistoricalChannels(ctx context.Context, db *gorm.DB, domain string) ([]channelCostHistoricalChannelView, bool, error) {
	rows := []channelCostHistoricalChannelView{}
	err := db.WithContext(ctx).Model(&ChannelSnap{}).
		Select("id, COALESCE(name,'') name, CASE WHEN deleted_at=0 THEN 1 ELSE 0 END AS current").
		Where("base_domain=? AND id>0", domain).
		Order("CASE WHEN deleted_at=0 THEN 0 ELSE 1 END, id").
		Limit(channelCostHistoricalChannelLimit + 1).Scan(&rows).Error
	if err != nil {
		return nil, false, err
	}
	truncated := len(rows) > channelCostHistoricalChannelLimit
	if truncated {
		rows = rows[:channelCostHistoricalChannelLimit]
	}
	for i := range rows {
		rows[i].Name = strings.TrimSpace(rows[i].Name)
		if rows[i].Name == "" {
			rows[i].Name = fmt.Sprintf("渠道 #%d", rows[i].ID)
		}
	}
	return rows, truncated, nil
}
