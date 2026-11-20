package monitor

import (
	"context"
	"database/sql/driver"
	"strings"

	"gorm.io/gorm"
)

// Fold the query's per-user/channel rows straight into the report dimensions.
// Keep the same SQL, row order and snapshot; never retain a second full copy of
// the grouped data or run the follow-up coverage queries with this cursor open.
func readModelStatisticsRows(ctx context.Context, db *gorm.DB, query string, args ...any) (map[string]map[string]*modelStatisticsAggregate, map[int64]string, error) {
	groups := make(map[string]map[string]*modelStatisticsAggregate)
	usernames := make(map[int64]string)
	// The pinned driver's scalar function adapter treats TEXT as NUL
	// terminated; non-empty BLOB retains the bytes from the old scanner.
	// The adapter cannot read a zero-byte BLOB; NULL decodes to empty text.
	const columns = `NULLIF(CAST(grp AS BLOB),X''), NULLIF(CAST(model AS BLOB),X''), user_id, channel_id,
		NULLIF(CAST(username AS BLOB),X''), routed_requests, unavailable_routes,
		ttft_500, ttft_1k, ttft_2k, ttft_5k, ttft_10k, ttft_inf, ttft_max_ms, ttft_observed, ttft_over_3s`
	err := consumeModelReportSQL(ctx, db, columns, query, args, func(values []driver.Value) error {
		var row modelStatisticsRow
		if err := scanModelReportValues(values, &row.Group, &row.Model, &row.UserID, &row.ChannelID, &row.Username, &row.RoutedRequests, &row.UnavailableRoutes,
			&row.Ttft500, &row.Ttft1k, &row.Ttft2k, &row.Ttft5k, &row.Ttft10k, &row.TtftInf, &row.TtftMaxMs, &row.TtftObserved, &row.TtftOver3s); err != nil {
			return err
		}
		addModelStatisticsRow(groups, usernames, row)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return groups, usernames, nil
}

func addModelStatisticsRow(groups map[string]map[string]*modelStatisticsAggregate, usernames map[int64]string, row modelStatisticsRow) {
	group, model := strings.TrimSpace(row.Group), strings.TrimSpace(row.Model)
	models := groups[group]
	if models == nil {
		models = make(map[string]*modelStatisticsAggregate)
		groups[group] = models
	}
	item := models[model]
	if item == nil {
		item = &modelStatisticsAggregate{customers: make(map[int64]int64), channels: make(map[int]*modelStatisticsChannelAggregate)}
		models[model] = item
	}
	routed, unavailable := maxNonNegative(row.RoutedRequests), maxNonNegative(row.UnavailableRoutes)
	item.routed += routed
	item.unavailable += unavailable
	item.customers[row.UserID] += routed + unavailable
	if routed > 0 {
		mergeModelStatisticsTTFT(&item.ttft, row)
		channel := item.addChannelRequests(row.ChannelID, routed)
		mergeModelStatisticsTTFT(&channel.ttft, row)
	}
	if unavailable > 0 {
		// A pre-route rejection has no channel or TTFT denominator.
		item.addChannelRequests(modelStatisticsUnavailableChannelKey, unavailable)
	}
	if name := strings.TrimSpace(row.Username); row.UserID > 0 && name != "" {
		usernames[row.UserID] = name
	}
}

func (a *modelStatisticsAggregate) addChannelRequests(id int, requests int64) *modelStatisticsChannelAggregate {
	channel := a.channels[id]
	if channel == nil {
		channel = &modelStatisticsChannelAggregate{}
		a.channels[id] = channel
	}
	channel.requests += requests
	return channel
}
