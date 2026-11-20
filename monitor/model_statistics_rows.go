package monitor

import (
	"context"
	"database/sql"
	"strings"

	"gorm.io/gorm"
)

// Fold the query's per-user/channel rows straight into the report dimensions.
// Keep the same SQL, row order and snapshot; never retain a second full copy of
// the grouped data or run the follow-up coverage queries with this cursor open.
func readModelStatisticsRows(ctx context.Context, query *gorm.DB) (map[string]map[string]*modelStatisticsAggregate, map[int64]string, error) {
	rows, err := query.WithContext(ctx).Rows()
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	groups := make(map[string]map[string]*modelStatisticsAggregate)
	usernames := make(map[int64]string)
	var row modelStatisticsRow
	// The report's historical GORM scanner treats NULL dimensions as zero
	// values. Keep that behavior for old SQLite rows and missing usernames.
	var group, model, username sql.NullString
	var channel sql.Null[int]
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		// Column order is the SELECT in buildModelStatisticsSelectedRead.
		if err := rows.Scan(&group, &model, &row.UserID, &channel, &username, &row.RoutedRequests, &row.UnavailableRoutes,
			&row.Ttft500, &row.Ttft1k, &row.Ttft2k, &row.Ttft5k, &row.Ttft10k, &row.TtftInf, &row.TtftMaxMs, &row.TtftObserved, &row.TtftOver3s); err != nil {
			return nil, nil, err
		}
		row.Group, row.Model, row.Username, row.ChannelID = group.String, model.String, username.String, channel.V
		addModelStatisticsRow(groups, usernames, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return groups, usernames, ctx.Err()
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
