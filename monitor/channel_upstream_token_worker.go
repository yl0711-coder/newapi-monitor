package monitor

import (
	"context"
	"errors"
	"math"
)

// Drain a durable job, not today's configuration. A cleared/replaced token must
// not strand old costs. Publication independently validates current signed
// evidence and historical ownership within its transaction. day=0 selects all
// days; a nonzero day constrains local acceptance/orchestration to one day.
func (m *Monitor) publishOneDueSub2TokenEconomicsHour(ctx context.Context, account ChannelUpstreamAccount, day, now int64, oldestFirst bool) (bool, error) {
	if m == nil || m.storeDB == nil {
		return false, errors.New("令牌成本存储未就绪")
	}
	if m.cfg.LocalSnapshotOnly {
		return false, errUpstreamTokenReadOnly
	}
	if !m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, account.Domain) || account.Provider != upstreamProviderSub2API || account.UsageAdapter == upstreamUsageAdapterSub2Stats || now <= 0 || now > math.MaxInt64-3840 || day < 0 || day > math.MaxInt64-86400 || day != 0 && (day != cstDayStart(day) || day+86400 > now) {
		return false, errors.New("Sub2 令牌发布任务范围无效")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	q := m.storeDB.WithContext(ctx).Where("domain=? AND account_epoch=? AND status='pending' AND next_attempt_at<=?", account.Domain, epoch, now)
	if day != 0 {
		q = q.Where("hour_ts>=? AND hour_ts<?", day, day+86400)
	}
	order := "hour_ts DESC, created_at ASC"
	if oldestFirst {
		order = "hour_ts ASC, created_at ASC"
	}
	var dirty ChannelEconomicsDirtyHour
	found := q.Order(order).Limit(1).Find(&dirty)
	if found.Error != nil || found.RowsAffected == 0 {
		return false, found.Error
	}
	if err := m.publishSub2TokenEconomicsHour(ctx, account, dirty.HourTs, dirty.Reason, now); err != nil {
		// Bounded exponential retry survives restarts. Error details can contain
		// upstream/private data: persist only a stable diagnostic category.
		attempts := dirty.Attempts
		if attempts < 0 {
			attempts = 0
		}
		if attempts < 7 {
			attempts++
		} else {
			attempts = 7
		}
		next := now + int64(60*(1<<(attempts-1)))
		update := m.storeDB.WithContext(ctx).Model(&ChannelEconomicsDirtyHour{}).
			Where("domain=? AND account_epoch=? AND hour_ts=? AND generation=?", account.Domain, epoch, dirty.HourTs, dirty.Generation).
			UpdateColumns(map[string]any{"attempts": attempts, "next_attempt_at": next, "last_error": "令牌成本报表发布待重试", "updated_at": now})
		return false, errors.Join(err, update.Error)
	}
	// A crash before ack repeats the same immutable publication, not a second
	// charge. A newer generation is never acknowledged by an older worker.
	err := m.storeDB.WithContext(ctx).Where("domain=? AND account_epoch=? AND hour_ts=? AND generation=?", account.Domain, epoch, dirty.HourTs, dirty.Generation).Delete(&ChannelEconomicsDirtyHour{}).Error
	return err == nil, err
}
