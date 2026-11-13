package monitor

import (
	"context"
	"fmt"
	"time"
)

// Follow the actual sampler cadence. Grace covers the bounded query and local
// publication without making a ten-minute snapshot stale after five minutes.
func (m *Monitor) groupGovernanceChannelMaxAge() time.Duration {
	every := max(10*time.Second, time.Duration(m.cfg.SampleSeconds)*time.Second)
	if m.cfg.LogChainOnlySource {
		every = m.logChainChannelSyncEvery()
	}
	return every + max(30*time.Second, min(2*time.Minute, every/5))
}

func (m *Monitor) groupGovernanceChannelCurrent(updated int64, now time.Time) bool {
	return !m.channelSnapshotSyncFailed.Load() && updated > 0 && now.Unix()-updated <= int64(m.groupGovernanceChannelMaxAge().Seconds())
}

// The restricted source mode has no users/tokens/subscriptions permission.
// Refresh only the seven existing configuration options and the local channel
// snapshot at the original low-frequency cadence. No guessed group census.
func (m *Monitor) syncGroupGovernanceConfigOnly(parent context.Context) (result error) {
	timing := &configSyncTiming{}
	timing.enter("publish")
	defer func() { timing.finish("group_config", result) }()
	if m.prodDB == nil {
		return errSourceNotReady
	}
	ctx, cancel := context.WithTimeout(parent, groupGovernanceQueueWait+groupGovernanceQueryTimeout)
	defer cancel()
	if err := m.requireGroupGovernanceChannels(ctx); err != nil {
		return err
	}
	timing.enter("queue")
	release, err := m.acquireGroupGovernanceSource(ctx)
	if err != nil {
		return err
	}
	queryCtx, cancelQuery := context.WithTimeout(ctx, groupGovernanceQueryTimeout)
	defer cancelQuery()
	ctx = queryCtx
	timing.enter("source")
	options, err := m.fetchGroupGovernanceOptions(ctx)
	release()
	if err != nil {
		return fmt.Errorf("当前分组配置未读取（需 options 的现有七项只读权限）: %w", err)
	}
	timing.enter("publish")
	channels, _, err := m.currentGroupGovernanceChannels(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	issues := append([]string{}, options.ParseErrors...)
	issues = append(issues, "当前来源模式仅核验分组配置与渠道；用户、令牌、订阅引用未采集")
	groups, _, state, err := buildGroupGovernanceSnapshot(groupGovernanceInput{Ratios: options.Ratios, Displays: options.Displays, Selectable: options.Selectable, Auto: options.Auto, ConfigSources: options.ConfigSources, Channels: channels, SourceErrors: issues}, now)
	if err != nil {
		return err
	}
	return m.publishGroupGovernanceSnapshot(ctx, groups, nil, state)
}
