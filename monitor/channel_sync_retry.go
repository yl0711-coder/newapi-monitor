package monitor

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

var errGroupGovernanceChannelsPending = errors.New("当前渠道配置快照未就绪或过期")

// Only a dependency failure arms the wakeup. Routine channel refreshes must
// not turn the users/tokens/subscriptions audit into a minute-level scan.
func (m *Monitor) groupGovernanceConfigWake() chan struct{} {
	m.groupGovernanceWakeOnce.Do(func() { m.groupGovernanceWake = make(chan struct{}, 1) })
	return m.groupGovernanceWake
}

func (m *Monitor) notifyGroupGovernanceChannelsReady() {
	if m.groupGovernanceWaitingChannels.Load() {
		select {
		case m.groupGovernanceConfigWake() <- struct{}{}:
		default:
		}
	}
}

func (m *Monitor) requireGroupGovernanceChannels(ctx context.Context) error {
	_, _, err := m.currentGroupGovernanceChannels(ctx)
	return err
}

func (m *Monitor) currentGroupGovernanceChannels(ctx context.Context) ([]ChannelSnap, int64, error) {
	// Arm before reading, so a publication racing this read cannot lose a wake.
	m.groupGovernanceWaitingChannels.Store(true)
	channels, updated, err := m.loadGroupGovernanceChannels(ctx)
	if err != nil {
		return nil, 0, err
	}
	if !m.groupGovernanceChannelCurrent(updated, time.Now()) {
		return nil, updated, errGroupGovernanceChannelsPending
	}
	m.groupGovernanceWaitingChannels.Store(false)
	return channels, updated, nil
}

func channelSyncRetryDelay(regular time.Duration, failures int) time.Duration {
	if failures <= 0 {
		return regular
	}
	// One bounded attempt at a time; persistent failures never busy-loop.
	return min(regular, 15*time.Second*time.Duration(1<<min(failures-1, 3)))
}

type configSyncTiming struct {
	queue, source, publish time.Duration
	phase                  string
	started                time.Time
}

func (t *configSyncTiming) enter(phase string) {
	if !t.started.IsZero() {
		d := time.Since(t.started)
		switch t.phase {
		case "queue":
			t.queue += d
		case "source":
			t.source += d
		case "publish":
			t.publish += d
		}
	}
	t.phase, t.started = phase, time.Now()
}

func (t *configSyncTiming) finish(name string, err error) {
	phase := t.phase
	t.enter("")
	slog.Info("配置同步阶段耗时", "task", name, "queue_ms", t.queue.Milliseconds(), "source_ms", t.source.Milliseconds(), "publish_ms", t.publish.Milliseconds(), "failed_phase", func() string {
		if err != nil {
			return phase
		}
		return ""
	}())
}
