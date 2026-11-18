package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Instance egress x upstream connection-timeout alert.
//
// Every new-api ECS task reaches upstreams from its own public IP. When an
// upstream's network starts dropping one task's IP, only that task logs
// `dial tcp <ip>:<port>: i/o timeout`; the other tasks are unaffected. On
// 2026-11-17 one task's recycled IP could not reach any mainland-China
// upstream for a whole night before it was noticed: 32 timeouts per 10 minutes
// on that task, zero on the other 13, zero on every task in a normal hour.
//
// Every five minutes one CloudWatch Logs Insights query counts these lines per
// task log stream and upstream address over the last ten minutes (counts only;
// no log text is read back). A task is reported when it has at least
// EgressTimeoutAlertMin timeouts and at least half of all timeouts in the
// window. An upstream that is down for everyone spreads its timeouts over all
// tasks and is not an instance problem; it shows up in the stability pages.
//
// Cost: the query scans the worker log group for the window, 13-20 MB per ten
// minutes at production volume, about 2.6-5.8 GB per day (under one US dollar
// per month at the Logs Insights price).

const (
	egressTimeoutAlertKind    = "infra_egress_dial_timeout"
	egressTimeoutWindow       = 10 * time.Minute
	egressTimeoutPollEvery    = 5 * time.Minute
	egressTimeoutIngestDelay  = 2 * time.Minute
	egressTimeoutQueryLimit   = 500
	egressTimeoutMinShare     = 0.5
	egressTimeoutMaxUpstreams = 8
)

type egressTimeoutUpstream struct {
	Address string
	Count   int64
}

type egressTimeoutFinding struct {
	Stream    string
	Task      string
	Timeouts  int64
	Total     int64
	Upstreams []egressTimeoutUpstream
}

// egressTimeoutTaskID is the ECS task id at the end of an awslogs stream name
// (`new-api/new-api/<task id>`).
func egressTimeoutTaskID(stream string) string {
	if i := strings.LastIndex(stream, "/"); i >= 0 && i < len(stream)-1 {
		return stream[i+1:]
	}
	return stream
}

// egressTimeoutFindings returns the task streams whose timeouts are both
// frequent and concentrated, most timeouts first.
func egressTimeoutFindings(rows []map[string]string, minTimeouts int64) []egressTimeoutFinding {
	if minTimeouts < 1 {
		minTimeouts = 1
	}
	byStream := map[string]*egressTimeoutFinding{}
	var total int64
	for _, row := range rows {
		stream := strings.TrimSpace(row["@logStream"])
		count, err := strconv.ParseInt(strings.TrimSpace(row["timeouts"]), 10, 64)
		if stream == "" || err != nil || count <= 0 {
			continue
		}
		total += count
		f := byStream[stream]
		if f == nil {
			f = &egressTimeoutFinding{Stream: stream, Task: egressTimeoutTaskID(stream)}
			byStream[stream] = f
		}
		f.Timeouts += count
		address := strings.TrimSpace(row["upstream"])
		if address == "" {
			address = "未解析"
		}
		f.Upstreams = append(f.Upstreams, egressTimeoutUpstream{Address: address, Count: count})
	}
	var out []egressTimeoutFinding
	for _, f := range byStream {
		if f.Timeouts < minTimeouts || float64(f.Timeouts) < egressTimeoutMinShare*float64(total) {
			continue
		}
		f.Total = total
		sort.Slice(f.Upstreams, func(i, j int) bool {
			if f.Upstreams[i].Count != f.Upstreams[j].Count {
				return f.Upstreams[i].Count > f.Upstreams[j].Count
			}
			return f.Upstreams[i].Address < f.Upstreams[j].Address
		})
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timeouts != out[j].Timeouts {
			return out[i].Timeouts > out[j].Timeouts
		}
		return out[i].Stream < out[j].Stream
	})
	return out
}

func egressTimeoutAlertText(f egressTimeoutFinding, window time.Duration) (subject, body string) {
	share := 100 * float64(f.Timeouts) / float64(max(f.Total, 1))
	subject = fmt.Sprintf("new-api 实例 %s 连接上游超时集中（近 %d 分钟 %d 次）", shortTaskID(f.Task), int(window.Minutes()), f.Timeouts)
	lines := make([]string, 0, len(f.Upstreams))
	for i, u := range f.Upstreams {
		if i == egressTimeoutMaxUpstreams {
			lines = append(lines, fmt.Sprintf("另有 %d 个上游地址", len(f.Upstreams)-i))
			break
		}
		lines = append(lines, fmt.Sprintf("  %s：%d 次", u.Address, u.Count))
	}
	body = fmt.Sprintf(`%s

实例（ECS 任务）：%s
日志流：%s
近 %d 分钟连接超时（dial tcp … i/o timeout）：%d 次，占全部实例的 %.0f%%
涉及上游地址：
%s

只有这个实例连不上，其他实例正常，通常是该实例的出口 IP 被上游所在网络阻断。
处理：确认后停止该任务，ECS 会重建新任务并换用新的出口 IP；或为这些渠道配置固定出口代理。

（monitor 每 5 分钟统计一次 new-api 日志，只读取计数，不读取日志内容）`,
		subject, f.Task, f.Stream, int(window.Minutes()), f.Timeouts, share, strings.Join(lines, "\n"))
	return subject, body
}

func shortTaskID(task string) string {
	if len(task) > 8 {
		return task[:8]
	}
	return task
}

type egressTimeoutWatchState struct {
	lastSuccess atomic.Int64
	lastFailure atomic.Int64
	lastAlerts  atomic.Int64
}

func (m *Monitor) startEgressTimeoutWatch(ctx context.Context) {
	if m == nil || !m.cfg.EgressTimeoutAlertEnabled || m.cloudWatchLogs == nil || !m.cloudWatchLogs.enabled {
		return
	}
	go func() {
		for {
			if !waitSourceLifecycle(ctx, egressTimeoutPollEvery) {
				return
			}
			m.runEgressTimeoutCheck(ctx, time.Now())
		}
	}()
}

func (m *Monitor) runEgressTimeoutCheck(ctx context.Context, now time.Time) {
	to := now.Add(-egressTimeoutIngestDelay).Truncate(time.Minute)
	from := to.Add(-egressTimeoutWindow)
	result, err := m.cloudWatchLogs.insights(ctx, cloudWatchInsightsRequest{
		Kind: cwQueryWorkerEgressTimeout, From: from, To: to, Limit: egressTimeoutQueryLimit,
	})
	if err != nil {
		if ctx.Err() == nil {
			m.egressTimeout.lastFailure.Store(now.Unix())
			slog.Warn("出口连接超时统计失败(下个周期重试)", "err", err)
		}
		return
	}
	m.egressTimeout.lastSuccess.Store(now.Unix())
	findings := egressTimeoutFindings(result.Rows, int64(m.cfg.EgressTimeoutAlertMin))
	if len(result.Rows) >= egressTimeoutQueryLimit {
		slog.Warn("出口连接超时统计结果达到行数上限，计数可能偏低", "rows", len(result.Rows))
	}
	m.egressTimeout.lastAlerts.Store(int64(len(findings)))
	for _, f := range findings {
		slog.Warn("new-api 实例连接上游超时集中", "task", f.Task, "timeouts", f.Timeouts, "total", f.Total, "upstreams", len(f.Upstreams))
		if m.cfg.AlertsDisabled {
			continue
		}
		subject, body := egressTimeoutAlertText(f, egressTimeoutWindow)
		m.fire(m.loadAlertConfig(), egressTimeoutAlertKind, f.Task, subject, body, now.Unix())
	}
}
