package monitor

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Alert delivery outbox.
//
// fire() used to send each e-mail synchronously (up to 15 s per message)
// from wherever the alert was evaluated, including the per-minute sampling
// loop, so a slow SMTP server delayed the next minute's sample. fire() now
// keeps the cooldown and category checks synchronous (local SQLite only) and
// hands the message to one delivery goroutine. The alert log row is written
// after delivery exactly as before: the kind on success, kind+"_FAILED" on
// failure, timestamped with the evaluation time, so cooldowns and the recent
// alerts page keep their meaning. While a message waits, the same
// (kind, target) is treated as in cooldown so it is not queued twice.
//
// Without a started outbox (unit tests, the manual test-mail endpoint path)
// fire() delivers synchronously as before.

const (
	alertOutboxSize          = 256
	alertOutboxFullDetail    = "告警发送队列已满，本条未发送"
	samplerWatchdogStaleRuns = 3
)

type alertDelivery struct {
	cfg                         AlertConfig
	kind, target, subject, body string
	now                         int64
}

type alertOutbox struct {
	queue   chan alertDelivery
	mu      sync.Mutex
	pending map[string]bool
}

func alertKey(kind, target string) string { return kind + "\x00" + target }

func (o *alertOutbox) isPending(kind, target string) bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pending[alertKey(kind, target)]
}

func (o *alertOutbox) setPending(kind, target string, on bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if on {
		o.pending[alertKey(kind, target)] = true
	} else {
		delete(o.pending, alertKey(kind, target))
	}
}

// startAlertOutbox starts the delivery goroutine and the sampler watchdog.
// It must run before any loop that can fire alerts.
func (m *Monitor) startAlertOutbox(ctx context.Context) {
	if m.alerts != nil {
		return
	}
	o := &alertOutbox{queue: make(chan alertDelivery, alertOutboxSize), pending: map[string]bool{}}
	m.alerts = o
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-o.queue:
				m.deliverAlert(d)
				o.setPending(d.kind, d.target, false)
			}
		}
	}()
	go m.runSamplerWatchdog(ctx)
}

// enqueueAlert hands a message to the delivery goroutine. A full queue (SMTP
// down for a long time) records a failed alert instead of blocking.
func (m *Monitor) enqueueAlert(d alertDelivery) {
	o := m.alerts
	o.setPending(d.kind, d.target, true)
	select {
	case o.queue <- d:
	default:
		o.setPending(d.kind, d.target, false)
		slog.Warn("告警发送队列已满，本条未发送", "kind", d.kind)
		m.logAlert(d.kind+"_FAILED", d.target, alertOutboxFullDetail, d.now)
	}
}

// alertSendMail is the SMTP send used for alerts; tests replace it.
var alertSendMail = sendMail

func (m *Monitor) deliverAlert(d alertDelivery) {
	if err := alertSendMail(d.cfg, "[new-api监控] "+d.subject, d.body); err != nil {
		// 发送失败也记一条,避免反复重试刷屏(并暴露问题)
		m.logAlert(d.kind+"_FAILED", d.target, err.Error(), d.now)
		return
	}
	m.logAlert(d.kind, d.target, d.subject, d.now)
}

// runSamplerWatchdog evaluates the sampler_down alert outside the sampling
// loop. Inside the loop it could not fire while the loop itself was stuck,
// which is exactly when it is needed.
func (m *Monitor) runSamplerWatchdog(ctx context.Context) {
	every := time.Duration(m.cfg.SampleSeconds) * time.Second
	if every <= 0 {
		every = time.Minute
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.evaluateSamplerDown(now.Unix())
		}
	}
}

func (m *Monitor) evaluateSamplerDown(nowUnix int64) {
	if m.cfg.AlertsDisabled {
		return
	}
	c := m.loadAlertConfig()
	if !c.Enabled || c.SMTPHost == "" || c.Recipients == "" || !c.SamplerDownEnabled {
		return
	}
	last := m.LastSampleRun()
	if last > 0 && last < nowUnix-int64(m.cfg.SampleSeconds)*samplerWatchdogStaleRuns {
		m.fire(c, "sampler_down", "", "采样器掉线", "监控采样器超过 3 个周期未成功运行,可能已停止或连不上数据库。", nowUnix)
	}
}
