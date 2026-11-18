package monitor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func alertOutboxTestMonitor(t *testing.T) (*Monitor, AlertConfig) {
	t.Helper()
	m := newTestMonitor(t)
	c := defaultAlertConfig()
	c.Enabled, c.SMTPHost, c.Recipients, c.ModelAlertsEnabled, c.ServerAlertsEnabled = true, "smtp.invalid", "ops@example.invalid", true, true
	c.CooldownMin = 30
	return m, c
}

func alertRows(t *testing.T, m *Monitor, kind string) []AlertLog {
	t.Helper()
	var rows []AlertLog
	if err := m.storeDB.Where("kind = ? OR kind = ?", kind, kind+"_FAILED").Order("id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestAlertFireDoesNotBlockOnSlowSMTP(t *testing.T) {
	m, c := alertOutboxTestMonitor(t)
	release := make(chan struct{})
	var sends atomic.Int32
	orig := alertSendMail
	alertSendMail = func(AlertConfig, string, string) error { sends.Add(1); <-release; return nil }
	defer func() { alertSendMail = orig }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.startAlertOutbox(ctx)

	started := time.Now()
	m.fire(c, "error_burst", "ch-1", "错误突发:ch-1", "body", 1000)
	if time.Since(started) > time.Second {
		t.Fatal("fire blocked on SMTP")
	}
	waitFor(t, "send to start", func() bool { return sends.Load() == 1 })
	// Same alert while the first is still being delivered: not queued twice.
	m.fire(c, "error_burst", "ch-1", "错误突发:ch-1", "body", 1010)
	// A different target is independent.
	m.fire(c, "error_burst", "ch-2", "错误突发:ch-2", "body", 1010)
	close(release)
	waitFor(t, "both deliveries", func() bool { return len(alertRows(t, m, "error_burst")) == 2 })
	if sends.Load() != 2 {
		t.Fatalf("sends = %d, want 2 (one per target)", sends.Load())
	}
	rows := alertRows(t, m, "error_burst")
	if rows[0].Target != "ch-1" || rows[0].Ts != 1000 || rows[0].Kind != "error_burst" || rows[0].Detail != "错误突发:ch-1" {
		t.Fatalf("logged row wrong: %+v", rows[0])
	}
	// Logged: now inside the cooldown.
	m.fire(c, "error_burst", "ch-1", "错误突发:ch-1", "body", 1100)
	time.Sleep(50 * time.Millisecond)
	if sends.Load() != 2 {
		t.Fatal("cooldown not applied after delivery")
	}
}

func TestAlertDeliveryFailureAndFullQueueAreLogged(t *testing.T) {
	m, c := alertOutboxTestMonitor(t)
	orig := alertSendMail
	alertSendMail = func(AlertConfig, string, string) error { return errors.New("smtp unreachable") }
	defer func() { alertSendMail = orig }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.startAlertOutbox(ctx)
	m.fire(c, "error_rate", "ch-9", "错误率告警:ch-9", "body", 2000)
	waitFor(t, "failed delivery", func() bool { return len(alertRows(t, m, "error_rate")) == 1 })
	if r := alertRows(t, m, "error_rate")[0]; r.Kind != "error_rate_FAILED" || r.Detail != "smtp unreachable" || r.Ts != 2000 {
		t.Fatalf("failure row wrong: %+v", r)
	}

	// A full queue records the alert as failed instead of blocking.
	full := &alertOutbox{queue: make(chan alertDelivery), pending: map[string]bool{}}
	m2, _ := alertOutboxTestMonitor(t)
	m2.alerts = full
	started := time.Now()
	m2.fire(c, "error_burst", "ch-x", "错误突发:ch-x", "body", 3000)
	if time.Since(started) > time.Second {
		t.Fatal("fire blocked on a full queue")
	}
	rows := alertRows(t, m2, "error_burst")
	if len(rows) != 1 || rows[0].Kind != "error_burst_FAILED" || rows[0].Detail != alertOutboxFullDetail || full.isPending("error_burst", "ch-x") {
		t.Fatalf("full queue not recorded: %+v", rows)
	}
}

func TestSamplerDownIsEvaluatedOutsideTheSamplingLoop(t *testing.T) {
	m, c := alertOutboxTestMonitor(t)
	c.SamplerDownEnabled = true
	if err := m.saveAlertConfig(c); err != nil {
		t.Fatal(err)
	}
	m.cfg.SampleSeconds = 60
	var sends atomic.Int32
	orig := alertSendMail
	alertSendMail = func(AlertConfig, string, string) error { sends.Add(1); return nil }
	defer func() { alertSendMail = orig }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.startAlertOutbox(ctx)
	now := int64(100000)
	m.lastRun.Store(now - 60) // fresh
	m.evaluateSamplerDown(now)
	m.lastRun.Store(now - 4*60) // stale for more than three periods
	// The sampling loop no longer evaluates it once the watchdog runs.
	m.evaluateAlerts(now)
	time.Sleep(50 * time.Millisecond)
	if sends.Load() != 0 {
		t.Fatalf("sampler_down fired from the wrong place: %d", sends.Load())
	}
	m.evaluateSamplerDown(now)
	waitFor(t, "sampler_down", func() bool { return len(alertRows(t, m, "sampler_down")) == 1 })
}
