package monitor

import (
	"log/slog"
	"time"
)

// startupTimer logs how long each startup phase takes. Every deployment is
// down for about three minutes, mostly before the HTTP listener opens; these
// lines show which phase the time goes to before any of it is optimized.
type startupTimer struct {
	start, last time.Time
}

func newStartupTimer() *startupTimer {
	now := time.Now()
	return &startupTimer{start: now, last: now}
}

// mark logs the time since the previous mark and since the timer started.
func (t *startupTimer) mark(phase string) {
	if t == nil {
		return
	}
	now := time.Now()
	slog.Info("启动阶段耗时", "phase", phase,
		"elapsed_ms", now.Sub(t.last).Milliseconds(), "total_ms", now.Sub(t.start).Milliseconds())
	t.last = now
}

// StartupMark records a startup phase from outside the package (main).
func (m *Monitor) StartupMark(phase string) {
	if m != nil {
		m.startupTimer.mark(phase)
	}
}
