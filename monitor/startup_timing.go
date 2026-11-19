package monitor

import (
	"log/slog"
	"time"
)

// startupTimer records startup timing before the HTTP listeners open.
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

// snapshotStep reports an inner step without changing the outer startup marks.
func (t *startupTimer) snapshotStep(phase string, began time.Time, err error) {
	if t != nil {
		slog.Info("启动快照分段耗时", "phase", phase, "elapsed_ms", time.Since(began).Milliseconds(), "ok", err == nil)
	}
}

// StartupMark records a startup phase from outside the package (main).
func (m *Monitor) StartupMark(phase string) {
	if m != nil {
		m.startupTimer.mark(phase)
	}
}
