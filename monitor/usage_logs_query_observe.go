package monitor

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Log failed read shapes, never filter values or the full SQL. The old
// username->current-user-ID heuristic has been retired: historical names can
// be renamed/reused, and cheap current-user metadata cannot prove equivalence.
func usageLogsWarnFailure(phase string, started time.Time, f usageLogFilter, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	span := int64(0)
	if f.Start > 0 && f.End > f.Start {
		span = (f.End - f.Start) / 3600
	}
	slog.Warn("使用日志从库查询失败", "phase", phase, "elapsed_ms", time.Since(started).Milliseconds(), "range_hours", span,
		"username", f.Username != "", "user_index", f.UserID > 0, "err", err)
}
