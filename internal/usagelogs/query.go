// Package usagelogs implements the Monitor administrator's read-only log queries.
// It has no dependency on Monitor, Gin, or the customer Usage application.
package usagelogs

// 使用日志（通用日志）：按 new-api v1.0.0-rc.48 管理员“使用日志 → 通用日志”的行为
// 重新实现（未拷贝其源码），只读查询 new-api 只读从库，供 monitor 管理员排查。
// 口径要点（与 new-api 保持一致）：
//   - 列表与统计保留完整查询范围；本模块不提供导出。
//   - 只有用户名、模型名支持 % 通配（最多 2 个、不能连续、关键词至少 2 字节，_ 按字面）；
//     其余筛选全部精确匹配。
//   - 统计忽略类型、请求 ID、上游请求 ID：用量 = 所选时间内消耗（type=2）的额度之和；
//     RPM/TPM = 服务器最近 60 秒的消耗记录，与所选时间无关。
//   - 排序 created_at DESC, id DESC；每页最多 100 条；总数不封顶。
//   - other 按超级管理员口径返回（旧的顶层 reject_reason 移入 admin_info）。
// 与 monitor 其他页面隔离：独立的并发闸门（容量 2），不占客户门户/客户排障共用的
// usageDetailGate；其中总数/金额统计最多占 1 个名额，给列表留出容量。
// 原列表接口默认仍返回精确总数；页面可用 defer_total=1 先取记录，再按相同
// 列表筛选独立精确计数。每条查询带 MAX_EXECUTION_TIME，不使用近似金额或计数。

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"net"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

const (
	usageLogsMaxPageSize      = 100
	usageLogsDefaultPageSize  = 10
	usageLogsGateCapacity     = 2
	usageLogsGateWait         = 15 * time.Second
	usageLogsQueryTimeout     = 25 * time.Second
	usageLogsRPMWindowSeconds = 60
	// A cancelled request kills its own replica statement within this budget,
	// so an abandoned aggregation does not keep running until MAX_EXECUTION_TIME.
	usageLogsKillTimeout = 3 * time.Second
	// MAX_EXECUTION_TIME hint (ms) keeps a slow replica query from running past
	// the HTTP timeout; SQLite (tests) treats it as a comment.
	// MySQL only recognizes optimizer hints AFTER SELECT, including on joins.
	// 18s, deliberately below the production DSN readTimeout (20s): the server
	// then ends the statement with a clear 3024 timeout before the driver's
	// socket read times out and reports a bare "invalid connection".
	usageLogsSelect = "SELECT /*+ MAX_EXECUTION_TIME(18000) */ "
)

var (
	errUsageLogsSourceUnavailable = errors.New("未连接 new-api 只读从库，无法查询使用日志")
	errUsageLogsBusy              = errors.New("使用日志查询繁忙，请稍后再试")
)

// usageLogsQuerier is satisfied by *sql.DB and by the per-request *sql.Conn.
type usageLogsQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// usageLogsSlowQuery is the duration above which a replica query from this
// page is logged, with the shape of the filter (never its values), so slow
// patterns can be found without exposing user data.
const usageLogsSlowQuery = 2 * time.Second

func usageLogsObserve(phase string, started time.Time, f Filter) {
	elapsed := time.Since(started)
	if elapsed < usageLogsSlowQuery {
		return
	}
	span := int64(0)
	if f.Start > 0 && f.End > f.Start {
		span = (f.End - f.Start) / 3600
	}
	slog.Warn("使用日志从库查询较慢", "phase", phase, "elapsed_ms", elapsed.Milliseconds(), "range_hours", span,
		"type", f.Type != 0, "model", f.ModelName != "", "username", f.Username != "", "token", f.TokenName != "",
		"channel", f.Channel != 0, "group", f.Group != "", "request_id", f.RequestID != "", "upstream_request_id", f.UpstreamRequestID != "",
		"user_index", f.userID > 0)
}

func usageLogsQueryErr(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	var serverErr *mysqlDriver.MySQLError
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(err, &serverErr) && serverErr.Number == 3024) {
		return errors.New("查询超时，请缩小时间范围或增加筛选条件")
	}
	// A socket read timeout (DSN readTimeout) or dropped tunnel surfaces from
	// the driver as an invalid connection; say what it means to the admin.
	var netErr net.Error
	if errors.Is(err, mysqlDriver.ErrInvalidConn) || errors.Is(err, driver.ErrBadConn) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return errors.New("查询超时或从库连接中断，请缩小时间范围后重试")
	}
	return err
}
