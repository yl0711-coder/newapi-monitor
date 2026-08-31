package monitor

// source_lifecycle.go 把 NewAPI MySQL 的可用性与 Monitor 进程、本地
// SQLite 解耦。来源短暂断开时页面仍能读已发布事实；后台
// worker 只在来源就绪且持有单例 lease 的 epoch 中运行。

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

var errSourceNotReady = errors.New("来源 worker 未就绪")
var errSourceLeaseResultInvalid = errors.New("来源 lease 返回结果无效")

type sourceLifecycleState int32

const (
	sourceStateDisabled sourceLifecycleState = iota
	sourceStateConnecting
	sourceStateReady
	sourceStateDegradedNetwork
	sourceStateStandbyLease
	sourceStateBlockedConfig
	sourceStateCriticalDrainTimeout
)

func (s sourceLifecycleState) String() string {
	switch s {
	case sourceStateDisabled:
		return "disabled"
	case sourceStateConnecting:
		return "connecting"
	case sourceStateReady:
		return "ready"
	case sourceStateDegradedNetwork:
		return "degraded_network"
	case sourceStateStandbyLease:
		return "standby_lease"
	case sourceStateBlockedConfig:
		return "blocked_config"
	case sourceStateCriticalDrainTimeout:
		return "critical_drain_timeout"
	default:
		return "unknown"
	}
}

type sourceErrorClass uint8

const (
	sourceErrorTransient sourceErrorClass = iota
	sourceErrorPermanent
)

func (s Settings) sourceWorkerIsEnabled() bool {
	if s.LocalSnapshotOnly {
		return false
	}
	if !s.sourceLifecycleConfigured {
		// 历史单元测试大量直接构造 Settings{}。保持原有
		// worker 行为，但不给 fake DB 额外加 MySQL lease SQL。
		return true
	}
	return s.SourceWorkerEnabled
}

func (s Settings) sourceLeaseIsRequired() bool {
	return s.sourceWorkerIsEnabled() && s.sourceLifecycleConfigured && s.SourceLeaseRequired
}

func (s Settings) sourceLeaseKey() string {
	name := strings.TrimSpace(s.SourceLeaseName)
	if name == "" {
		return "newapi-monitor-source-worker-v1"
	}
	return name
}

func sourceLeaseNameForDatabase(base, database string) (string, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		base = "newapi-monitor-source-worker-v1"
	}
	// MySQL lock 名上限 64 字节。数据库名进入稳定摘要：
	// 同一 MySQL 上不同 schema 不会误互斥，host/IP 别名又不会
	// 使同一来源拿到不同的锁。
	if len([]byte(base)) > 51 {
		return "", errors.New("MONITOR_SOURCE_LEASE_NAME 不能超过 51 字节")
	}
	digest := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(database))))
	return fmt.Sprintf("%s-%x", base, digest[:6]), nil
}

func (m *Monitor) setSourceState(state sourceLifecycleState) {
	m.sourceState.Store(int32(state))
	// 唤醒正在等待 high/low 闸门的旧 epoch，使其在取得槽位
	// 之前再次观察 ready/lease 并立即退出。
	m.backgroundSourceScheduleMu.Lock()
	m.signalBackgroundSourceLocked()
	m.backgroundSourceScheduleMu.Unlock()
}

func (m *Monitor) currentSourceState() sourceLifecycleState {
	return sourceLifecycleState(m.sourceState.Load())
}

func (m *Monitor) sourceAccessAllowed() bool {
	if !m.sourceLifecycleInitialized.Load() {
		// 保持直接构造 Monitor{} 的历史调度单测；生产 New
		// 在开放任何路由前一定会将 lifecycleInitialized 置 true。
		return true
	}
	if !m.cfg.sourceWorkerIsEnabled() || m.currentSourceState() != sourceStateReady {
		return false
	}
	return !m.cfg.sourceLeaseIsRequired() || m.sourceLeaseHeld.Load()
}

func (m *Monitor) initializeSource() error {
	dsn := strings.TrimSpace(m.cfg.ProdDSN)
	if dsn == "" {
		m.setSourceState(sourceStateBlockedConfig)
		return errors.New("未设置 NEWAPI_LOG_DSN(只读日志库),监控无数据来源")
	}
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil || strings.TrimSpace(parsed.DBName) == "" {
		m.setSourceState(sourceStateBlockedConfig)
		if err == nil {
			err = errors.New("DSN 未指定数据库")
		}
		return fmt.Errorf("NEWAPI_LOG_DSN 格式错误: %w", err)
	}
	leaseName, err := sourceLeaseNameForDatabase(m.cfg.sourceLeaseKey(), parsed.DBName)
	if err != nil {
		m.setSourceState(sourceStateBlockedConfig)
		return err
	}
	m.sourceLeaseName = leaseName

	open := m.cfg.sourceOpen
	if open == nil {
		open = func(dsn string) (*sql.DB, error) { return sql.Open("mysql", dsn) }
	}
	conn, err := open(dsn)
	if err != nil {
		m.setSourceState(sourceStateBlockedConfig)
		return fmt.Errorf("打开生产库失败: %w", err)
	}
	// 持有 lease 的专用连接占 1 条，剩余 3 条供后台单并发
	// 与偶发交互查询；既不扩大原有池，也不让 lease 被复用。
	conn.SetMaxOpenConns(4)
	conn.SetMaxIdleConns(2)
	conn.SetConnMaxLifetime(5 * time.Minute)
	m.prodDB = conn
	m.setSourceState(sourceStateConnecting)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = m.probeSource(ctx)
	cancel()
	if err != nil {
		class := classifySourceError(err)
		m.recordSourceFailure(class)
		if class == sourceErrorPermanent {
			m.setSourceState(sourceStateBlockedConfig)
			_ = conn.Close()
			m.prodDB = nil
			return fmt.Errorf("生产库配置/权限/表结构校验失败: %w", err)
		}
		m.setSourceState(sourceStateDegradedNetwork)
		slog.Warn("来源库暂时不可达，已降级为 SQLite 服务并后台恢复",
			"class", sourceFailureCode(err))
		return nil
	}
	m.recordSourceSuccess()
	if !m.cfg.sourceWorkerIsEnabled() {
		m.setSourceState(sourceStateDisabled)
	} else if m.cfg.sourceLeaseIsRequired() {
		m.setSourceState(sourceStateConnecting)
	} else {
		m.setSourceState(sourceStateReady)
	}
	return nil
}

var sourcePreflightQueries = []string{
	"SELECT id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name,use_time,is_stream,content,other,request_id,username FROM logs WHERE 1=0",
	"SELECT id,name,type,status,`group`,models,base_url FROM channels WHERE 1=0",
	"SELECT id,username,email,quota,used_quota,created_at FROM users WHERE 1=0",
	"SELECT id,user_id,name,`key`,`group`,used_quota,deleted_at FROM tokens WHERE 1=0",
	"SELECT `key`,`value` FROM options WHERE 1=0",
}

const usageFactFullHistoryIndexPreflightQuery = "SELECT id FROM logs FORCE INDEX (idx_user_created_type) WHERE 1=0"

func sourcePreflightQueriesForSettings(cfg Settings) []string {
	queries := append([]string(nil), sourcePreflightQueries...)
	// Full-history boundary discovery deliberately forces this production index.
	// Probe it before an epoch becomes ready so a restored or newly provisioned
	// source cannot appear healthy and only fail after durable jobs are claimed.
	// Offline snapshots retain full-history read semantics but never probe MySQL.
	if cfg.UsageFactsEnabled && cfg.UsageFactsFullHistoryEnabled && !cfg.LocalSnapshotOnly {
		queries = append(queries, usageFactFullHistoryIndexPreflightQuery)
	}
	return queries
}

func (m *Monitor) probeSource(ctx context.Context) error {
	if m.prodDB == nil {
		return errors.New("来源连接未初始化")
	}
	if m.cfg.sourceProbe != nil {
		return m.cfg.sourceProbe(ctx, m.prodDB)
	}
	if err := m.prodDB.PingContext(ctx); err != nil {
		return err
	}
	for _, query := range sourcePreflightQueriesForSettings(m.cfg) {
		rows, err := m.prodDB.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Monitor) pingSource(ctx context.Context) error {
	if m.prodDB == nil {
		return errors.New("来源连接未初始化")
	}
	if m.cfg.sourceProbe != nil {
		return m.cfg.sourceProbe(ctx, m.prodDB)
	}
	return m.prodDB.PingContext(ctx)
}

func classifySourceError(err error) sourceErrorClass {
	if err == nil {
		return sourceErrorTransient
	}
	var my *mysqlDriver.MySQLError
	if errors.As(err, &my) {
		switch my.Number {
		case 1044, 1045, 1046, 1049, 1054, 1142, 1143, 1146, 1176, 1227:
			return sourceErrorPermanent
		case 1040, 1203, 1205, 1213, 2002, 2003, 2006, 2013, 3024:
			return sourceErrorTransient
		}
	}
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalidCert) {
		return sourceErrorPermanent
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		// 容器/服务发现的记录可在 Monitor 启动后才发布；
		// NXDOMAIN 在这种场景也可能是短暂状态。坏 hostname 会通过
		// degraded_network/failure_streak 持续暴露，但不应触发启动风暴。
		return sourceErrorTransient
	}
	if errors.Is(err, errSourceLeaseResultInvalid) {
		return sourceErrorTransient
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return sourceErrorTransient
	}
	for _, transient := range []error{
		context.Canceled, context.DeadlineExceeded, driver.ErrBadConn, io.EOF,
		syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.EHOSTUNREACH,
		syscall.ENETUNREACH, syscall.ETIMEDOUT, syscall.EPIPE,
	} {
		if errors.Is(err, transient) {
			return sourceErrorTransient
		}
	}
	message := strings.ToLower(err.Error())
	for _, fragment := range []string{
		"connection refused", "connection reset", "broken pipe", "i/o timeout",
		"server has gone away", "lost connection", "bad connection", "unexpected eof",
	} {
		if strings.Contains(message, fragment) {
			return sourceErrorTransient
		}
	}
	for _, fragment := range []string{
		"access denied", "unknown database", "doesn't exist", "does not exist",
		"permission denied", "command denied", "x509:", "certificate",
		"unknown tls config", "invalid dsn",
	} {
		if strings.Contains(message, fragment) {
			return sourceErrorPermanent
		}
	}
	// 未知错误在首启时宁可 fail-fast，避免把错表/错驱动当成
	// 网络抖动后长期空跑。已运行进程会以低频周期复核。
	return sourceErrorPermanent
}

func sourceFailureCode(err error) string {
	var my *mysqlDriver.MySQLError
	if errors.As(err, &my) {
		return fmt.Sprintf("mysql_%d", my.Number)
	}
	if classifySourceError(err) == sourceErrorPermanent {
		return "permanent"
	}
	return "network"
}

func (m *Monitor) recordSourceFailure(class sourceErrorClass) int {
	now := time.Now().Unix()
	m.sourceLastFailureAt.Store(now)
	streak := int(m.sourceFailureStreak.Add(1))
	if class == sourceErrorPermanent {
		m.setSourceState(sourceStateBlockedConfig)
	} else {
		m.setSourceState(sourceStateDegradedNetwork)
	}
	return streak
}

func (m *Monitor) recordSourceSuccess() {
	m.sourceLastSuccessAt.Store(time.Now().Unix())
	m.sourceFailureStreak.Store(0)
	m.sourceNextRetryAt.Store(0)
}

func (m *Monitor) reportSourceQueryError(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errSourceNotReady) {
		return
	}
	class := classifySourceError(err)
	want := int32(1)
	if class == sourceErrorPermanent {
		want = 2
	} else if !isSourceConnectionFailure(err) {
		// 单条批量查询超时/死锁只由该任务退避，不重启整个
		// epoch。只有连接级错误或确定的永久配置错误才上报。
		return
	}
	for {
		current := m.sourceRuntimeFailureClass.Load()
		if current >= want || m.sourceRuntimeFailureClass.CompareAndSwap(current, want) {
			break
		}
	}
	if m.sourceFailureNotify != nil {
		select {
		case m.sourceFailureNotify <- struct{}{}:
		default:
		}
	}
}

func isSourceConnectionFailure(err error) bool {
	var my *mysqlDriver.MySQLError
	if errors.As(err, &my) {
		switch my.Number {
		case 1040, 1203, 2002, 2003, 2006, 2013:
			return true
		default:
			return false
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	for _, target := range []error{
		driver.ErrBadConn, io.EOF, syscall.ECONNREFUSED, syscall.ECONNRESET,
		syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.ETIMEDOUT, syscall.EPIPE,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	message := strings.ToLower(err.Error())
	for _, fragment := range []string{"connection refused", "connection reset", "broken pipe", "server has gone away", "lost connection", "bad connection", "unexpected eof"} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

type sourceLeaseHandle interface {
	Check(context.Context) (bool, error)
	Release() error
}

type mysqlSourceLease struct {
	conn *sql.Conn
	name string
	once sync.Once
	err  error
}

func acquireMySQLSourceLease(ctx context.Context, db *sql.DB, name string) (sourceLeaseHandle, bool, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	held, err := decodeMySQLSourceLeaseResult(acquired)
	if err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	if !held {
		_ = conn.Close()
		return nil, false, nil
	}
	return &mysqlSourceLease{conn: conn, name: name}, true, nil
}

func decodeMySQLSourceLeaseResult(acquired sql.NullInt64) (bool, error) {
	if !acquired.Valid {
		// MySQL GET_LOCK 只有在执行错误时返回 NULL，不能把它
		// 伪装成“另一实例持锁”的正常 standby。
		return false, errSourceLeaseResultInvalid
	}
	switch acquired.Int64 {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errSourceLeaseResultInvalid
	}
}

func (l *mysqlSourceLease) Check(ctx context.Context) (bool, error) {
	if l == nil || l.conn == nil {
		return false, errors.New("lease 连接不存在")
	}
	var held sql.NullInt64
	if err := l.conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?) = CONNECTION_ID()", l.name).Scan(&held); err != nil {
		return false, err
	}
	return held.Valid && held.Int64 == 1, nil
}

func (l *mysqlSourceLease) Release() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var released sql.NullInt64
		if l.conn != nil {
			if err := l.conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", l.name).Scan(&released); err != nil && !errors.Is(err, sql.ErrConnDone) {
				l.err = err
			}
			if err := l.conn.Close(); l.err == nil && err != nil {
				l.err = err
			}
		}
	})
	return l.err
}

func (m *Monitor) acquireSourceLease(ctx context.Context) (sourceLeaseHandle, bool, error) {
	if !m.cfg.sourceLeaseIsRequired() {
		return nil, true, nil
	}
	if m.cfg.sourceAcquireLease != nil {
		return m.cfg.sourceAcquireLease(ctx, m.prodDB, m.sourceLeaseName)
	}
	return acquireMySQLSourceLease(ctx, m.prodDB, m.sourceLeaseName)
}

// sourceEpochGroup 解决 sync.WaitGroup 的 Add/Wait 竞态：seal 之后不再
// 允许任何子任务 Add，然后才能安全 Wait。
type sourceEpochGroup struct {
	mu     sync.Mutex
	sealed bool
	wg     sync.WaitGroup
}

type sourceEpochContextKey struct{}

func newSourceEpoch(parent context.Context) (context.Context, context.CancelFunc, *sourceEpochGroup) {
	ctx, cancel := context.WithCancel(parent)
	group := &sourceEpochGroup{}
	ctx = context.WithValue(ctx, sourceEpochContextKey{}, group)
	return ctx, cancel, group
}

func (g *sourceEpochGroup) Go(ctx context.Context, fn func(context.Context)) bool {
	g.mu.Lock()
	if g.sealed {
		g.mu.Unlock()
		return false
	}
	g.wg.Add(1)
	g.mu.Unlock()
	go func() {
		defer g.wg.Done()
		fn(ctx)
	}()
	return true
}

func goSourceEpoch(ctx context.Context, fn func(context.Context)) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	group, _ := ctx.Value(sourceEpochContextKey{}).(*sourceEpochGroup)
	if group == nil {
		go fn(ctx)
		return true
	}
	return group.Go(ctx, fn)
}

func (g *sourceEpochGroup) SealAndWait(timeout time.Duration) (bool, <-chan struct{}) {
	g.mu.Lock()
	g.sealed = true
	g.mu.Unlock()
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true, done
	case <-timer.C:
		return false, done
	}
}

func (m *Monitor) setSourceEpochContext(ctx context.Context) {
	m.sourceEpochMu.Lock()
	m.sourceEpochCtx = ctx
	m.sourceEpochMu.Unlock()
}

func (m *Monitor) clearSourceEpochContext(ctx context.Context) {
	m.sourceEpochMu.Lock()
	if m.sourceEpochCtx == ctx {
		m.sourceEpochCtx = nil
	}
	m.sourceEpochMu.Unlock()
}

func (m *Monitor) sourceTaskContext() context.Context {
	if !m.sourceLifecycleInitialized.Load() {
		return m.taskContext()
	}
	m.sourceEpochMu.RLock()
	ctx := m.sourceEpochCtx
	m.sourceEpochMu.RUnlock()
	if ctx != nil {
		return ctx
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	return canceled
}

func (m *Monitor) sourceRetryDuration(streak int, permanent bool) time.Duration {
	if m.cfg.sourceRetryDelay != nil {
		return m.cfg.sourceRetryDelay(streak)
	}
	if permanent {
		return 5 * time.Minute
	}
	if streak < 1 {
		streak = 1
	}
	shift := streak - 1
	if shift > 6 {
		shift = 6
	}
	delay := 5 * time.Second * time.Duration(1<<shift)
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	// 稳定的单向 0..20% 抖动，避免多实例同时重连。
	jitterPct := time.Duration((streak * 37) % 21)
	return delay + delay*jitterPct/100
}

func (m *Monitor) sourceCheckEvery() time.Duration {
	if m.cfg.sourceCheckInterval > 0 {
		return m.cfg.sourceCheckInterval
	}
	return 30 * time.Second
}

func (m *Monitor) sourcePreflightEvery() time.Duration {
	if m.cfg.sourcePreflightInterval > 0 {
		return m.cfg.sourcePreflightInterval
	}
	return 5 * time.Minute
}

func (m *Monitor) sourceDrainWithin() time.Duration {
	if m.cfg.sourceDrainTimeout > 0 {
		return m.cfg.sourceDrainTimeout
	}
	return 20 * time.Second
}

func waitSourceLifecycle(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		delay = time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (m *Monitor) startSourceSupervisor(parent context.Context) {
	if !m.cfg.sourceWorkerIsEnabled() || m.prodDB == nil {
		m.setSourceState(sourceStateDisabled)
		return
	}
	m.sourceSupervisorOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		done := make(chan struct{})
		m.sourceSupervisorCancelMu.Lock()
		m.sourceSupervisorCancel = cancel
		m.sourceSupervisorDone = done
		m.sourceSupervisorCancelMu.Unlock()
		go func() {
			defer close(done)
			m.superviseSource(ctx)
		}()
	})
}

func waitSourceDoneUntil(done <-chan struct{}, deadline time.Time) bool {
	if done == nil {
		return true
	}
	wait := time.Until(deadline)
	if wait <= 0 {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// stopAndWaitSource 与 Compose stop_grace_period 共用一个明确预算：
// HTTP Shutdown 最多 5s，这里最多 20s，Compose 保留 40s。
func (m *Monitor) stopAndWaitSource() bool {
	deadline := time.Now().Add(m.sourceDrainWithin())
	m.sourceSupervisorCancelMu.Lock()
	cancel := m.sourceSupervisorCancel
	done := m.sourceSupervisorDone
	m.sourceSupervisorCancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if !waitSourceDoneUntil(done, deadline) {
		slog.Error("关闭时来源 supervisor 未在时限内退出，将保留 DB/lease 至进程终止",
			"critical", true, "timeout", m.sourceDrainWithin().String())
		return false
	}
	if !m.sourceDrainTimedOut.Load() {
		return true
	}
	m.sourceQuarantineMu.Lock()
	quarantineDone := m.sourceQuarantineDone
	m.sourceQuarantineMu.Unlock()
	if !waitSourceDoneUntil(quarantineDone, deadline) {
		slog.Error("关闭时隔离 epoch 仍未退出，将保留 DB/lease 至进程终止",
			"critical", true, "timeout", m.sourceDrainWithin().String())
		return false
	}
	return true
}

func (m *Monitor) superviseSource(ctx context.Context) {
	needPreflight := m.sourceLastSuccessAt.Load() == 0
	for ctx.Err() == nil {
		if needPreflight {
			m.setSourceState(sourceStateConnecting)
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := m.probeSource(probeCtx)
			cancel()
			if err != nil {
				if errors.Is(err, context.Canceled) && ctx.Err() != nil {
					return
				}
				class := classifySourceError(err)
				streak := m.recordSourceFailure(class)
				if class == sourceErrorPermanent {
					m.sourceNextRetryAt.Store(0)
					slog.Error("来源库配置/权限/结构错误，已阻断自动重试，需修复配置后重启",
						"class", sourceFailureCode(err))
					return
				}
				delay := m.sourceRetryDuration(streak, class == sourceErrorPermanent)
				m.sourceNextRetryAt.Store(time.Now().Add(delay).Unix())
				slog.Warn("来源库恢复探测未通过", "class", sourceFailureCode(err), "retry_in", delay.String())
				if !waitSourceLifecycle(ctx, delay) {
					return
				}
				continue
			}
			m.recordSourceSuccess()
			needPreflight = false
		}

		leaseCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		lease, acquired, err := m.acquireSourceLease(leaseCtx)
		cancel()
		if err != nil {
			class := classifySourceError(err)
			streak := m.recordSourceFailure(class)
			if class == sourceErrorPermanent {
				m.sourceNextRetryAt.Store(0)
				slog.Error("来源 lease 配置错误，已阻断自动重试，需修复后重启",
					"class", sourceFailureCode(err))
				return
			}
			delay := m.sourceRetryDuration(streak, class == sourceErrorPermanent)
			m.sourceNextRetryAt.Store(time.Now().Add(delay).Unix())
			needPreflight = class == sourceErrorTransient
			if !waitSourceLifecycle(ctx, delay) {
				return
			}
			continue
		}
		if !acquired {
			m.sourceLeaseHeld.Store(false)
			m.setSourceState(sourceStateStandbyLease)
			delay := m.sourceRetryDuration(1, false)
			m.sourceNextRetryAt.Store(time.Now().Add(delay).Unix())
			if !waitSourceLifecycle(ctx, delay) {
				return
			}
			continue
		}

		m.sourceLeaseHeld.Store(m.cfg.sourceLeaseIsRequired())
		m.setSourceState(sourceStateReady)
		m.sourceNextRetryAt.Store(0)
		epochCtx, epochCancel, group := newSourceEpoch(ctx)
		m.setSourceEpochContext(epochCtx)
		m.sourceWorkerRunning.Store(true)
		m.startSourceEpoch(epochCtx, group)

		class, disconnected, leaseLost := m.monitorSourceEpoch(epochCtx, lease)
		switch {
		case class == sourceErrorPermanent:
			m.setSourceState(sourceStateBlockedConfig)
		case leaseLost:
			m.setSourceState(sourceStateStandbyLease)
		default:
			m.setSourceState(sourceStateDegradedNetwork)
		}
		m.sourceLeaseHeld.Store(false)
		m.sourceWorkerRunning.Store(false)
		m.clearSourceEpochContext(epochCtx)
		epochCancel()
		drained, drainDone := group.SealAndWait(m.sourceDrainWithin())
		if !drained {
			// 继续持有 lease，不允许另一实例或新 epoch 与尚未退出
			// 的 worker 重叠访问来源。只有进程关闭才最终释放。
			m.setSourceState(sourceStateCriticalDrainTimeout)
			m.sourceQuarantineMu.Lock()
			m.sourceQuarantinedLease = lease
			m.sourceQuarantineDone = drainDone
			m.sourceQuarantineMu.Unlock()
			m.sourceDrainTimedOut.Store(true)
			slog.Error("来源 epoch 取消后未在时限内退场，已停止重启并隔离 lease",
				"critical", true, "drain_timeout", m.sourceDrainWithin().String())
			return
		}
		if lease != nil {
			if err := lease.Release(); err != nil && ctx.Err() == nil {
				slog.Warn("释放来源 lease 失败", "class", sourceFailureCode(err))
			}
		}
		if ctx.Err() != nil {
			return
		}
		if class == sourceErrorPermanent {
			m.sourceNextRetryAt.Store(0)
			return
		}
		needPreflight = disconnected
		streak := int(m.sourceFailureStreak.Load())
		if streak < 1 {
			streak = 1
		}
		delay := m.sourceRetryDuration(streak, class == sourceErrorPermanent)
		m.sourceNextRetryAt.Store(time.Now().Add(delay).Unix())
		if !waitSourceLifecycle(ctx, delay) {
			return
		}
	}
}

func (m *Monitor) startSourceEpoch(ctx context.Context, group *sourceEpochGroup) {
	if m.cfg.sourceWorkerStart != nil {
		group.Go(ctx, func(workerCtx context.Context) { m.cfg.sourceWorkerStart(workerCtx, m) })
		return
	}
	group.Go(ctx, func(workerCtx context.Context) { m.startSampler(workerCtx) })
	if m.usageFactsEnabled() {
		group.Go(ctx, func(workerCtx context.Context) { m.superviseUsageFactsSync(workerCtx) })
		if m.usageFactsFullHistoryEnabled() {
			group.Go(ctx, func(workerCtx context.Context) { m.superviseUsageFactHistoryWorker(workerCtx) })
		}
	}
	// 上游账户 worker 同样只在持 lease 的 epoch 中运行，避免
	// 多 Monitor 实例重复轮询触发上游风控。其内部 goroutine 在
	// channel_upstream.go 中通过 goSourceEpoch 加入同一 group。
	m.startChannelUpstreamSync(ctx)
}

func (m *Monitor) monitorSourceEpoch(ctx context.Context, lease sourceLeaseHandle) (sourceErrorClass, bool, bool) {
	ticker := time.NewTicker(m.sourceCheckEvery())
	defer ticker.Stop()
	nextPreflight := time.Now().Add(m.sourcePreflightEvery())
	for {
		select {
		case <-ctx.Done():
			return sourceErrorTransient, false, false
		case <-m.sourceFailureNotify:
			reported := m.sourceRuntimeFailureClass.Swap(0)
			class := sourceErrorTransient
			if reported >= 2 {
				class = sourceErrorPermanent
			}
			m.recordSourceFailure(class)
			return class, true, false
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			var err error
			if !time.Now().Before(nextPreflight) {
				err = m.probeSource(checkCtx)
				nextPreflight = time.Now().Add(m.sourcePreflightEvery())
			} else {
				err = m.pingSource(checkCtx)
			}
			if err == nil && lease != nil {
				var held bool
				held, err = lease.Check(checkCtx)
				if err == nil && !held {
					cancel()
					return sourceErrorTransient, false, true
				}
			}
			cancel()
			if err != nil {
				if errors.Is(err, context.Canceled) && ctx.Err() != nil {
					return sourceErrorTransient, false, false
				}
				class := classifySourceError(err)
				m.recordSourceFailure(class)
				slog.Warn("来源库连接或 lease 丢失，正在停止当前 epoch", "class", sourceFailureCode(err))
				return class, true, false
			}
			m.recordSourceSuccess()
		}
	}
}

func (m *Monitor) localProbeEvery() time.Duration {
	if m.cfg.localProbeInterval > 0 {
		return m.cfg.localProbeInterval
	}
	return 30 * time.Second
}

func probeGORMStore(ctx context.Context, db *gorm.DB) bool {
	if db == nil {
		return false
	}
	sqlDB, err := db.DB()
	if err != nil {
		return false
	}
	var one int
	return sqlDB.QueryRowContext(ctx, "SELECT 1").Scan(&one) == nil && one == 1
}

func (m *Monitor) factsStoreRequired() bool {
	return m.cfg.UsageFactsEnabled || m.cfg.UsageFactsReadEnabled || m.cfg.UsageFactsLocalReadOnly
}

func (m *Monitor) probeLocalStores(parent context.Context) {
	// Capacity is part of the local serving contract, not merely a history
	// worker metric. Sample it on the very first readiness probe so a missing
	// mount or already-critical volume cannot appear "normal" until the first
	// cold claim/backup/write happens. Statfs is read-only and never contacts the
	// source database; restored full-history snapshots benefit from the same
	// early warning even though their mutation worker is disabled.
	if m.usageFactsFullHistoryMode() && m.factsStoreRequired() {
		_, _ = m.usageFactHistoryCapacityOK()
	}
	ctx, cancel := context.WithTimeout(parent, 500*time.Millisecond)
	mainOK := probeGORMStore(ctx, m.storeDB)
	cancel()
	now := time.Now().Unix()
	m.localStoreProbeOK.Store(mainOK)
	m.localStoreProbeAt.Store(now)
	if mainOK {
		m.probeLocalOperationalState(parent, now)
	}

	factsDB := m.usageFactsStore()
	if factsDB == m.storeDB {
		m.localFactsProbeOK.Store(mainOK)
		m.localFactsProbeAt.Store(now)
		return
	}
	if factsDB == nil {
		m.localFactsProbeOK.Store(!m.factsStoreRequired())
		m.localFactsProbeAt.Store(now)
		return
	}
	ctx, cancel = context.WithTimeout(parent, 500*time.Millisecond)
	factsOK := probeGORMStore(ctx, factsDB)
	cancel()
	m.localFactsProbeOK.Store(factsOK)
	m.localFactsProbeAt.Store(now)
}

func (m *Monitor) probeLocalOperationalState(parent context.Context, now int64) {
	if !m.cfg.StabilityEnabled || m.storeDB == nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	to := finalizedStabilityHourTo(now)
	from := to - int64(m.cfg.stabilityStorageDays())*86400
	if from < 0 {
		from = 0
	}
	expected := (to - from) / 3600
	var completed int64
	err := m.storeDB.WithContext(ctx).Model(&StabilityHourIngestState{}).
		Where("hour_ts >= ? AND hour_ts < ? AND status = ? AND traffic_class_version = ?",
			from, to, "complete", userTrafficClassificationVersion).Count(&completed).Error
	bps := int64(0)
	if err == nil {
		if expected <= 0 {
			bps = 10000
		} else {
			bps = completed * 10000 / expected
			if bps > 10000 {
				bps = 10000
			}
		}
	}
	m.stabilityCoverageCheckedAt.Store(now)
	m.stabilityCoverageBPS.Store(bps)

	var stalled int64
	stalledBefore := now - 15*60
	if tx := m.storeDB.WithContext(ctx).Model(&StabilityBackfillJob{}).
		Where("status IN ? AND updated_at > 0 AND updated_at < ?", []string{"queued", "running"}, stalledBefore).
		Count(&stalled); tx.Error != nil {
		m.stabilityBackfillStalled.Store(true)
	} else {
		m.stabilityBackfillStalled.Store(stalled > 0)
	}

	var problem StabilityProblemLiveCursor
	if tx := m.storeDB.WithContext(ctx).First(&problem, "id = ? AND traffic_class_version = ?", 1, userTrafficClassificationVersion); tx.Error == nil {
		m.stabilityProblemCoverageTo.Store(problem.NextTs)
		if problem.NextTs < problem.TargetThroughTs || problem.Status != "caught_up" {
			m.stabilityProblemPending.Store(1)
		} else {
			m.stabilityProblemPending.Store(0)
		}
	} else {
		m.stabilityProblemCoverageTo.Store(0)
		m.stabilityProblemPending.Store(1)
	}

	var incompleteProblemMigrations int64
	migrationQuery := m.storeDB.WithContext(ctx).Model(&StabilityProblemClassificationMigration{}).
		Where("id = ? AND traffic_class_version = ? AND status NOT IN ?", 1, userTrafficClassificationVersion,
			[]string{"complete", "not_required"}).Count(&incompleteProblemMigrations)
	// A local read error is not evidence that the migration is complete. Keep
	// readiness degraded until the next successful probe can prove otherwise.
	m.stabilityProblemMigrationIncomplete.Store(migrationQuery.Error != nil || incompleteProblemMigrations > 0)
}

func (m *Monitor) startLocalStoreProbe(ctx context.Context) {
	m.localProbeOnce.Do(func() {
		go func() {
			m.probeLocalStores(ctx)
			ticker := time.NewTicker(m.localProbeEvery())
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					m.probeLocalStores(ctx)
				}
			}
		}()
	})
}

type lifecycleComponentStatus struct {
	OK        bool  `json:"ok"`
	CheckedAt int64 `json:"checked_at"`
}

type sourceReadyStatus struct {
	State         string `json:"state"`
	WorkerEnabled bool   `json:"worker_enabled"`
	WorkerRunning bool   `json:"worker_running"`
	LeaseRequired bool   `json:"lease_required"`
	LeaseHeld     bool   `json:"lease_held"`
	LastSuccessAt int64  `json:"last_success_at"`
	LastFailureAt int64  `json:"last_failure_at"`
	NextRetryAt   int64  `json:"next_retry_at"`
	FailureStreak int64  `json:"failure_streak"`
}

type readyStatusResponse struct {
	Status          string                   `json:"status"`
	StartedAt       int64                    `json:"started_at"`
	Store           lifecycleComponentStatus `json:"store"`
	FactsStore      lifecycleComponentStatus `json:"facts_store"`
	Source          sourceReadyStatus        `json:"source"`
	SampledAt       int64                    `json:"sampled_at"`
	FactsHeartbeat  int64                    `json:"facts_heartbeat_at"`
	FactsDisk       factsDiskReadyStatus     `json:"facts_disk"`
	DegradedReasons []string                 `json:"degraded_reasons,omitempty"`
}

type factsDiskReadyStatus struct {
	Pressure    string  `json:"pressure"`
	FreeBytes   int64   `json:"free_bytes"`
	UsedPercent float64 `json:"used_percent"`
}

func appendReason(reasons []string, reason string) []string {
	for _, existing := range reasons {
		if existing == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

func (m *Monitor) readyStatus(now time.Time) (readyStatusResponse, int) {
	mainOK := m.localStoreProbeOK.Load() && m.storeIntegrityOK.Load()
	factsOK := m.localFactsProbeOK.Load()
	if m.factsStoreRequired() {
		factsOK = factsOK && m.usageFactsIntegrityOK.Load()
	}
	response := readyStatusResponse{
		Status:    "ready",
		StartedAt: m.processStartedAt.Load(),
		Store: lifecycleComponentStatus{
			OK: mainOK, CheckedAt: m.localStoreProbeAt.Load(),
		},
		FactsStore: lifecycleComponentStatus{
			OK: factsOK, CheckedAt: m.localFactsProbeAt.Load(),
		},
		Source: sourceReadyStatus{
			State:         m.currentSourceState().String(),
			WorkerEnabled: m.cfg.sourceWorkerIsEnabled(),
			WorkerRunning: m.sourceWorkerRunning.Load(),
			LeaseRequired: m.cfg.sourceLeaseIsRequired(),
			LeaseHeld:     m.sourceLeaseHeld.Load(),
			LastSuccessAt: m.sourceLastSuccessAt.Load(),
			LastFailureAt: m.sourceLastFailureAt.Load(),
			NextRetryAt:   m.sourceNextRetryAt.Load(),
			FailureStreak: m.sourceFailureStreak.Load(),
		},
		SampledAt:      m.lastRun.Load(),
		FactsHeartbeat: m.usageFactsLoopHeartbeat.Load(),
		FactsDisk: factsDiskReadyStatus{
			Pressure:    usageFactDiskPressureLevel(m.usageFactsHistoryDiskLevel.Load()).String(),
			FreeBytes:   m.usageFactsHistoryDiskFreeBytes.Load(),
			UsedPercent: float64(m.usageFactsHistoryDiskUsedBPS.Load()) / 100,
		},
	}
	if m.shuttingDown.Load() || !mainOK || (m.factsStoreRequired() && !factsOK) {
		response.Status = "not_ready"
		if m.shuttingDown.Load() {
			response.DegradedReasons = appendReason(response.DegradedReasons, "process_shutting_down")
		}
		if !mainOK {
			response.DegradedReasons = appendReason(response.DegradedReasons, "local_store_unavailable")
		}
		if m.factsStoreRequired() && !factsOK {
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_store_unavailable")
		}
		return response, http.StatusServiceUnavailable
	}

	if m.cfg.sourceWorkerIsEnabled() {
		if m.currentSourceState() != sourceStateReady {
			response.DegradedReasons = appendReason(response.DegradedReasons, "source_"+m.currentSourceState().String())
		} else if !m.sourceWorkerRunning.Load() {
			response.DegradedReasons = appendReason(response.DegradedReasons, "source_worker_warming_up")
		}
		last := m.lastRun.Load()
		if last == 0 {
			response.DegradedReasons = appendReason(response.DegradedReasons, "sampler_warming_up")
		} else {
			seconds := m.cfg.SampleSeconds
			if seconds < 10 {
				seconds = 10
			}
			if now.Unix()-last > int64(seconds*3+60) {
				response.DegradedReasons = appendReason(response.DegradedReasons, "sampler_stale")
			}
		}
	}
	if m.cfg.UsageFactsEnabled {
		switch usageFactDiskPressureLevel(m.usageFactsHistoryDiskLevel.Load()) {
		case usageFactDiskWarning:
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_disk_warning")
		case usageFactDiskThrottled:
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_disk_throttled")
		case usageFactDiskColdBlocked:
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_disk_cold_blocked")
		case usageFactDiskCritical:
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_disk_critical")
		}
		heartbeat := m.usageFactsLoopHeartbeat.Load()
		if heartbeat == 0 {
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_sync_warming_up")
		} else {
			minutes := m.cfg.UsageFactsSyncMinutes
			if minutes < 1 {
				minutes = 5
			}
			if now.Unix()-heartbeat > int64(minutes*3*60+60) {
				response.DegradedReasons = appendReason(response.DegradedReasons, "facts_sync_stale")
			}
		}
		if m.usageFactsSourceFailureStreak.Load() > 0 {
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_source_backoff")
		}
	}
	if m.cfg.UsageFactsReadEnabled && !m.usageFactsReadReady.Load() {
		response.DegradedReasons = appendReason(response.DegradedReasons, "facts_not_published")
	}
	if m.nginxSourceV2Active.Load() && !m.nginxSourceV2RuntimeConfigOK.Load() {
		response.DegradedReasons = appendReason(response.DegradedReasons, "nginx_source_v2_runtime_config_mismatch")
	}
	if m.cfg.StabilityEnabled && !m.cfg.LocalSnapshotOnly {
		if m.stabilityCoverageCheckedAt.Load() == 0 || m.stabilityCoverageBPS.Load() < 10000 {
			response.DegradedReasons = appendReason(response.DegradedReasons, "stability_coverage_incomplete")
		}
		if m.stabilityBackfillStalled.Load() {
			response.DegradedReasons = appendReason(response.DegradedReasons, "stability_backfill_stalled")
		}
		problemEvery := stabilityProblemIntervalSeconds(m.cfg.StabilityProblemSampleSec)
		problemLast := m.problemLastSuccess.Load()
		if problemLast == 0 || now.Unix()-problemLast > problemEvery*3+120 {
			response.DegradedReasons = appendReason(response.DegradedReasons, "stability_problem_stale")
		}
		if m.problemLastFailure.Load() > problemLast {
			response.DegradedReasons = appendReason(response.DegradedReasons, "stability_problem_failed")
		}
		if m.stabilityProblemCoverageTo.Load() == 0 || m.stabilityProblemPending.Load() > 0 {
			response.DegradedReasons = appendReason(response.DegradedReasons, "stability_problem_coverage_incomplete")
		}
		if m.stabilityProblemMigrationIncomplete.Load() {
			response.DegradedReasons = appendReason(response.DegradedReasons, "stability_problem_migration_incomplete")
		}
	}
	backupGrace := int64((10 * time.Minute).Seconds())
	uptime := now.Unix() - m.processStartedAt.Load()
	mainBackupEnabled := m.cfg.StoreBackupEnabled && m.storeDB != nil && storeUsesFile(m.cfg.StorePath)
	if mainBackupEnabled {
		lastSuccess, lastFailure := m.storeBackupLastSuccess.Load(), m.storeBackupLastFailure.Load()
		if lastFailure > lastSuccess {
			response.DegradedReasons = appendReason(response.DegradedReasons, "local_backup_failed")
		} else if lastSuccess == 0 && uptime > backupGrace {
			response.DegradedReasons = appendReason(response.DegradedReasons, "local_backup_missing")
		} else if lastSuccess > 0 && now.Unix()-lastSuccess > int64((2*m.storeBackupInterval()+10*time.Minute).Seconds()) {
			response.DegradedReasons = appendReason(response.DegradedReasons, "local_backup_stale")
		}
	}
	factsBackupEnabled := m.cfg.StoreBackupEnabled && m.usageFactsDB != nil && m.usageFactsDB != m.storeDB && storeUsesFile(m.cfg.UsageFactsStorePath)
	if factsBackupEnabled {
		lastSuccess, lastFailure := m.usageFactsBackupLastSuccess.Load(), m.usageFactsBackupLastFailure.Load()
		if lastFailure > lastSuccess {
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_backup_failed")
		} else if lastSuccess == 0 && uptime > backupGrace {
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_backup_missing")
		} else if lastSuccess > 0 && now.Unix()-lastSuccess > int64((2*m.storeBackupInterval()+10*time.Minute).Seconds()) {
			response.DegradedReasons = appendReason(response.DegradedReasons, "facts_backup_stale")
		}
	}
	if mainBackupEnabled || factsBackupEnabled {
		setSuccess, setFailure := m.storeBackupSetLastSuccess.Load(), m.storeBackupSetLastFailure.Load()
		switch {
		case setFailure > setSuccess:
			response.DegradedReasons = appendReason(response.DegradedReasons, "backup_set_failed")
		case setSuccess == 0 && uptime > backupGrace:
			response.DegradedReasons = appendReason(response.DegradedReasons, "backup_set_missing")
		case setSuccess > 0 && !m.storeBackupSetVerified.Load():
			response.DegradedReasons = appendReason(response.DegradedReasons, "backup_set_unverified")
		case setSuccess > 0 && now.Unix()-setSuccess > int64((2*m.storeBackupInterval()+10*time.Minute).Seconds()):
			response.DegradedReasons = appendReason(response.DegradedReasons, "backup_set_stale")
		}
	}
	if m.usageCache != nil && m.usageCache.remote != nil && m.usageCache.remoteBackoffUntil.Load() > now.UnixNano() {
		response.DegradedReasons = appendReason(response.DegradedReasons, "redis_degraded")
	}
	if len(response.DegradedReasons) > 0 {
		response.Status = "degraded"
	}
	return response, http.StatusOK
}

func (m *Monitor) serveLive(c *gin.Context) {
	if m.shuttingDown.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "stopping"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (m *Monitor) serveReady(c *gin.Context) {
	status, code := m.readyStatus(time.Now())
	c.JSON(code, status)
}
