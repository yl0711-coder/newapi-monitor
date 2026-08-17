package monitor

// usage_facts_test.go 锁住“生产只读采集 → Monitor 本地事实 → 页面读取”的边界：
// 事实读开关开启后，页面统计不得退回扫描生产 logs；成员调整和日/小时 rollup
// 也不得让已经完成的历史数据消失或重复计数。所有测试均使用临时 SQLite。

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func enableUsageFactsForTest(m *Monitor) {
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsReadEnabled = true
	m.cfg.UsageFactsBackfillDays = 30
	m.cfg.UsageFactsRetentionDays = 60
	m.cfg.UsageFactsHourRetentionDays = 8
	m.cfg.UsageFactsQueryTimeoutSec = 5
	// 大多数事实读取测试直接构造了已核验的本地数据；专门覆盖切读许可的测试
	// 会自行重置该值。
	m.setUsageFactsReadiness(true, m.usageFactFinalizedHour(time.Now()))
}

func usageFactTestDay() time.Time {
	return time.Date(2026, 7, 1, 0, 0, 0, 0, usageCST)
}

// seedPublishedUsageFactsForTest 模拟一份已经完整核验并原子发布的服务快照。
// 候选名单/窗口随后可以独立变化，页面仍应只读取这里冻结的成员与范围。
func seedPublishedUsageFactsForTest(t *testing.T, m *Monitor, ids []int64, from, through int64) {
	t.Helper()
	publishedAt := time.Now().Unix()
	fingerprint := portalMemberFingerprintFromIDs(ids)
	var published UsageFactSyncState
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("1 = 1").Delete(&UsageFactPublishedMember{}).Error; err != nil {
			return err
		}
		members := make([]UsageFactPublishedMember, 0, len(ids))
		for _, id := range ids {
			members = append(members, UsageFactPublishedMember{UserID: id, PublishedAt: publishedAt})
		}
		if len(members) > 0 {
			if err := tx.Create(&members).Error; err != nil {
				return err
			}
		}
		if err := tx.First(&published, 1).Error; err != nil {
			return err
		}
		published.PublishedFingerprint = fingerprint
		published.PublishedWindowDays = m.usageFactBackfillDays()
		published.PublishedRangeStart = from
		published.PublishedThrough = through
		published.PublishedAt = publishedAt
		published.TrafficClassVersion = userTrafficClassificationVersion
		published.Generation++
		published.ServingGeneration++
		return tx.Save(&published).Error
	})
	if err != nil {
		t.Fatal(err)
	}
	m.publishUsageFactGenerations(published.Generation, published.ServingGeneration)
	m.setUsageFactsPublishedReadiness(true, from, through)
}

func TestUsageAggregateGuardRejectsServingGenerationHandoff(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	from := usageFactTestDay().Unix()
	through := from + usageFactDaySeconds
	seedPublishedUsageFactsForTest(t, m, []int64{1}, from, through)
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"generation": 42, "serving_generation": 42,
		"traffic_class_version": userTrafficClassificationVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.usageFactsRevision.Store(42)
	m.usageFactsServingRevision.Store(41) // durable commit has landed; cache namespace has not.
	m.setUsageFactsPublishedReadiness(true, from, through)

	called := 0
	handler := m.usageAggregateAuthorizationGuard(func(c *gin.Context) {
		called++
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	request := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/usage/matrix", nil)
		handler(c)
		return w
	}
	if got := request(); got.Code != http.StatusServiceUnavailable || called != 0 {
		t.Fatalf("DB/atomic 交接窗必须在执行聚合前拒绝: status=%d called=%d body=%q", got.Code, called, got.Body.String())
	}
	m.usageFactsServingRevision.Store(42)
	if got := request(); got.Code != http.StatusOK || called != 1 {
		t.Fatalf("世代追平后应恢复聚合: status=%d called=%d body=%q", got.Code, called, got.Body.String())
	}
}

func TestStaleReadinessRefreshCannotOverwriteNewerPublicationBounds(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	oldFrom := usageFactTestDay().Unix()
	oldThrough := oldFrom + usageFactDaySeconds
	newFrom := oldFrom + usageFactDaySeconds
	newThrough := newFrom + 2*usageFactDaySeconds
	seedPublishedUsageFactsForTest(t, m, []int64{1}, oldFrom, oldThrough)
	var current UsageFactSyncState
	if err := m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&current, 1).Error; err != nil {
			return err
		}
		current.PublishedRangeStart = newFrom
		current.PublishedThrough = newThrough
		current.Generation++
		current.ServingGeneration++
		return tx.Save(&current).Error
	}); err != nil {
		t.Fatal(err)
	}
	m.publishUsageFactGenerations(current.Generation, current.ServingGeneration)
	m.setUsageFactsPublishedReadiness(true, newFrom, newThrough)
	if m.setUsageFactsPublishedReadinessIfCurrent(context.Background(), true, oldFrom, oldThrough) {
		t.Fatal("stale readiness audit unexpectedly published its old range")
	}
	if gotFrom, gotThrough := m.usageFactsReadyFrom.Load(), m.usageFactsReadyThrough.Load(); gotFrom != newFrom || gotThrough != newThrough || !m.usageFactsReadReady.Load() {
		t.Fatalf("stale refresh overwrote newer publication bounds: from=%d through=%d ready=%v",
			gotFrom, gotThrough, m.usageFactsReadReady.Load())
	}
}

// 后台历史回填默认不能高频命中来源 logs；即使环境变量误填过小值，也必须被
// 下限钳制。这里只验证配置计算，不启动采集、不触碰任何外部数据库。
func TestUsageFactBackfillDelayIsConservative(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UsageFactsBackfillDelayMS = 0
	if got := m.usageFactBackfillDelay(); got != 15*time.Second {
		t.Fatalf("默认回填节流应为 15s，实际 %s", got)
	}
	m.cfg.UsageFactsBackfillDelayMS = 1
	if got := m.usageFactBackfillDelay(); got != 5*time.Second {
		t.Fatalf("过小回填节流应被钳制为 5s，实际 %s", got)
	}
}

func TestUsageFactScheduleJitterNeverShortensSafeDelay(t *testing.T) {
	m := newTestMonitor(t)
	base := 15 * time.Second
	for i := 0; i < 100; i++ {
		got := m.usageFactJitteredDelay(base, 20)
		if got < base || got > 18*time.Second {
			t.Fatalf("单向抖动超出安全范围: %s", got)
		}
	}
}

func TestUsageFactSourceFailuresUseGlobalExponentialBackoff(t *testing.T) {
	m := newTestMonitor(t)
	sourceErr := errors.New("source unavailable")
	called := 0
	err := m.withUsageFactSourceQuery(context.Background(), func(context.Context) error {
		called++
		return sourceErr
	})
	if !errors.Is(err, sourceErr) || called != 1 {
		t.Fatalf("首次来源失败未透传: err=%v called=%d", err, called)
	}
	if got := m.usageFactsSourceFailureStreak.Load(); got != 1 {
		t.Fatalf("首次失败 streak=%d", got)
	}
	remaining := time.Until(time.Unix(0, m.usageFactsSourceBackoffUntil.Load()))
	if remaining < 5*time.Minute-time.Second || remaining > 6*time.Minute+time.Second {
		t.Fatalf("首次退避不在 5~6 分钟: %s", remaining)
	}
	if err := m.withUsageFactSourceQuery(context.Background(), func(context.Context) error {
		called++
		return nil
	}); !errors.Is(err, errUsageFactSourceBusy) {
		t.Fatalf("退避期间应拒绝来源查询，实际 %v", err)
	}
	if called != 1 {
		t.Fatalf("退避期间仍执行来源回调: %d", called)
	}

	// 模拟退避到期后再次失败，验证阶梯增长；随后成功必须立即恢复。
	m.usageFactsSourceBackoffUntil.Store(0)
	_ = m.withUsageFactSourceQuery(context.Background(), func(context.Context) error { return sourceErr })
	if got := m.usageFactsSourceFailureStreak.Load(); got != 2 {
		t.Fatalf("第二次失败 streak=%d", got)
	}
	remaining = time.Until(time.Unix(0, m.usageFactsSourceBackoffUntil.Load()))
	if remaining < 10*time.Minute-time.Second || remaining > 12*time.Minute+time.Second {
		t.Fatalf("第二次退避不在 10~12 分钟: %s", remaining)
	}
	m.usageFactsSourceBackoffUntil.Store(0)
	if err := m.withUsageFactSourceQuery(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if m.usageFactsSourceFailureStreak.Load() != 0 || m.usageFactsSourceBackoffUntil.Load() != 0 {
		t.Fatal("来源成功后未清除全局退避")
	}
}

func waitForBackgroundSourceWaiters(t *testing.T, m *Monitor, wantHigh, wantLow int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		high, low := m.backgroundSourceWaiterCounts()
		if high == wantHigh && low == wantLow {
			return
		}
		time.Sleep(time.Millisecond)
	}
	high, low := m.backgroundSourceWaiterCounts()
	t.Fatalf("background waiters high/low=%d/%d want=%d/%d", high, low, wantHigh, wantLow)
}

func TestUsageFactsWaitForCurrentBackgroundSourceInsteadOfDroppingTail(t *testing.T) {
	m := newTestMonitor(t)
	release, err := m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- m.withUsageFactSourceQuery(context.Background(), func(context.Context) error {
			close(called)
			return nil
		})
	}()
	waitForBackgroundSourceWaiters(t, m, 1, 0)
	select {
	case <-called:
		t.Fatal("facts ran while another background query still owned the source")
	default:
	}
	release()
	select {
	case err = <-done:
		if err != nil {
			t.Fatalf("facts did not run in the next source window: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("facts remained starved after the source was released")
	}
	if m.usageFactsSourceFailureStreak.Load() != 0 || m.usageFactsSourceBackoffUntil.Load() != 0 {
		t.Fatal("scheduler contention was incorrectly counted as an upstream failure")
	}
	release, err = m.acquireBackgroundSource(context.Background())
	if err != nil {
		t.Fatalf("background source gate did not recover: %v", err)
	}
	release()
}

func TestBackgroundSourceHighPriorityWinsNextWindowUnderLowCompetition(t *testing.T) {
	m := &Monitor{cfg: Settings{BackgroundSourceMinStartIntervalMS: 30}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Model a migration query that has just completed. Its long duty cooldown
	// applies to the next low query, while the high Tail still observes the
	// global 30ms minimum spacing.
	releaseCurrent, err := m.acquireBackgroundSourceLow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m.deferBackgroundSourceStart(2 * time.Second)

	type acquisition struct {
		priority string
		release  func()
		err      error
	}
	acquired := make(chan acquisition, 2)
	go func() {
		release, err := m.acquireBackgroundSourceLow(ctx)
		acquired <- acquisition{priority: "low", release: release, err: err}
	}()
	waitForBackgroundSourceWaiters(t, m, 0, 1)
	go func() {
		release, err := m.acquireBackgroundSource(ctx)
		acquired <- acquisition{priority: "high", release: release, err: err}
	}()
	waitForBackgroundSourceWaiters(t, m, 1, 1)

	releasedAt := time.Now()
	releaseCurrent()
	select {
	case got := <-acquired:
		if got.err != nil {
			t.Fatalf("next source acquisition failed: %v", got.err)
		}
		if got.priority != "high" {
			got.release()
			t.Fatalf("queued low migration beat waiting high Tail: %s", got.priority)
		}
		if elapsed := time.Since(releasedAt); elapsed >= time.Second {
			got.release()
			t.Fatalf("high Tail inherited low migration cooldown: %s", elapsed)
		}
		got.release()
	case <-time.After(time.Second):
		t.Fatal("high Tail did not receive the next globally-spaced source window")
	}

	// The low query is still governed by its 2s duty window. Cancel it so
	// the test also proves a queued low waiter can be removed cleanly.
	cancel()
	select {
	case got := <-acquired:
		if !errors.Is(got.err, context.Canceled) {
			if got.release != nil {
				got.release()
			}
			t.Fatalf("queued low migration was not canceled cleanly: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queued low migration leaked after cancellation")
	}
	waitForBackgroundSourceWaiters(t, m, 0, 0)
}

func TestUsageFactsWaitPastLegacy250msWindowWithoutSpendingSQLTimeout(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.BackgroundSourceMinStartIntervalMS = 350
	m.cfg.UsageFactsQueryTimeoutSec = 5

	release, err := m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m.deferBackgroundSourceStart(2 * time.Second)
	release()

	started := time.Now()
	var sqlBudget time.Duration
	err = m.withUsageFactSourceQuery(context.Background(), func(cctx context.Context) error {
		deadline, ok := cctx.Deadline()
		if !ok {
			t.Fatal("facts SQL callback has no execution deadline")
		}
		sqlBudget = time.Until(deadline)
		return nil
	})
	if err != nil {
		t.Fatalf("facts Tail was dropped while waiting for source spacing: %v", err)
	}
	elapsed := time.Since(started)
	if elapsed < 300*time.Millisecond {
		t.Fatalf("facts bypassed global minimum start spacing: %s", elapsed)
	}
	if elapsed >= time.Second {
		t.Fatalf("facts inherited the low-only 2s Stability duty window: %s", elapsed)
	}
	if sqlBudget < 4500*time.Millisecond {
		t.Fatalf("source wait consumed the 5s SQL execution timeout: remaining %s", sqlBudget)
	}
}

func TestBackgroundSourceCanceledWaitersDoNotLeakOrDeadlock(t *testing.T) {
	m := &Monitor{cfg: Settings{BackgroundSourceMinStartIntervalMS: 30}}
	release, err := m.acquireBackgroundSourceLow(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.acquireBackgroundSource(ctx)
		done <- err
	}()
	waitForBackgroundSourceWaiters(t, m, 1, 0)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active-gate waiter returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled high waiter deadlocked behind an active low query")
	}
	waitForBackgroundSourceWaiters(t, m, 0, 0)
	release()

	// Cancellation while waiting only for the low duty timer must also remove
	// the waiter. The scheduler uses a cancelable timer/notification, not polling.
	m.deferBackgroundSourceStart(time.Second)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer waitCancel()
	_, err = m.acquireBackgroundSourceLow(waitCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("low duty waiter returned %v, want context deadline", err)
	}
	waitForBackgroundSourceWaiters(t, m, 0, 0)

	// A canceled low waiter cannot leave the gate or its low-only timer stuck
	// for high-priority work.
	recoverCtx, recoverCancel := context.WithTimeout(context.Background(), time.Second)
	defer recoverCancel()
	release, err = m.acquireBackgroundSource(recoverCtx)
	if err != nil {
		t.Fatalf("scheduler did not recover after canceled waiters: %v", err)
	}
	release()
}

func TestBackgroundSourceAlreadyCanceledContextNeverClaims(t *testing.T) {
	m := &Monitor{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name    string
		acquire func(context.Context) (func(), error)
	}{
		{name: "high", acquire: m.acquireBackgroundSource},
		{name: "low", acquire: m.acquireBackgroundSourceLow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release, err := tc.acquire(ctx)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("acquire error=%v want context.Canceled", err)
			}
			if release != nil {
				t.Fatal("already-canceled context returned a release function")
			}
		})
	}
	if starts := m.backgroundSourceStarts.Load(); starts != 0 {
		t.Fatalf("already-canceled acquisitions incremented query starts: %d", starts)
	}
	if last := m.backgroundSourceLastStart.Load(); last != 0 {
		t.Fatalf("already-canceled acquisitions changed last start: %d", last)
	}
	waitForBackgroundSourceWaiters(t, m, 0, 0)
	m.backgroundSourceScheduleMu.Lock()
	active := m.backgroundSourceActive
	m.backgroundSourceScheduleMu.Unlock()
	if active {
		t.Fatal("already-canceled acquisition left the source gate active")
	}
}

func TestBackgroundSourceManyHighWaitersDrainBeforeManyLowWaiters(t *testing.T) {
	const waitersPerPriority = 8
	m := &Monitor{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Keep one low query active while all 16 contenders register. This removes
	// goroutine scheduling from the assertion: the release is the single event
	// that opens the next source window.
	releaseCurrent, err := m.acquireBackgroundSourceLow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type acquisition struct {
		priority string
		err      error
	}
	acquired := make(chan acquisition, 2*waitersPerPriority)
	var workers sync.WaitGroup
	startWaiters := func(priority string, acquire func(context.Context) (func(), error)) {
		for i := 0; i < waitersPerPriority; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				release, err := acquire(ctx)
				// Send before release so channel order is the actual serialized
				// acquisition order, not post-release goroutine scheduling order.
				acquired <- acquisition{priority: priority, err: err}
				if release != nil {
					release()
				}
			}()
		}
	}
	startWaiters("low", m.acquireBackgroundSourceLow)
	waitForBackgroundSourceWaiters(t, m, 0, waitersPerPriority)
	startWaiters("high", m.acquireBackgroundSource)
	waitForBackgroundSourceWaiters(t, m, waitersPerPriority, waitersPerPriority)

	releaseCurrent()
	order := make([]string, 0, 2*waitersPerPriority)
	for i := 0; i < 2*waitersPerPriority; i++ {
		select {
		case got := <-acquired:
			if got.err != nil {
				t.Fatalf("waiter %d failed: priority=%s err=%v", i, got.priority, got.err)
			}
			order = append(order, got.priority)
		case <-ctx.Done():
			t.Fatalf("N:N source waiters deadlocked after %d acquisitions: %v", len(order), ctx.Err())
		}
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("N:N source workers did not finish: %v", ctx.Err())
	}

	for i := 0; i < waitersPerPriority; i++ {
		if order[i] != "high" {
			t.Fatalf("low waiter entered before all high waiters drained: order=%v", order)
		}
	}
	for i := waitersPerPriority; i < len(order); i++ {
		if order[i] != "low" {
			t.Fatalf("unexpected acquisition order after high drain: %v", order)
		}
	}
	if starts := m.backgroundSourceStarts.Load(); starts != 1+2*waitersPerPriority {
		t.Fatalf("source start count=%d want=%d", starts, 1+2*waitersPerPriority)
	}
	waitForBackgroundSourceWaiters(t, m, 0, 0)
	m.backgroundSourceScheduleMu.Lock()
	active := m.backgroundSourceActive
	m.backgroundSourceScheduleMu.Unlock()
	if active {
		t.Fatal("N:N source workers completed but gate remained active")
	}
}

// 单用户详情是本地事实层最容易被忽略的长窗查询：若只有(date,user)索引，
// SQLite 会先扫描日期窗内全部成员再过滤用户，名单越大越慢。这里锁住反向
// (user,date/day)索引确实被代表性查询采用，避免把 MySQL 压力转移成本机全表扫。
func TestUsageFactsSingleMemberLongRangeUsesMemberFirstIndexes(t *testing.T) {
	m := newTestMonitor(t)
	plan := func(query string, args ...any) string {
		t.Helper()
		rows, err := m.usageFactsStore().Raw("EXPLAIN QUERY PLAN "+query, args...).Rows()
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return strings.Join(details, "\n")
	}

	dailyPlan := plan("SELECT requests FROM usage_daily_facts WHERE user_id = ? AND date_ts >= ? AND date_ts < ?", 42, 1, int64(1<<62))
	if !strings.Contains(dailyPlan, "idx_usage_daily_fact_user_date") {
		t.Fatalf("单用户日事实查询未采用(user_id,date_ts)索引:\n%s", dailyPlan)
	}
	tokenPlan := plan("SELECT requests FROM usage_daily_facts WHERE user_id = ? AND token_id = ? AND date_ts >= ? AND date_ts < ?", 42, 7, 1, int64(1<<62))
	if !strings.Contains(tokenPlan, "idx_usage_daily_fact_user_token_date") {
		t.Fatalf("单用户令牌长窗查询未采用(user_id,token_id,date_ts)索引:\n%s", tokenPlan)
	}
	hourPlan := plan("SELECT requests FROM usage_hour_facts WHERE user_id = ? AND day_ts >= ? AND day_ts < ?", 42, 1, int64(1<<62))
	if !strings.Contains(hourPlan, "idx_usage_hour_fact_user_day") {
		t.Fatalf("单用户小时事实查询未采用(user_id,day_ts)索引:\n%s", hourPlan)
	}
}

func assertUsageBillingEqual(t *testing.T, got, want UsageBilling) {
	t.Helper()
	if got.Requests != want.Requests ||
		got.RefundRecords != want.RefundRecords ||
		got.ConsumeQuota != want.ConsumeQuota ||
		got.RefundQuota != want.RefundQuota ||
		got.NetQuota != want.NetQuota {
		t.Fatalf("计费汇总不一致: got=%+v want=%+v", got, want)
	}
}

// TestUsageFactsReadMatchesLegacyAndNeverQueriesSource 锁住最关键的切换边界：
// 事实表经后台小时采集写入后，管理端/门户会用到的统计与资料读取必须只读本地库，
// 且消费、退款、tokens 与旧口径一致。
func TestUsageFactsReadMatchesLegacyAndNeverQueriesSource(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)

	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "snapshot", Email: "snapshot@example.test"}).Error; err != nil {
		t.Fatal(err)
	}
	day := usageFactTestDay()
	hour1 := day.Add(9 * time.Hour).Unix()
	hour2 := day.Add(10 * time.Hour).Unix()
	for _, stmt := range []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'alice','alice@example.test',900000,1250000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,1,'token-a','abcdefghijklmnop','g1',700000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (11,1,'token-b','qrstuvwxyz012345','g2',550000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,33,%d,2,'model-a',500000,100,20,'g1',10,'token-a')", hour1+1),
		fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (2,1,33,%d,6,'model-a',100000,0,0,'g1',10,'token-a')", hour1+2),
		fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (3,1,44,%d,2,'model-b',250000,50,10,'g2',11,'token-b')", hour2+1),
	} {
		if _, err := prodDB.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	legacy, err := m.computeUsageStats(ctx, []int64{1}, day.Unix(), day.AddDate(0, 0, 1).Unix(), 0)
	if err != nil {
		t.Fatalf("旧聚合失败: %v", err)
	}
	if err := m.syncUsageFactHour(ctx, hour1, []int64{1}); err != nil {
		t.Fatalf("采集第一小时失败: %v", err)
	}
	if err := m.syncUsageFactHour(ctx, hour2, []int64{1}); err != nil {
		t.Fatalf("采集第二小时失败: %v", err)
	}
	if err := m.syncUsageProfiles(ctx, day.Add(12*time.Hour)); err != nil {
		t.Fatalf("资料快照失败: %v", err)
	}

	from, to := day.Unix(), day.AddDate(0, 0, 1).Unix()
	local, err := m.computeUsageStatsForRead(ctx, []int64{1}, from, to, 0)
	if err != nil {
		t.Fatalf("本地事实聚合失败: %v", err)
	}
	assertUsageBillingEqual(t, local.Summary.UsageBilling, legacy.Summary.UsageBilling)
	if local.Summary.Tokens != legacy.Summary.Tokens || len(local.Daily) != len(legacy.Daily) || len(local.ByGroup) != len(legacy.ByGroup) || len(local.ByModel) != len(legacy.ByModel) {
		t.Fatalf("本地事实维度与旧口径不一致: local=%+v legacy=%+v", local, legacy)
	}

	counts.reset()
	if _, err := m.computeUsageStatsForRead(ctx, []int64{1}, from, to, 0); err != nil {
		t.Fatalf("事实统计读取失败: %v", err)
	}
	if _, err := m.computeUsageMatrixForRead(ctx, []int64{1}, from, to); err != nil {
		t.Fatalf("事实矩阵读取失败: %v", err)
	}
	if _, err := m.computeUserTokenAggregatesForRead(ctx, 1, from, to); err != nil {
		t.Fatalf("事实令牌统计读取失败: %v", err)
	}
	tracked, balances, used := m.refreshTrackedLabelsForRead(ctx, []TrackedUser{{UserID: 1}})
	if len(tracked) != 1 || tracked[0].Username != "alice" || balances[1] != 900000 || used[1] != 1250000 {
		t.Fatalf("本地资料快照错误: tracked=%+v balances=%v used=%v", tracked, balances, used)
	}
	if owner, balance, total := m.userLiveUsageForRead(ctx, 1); owner != "alice" || balance == nil || *balance != 900000 || total == nil || *total != 1250000 {
		t.Fatalf("本地用户快照错误: owner=%q balance=%v total=%v", owner, balance, total)
	}
	if token := m.tokenMetaOfForRead(ctx, 1, 10); token == nil || token.Name != "token-a" || token.MaskedKey == "" {
		t.Fatalf("本地令牌快照错误: %+v", token)
	}
	var tokenSnapshot UsageTokenSnapshot
	if err := m.storeDB.First(&tokenSnapshot, "token_id = ?", 10).Error; err != nil {
		t.Fatalf("读取令牌快照失败: %v", err)
	}
	if tokenSnapshot.MaskedKey != maskTokenKey("abcdefghijklmnop") || tokenSnapshot.MaskedKey == "abcdefghijklmnop" {
		t.Fatalf("本地令牌快照不得保存明文 key: %+v", tokenSnapshot)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("事实读模式不应扫描生产 logs，实际 %d 次", got)
	}
	if got := counts.users.Load(); got != 0 {
		t.Fatalf("事实读模式不应查询生产 users，实际 %d 次", got)
	}
	if got := counts.tokens.Load(); got != 0 {
		t.Fatalf("事实读模式不应查询生产 tokens，实际 %d 次", got)
	}
}

// TestUsageFactsMembershipMutationKeepsPublishedSnapshot 锁住两阶段名单切换：重复
// 添加已有成员不降级；真正新增成员后旧服务版继续可读，而新成员必须等候选历史
// 补齐并发布后才能出现，不能用半份历史拖垮整个页面。
func TestUsageFactsMembershipMutationKeepsPublishedSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	for _, stmt := range []string{
		"INSERT INTO users (id,username,email) VALUES (1,'alice','alice@test.local')",
		"INSERT INTO users (id,username,email) VALUES (2,'bob','bob@test.local')",
	} {
		if _, err := m.prodDB.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice", Email: "alice@test.local"}).Error; err != nil {
		t.Fatal(err)
	}
	day := usageFactTestDay().Unix()
	seedPublishedUsageFactsForTest(t, m, []int64{1}, day, day+usageFactDaySeconds)

	callAdd := func(input string) int {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/usage/users", strings.NewReader(`{"input":"`+input+`"}`))
		c.Request.Header.Set("Content-Type", "application/json")
		m.addTrackedUser(c)
		return w.Code
	}

	if status := callAdd("1"); status != 200 {
		t.Fatalf("重复添加已有成员失败: status=%d", status)
	}
	if !m.usageFactsReadReady.Load() {
		t.Fatal("重复添加已有成员不应关闭已完整的事实读")
	}
	if status := callAdd("2"); status != 200 {
		t.Fatalf("新增成员失败: status=%d", status)
	}
	if !m.usageFactsReadReady.Load() {
		t.Fatal("新增成员回填期间必须继续服务上一份已发布快照")
	}
	serving, err := m.listTrackedForUsageRead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(serving) != 1 || serving[0].UserID != 1 {
		t.Fatalf("新成员未发布前不得进入读取面: %+v", serving)
	}
	var all []TrackedUser
	if err := m.storeDB.Order("user_id").Find(&all).Error; err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("候选名单应已包含新成员: %+v", all)
	}
}

// TestUsageFactsPublishedRangeRejectsOlderQueries 锁住发布窗口左边界：页面请求
// 更早历史时必须明确不可用，不能把“本地没有回填”静默显示成零，也不能退回
// 扫描生产 logs。
func TestUsageFactsPublishedRangeRejectsOlderQueries(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	day := usageFactTestDay().Unix()
	m.setUsageFactsPublishedReadiness(true, day, day+2*usageFactDaySeconds)
	if err := m.storeDB.Create(&UsageDailyFact{
		DateTs: day, UserID: 1, ChannelID: 33, Grp: "g", ModelName: "m", TokenID: 10,
		Requests: 1, ConsumeQuota: 100, PromptTokens: 10, CompletionTokens: 2,
	}).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	if _, err := m.computeUsageStatsForRead(context.Background(), []int64{1}, day-usageFactHourSeconds, day+usageFactDaySeconds, 0); !errors.Is(err, errUsageFactsNotReady) {
		t.Fatalf("超出发布左边界应拒绝，err=%v", err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("超出发布边界不得扫描生产 logs，实际 %d 次", got)
	}
	stats, err := m.computeUsageStatsForRead(context.Background(), []int64{1}, day, day+usageFactDaySeconds, 0)
	if err != nil || stats.Summary.Requests != 1 || stats.Summary.ConsumeQuota != 100 {
		t.Fatalf("发布窗口内事实读取错误: stats=%+v err=%v", stats, err)
	}
}

// TestUsageFactsCorruptPublishedMembershipFailsClosed 确保生产服务版的成员表或
// 指纹损坏时不会退回候选名单。否则新增但尚未补齐的成员会带着半份历史进入页面。
func TestUsageFactsCorruptPublishedMembershipFailsClosed(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	for _, user := range []TrackedUser{{UserID: 1, GroupID: 7}, {UserID: 2, GroupID: 7}} {
		if err := m.storeDB.Create(&user).Error; err != nil {
			t.Fatal(err)
		}
	}
	day := usageFactTestDay().Unix()
	seedPublishedUsageFactsForTest(t, m, []int64{1}, day, day+usageFactDaySeconds)
	if err := m.storeDB.Where("user_id = ?", 1).Delete(&UsageFactPublishedMember{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.listTrackedForUsageRead(context.Background()); !errors.Is(err, errUsageFactsNotReady) {
		t.Fatalf("生产发布成员损坏应 fail-closed，err=%v", err)
	}
	if _, err := m.portalTrackedMembersForUsageRead(context.Background(), 7); !errors.Is(err, errUsageFactsNotReady) {
		t.Fatalf("门户发布成员损坏应 fail-closed，err=%v", err)
	}
}

// TestUsageFactsOfflineSnapshotAllowsStrictlyValidatedCandidate 本机离线快照没有
// 新版发布成员表时，只要完整小时校验已经开放读取，就应使用快照当前名单；该
// 兼容路径必须同时要求 LocalSnapshotOnly，不能影响生产发布规则。
func TestUsageFactsOfflineSnapshotAllowsStrictlyValidatedCandidate(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.LocalSnapshotOnly = true
	m.cfg.UsageFactsLocalReadOnly = true
	m.cfg.UsageFactsReadEnabled = true
	day := usageFactTestDay().Unix()
	m.setUsageFactsPublishedReadiness(true, day, day+usageFactDaySeconds)
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, GroupID: 9, Username: "snapshot"}).Error; err != nil {
		t.Fatal(err)
	}
	tracked, err := m.listTrackedForUsageRead(context.Background())
	if err != nil || len(tracked) != 1 || tracked[0].UserID != 1 {
		t.Fatalf("离线快照当前名单读取错误: tracked=%+v err=%v", tracked, err)
	}
	portal, err := m.portalTrackedMembersForUsageRead(context.Background(), 9)
	if err != nil || len(portal) != 1 || portal[0].UserID != 1 {
		t.Fatalf("离线门户快照成员读取错误: tracked=%+v err=%v", portal, err)
	}
}

// TestUsageFactsCandidateHourSyncPreservesPublishedOnlyRows 锁住候选回填事务：
// 新名单同步某小时只能替换候选成员，不能删除仍由旧服务版提供的成员事实。
func TestUsageFactsCandidateHourSyncPreservesPublishedOnlyRows(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	hour := usageFactTestDay().Add(9 * time.Hour).Unix()
	day := usageFactDayStart(hour)
	if err := m.storeDB.Create(&UsageHourFact{
		HourTs: hour, DayTs: day, UserID: 1, ChannelID: 11, Grp: "old", ModelName: "old-model", TokenID: 1,
		Requests: 2, ConsumeQuota: 200, PromptTokens: 20, CompletionTokens: 2,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec(fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,2,22,%d,2,'new-model',300,30,3,'new',2,'new-token')", hour+1)); err != nil {
		t.Fatal(err)
	}
	if err := m.syncUsageFactHour(context.Background(), hour, []int64{2}); err != nil {
		t.Fatal(err)
	}
	var rows []UsageHourFact
	if err := m.storeDB.Where("hour_ts = ?", hour).Order("user_id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].UserID != 1 || rows[0].ConsumeQuota != 200 || rows[1].UserID != 2 || rows[1].ConsumeQuota != 300 {
		t.Fatalf("候选小时同步破坏服务版事实: %+v", rows)
	}
}

// TestUsageFactsPortalEmptyGroupUsesLocalFacts 空分组是门户的正常状态之一。事实读
// 开启后，它不应为了生成空矩阵而访问生产 logs。
func TestUsageFactsPortalEmptyGroupUsesLocalFacts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	if err := m.storeDB.Create(&CustomerGroup{ID: 1, Name: "空分组"}).Error; err != nil {
		t.Fatal(err)
	}
	day := usageFactTestDay()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/overview", nil)

	counts.reset()
	p, err := m.buildPortalOverview(c, 1, day.Unix(), day.AddDate(0, 0, 1).Unix())
	if err != nil || p == nil || p.GroupName != "空分组" || len(p.Users) != 0 || len(p.Cells) != 0 {
		t.Fatalf("空分组本地事实读取错误: payload=%+v err=%v", p, err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("空分组事实读不得扫描生产 logs，实际 %d 次", got)
	}

}

// TestUsageFactsReadFailureNeverFallsBackToSource 确保本地事实层故障不会为了“补救”
// 而把大查询退回生产 logs；调用方会得到可处理的错误，而不是悄悄加压主站。
func TestUsageFactsReadFailureNeverFallsBackToSource(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	if err := m.storeDB.Exec("DROP TABLE usage_daily_facts").Error; err != nil {
		t.Fatal(err)
	}
	counts.reset()
	day := usageFactTestDay()
	if _, err := m.computeUsageStatsForRead(context.Background(), []int64{1}, day.Unix(), day.AddDate(0, 0, 1).Unix(), 0); err == nil {
		t.Fatal("本地事实表损坏时应返回错误")
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("本地事实读取失败后不得回退扫描生产 logs，实际 %d 次", got)
	}
}

// TestUsageFactsFollowUpsNeverQuerySource 跟进页同样属于用量读取；事实读开启后，
// 即使所有成员都没有消费事实，也只能用本地快照和本地空事实得出“待跟进”，不得
// 为了补余额或 30 天矩阵回退查询生产 users/logs。
func TestUsageFactsFollowUpsNeverQuerySource(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)
	enableUsageFactsForTest(m)

	if err := m.storeDB.Create(&CustomerGroup{ID: 1, Name: "测试公司", CreatedAt: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, GroupID: 1, Username: "名单名称"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageUserSnapshot{
		UserID: 1, Username: "快照名称", BalanceQuota: 500000, Exists: true, CapturedAt: usageFactTestDay().Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, usageCST).Unix()
	items, err := m.computeFollowUps(context.Background(), now)
	if err != nil {
		t.Fatalf("事实模式待跟进失败: %v", err)
	}
	if len(items) != 1 || len(items[0].Members) != 1 || items[0].Members[0].Username != "快照名称" {
		t.Fatalf("事实模式待跟进结果错误: %+v", items)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("事实模式跟进页不应扫描生产 logs，实际 %d 次", got)
	}
	if got := counts.users.Load(); got != 0 {
		t.Fatalf("事实模式跟进页不应查询生产 users，实际 %d 次", got)
	}
	if got := counts.tokens.Load(); got != 0 {
		t.Fatalf("事实模式跟进页不应查询生产 tokens，实际 %d 次", got)
	}
}

// TestUsageFactsBackgroundReadYieldsToInteractiveGate 锁住后台任务的让路语义：
// 当前台仍在查询生产库时，事实同步/资料同步不得排队扫描来源库，更不能把“让路”
// 记成采集失败。下个低频周期会再试，前台请求优先。
func TestUsageFactsBackgroundReadYieldsToInteractiveGate(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.acquireUsageGate(context.Background()); err != nil {
		t.Fatalf("占用前台用量槽位失败: %v", err)
	}
	defer m.releaseUsageGate()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	hour := usageFactTestDay().Add(9 * time.Hour).Unix()
	if err := m.syncUsageFactHour(ctx, hour, []int64{1}); !errors.Is(err, errUsageFactSourceBusy) {
		t.Fatalf("后台事实同步应让路，err=%v", err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("后台让路时不得扫描生产 logs，实际 %d 次", got)
	}
	var states int64
	if err := m.storeDB.Model(&UsageHourIngestState{}).Count(&states).Error; err != nil {
		t.Fatal(err)
	}
	if states != 0 {
		t.Fatalf("后台让路不能写失败台账，states=%d", states)
	}

	counts.reset()
	if err := m.syncUsageProfiles(ctx, usageFactTestDay()); !errors.Is(err, errUsageFactSourceBusy) {
		t.Fatalf("后台资料同步应让路，err=%v", err)
	}
	if got := counts.users.Load(); got != 0 {
		t.Fatalf("后台让路时不得读取生产 users，实际 %d 次", got)
	}
	if got := counts.tokens.Load(); got != 0 {
		t.Fatalf("后台让路时不得读取生产 tokens，实际 %d 次", got)
	}
}

// TestUsageFactsBackgroundReadYieldsToQueuedInteractiveWaiter 锁住一个更窄的竞争
// 窗口：前台已经在等待来源库闸门时，后台不能因为调度顺序恰好先拿到闸门就开始
// 扫描。它应主动归还闸门、留待下一轮，而不是制造用户查询排队。
func TestUsageFactsBackgroundReadYieldsToQueuedInteractiveWaiter(t *testing.T) {
	m := newTestMonitor(t)
	m.usageInteractiveWaiters.Store(1)
	called := false
	err := m.withUsageFactSourceRead(context.Background(), func() error {
		called = true
		return nil
	})
	if !errors.Is(err, errUsageFactSourceBusy) {
		t.Fatalf("后台看到交互等待者应让路，err=%v", err)
	}
	if called {
		t.Fatal("后台让路时不得执行来源查询")
	}
	if err := m.acquireUsageGate(context.Background()); err != nil {
		t.Fatalf("后台让路后闸门必须保持可用: %v", err)
	}
	m.releaseUsageGate()
}

func TestUsageProfilesSlowSourceDoesNotHoldLocalFactsMutex(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := prodDB.Exec("INSERT INTO users (id,username,email,quota,used_quota) VALUES (1,'alice','alice@example.test',100,20)"); err != nil {
		t.Fatal(err)
	}
	if _, err := prodDB.Exec("INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (10,1,'t','abcdefghijkl','g',20)"); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	counts.blockUsers = block
	counts.usersStarted = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- m.syncUsageProfiles(context.Background(), time.Now()) }()
	select {
	case <-counts.usersStarted:
	case <-time.After(time.Second):
		close(block)
		<-done
		t.Fatal("资料来源查询没有开始")
	}
	if !m.usageFactsSyncMu.TryLock() {
		close(block)
		<-done
		t.Fatal("慢 users SQL 不得持有本地事实写锁")
	}
	m.usageFactsSyncMu.Unlock()
	close(block)
	if err := <-done; err != nil {
		t.Fatalf("解除来源阻塞后资料同步失败: %v", err)
	}
}

// TestUsageFactsBackfillCursorResumesAtMemberWatermark 锁住历史补齐的恢复性能：
// 服务重启后直接从成员自己的持久游标继续，不能因重新核对名单而从窗口起点重扫。
func TestUsageFactsBackfillCursorResumesAtMemberWatermark(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	resume := start + 2*usageFactHourSeconds
	if _, err := prodDB.Exec(fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,33,%d,2,'model-a',500000,100,20,'g1',10,'token-a')", resume+1)); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageFactMemberState{
		UserID: 1, Active: true, BackfillWindowDays: 1, RangeStart: start, NextBackfillHour: resume,
	}).Error; err != nil {
		t.Fatal(err)
	}
	emptyHash := usageFactContentHash(nil)
	for _, hour := range []int64{start, start + usageFactHourSeconds} {
		if err := m.storeDB.Create(&UsageFactMemberHourState{
			UserID: 1, HourTs: hour, Status: "complete", ContentHash: emptyHash,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": 1,
		"next_backfill_hour":   resume,
	}).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	worked, err := m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("按成员游标恢复失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("恢复后只应读取游标指向的一小时，实际 %d 次", got)
	}
	var member UsageFactMemberState
	if err := m.storeDB.First(&member, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if want := resume + usageFactHourSeconds; member.NextBackfillHour != want {
		t.Fatalf("成员回填游标错误: got=%d want=%d", member.NextBackfillHour, want)
	}
	var oldRows int64
	if err := m.storeDB.Model(&UsageHourFact{}).Where("hour_ts < ?", resume).Count(&oldRows).Error; err != nil {
		t.Fatal(err)
	}
	if oldRows != 0 {
		t.Fatalf("恢复时不应重写游标之前的小时事实: rows=%d", oldRows)
	}
}

// TestUsageFactsBackfillSkipsVerifiedLocalHoursWithoutSource 锁住 Tail/重启/扩窗
// 的增量性能：成员游标前方已有完整 proof 且本地事实与 hash 一致时，
// 只前移游标，不得重复读取来源 logs。
func TestUsageFactsBackfillSkipsVerifiedLocalHoursWithoutSource(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	emptyHash := usageFactContentHash(nil)
	for hour := start; hour < start+3*usageFactHourSeconds; hour += usageFactHourSeconds {
		if err := m.storeDB.Create(&UsageFactMemberHourState{
			UserID: 1, HourTs: hour, Status: "complete", ContentHash: emptyHash,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	counts.reset()
	worked, err := m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("跳过已验证小时失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("已验证本地小时不得重读来源 logs，实际=%d", got)
	}
	var member UsageFactMemberState
	if err := m.storeDB.First(&member, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if want := start + 3*usageFactHourSeconds; member.NextBackfillHour != want {
		t.Fatalf("本地 proof 跳过后游标错误: got=%d want=%d", member.NextBackfillHour, want)
	}

	// 完成证明与事实内容不符时不能跳过；正常来源路径应立即修复。
	brokenHour := member.NextBackfillHour
	expected := []UsageHourFact{{
		HourTs: brokenHour, DayTs: usageFactDayStart(brokenHour), UserID: 1,
		ChannelID: 9, Grp: "g", ModelName: "missing", TokenID: 10, Requests: 1, ConsumeQuota: 123,
	}}
	if err := m.storeDB.Create(&UsageFactMemberHourState{
		UserID: 1, HourTs: brokenHour, Status: "complete", Rows: 1, Requests: 1,
		ContentHash: usageFactContentHash(expected),
	}).Error; err != nil {
		t.Fatal(err)
	}
	counts.reset()
	worked, err = m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("逻辑缺行修复失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("证明与事实不符时应查来源一次，实际=%d", got)
	}
}

// 小时明细超过留存期会被正常清理。90→366 天扩窗后游标再次
// 经过已发布的老日期时，应使用完整的日语义证明跳过，不能因为
// 小时行已清理就重读来源 logs。日事实被改坏时则必须拒绝跳过。
func TestUsageFactsBackfillSkipsPrunedHoursWithVerifiedDayProof(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 0, 15, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}

	daily := []UsageDailyFact{{
		DateTs: start, UserID: 1, ChannelID: 9, Grp: "g", ModelName: "model-a", TokenID: 10, TokenName: "token-a",
		Requests: 24, PromptTokens: 240, CompletionTokens: 48, ConsumeQuota: 2400,
	}}
	if err := m.storeDB.Create(&daily).Error; err != nil {
		t.Fatal(err)
	}
	metrics := dailyFactsMetrics(daily)
	if err := m.storeDB.Create(&UsageFactMemberDayState{
		UserID: 1, DateTs: start, Rows: len(daily), Requests: metrics.Requests,
		Tokens: metrics.tokens(), ContentHash: usageDailyFactContentHash(daily),
	}).Error; err != nil {
		t.Fatal(err)
	}
	for hour := start; hour < end; hour += usageFactHourSeconds {
		prunedRow := []UsageHourFact{{
			HourTs: hour, DayTs: start, UserID: 1, ChannelID: 9, Grp: "g", ModelName: "model-a",
			TokenID: 10, TokenName: "token-a", Requests: 1, PromptTokens: 10, CompletionTokens: 2, ConsumeQuota: 100,
		}}
		if err := m.storeDB.Create(&UsageFactMemberHourState{
			UserID: 1, HourTs: hour, Status: "complete", Rows: 1, Requests: 1, Tokens: 12,
			ContentHash: usageFactContentHash(prunedRow),
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// 故意不保留 UsageHourFact，模拟 8 天留存期后的正常剪枝。
	counts.reset()
	worked, err := m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("日 proof 跳过失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("已核验日不应重读来源 logs，实际=%d", got)
	}
	var member UsageFactMemberState
	if err := m.storeDB.First(&member, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if member.NextBackfillHour != end {
		t.Fatalf("已剪枝整日应一次跳过 24 小时: got=%d want=%d", member.NextBackfillHour, end)
	}

	if err := m.storeDB.Model(&UsageDailyFact{}).Where("date_ts = ? AND user_id = ?", start, 1).
		Update("consume_quota", 9999).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.rewindUsageFactMemberBackfillCursor(1, start); err != nil {
		t.Fatal(err)
	}
	counts.reset()
	worked, err = m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("日事实损坏后修复路径失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("日事实指纹不符时必须查来源修复，实际=%d", got)
	}
}

func TestUsageFactsReadBudgetCapsDifferentColdQueries(t *testing.T) {
	m := &Monitor{}
	release1, err := m.acquireUsageFactsReadBudget(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release2, err := m.acquireUsageFactsReadBudget(context.Background())
	if err != nil {
		release1()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if release3, err := m.acquireUsageFactsReadBudget(ctx); err == nil {
		release3()
		release2()
		release1()
		t.Fatal("第三个不同冷查应在容量 2 的本地事实预算上等待")
	}
	release2()
	release1()
	stats := m.usageFactsReadBudgetStats()
	if stats.Capacity != 2 || stats.Acquired != 2 || stats.Failed != 1 || stats.Completed != 2 || stats.InUse != 0 || stats.Waiters != 0 {
		t.Fatalf("本地事实查询指标错误: %+v", stats)
	}
}

// TestUsageFactsFinishedCursorRepairsIntermediateGap 锁住恢复/局部损坏场景：
// 游标已经到达窗口末尾并不等于中间每个小时都完整。后台应先只读本地台账
// 找到最早缺口、回退游标，再按既有限速路径只补这个小时；不能永久卡在
// “游标完成但覆盖率不足”，也不能在定位缺口时扫描生产 logs。
func TestUsageFactsFinishedCursorRepairsIntermediateGap(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	gap := start + 7*usageFactHourSeconds
	emptyHash := usageFactContentHash(nil)
	if err := m.storeDB.Create(&UsageFactMemberState{
		UserID: 1, Active: true, BackfillWindowDays: 1, RangeStart: start, NextBackfillHour: end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	states := make([]UsageFactMemberHourState, 0, 23)
	for hour := start; hour < end; hour += usageFactHourSeconds {
		if hour == gap {
			continue
		}
		states = append(states, UsageFactMemberHourState{
			UserID:      1,
			HourTs:      hour,
			Status:      "complete",
			ContentHash: emptyHash,
		})
	}
	if err := m.storeDB.CreateInBatches(states, len(states)).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": 1,
		"next_backfill_hour":   end,
	}).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	worked, err := m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("定位中间缺口失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("定位本地台账缺口不得扫描生产 logs，实际 %d 次", got)
	}
	var memberState UsageFactMemberState
	if err := m.storeDB.First(&memberState, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if memberState.NextBackfillHour != gap {
		t.Fatalf("成员完成游标未回退到最早缺口: got=%d want=%d", memberState.NextBackfillHour, gap)
	}

	worked, err = m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("补齐中间缺口失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("只应查询缺失的一小时，实际 logs=%d", got)
	}
	var repaired UsageHourIngestState
	if err := m.storeDB.First(&repaired, "hour_ts = ?", gap).Error; err != nil {
		t.Fatal(err)
	}
	if repaired.Status != "complete" || repaired.ContentHash == "" {
		t.Fatalf("缺口没有形成可验证的完成台账: %+v", repaired)
	}
	if err := m.storeDB.First(&memberState, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if memberState.NextBackfillHour != gap+usageFactHourSeconds {
		t.Fatalf("补齐后成员游标错误: got=%d want=%d", memberState.NextBackfillHour, gap+usageFactHourSeconds)
	}
}

// TestFindEarliestUsageFactHourGapAcceptsCompleteRange 确保完整窗口不会被
// 误判为缺口，避免后台到达尾部后无意义地重复读取来源库。
func TestFindEarliestUsageFactHourGapAcceptsCompleteRange(t *testing.T) {
	m := newTestMonitor(t)
	start := usageFactTestDay().Unix()
	end := start + 4*usageFactHourSeconds
	for hour := start; hour < end; hour += usageFactHourSeconds {
		if err := m.storeDB.Create(&UsageHourIngestState{
			HourTs:      hour,
			Status:      "complete",
			ContentHash: usageFactContentHash(nil),
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if gap, found, err := m.findEarliestUsageFactHourGap(start, end); err != nil {
		t.Fatal(err)
	} else if found || gap != 0 {
		t.Fatalf("完整窗口被误判为缺口: found=%v gap=%d", found, gap)
	}
}

// TestUsageFactsExpandedBackfillWindowResetsFinishedCursor 锁住配置变更恢复场景：
// 原先较短窗口已补到尾部后，扩大保留天数必须重新从新增历史起点补齐；不能因
// NextBackfillHour 已等于旧 end 就永久跳过新的更早时段。
func TestUsageFactsExpandedBackfillWindowResetsFinishedCursor(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 2
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - 2*usageFactDaySeconds
	if _, err := prodDB.Exec(fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,33,%d,2,'model-a',500000,100,20,'g1',10,'token-a')", start+1)); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": 1,
		"next_backfill_hour":   end,
		"generation":           7,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageFactMemberState{
		UserID: 1, Active: true, BackfillWindowDays: 1,
		RangeStart: end - usageFactDaySeconds, NextBackfillHour: end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.usageFactsRevision.Store(7)
	// 服务版仍是旧的一天窗口；候选配置已扩大到两天。
	m.cfg.UsageFactsBackfillDays = 1
	seedPublishedUsageFactsForTest(t, m, []int64{1}, end-usageFactDaySeconds, end)
	m.cfg.UsageFactsBackfillDays = 2

	counts.reset()
	worked, err := m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("扩大窗口后的首小时回填失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("扩大窗口后应只读取新增起点的一小时聚合，实际 logs=%d", got)
	}
	var state UsageFactSyncState
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.BackfillWindowDays != 2 || state.NextBackfillHour != start+usageFactHourSeconds {
		t.Fatalf("扩大窗口后的恢复游标错误: %+v start=%d", state, start)
	}
	var memberState UsageFactMemberState
	if err := m.storeDB.First(&memberState, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if memberState.BackfillWindowDays != 2 || memberState.RangeStart != start || memberState.NextBackfillHour != start+usageFactHourSeconds {
		t.Fatalf("扩大窗口后的成员恢复游标错误: %+v start=%d", memberState, start)
	}
	if !m.usageFactsReadEnabled() {
		t.Fatal("扩大候选窗口期间必须继续读取旧服务版")
	}
	if got := m.usageFactsReadyFrom.Load(); got != end-usageFactDaySeconds {
		t.Fatalf("候选回填不得提前扩大服务版左边界: got=%d", got)
	}
	var rows int64
	if err := m.storeDB.Model(&UsageHourFact{}).Where("hour_ts = ?", start).Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("新增历史起点未写入本地事实: rows=%d", rows)
	}
}

// TestUsageFactsSlidingWindowDoesNotResetBackfillCursor 锁住自然跨整点场景。
// 回填窗口左边界每小时前移一小时，此时已前进的成员游标必须继续
// 向前，不能回退到新的窗口起点并重扫几百天来源 logs。
func TestUsageFactsSlidingWindowDoesNotResetBackfillCursor(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 7, 3, 12, 30, 0, 0, usageCST)
	oldEnd := m.usageFactFinalizedHour(now)
	oldStart := oldEnd - usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, oldStart); err != nil {
		t.Fatal(err)
	}
	resume := oldStart + 6*usageFactHourSeconds
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).
		Update("next_backfill_hour", resume).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = ?", 1).
		Update("next_backfill_hour", resume).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	worked, err := m.syncNextUsageFactHour(context.Background(), now.Add(time.Hour))
	if err != nil || !worked {
		t.Fatalf("跨整点后继续回填失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("跨整点后应只读取游标所在的一小时，实际 logs=%d", got)
	}
	var member UsageFactMemberState
	if err := m.storeDB.First(&member, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	newStart := oldStart + usageFactHourSeconds
	if member.RangeStart != newStart {
		t.Fatalf("滑动窗口起点未前移: got=%d want=%d", member.RangeStart, newStart)
	}
	if want := resume + usageFactHourSeconds; member.NextBackfillHour != want {
		t.Fatalf("滑动窗口错误回退成员游标: got=%d want=%d", member.NextBackfillHour, want)
	}
}

// TestUsageFactsStatusUsesOnlyLocalWatermarks 覆盖率状态是切读前的运维护栏：
// 必须只读本地 SQLite、正确识别成员变更，并且不能为“看状态”访问生产 logs。
func TestUsageFactsStatusUsesOnlyLocalWatermarks(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	states := make([]UsageHourIngestState, 0, 24)
	for hour := start; hour < end; hour += usageFactHourSeconds {
		states = append(states, UsageHourIngestState{
			HourTs:      hour,
			Status:      "complete",
			ContentHash: usageFactContentHash(nil),
		})
	}
	if err := m.storeDB.CreateInBatches(states, 24).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": 1,
		"next_backfill_hour":   end,
		"last_fact_sync_at":    now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	status, err := m.usageFactsStatus(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if status.ExpectedHours != 24 || status.CompleteHours != 24 || status.CoveragePercent != 100 || !status.MembershipSynchronized || !status.FactCoverageReady {
		t.Fatalf("本地事实状态错误: %+v", status)
	}
	if status.ReadActive {
		t.Fatalf("候选已完整但尚未发布时不能提前进入读取面: %+v", status)
	}
	if status.RangeStart == "" || status.RangeEnd == "" || status.NextBackfillHour != end {
		t.Fatalf("本地事实状态缺少范围/游标: %+v", status)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("读取事实状态不得扫描生产 logs，实际 %d 次", got)
	}
	m.refreshUsageFactsReadiness(context.Background(), now)
	status, err = m.usageFactsStatus(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if !status.ReadActive || status.ServingUsers != 1 || status.PublishedRangeStart != start || status.PublishedThrough != end {
		t.Fatalf("完整候选发布后服务版状态错误: %+v", status)
	}

	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Update("member_fingerprint", "stale").Error; err != nil {
		t.Fatal(err)
	}
	status, err = m.usageFactsStatus(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if status.MembershipSynchronized || status.FactCoverageReady || !status.ReadActive {
		t.Fatalf("候选指纹过期时旧服务版仍须可读: %+v", status)
	}
}

// 覆盖率必须从每个成员已核验的连续游标计算，不再为显示一个
// 百分比每次扫全部 member-hour proof。业务内容是否可读仍由发布
// 快照和语义审计独立决定。
func TestUsageFactsStatusUsesContiguousMemberCursorProgress(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create([]TrackedUser{
		{UserID: 1, Username: "alice"}, {UserID: 2, Username: "bob"},
	}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1, 2}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).
		Update("next_backfill_hour", start+12*usageFactHourSeconds).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 2).
		Update("next_backfill_hour", end).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = ?", 1).Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1, 2}),
		"backfill_window_days": 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	status, err := m.usageFactsStatus(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if status.CoverageBasis != "contiguous_member_cursor" || status.ExpectedMemberHours != 48 ||
		status.CompleteMemberHours != 36 || status.CompleteHours != 12 || status.CoveragePercent != 75 ||
		status.FactCoverageReady || status.ReadActive {
		t.Fatalf("连续成员游标进度错误: %+v", status)
	}
}

func TestUsageFactsGapAuditRotatesBoundedMemberBatches(t *testing.T) {
	m := newTestMonitor(t)
	start := usageFactTestDay().Unix()
	end := start + usageFactHourSeconds
	emptyHash := usageFactContentHash(nil)
	for id := 1; id <= 25; id++ {
		if err := m.storeDB.Create(&UsageFactMemberState{
			UserID: int64(id), Active: true, RangeStart: start, NextBackfillHour: end,
		}).Error; err != nil {
			t.Fatal(err)
		}
		if id != 25 {
			if err := m.storeDB.Create(&UsageFactMemberHourState{
				UserID: int64(id), HourTs: start, Status: "complete", ContentHash: emptyHash,
			}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	for batch := 1; batch <= 2; batch++ {
		if user, hour, found, err := m.findEarliestUsageFactMemberHourGap(start, end); err != nil {
			t.Fatal(err)
		} else if found {
			t.Fatalf("第 %d 个分片不应提前扫到末尾缺口: user=%d hour=%d", batch, user, hour)
		}
	}
	user, hour, found, err := m.findEarliestUsageFactMemberHourGap(start, end)
	if err != nil {
		t.Fatal(err)
	}
	if !found || user != 25 || hour != start {
		t.Fatalf("分片轮转未找到第 25 个成员的缺口: user=%d hour=%d found=%v", user, hour, found)
	}
}

// Shadow/read=false 必须能够产出一份经过完整性核验的发布快照，但不能提前
// 改变页面读源。否则运维手册要求的切读前 published_at 验收永远无法完成，
// 低频历史复核也会一直被 PublishedAt=0 挡住。
func TestUsageFactsShadowPublishesWithoutServingAndEnablesReconcile(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsReadEnabled = false
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "shadow"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	proofs := make([]UsageFactMemberHourState, 0, 24)
	for hour := start; hour < end; hour += usageFactHourSeconds {
		proofs = append(proofs, UsageFactMemberHourState{
			UserID: 1, HourTs: hour, Status: "complete", ContentHash: usageFactContentHash(nil),
		})
	}
	if err := m.storeDB.CreateInBatches(proofs, 24).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).Update("next_backfill_hour", end).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Update("next_backfill_hour", end).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	m.refreshUsageFactsReadiness(context.Background(), now)
	status, err := m.usageFactsStatus(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if status.PublishedAt == 0 || status.PublishedRangeStart != start || status.PublishedThrough != end || status.ServingUsers != 1 {
		t.Fatalf("shadow 完整候选未生成可验收发布快照: %+v", status)
	}
	if status.ReadActive || m.usageFactsReadEnabled() {
		t.Fatalf("shadow 发布不得提前切换页面读源: %+v", status)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("shadow 发布只应读取本地台账，实际扫描 logs %d 次", got)
	}

	worked, err := m.reconcileNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("shadow 发布后历史复核应可运行: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("一次历史复核只应执行一条单小时来源聚合，实际 %d", got)
	}
}

// TestUsageFactsReadPermissionRequiresCurrentCoverage 锁住“ReadEnabled 是请求，不是
// 无条件许可”：首版只有完整发布后才可读；后续名单变化则继续服务上一版，新成员
// 在候选回填完成前不可见。整个测试不访问生产 logs。
func TestUsageFactsReadPermissionRequiresCurrentCoverage(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsReadEnabled = true
	m.cfg.UsageFactsBackfillDays = 1
	m.setUsageFactsReadiness(false, 0)
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)

	// 刚启动/无水位时，不得把空事实当作真实数据源。
	m.refreshUsageFactsReadiness(context.Background(), now)
	if m.usageFactsReadEnabled() {
		t.Fatal("事实水位未完成时不得切读")
	}

	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	states := make([]UsageHourIngestState, 0, 24)
	for hour := start; hour < end; hour += usageFactHourSeconds {
		states = append(states, UsageHourIngestState{
			HourTs:      hour,
			Status:      "complete",
			ContentHash: usageFactContentHash(nil),
		})
	}
	if err := m.storeDB.CreateInBatches(states, len(states)).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": 1,
		"next_backfill_hour":   end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	counts.reset()
	m.refreshUsageFactsReadiness(context.Background(), now)
	if !m.usageFactsReadEnabled() {
		t.Fatal("当前名单已完整回填后应允许切读")
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("核验本地完整性不得扫描生产 logs，实际 %d 次", got)
	}

	// 新成员加入时 ensure 会清掉候选小时完成台账，但旧服务版继续可读。
	if err := m.storeDB.Create(&TrackedUser{UserID: 2, Username: "bob"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.ensureUsageFactMembership([]int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if !m.usageFactsReadEnabled() {
		t.Fatal("名单变化且未补齐时不得关闭旧服务版")
	}
	serving, err := m.listTrackedForUsageRead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(serving) != 1 || serving[0].UserID != 1 {
		t.Fatalf("候选新成员未发布前不得进入读取面: %+v", serving)
	}
}

// TestUsageFactsLagKeepsReadsLocalAndReportsWatermark 锁住“事实同步短暂落后”
// 的关键降级语义：不能回退扫描生产 logs；只有查询跨过已验证水位时才附带明确、
// 不夸大的本地更新水位提示。
func TestUsageFactsLagKeepsReadsLocalAndReportsWatermark(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	readyThrough := m.usageFactFinalizedHour(now) - usageFactHourSeconds
	m.setUsageFactsReadiness(true, readyThrough)

	if !m.usageFactsReadEnabled() {
		t.Fatal("同步水位暂时落后时仍应保持本地事实读")
	}
	counts.reset()
	from := readyThrough - usageFactDaySeconds
	to := m.usageFactFinalizedHour(now)
	if _, err := m.computeUsageStatsForRead(context.Background(), []int64{1}, from, to, 0); err != nil {
		t.Fatalf("落后水位期间本地事实读取失败: %v", err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("事实水位落后时不得回退扫描生产 logs，实际 %d 次", got)
	}
	stale, message := m.usageFactsRangeStaleness(from, to, now)
	if !stale || !strings.Contains(message, "已汇总至") {
		t.Fatalf("应明确报告事实水位滞后: stale=%v message=%q", stale, message)
	}
	// 已完成的历史区间不应因当前尾部落后而被误标为不完整。
	stale, _ = m.usageFactsRangeStaleness(from, readyThrough, now)
	if stale {
		t.Fatal("已在验证水位内的历史区间不应标记为滞后")
	}
}

// TestUsageFactsPublishedTailAdvancesWhileHistoryExpands 锁住“扩容历史窗口
// 不阻塞现有服务版追尾”：新闭合小时已在本地通过完整性校验时，
// 推进右侧水位，并让旧服务版按自己的 30 天范围等长滑动；不能提前
// 暴露正在回填的 90 天候选左边界。
func TestUsageFactsPublishedTailAdvancesWhileHistoryExpands(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 15, 0, 0, usageCST)
	through := m.usageFactFinalizedHour(now)
	oldThrough := through - usageFactHourSeconds
	oldFrom := oldThrough - 30*usageFactDaySeconds
	seedPublishedUsageFactsForTest(t, m, []int64{1}, oldFrom, oldThrough)
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": 90,
		"generation":           11,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.usageFactsRevision.Store(11)
	if err := m.storeDB.Create(&UsageFactMemberHourState{
		UserID:      1,
		HourTs:      oldThrough,
		Status:      "complete",
		ContentHash: usageFactContentHash(nil),
	}).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	if err := m.advanceUsageFactsPublishedTail(context.Background(), through); err != nil {
		t.Fatal(err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("推进本地发布水位不得读取生产 logs，实际 %d 次", got)
	}
	var state UsageFactSyncState
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.PublishedThrough != through || state.PublishedRangeStart != oldFrom+usageFactHourSeconds || state.PublishedWindowDays != 30 {
		t.Fatalf("应按旧服务版窗口等长推进边界: %+v", state)
	}
	if gotFrom, gotThrough := m.usageFactsReadyFrom.Load(), m.usageFactsReadyThrough.Load(); gotFrom != oldFrom+usageFactHourSeconds || gotThrough != through {
		t.Fatalf("内存读取边界必须与原子发布水位同步: from=%d through=%d", gotFrom, gotThrough)
	}
	if state.Generation != 12 || m.usageFactsRevision.Load() != 12 {
		t.Fatalf("推进水位必须切换缓存世代: state=%d revision=%d", state.Generation, m.usageFactsRevision.Load())
	}
}

func TestUsageFactsPublishedTailRefusesGapsButKeepsServingPublishedMembers(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, _ := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 15, 0, 0, usageCST)
	through := m.usageFactFinalizedHour(now)
	oldThrough := through - 2*usageFactHourSeconds
	seedPublishedUsageFactsForTest(t, m, []int64{1}, oldThrough-usageFactDaySeconds, oldThrough)
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint": portalMemberFingerprintFromIDs([]int64{1}),
		"generation":         20,
	}).Error; err != nil {
		t.Fatal(err)
	}
	// 只有第二个小时完成，中间缺口不得跨过。
	if err := m.storeDB.Create(&UsageFactMemberHourState{
		UserID:      1,
		HourTs:      oldThrough + usageFactHourSeconds,
		Status:      "complete",
		ContentHash: usageFactContentHash(nil),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.advanceUsageFactsPublishedTail(context.Background(), through); err != nil {
		t.Fatal(err)
	}
	var state UsageFactSyncState
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.PublishedThrough != oldThrough {
		t.Fatalf("存在小时缺口时不得推进: %+v", state)
	}

	if err := m.storeDB.Create(&UsageFactMemberHourState{
		UserID:      1,
		HourTs:      oldThrough,
		Status:      "complete",
		ContentHash: usageFactContentHash(nil),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Update("member_fingerprint", "different-members").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.advanceUsageFactsPublishedTail(context.Background(), through); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.PublishedThrough != through {
		t.Fatalf("候选名单变化不应阻塞已发布成员追尾: %+v", state)
	}
}

// TestUsageFactsMemberChangeKeepsExistingMemberWatermark 新增关注客户后，已有事实、
// 完成证明和老成员游标都必须保留；只有新成员从窗口起点补历史。
func TestUsageFactsMemberChangeKeepsExistingMemberWatermark(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UsageFactsBackfillDays = 30
	if err := m.storeDB.Create([]TrackedUser{
		{UserID: 1, Username: "alice"},
		{UserID: 2, Username: "bob"},
	}).Error; err != nil {
		t.Fatal(err)
	}
	day := usageFactTestDay().Unix()
	hour := day + usageFactHourSeconds
	start := day
	end := day + usageFactDaySeconds
	if err := m.storeDB.Create(&UsageHourFact{HourTs: hour, DayTs: day, UserID: 1, ChannelID: 33, Grp: "g", ModelName: "m", TokenID: 10, Requests: 1, ConsumeQuota: 100}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageDailyFact{DateTs: day, UserID: 1, ChannelID: 33, Grp: "g", ModelName: "m", TokenID: 10, Requests: 1, ConsumeQuota: 100}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageHourIngestState{HourTs: hour, Status: "complete", ContentHash: usageFactContentHash(nil)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageFactMemberState{
		UserID: 1, Active: true, BackfillWindowDays: 30, RangeStart: start, NextBackfillHour: end,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageFactMemberHourState{
		UserID: 1, HourTs: hour, Status: "complete", ContentHash: usageFactContentHash([]UsageHourFact{{
			HourTs: hour, DayTs: day, UserID: 1, ChannelID: 33, Grp: "g", ModelName: "m", TokenID: 10,
			Requests: 1, ConsumeQuota: 100,
		}}),
	}).Error; err != nil {
		t.Fatal(err)
	}
	var state UsageFactSyncState
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	state.MemberFingerprint = portalMemberFingerprintFromIDs([]int64{1})
	state.BackfillWindowDays = 30
	state.NextBackfillHour = end
	state.Generation = 7
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	m.usageFactsRevision.Store(7)

	if err := m.ensureUsageFactMembershipAt([]int64{1, 2}, start); err != nil {
		t.Fatal(err)
	}
	var hourly, daily, watermarks int64
	if err := m.storeDB.Model(&UsageHourFact{}).Count(&hourly).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageDailyFact{}).Count(&daily).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageHourIngestState{}).Count(&watermarks).Error; err != nil {
		t.Fatal(err)
	}
	if hourly != 1 || daily != 1 || watermarks != 1 {
		t.Fatalf("成员变更不应清事实: hourly=%d daily=%d states=%d", hourly, daily, watermarks)
	}
	var members []UsageFactMemberState
	if err := m.storeDB.Order("user_id").Find(&members).Error; err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0].UserID != 1 || members[0].NextBackfillHour != end ||
		members[1].UserID != 2 || members[1].NextBackfillHour != start {
		t.Fatalf("成员独立游标错误: %+v", members)
	}
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.Generation != 8 || m.usageFactsRevision.Load() != 8 || state.MemberFingerprint != portalMemberFingerprintFromIDs([]int64{1, 2}) {
		t.Fatalf("成员变更状态更新错误: %+v revision=%d", state, m.usageFactsRevision.Load())
	}
}

// TestUsageFactsAddingMemberOnlyBackfillsNewMember 是成员扩容的性能闸：老成员
// 已完成的事实、证明和游标不能被触碰，来源查询只包含新成员。
func TestUsageFactsAddingMemberOnlyBackfillsNewMember(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds

	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.setUsageFactMemberBackfillCursor([]int64{1}, end); err != nil {
		t.Fatal(err)
	}
	oldFact := UsageHourFact{
		HourTs: start, DayTs: usageFactDayStart(start), UserID: 1, ChannelID: 9,
		Grp: "old", ModelName: "sentinel", TokenID: 91, Requests: 1, ConsumeQuota: 777,
	}
	if err := m.storeDB.Create(&oldFact).Error; err != nil {
		t.Fatal(err)
	}
	oldProof := UsageFactMemberHourState{
		UserID: 1, HourTs: start, Status: "complete", Rows: 1, Requests: 1,
		ContentHash: usageFactContentHash([]UsageHourFact{oldFact}), Attempts: 4,
	}
	if err := m.storeDB.Create(&oldProof).Error; err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,1,33,%d,2,'should-not-be-read',111,1,1,'g1',10,'token-a')", start+1),
		fmt.Sprintf("INSERT INTO logs (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (2,2,44,%d,2,'new-member',222,2,1,'g2',20,'token-b')", start+2),
	} {
		if _, err := prodDB.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 2, Username: "bob"}).Error; err != nil {
		t.Fatal(err)
	}

	counts.reset()
	worked, err := m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("新增成员首小时回填失败: worked=%v err=%v", worked, err)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("新增成员每小时只应产生一条批量来源查询，logs=%d", got)
	}
	var facts []UsageHourFact
	if err := m.storeDB.Where("hour_ts = ?", start).Order("user_id").Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 2 || facts[0].UserID != 1 || facts[0].ConsumeQuota != 777 ||
		facts[1].UserID != 2 || facts[1].ConsumeQuota != 222 {
		t.Fatalf("新增成员回填改写了老成员或漏写新成员: %+v", facts)
	}
	var proof UsageFactMemberHourState
	if err := m.storeDB.First(&proof, "user_id = ? AND hour_ts = ?", 1, start).Error; err != nil {
		t.Fatal(err)
	}
	if proof.Attempts != oldProof.Attempts || proof.ContentHash != oldProof.ContentHash {
		t.Fatalf("老成员完成证明不应被重新领取: before=%+v after=%+v", oldProof, proof)
	}
	var members []UsageFactMemberState
	if err := m.storeDB.Order("user_id").Find(&members).Error; err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0].NextBackfillHour != end || members[1].NextBackfillHour != start+usageFactHourSeconds {
		t.Fatalf("新增成员回填后独立游标错误: %+v", members)
	}
}

// TestUsageFactsCandidateBackfillDoesNotRotateServingCacheGeneration 确保候选新成员
// 的长历史回填不会每个小时换掉旧服务版缓存键。
func TestUsageFactsCandidateBackfillDoesNotRotateServingCacheGeneration(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, _ := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds

	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "served"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	if err := m.setUsageFactMemberBackfillCursor([]int64{1}, end); err != nil {
		t.Fatal(err)
	}
	seedPublishedUsageFactsForTest(t, m, []int64{1}, start, end)
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = ?", 1).Updates(map[string]any{
		"generation": 5, "serving_generation": 5,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.usageFactsRevision.Store(5)
	m.usageFactsServingRevision.Store(5)
	keyBefore := m.usageFactCacheKey("candidate-isolation")

	if err := m.storeDB.Create(&TrackedUser{UserID: 2, Username: "candidate"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := prodDB.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,quota,`group`) VALUES(1,2,9,?,2,'candidate-model',123,'g')", start+1); err != nil {
		t.Fatal(err)
	}
	worked, err := m.syncNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("新成员候选回填失败: worked=%v err=%v", worked, err)
	}
	var state UsageFactSyncState
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.Generation <= 5 || state.ServingGeneration != 5 {
		t.Fatalf("候选变化应只推进 candidate generation: %+v", state)
	}
	if got := m.usageFactCacheKey("candidate-isolation"); got != keyBefore {
		t.Fatalf("候选回填不得改变服务版缓存键: before=%q after=%q", keyBefore, got)
	}
}

func TestUsageFactsTailBoundsLargeMemberQueries(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	tracked := make([]TrackedUser, 0, 401)
	for id := int64(1); id <= 401; id++ {
		tracked = append(tracked, TrackedUser{UserID: id, Username: fmt.Sprintf("u-%d", id)})
	}
	if err := m.storeDB.CreateInBatches(tracked, 200).Error; err != nil {
		t.Fatal(err)
	}
	counts.reset()
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, usageCST)
	if err := m.syncUsageFactsTail(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	// Tail 固定回读 3 小时；401 人按 200/200/1 拆成 3 批，共 9 条 SQL。
	if got := counts.logs.Load(); got != 9 {
		t.Fatalf("大名单 Tail 来源查询批次数错误: got=%d want=9", got)
	}
	// fetchUsageFactHour 参数 = 起止时间 2 个 + user IDs + LIMIT 1 个。
	if got := counts.maxLogArgs.Load(); got > usageFactProfileBatch+3 {
		t.Fatalf("单条来源 SQL 的成员参数未受 200 上限保护: args=%d", got)
	}
}

func TestUsageFactHourLeaseIsShortAndExpiredClaimIsRecoverable(t *testing.T) {
	m := newTestMonitor(t)
	hour := usageFactTestDay().Add(3 * time.Hour).Unix()
	now := time.Now().Unix()
	stale := UsageFactMemberHourState{
		UserID: 1, HourTs: hour, Status: "running", LeaseToken: "old", LeaseUntil: now + 60,
	}
	if err := m.storeDB.Create(&stale).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.claimUsageFactHour(hour, []int64{1}); !errors.Is(err, errUsageFactLeaseBusy) {
		t.Fatalf("未过期租约必须拒绝重复领取: %v", err)
	}
	if err := m.storeDB.Model(&UsageFactMemberHourState{}).
		Where("user_id = ? AND hour_ts = ?", 1, hour).Update("lease_until", now-1).Error; err != nil {
		t.Fatal(err)
	}
	claim, err := m.claimUsageFactHour(hour, []int64{1})
	if err != nil || claim.leaseToken == "" || claim.leaseToken == "old" {
		t.Fatalf("过期租约应可重领: claim=%+v err=%v", claim, err)
	}
	if !m.usageFactsSyncMu.TryLock() {
		t.Fatal("领取租约后必须立即释放本地同步锁，远程 SQL 不得持有它")
	}
	m.usageFactsSyncMu.Unlock()
	if err := m.failUsageFactHourClaim(hour, []int64{1}, claim, context.Canceled, false); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.storeDB.Model(&UsageFactMemberHourState{}).
		Where("user_id = ? AND hour_ts = ?", 1, hour).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("取消过期租约的重试后不应遗留无主 running 状态: count=%d", count)
	}
}

// TestUsageFactsLastGoodDailySuppressesPartialHourly 锁定成员日级的原子可见性。
// 只要上一份可服务的日事实仍在，repair 中逐小时写入的同维度或新维度
// 都不得提前进入页面；否则会把旧日版本和新候选版本混合。
func TestUsageFactsLastGoodDailySuppressesPartialHourly(t *testing.T) {
	m := newTestMonitor(t)
	day := usageFactTestDay().Unix()
	if err := m.storeDB.Create(&UsageDailyFact{DateTs: day, UserID: 1, ChannelID: 1, Grp: "g1", ModelName: "m1", TokenID: 1, Requests: 1, ConsumeQuota: 100, PromptTokens: 10, CompletionTokens: 1}).Error; err != nil {
		t.Fatal(err)
	}
	// A 维度已有日事实，此候选行不能再次计入。
	if err := m.storeDB.Create(&UsageHourFact{HourTs: day + usageFactHourSeconds, DayTs: day, UserID: 1, ChannelID: 1, Grp: "g1", ModelName: "m1", TokenID: 1, Requests: 2, ConsumeQuota: 200, PromptTokens: 20, CompletionTokens: 2}).Error; err != nil {
		t.Fatal(err)
	}
	// B 是 repair 后新采到的维度，整天重建完成前也必须保持不可见。
	if err := m.storeDB.Create(&UsageHourFact{HourTs: day + 2*usageFactHourSeconds, DayTs: day, UserID: 1, ChannelID: 2, Grp: "g2", ModelName: "m2", TokenID: 2, Requests: 1, ConsumeQuota: 30, PromptTokens: 3, CompletionTokens: 1}).Error; err != nil {
		t.Fatal(err)
	}

	st, err := m.computeUsageStatsFromFacts(context.Background(), []int64{1}, day, day+usageFactDaySeconds, 0)
	if err != nil {
		t.Fatal(err)
	}
	if st.Summary.Requests != 1 || st.Summary.ConsumeQuota != 100 || st.Summary.Tokens != 11 {
		t.Fatalf("旧日版本混入了 repair 小时事实: %+v", st.Summary)
	}
	mx, err := m.computeUsageMatrixFromFacts(context.Background(), []int64{1}, day, day+usageFactDaySeconds)
	if err != nil {
		t.Fatal(err)
	}
	if len(mx.Cells) != 1 || mx.Cells[0].Requests != 1 || mx.Cells[0].ConsumeQuota != 100 {
		t.Fatalf("矩阵混入了 repair 小时事实: %+v", mx)
	}
}

func TestUsageFactsReadIsBoundedByPublishedHourAndPerMemberFloor(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t) // enables the production facts read gate; no query is issued below
	enableUsageFactsForTest(m)
	day := usageFactTestDay().Unix()
	previousDay := day - usageFactDaySeconds
	publishedThrough := day + 10*usageFactHourSeconds
	m.setUsageFactsPublishedReadiness(true, previousDay, publishedThrough)

	published := []UsageFactPublishedMember{
		{UserID: 1, TrackedRevision: 1, SourceFloorHour: previousDay, PublishedAt: time.Now().Unix()},
		{UserID: 2, TrackedRevision: 1, SourceFloorHour: day, PublishedAt: time.Now().Unix()},
	}
	if err := m.usageFactsStore().Create(&published).Error; err != nil {
		t.Fatal(err)
	}
	// User 1 has one complete published day. The current-day daily row is a
	// candidate containing future hours and must never be selected at 10:00.
	daily := []UsageDailyFact{
		{DateTs: previousDay, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, Requests: 1, ConsumeQuota: 10},
		{DateTs: day, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, Requests: 100, ConsumeQuota: 1000},
		// Stale pre-floor facts for user 2 can survive a migration/restore but are
		// outside that member's signed authority and must remain invisible.
		{DateTs: previousDay, UserID: 2, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 2, Requests: 50, ConsumeQuota: 500},
	}
	if err := m.usageFactsStore().Create(&daily).Error; err != nil {
		t.Fatal(err)
	}
	hours := []UsageHourFact{
		{HourTs: day + 9*usageFactHourSeconds, DayTs: day, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, Requests: 2, ConsumeQuota: 20},
		{HourTs: day + 10*usageFactHourSeconds, DayTs: day, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, Requests: 4, ConsumeQuota: 40},
		{HourTs: day + 11*usageFactHourSeconds, DayTs: day, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, Requests: 8, ConsumeQuota: 80},
		{HourTs: previousDay + 9*usageFactHourSeconds, DayTs: previousDay, UserID: 2, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 2, Requests: 50, ConsumeQuota: 500},
		{HourTs: day + 9*usageFactHourSeconds, DayTs: day, UserID: 2, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 2, TokenName: "t2", Requests: 3, ConsumeQuota: 30},
	}
	if err := m.usageFactsStore().Create(&hours).Error; err != nil {
		t.Fatal(err)
	}

	requestedThrough := day + usageFactDaySeconds
	stats, err := m.computeUsageStatsForRead(context.Background(), []int64{1, 2}, previousDay, requestedThrough, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Summary.Requests != 6 || stats.Summary.ConsumeQuota != 60 {
		t.Fatalf("facts escaped published hour/member floor: %+v", stats.Summary)
	}
	matrix, err := m.computeUsageMatrixForRead(context.Background(), []int64{1, 2}, previousDay, requestedThrough)
	if err != nil {
		t.Fatal(err)
	}
	var matrixRequests int64
	for _, cell := range matrix.Cells {
		matrixRequests += cell.Requests
		if cell.UserID == 2 && cell.Date != time.Unix(day, 0).In(usageCST).Format("2006-01-02") {
			t.Fatalf("member pre-floor row became visible: %+v", cell)
		}
	}
	if matrixRequests != 6 {
		t.Fatalf("matrix escaped published boundary: cells=%+v", matrix.Cells)
	}
	wantCurrentDay := time.Unix(day, 0).In(usageCST).Format("2006-01-02")
	wantPreviousDay := time.Unix(previousDay, 0).In(usageCST).Format("2006-01-02")
	if len(matrix.Days) != 2 || matrix.Days[0] != wantCurrentDay || matrix.Days[1] != wantPreviousDay {
		t.Fatalf("partial-day readiness produced a broken matrix axis: %+v", matrix.Days)
	}
	daySet := map[string]bool{wantCurrentDay: true, wantPreviousDay: true}
	for _, cell := range matrix.Cells {
		if !daySet[cell.Date] {
			t.Fatalf("matrix cell has no rendered day column: cell=%+v days=%+v", cell, matrix.Days)
		}
	}
	tokens, err := m.computeUserTokenAggregatesForRead(context.Background(), 2, previousDay, requestedThrough)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 || tokens[0].Requests != 3 || tokens[0].ConsumeQuota != 30 {
		t.Fatalf("token aggregate escaped per-member floor/published hour: %+v", tokens)
	}
}

// TestUsageFactHourSyncCorrectsLateLogsAndIsIdempotent 验证近期小时已经
// complete 后，来源库追加的晚到日志仍会替换本地事实；来源内容不变时则不
// 重写事实、不递增缓存世代。
func TestUsageFactHourSyncCorrectsLateLogsAndIsIdempotent(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, _ := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)

	hour := usageFactTestDay().Add(9 * time.Hour).Unix()
	insert := func(id int, createdAt, quota, prompt, completion int64) {
		t.Helper()
		_, err := prodDB.Exec(`INSERT INTO logs
 (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name)
 VALUES (?,?,?,?,2,'model-a',?,?,?,'g1',10,'token-a')`, id, int64(1), int64(33), createdAt, quota, prompt, completion)
		if err != nil {
			t.Fatal(err)
		}
	}
	insert(1, hour+1, 500, 10, 2)
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").
		Update("last_fact_failure_at", time.Now().Add(-time.Hour).Unix()).Error; err != nil {
		t.Fatal(err)
	}

	options := usageFactHourSyncOptions{recordFailure: true, updateLastFactSync: true}
	first, err := m.syncUsageFactHourWithOptions(context.Background(), hour, []int64{1}, options)
	if err != nil || !first.Changed || first.HadPriorFingerprint {
		t.Fatalf("首次同步结果错误: result=%+v err=%v", first, err)
	}
	var firstState UsageHourIngestState
	if err := m.storeDB.First(&firstState, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if firstState.ContentHash == "" {
		t.Fatal("首次同步必须写入内容指纹")
	}
	var firstGlobal UsageFactSyncState
	if err := m.storeDB.First(&firstGlobal, 1).Error; err != nil {
		t.Fatal(err)
	}
	if firstGlobal.LastFactFailureAt != 0 {
		t.Fatalf("来源同步成功后应清除旧失败状态: %d", firstGlobal.LastFactFailureAt)
	}

	unchanged, err := m.syncUsageFactHourWithOptions(context.Background(), hour, []int64{1}, options)
	if err != nil || unchanged.Changed || !unchanged.HadPriorFingerprint {
		t.Fatalf("幂等同步结果错误: result=%+v err=%v", unchanged, err)
	}
	var unchangedGlobal UsageFactSyncState
	if err := m.storeDB.First(&unchangedGlobal, 1).Error; err != nil {
		t.Fatal(err)
	}
	if unchangedGlobal.Generation != firstGlobal.Generation {
		t.Fatalf("来源未变不应递增缓存世代: first=%d unchanged=%d", firstGlobal.Generation, unchangedGlobal.Generation)
	}

	insert(2, hour+120, 250, 4, 1)
	corrected, err := m.syncUsageFactHourWithOptions(context.Background(), hour, []int64{1}, options)
	if err != nil || !corrected.Changed || !corrected.HadPriorFingerprint {
		t.Fatalf("晚到日志纠正失败: result=%+v err=%v", corrected, err)
	}
	rows, err := loadUsageFactHour(m.storeDB, hour, []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	metrics := factsMetrics(rows)
	if metrics.Rows != 1 || metrics.Requests != 2 || metrics.ConsumeQuota != 750 || metrics.PromptTokens != 14 || metrics.CompletionTokens != 3 {
		t.Fatalf("晚到日志纠正后的事实错误: rows=%+v metrics=%+v", rows, metrics)
	}
	var correctedState UsageHourIngestState
	if err := m.storeDB.First(&correctedState, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if correctedState.ContentHash == firstState.ContentHash {
		t.Fatal("晚到日志改变内容后指纹必须变化")
	}
	var correctedGlobal UsageFactSyncState
	if err := m.storeDB.First(&correctedGlobal, 1).Error; err != nil {
		t.Fatal(err)
	}
	if correctedGlobal.Generation != firstGlobal.Generation+1 {
		t.Fatalf("晚到日志只应递增一次缓存世代: first=%d corrected=%d", firstGlobal.Generation, correctedGlobal.Generation)
	}
}

// TestUsageFactHourStateCannotHideMissingLocalRows 验证完成台账不能掩盖本地
// 事实丢失。即使状态仍是 complete，回填也必须拒绝跳过并从来源恢复该小时。
func TestUsageFactHourStateCannotHideMissingLocalRows(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, _ := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	hour := usageFactTestDay().Add(10 * time.Hour).Unix()
	if _, err := prodDB.Exec(`INSERT INTO logs
 (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name)
 VALUES (1,1,44,?,2,'model-b',300,6,2,'g2',11,'token-b')`, hour+1); err != nil {
		t.Fatal(err)
	}
	if err := m.syncUsageFactHour(context.Background(), hour, []int64{1}); err != nil {
		t.Fatal(err)
	}
	var state UsageHourIngestState
	if err := m.storeDB.First(&state, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Where("hour_ts = ?", hour).Delete(&UsageHourFact{}).Error; err != nil {
		t.Fatal(err)
	}
	verified, err := m.verifyCompletedUsageFactHour(hour, []int64{1}, state)
	if err != nil {
		t.Fatal(err)
	}
	if verified {
		t.Fatal("事实行缺失时不得把 complete 台账视为可信")
	}
	result, err := m.syncUsageFactHourWithOptions(context.Background(), hour, []int64{1}, usageFactHourSyncOptions{recordFailure: true})
	if err != nil || !result.Changed || !result.HadPriorFingerprint {
		t.Fatalf("缺失事实恢复失败: result=%+v err=%v", result, err)
	}
	rows, err := loadUsageFactHour(m.storeDB, hour, []int64{1})
	if err != nil || len(rows) != 1 || rows[0].Requests != 1 || rows[0].ConsumeQuota != 300 {
		t.Fatalf("缺失事实恢复结果错误: rows=%+v err=%v", rows, err)
	}
}

// TestUsageFactSemanticAuditDetectsDeletedPublishedDailyRowAndFailsClosed
// 覆盖 quick_check 无法发现的逻辑损坏：已发布日事实被合法 DELETE 后，成员小时
// 覆盖率仍是 100%，但周期语义审计必须用日指纹识别并立即关闭事实读取；页面
// 包装器只能返回 not-ready，绝不能因此重新扫描来源 logs。
func TestUsageFactSemanticAuditDetectsDeletedPublishedDailyRowAndFailsClosed(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsReadEnabled = true
	m.cfg.UsageFactsBackfillDays = 2
	m.cfg.UsageFactsRetentionDays = 4
	m.cfg.UsageFactsHourRetentionDays = 2
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - 2*usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	fullDay := usageFactDayStart(start) + usageFactDaySeconds
	factHour := fullDay + 10*usageFactHourSeconds
	fact := UsageHourFact{
		HourTs: factHour, DayTs: fullDay, UserID: 1, ChannelID: 9, Grp: "g", ModelName: "m", TokenID: 7,
		TokenName: "token", Requests: 2, PromptTokens: 12, CompletionTokens: 3, ConsumeQuota: 500,
	}
	if err := m.storeDB.Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	proofs := make([]UsageFactMemberHourState, 0, 48)
	for hour := start; hour < end; hour += usageFactHourSeconds {
		rows := []UsageHourFact(nil)
		if hour == factHour {
			rows = []UsageHourFact{fact}
		}
		metrics := factsMetrics(rows)
		proofs = append(proofs, UsageFactMemberHourState{
			UserID: 1, HourTs: hour, Status: "complete", Rows: int(metrics.Rows),
			Requests: metrics.Requests, Tokens: metrics.tokens(), ContentHash: usageFactContentHash(rows),
			UpdatedAt: now.Unix(), CompletedAt: now.Unix(),
		})
	}
	if err := m.storeDB.CreateInBatches(proofs, 48).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.rebuildUsageDailyFact(m.storeDB, fullDay, []int64{1}); err != nil {
		t.Fatal(err)
	}
	var dayProof UsageFactMemberDayState
	if err := m.storeDB.First(&dayProof, "user_id = ? AND date_ts = ?", 1, fullDay).Error; err != nil {
		t.Fatalf("完整自然日必须原子写入语义证明: %v", err)
	}
	if dayProof.Rows != 1 || dayProof.ContentHash == "" {
		t.Fatalf("日事实语义证明错误: %+v", dayProof)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).
		Updates(map[string]any{"next_backfill_hour": end, "updated_at": now.Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").
		Updates(map[string]any{"next_backfill_hour": end}).Error; err != nil {
		t.Fatal(err)
	}
	m.refreshUsageFactsReadiness(context.Background(), now)
	if !m.usageFactsReadEnabled() || !m.usageFactsSemanticAuditOK.Load() {
		t.Fatal("完整候选通过发布前语义审计后应允许事实读取")
	}

	if err := m.storeDB.Where("date_ts = ? AND user_id = ?", fullDay, 1).Delete(&UsageDailyFact{}).Error; err != nil {
		t.Fatal(err)
	}
	// 绕过一小时正常节流，模拟周期审计到期。
	m.usageFactsSemanticAuditAt.Store(0)
	m.usageFactsSemanticAuditNextDay.Store(fullDay)
	counts.reset()
	m.refreshUsageFactsReadiness(context.Background(), now.Add(time.Hour))
	if m.usageFactsReadEnabled() || m.usageFactsSemanticAuditOK.Load() {
		t.Fatal("已发布日事实逻辑删除后必须 fail closed")
	}
	_, err := m.computeUsageStatsForRead(context.Background(), []int64{1}, fullDay, fullDay+usageFactDaySeconds, 0)
	if !errors.Is(err, errUsageFactsNotReady) {
		t.Fatalf("语义损坏必须返回 facts-not-ready: %v", err)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("语义损坏后不得回扫来源 logs，实际 %d 次", got)
	}
	status, err := m.usageFactsStatus(context.Background(), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if status.ReadActive || status.SemanticAuditOK || status.SemanticAuditFailureAt == 0 {
		t.Fatalf("状态接口必须明确暴露语义审计失败: %+v", status)
	}
}

// TestUsageFactReconcileCorrectsLateLogsAndAdvancesAfterFailure 验证低频复核
// 能纠正已发布小时；来源故障时记录失败并前移游标，避免在同一小时忙循环。
func TestUsageFactReconcileCorrectsLateLogsAndAdvancesAfterFailure(t *testing.T) {
	m := newTestMonitor(t)
	prodDB, _ := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)
	m.cfg.UsageFactsBackfillDays = 1
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, Username: "alice"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 3, 12, 15, 0, 0, usageCST)
	end := m.usageFactFinalizedHour(now)
	start := end - usageFactDaySeconds
	if err := m.ensureUsageFactMembershipAt([]int64{1}, start); err != nil {
		t.Fatal(err)
	}
	hour := end - 5*usageFactHourSeconds
	if _, err := prodDB.Exec(`INSERT INTO logs
 (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name)
 VALUES (1,1,33,?,2,'model-a',500,10,2,'g1',10,'token-a')`, hour+1); err != nil {
		t.Fatal(err)
	}
	if err := m.syncUsageFactHour(context.Background(), hour, []int64{1}); err != nil {
		t.Fatal(err)
	}
	seedPublishedUsageFactsForTest(t, m, []int64{1}, end-usageFactDaySeconds, end)
	if err := m.storeDB.Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"member_fingerprint":   portalMemberFingerprintFromIDs([]int64{1}),
		"backfill_window_days": 1,
		"next_backfill_hour":   end,
		"next_reconcile_hour":  hour,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageFactMemberState{}).Where("user_id = ?", 1).Updates(map[string]any{
		"next_backfill_hour": end,
		"range_start":        start,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := prodDB.Exec(`INSERT INTO logs
 (id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name)
 VALUES (2,1,33,?,2,'model-a',250,4,1,'g1',10,'token-a')`, hour+120); err != nil {
		t.Fatal(err)
	}

	worked, err := m.reconcileNextUsageFactHour(context.Background(), now)
	if err != nil || !worked {
		t.Fatalf("历史复核纠正失败: worked=%v err=%v", worked, err)
	}
	rows, err := loadUsageFactHour(m.storeDB, hour, []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	if metrics := factsMetrics(rows); metrics.Requests != 2 || metrics.ConsumeQuota != 750 {
		t.Fatalf("历史复核未写入晚到日志: %+v", metrics)
	}
	var state UsageFactSyncState
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.ReconcileCorrections != 1 || state.LastReconciledHour != hour || state.NextReconcileHour != hour+usageFactHourSeconds {
		t.Fatalf("历史复核状态错误: %+v", state)
	}

	failingHour := state.NextReconcileHour
	if err := prodDB.Close(); err != nil {
		t.Fatal(err)
	}
	worked, err = m.reconcileNextUsageFactHour(context.Background(), now.Add(time.Minute))
	if err == nil || !worked {
		t.Fatalf("来源关闭后应记录一次可恢复失败: worked=%v err=%v", worked, err)
	}
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	expectedNext := failingHour + usageFactHourSeconds
	reconcileStart := end - int64(m.usageFactHourRetentionDays())*usageFactDaySeconds
	if state.PublishedRangeStart > reconcileStart {
		reconcileStart = state.PublishedRangeStart
	}
	if expectedNext >= end-3*usageFactHourSeconds {
		expectedNext = reconcileStart
	}
	if state.LastReconcileFailureAt == 0 || state.NextReconcileHour != expectedNext || state.NextReconcileHour == failingHour {
		t.Fatalf("复核失败后必须前移游标并记录时间: %+v", state)
	}
}

// TestUsageFactsPruneKeepsHistoricalWatermarks 已压缩为日事实的日期仍需保留
// complete 台账；否则后台会把已完成历史误认为缺失而重新扫描生产 logs。
func TestUsageFactsPruneKeepsHistoricalWatermarks(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UsageFactsBackfillDays = 30
	m.cfg.UsageFactsHourRetentionDays = 8
	m.cfg.UsageFactsRetentionDays = 60
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, usageCST)
	// 小时事实独立保留 8 天；即使历史回填覆盖 30 天，10 天前的小时
	// 明细也应清理，而日事实和完成台账必须继续保留。
	oldHour := now.AddDate(0, 0, -10).Truncate(time.Hour).Unix()
	oldDay := usageFactDayStart(oldHour)
	if err := m.storeDB.Create(&UsageHourFact{HourTs: oldHour, DayTs: oldDay, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageDailyFact{DateTs: oldDay, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageHourIngestState{HourTs: oldHour, Status: "complete"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneUsageFacts(now); err != nil {
		t.Fatal(err)
	}
	var hours, days, states int64
	if err := m.storeDB.Model(&UsageHourFact{}).Count(&hours).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageDailyFact{}).Count(&days).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&UsageHourIngestState{}).Count(&states).Error; err != nil {
		t.Fatal(err)
	}
	if hours != 0 || days != 1 || states != 1 {
		t.Fatalf("清理留存错误: hours=%d days=%d states=%d", hours, days, states)
	}
}

// TestUsageFactDailyRebuildPreservesLastGoodVersionWhenHoursAreIncomplete
// 防止小时明细被清理或局部丢失后，仍凭 complete 台账把上一版正确的日事实
// 覆盖成残缺数据。缺口修复完成前，页面应继续使用最后一份完整日汇总。
func TestUsageFactDailyRebuildPreservesLastGoodVersionWhenHoursAreIncomplete(t *testing.T) {
	m := newTestMonitor(t)
	day := usageFactTestDay().Unix()
	existing := UsageDailyFact{
		DateTs: day, UserID: 1, ChannelID: 33, Grp: "g", ModelName: "m", TokenID: 10,
		Requests: 99, PromptTokens: 900, CompletionTokens: 90, ConsumeQuota: 9900,
	}
	if err := m.storeDB.Create(&existing).Error; err != nil {
		t.Fatal(err)
	}
	emptyHash := usageFactContentHash(nil)
	for offset := int64(0); offset < 24; offset++ {
		hour := day + offset*usageFactHourSeconds
		state := UsageHourIngestState{
			HourTs: hour, Status: "complete", ContentHash: emptyHash,
			UpdatedAt: hour + usageFactHourSeconds, CompletedAt: hour + usageFactHourSeconds,
		}
		if offset == 5 {
			// 台账期待这一小时有一行，但本地小时事实故意缺失。
			expected := []UsageHourFact{{
				HourTs: hour, DayTs: day, UserID: 1, ChannelID: 33, Grp: "g", ModelName: "m", TokenID: 10,
				Requests: 1, PromptTokens: 10, CompletionTokens: 2, ConsumeQuota: 100,
			}}
			state.Rows = 1
			state.Requests = 1
			state.Tokens = 12
			state.ContentHash = usageFactContentHash(expected)
		}
		if err := m.storeDB.Create(&state).Error; err != nil {
			t.Fatal(err)
		}
	}

	if err := m.rebuildUsageDailyFact(m.storeDB, day, []int64{1}); err != nil {
		t.Fatal(err)
	}
	var got UsageDailyFact
	if err := m.storeDB.First(&got, "date_ts = ? AND user_id = ?", day, 1).Error; err != nil {
		t.Fatal(err)
	}
	if got.Requests != existing.Requests || got.PromptTokens != existing.PromptTokens ||
		got.CompletionTokens != existing.CompletionTokens || got.ConsumeQuota != existing.ConsumeQuota {
		t.Fatalf("小时事实不完整时不应覆盖上一版日事实: got=%+v want=%+v", got, existing)
	}
}

// TestUsageFactsReadKeepsMemberDayAtomicDuringRepair 锁定已发布成员日的可见性。
// repair 会保留上一份日事实，再逐小时重读来源；新出现的维度
// 在整天重建完成前不得混入旧日版本。即使损坏注入刻意删掉了 day proof，
// 保留的旧 daily 也必须屏蔽 partial hour，等待原子重建。
func TestUsageFactsReadKeepsMemberDayAtomicDuringRepair(t *testing.T) {
	m := newTestMonitor(t)
	enableUsageFactsForTest(m)
	day := usageFactTestDay().Unix()
	daily := []UsageDailyFact{{
		DateTs: day, UserID: 1, ChannelID: 10, Grp: "old-group", ModelName: "old-model",
		TokenID: 10, TokenName: "old-token", Requests: 10, PromptTokens: 100,
		CompletionTokens: 20, ConsumeQuota: 1000,
	}}
	if err := m.storeDB.Create(&daily).Error; err != nil {
		t.Fatal(err)
	}
	metrics := dailyFactsMetrics(daily)
	if err := m.storeDB.Create(&UsageFactMemberDayState{
		UserID: 1, DateTs: day, Rows: len(daily), Requests: metrics.Requests,
		Tokens: metrics.tokens(), ContentHash: usageDailyFactContentHash(daily),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UsageHourFact{
		HourTs: day + 5*usageFactHourSeconds, DayTs: day, UserID: 1, ChannelID: 99,
		Grp: "new-group", ModelName: "new-model", TokenID: 99, TokenName: "new-token",
		Requests: 3, PromptTokens: 30, CompletionTokens: 6, ConsumeQuota: 300,
	}).Error; err != nil {
		t.Fatal(err)
	}

	assertRequests := func(want int64) {
		t.Helper()
		got, err := m.computeUsageStatsFromFacts(context.Background(), []int64{1}, day, day+usageFactDaySeconds, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got.Summary.Requests != want {
			t.Fatalf("成员日可见性破坏: requests=%d want=%d daily=%+v", got.Summary.Requests, want, got.Daily)
		}
	}
	assertRequests(10)

	// candidate-gap repair 可能正是因为 proof 缺失而启动。旧日事实存在时
	// 仍不能让已回填的新维度提前进入页面。
	if err := m.storeDB.Where("user_id = ? AND date_ts = ?", 1, day).Delete(&UsageFactMemberDayState{}).Error; err != nil {
		t.Fatal(err)
	}
	assertRequests(10)

	// 真正没有日版本和 proof 的 partial 日才允许从小时事实展示已有数据。
	if err := m.storeDB.Where("user_id = ? AND date_ts = ?", 1, day).Delete(&UsageDailyFact{}).Error; err != nil {
		t.Fatal(err)
	}
	assertRequests(3)

	// 整天完成时，daily + proof 在同一事务一次性切换。小时明细仍在
	// 近期留存表中，但新日版本必须完全屏蔽它，不得双计。
	rebuilt := []UsageDailyFact{{
		DateTs: day, UserID: 1, ChannelID: 99, Grp: "new-group", ModelName: "new-model",
		TokenID: 99, TokenName: "new-token", Requests: 13, PromptTokens: 130,
		CompletionTokens: 26, ConsumeQuota: 1300,
	}}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&rebuilt).Error; err != nil {
			return err
		}
		newMetrics := dailyFactsMetrics(rebuilt)
		return tx.Create(&UsageFactMemberDayState{
			UserID: 1, DateTs: day, Rows: len(rebuilt), Requests: newMetrics.Requests,
			Tokens: newMetrics.tokens(), ContentHash: usageDailyFactContentHash(rebuilt),
		}).Error
	}); err != nil {
		t.Fatal(err)
	}
	assertRequests(13)
}

// TestUsageFactHourRetentionIsIndependentFromHistoricalBackfill 锁定两层留存：
// 日事实可覆盖 366 天，但用于晚到日志复核的小时事实默认只保留 8 天。
func TestUsageFactHourRetentionIsIndependentFromHistoricalBackfill(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.UsageFactsBackfillDays = 366
	m.cfg.UsageFactsHourRetentionDays = 0
	if got := m.usageFactHourRetentionDays(); got != 8 {
		t.Fatalf("366 天回填不应抬高默认小时留存: got=%d want=8", got)
	}
	m.cfg.UsageFactsHourRetentionDays = 999
	if got := m.usageFactHourRetentionDays(); got != 366 {
		t.Fatalf("小时留存不得超过实际回填窗口: got=%d want=366", got)
	}
	m.cfg.UsageFactsBackfillDays = 3
	m.cfg.UsageFactsHourRetentionDays = 8
	if got := m.usageFactHourRetentionDays(); got != 3 {
		t.Fatalf("短回填窗口应收窄小时留存: got=%d want=3", got)
	}
}
