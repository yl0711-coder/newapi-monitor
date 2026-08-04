package monitor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSnapshotCache:TTL 内命中缓存(同一指针)、超 TTL 重算。
func TestSnapshotCache(t *testing.T) {
	m := newTestMonitor(t)
	now := int64(1_700_000_000)
	s1, err := m.GetSnapshot(60, now)
	if err != nil {
		t.Fatal(err)
	}
	if s2, _ := m.GetSnapshot(60, now+5); s1 != s2 {
		t.Error("TTL 内应命中缓存(返回同一快照指针)")
	}
	if s3, _ := m.GetSnapshot(60, now+snapCacheTTL+1); s3 == s1 {
		t.Error("超过 TTL 应重算(新指针)")
	}
}

type memoryByteCacheItem struct {
	value []byte
	exp   time.Time
}

type memoryByteCacheStore struct {
	mu        sync.Mutex
	items     map[string]memoryByteCacheItem
	getErr    error
	setErr    error
	deleteErr error
	blockGet  bool
	gets      int
	sets      int
	deletes   int
}

func newMemoryByteCacheStore() *memoryByteCacheStore {
	return &memoryByteCacheStore{items: make(map[string]memoryByteCacheItem)}
}

func (s *memoryByteCacheStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.blockGet {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if s.getErr != nil {
		return nil, s.getErr
	}
	item, ok := s.items[key]
	if !ok || !time.Now().Before(item.exp) {
		delete(s.items, key)
		return nil, errUsageCacheMiss
	}
	return append([]byte(nil), item.value...), nil
}

func (s *memoryByteCacheStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sets++
	if s.setErr != nil {
		return s.setErr
	}
	s.items[key] = memoryByteCacheItem{value: append([]byte(nil), value...), exp: time.Now().Add(ttl)}
	return nil
}

func (s *memoryByteCacheStore) Delete(ctx context.Context, keys ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	for _, key := range keys {
		delete(s.items, key)
	}
	return nil
}

func (s *memoryByteCacheStore) Close() error { return nil }

func (s *memoryByteCacheStore) putRaw(key string, value []byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = memoryByteCacheItem{value: append([]byte(nil), value...), exp: time.Now().Add(ttl)}
}

func TestUsageRedisOptionsDisableRetriesAndCapConnections(t *testing.T) {
	opts := usageRedisOptions(Settings{UsageRedisAddr: "redis.example:6379"})
	if opts.MaxRetries != -1 {
		t.Fatalf("MaxRetries=%d，必须为 -1 才是真正禁用 go-redis 内部重试", opts.MaxRetries)
	}
	if opts.PoolSize != usageCacheRedisPoolSize || opts.MaxActiveConns != usageCacheRedisPoolSize {
		t.Fatalf("Redis 连接池没有硬限制: pool=%d active=%d want=%d",
			opts.PoolSize, opts.MaxActiveConns, usageCacheRedisPoolSize)
	}
}

func TestUsageCacheStatsExposeOnlyOperationalCounters(t *testing.T) {
	remote := newMemoryByteCacheStore()
	c := newUsageResultCacheForTest(remote, 32, 1<<20)
	var first, second map[string]int
	fill := func() (any, error) { return map[string]int{"requests": 7}, nil }
	if err := c.DoJSON(context.Background(), "customer:secret-filter", time.Minute, &first, fill); err != nil {
		t.Fatal(err)
	}
	if err := c.DoJSON(context.Background(), "customer:secret-filter", time.Minute, &second, fill); err != nil {
		t.Fatal(err)
	}
	stats := c.Stats(time.Now())
	if !stats.RemoteConfigured || stats.Requests != 2 || stats.SourceFills != 1 || stats.LocalHits != 1 || stats.RemoteMisses != 1 || stats.LocalEntries != 1 || stats.LocalBytes <= 0 {
		t.Fatalf("缓存运维计数错误: %+v", stats)
	}
	// 指标结构只能有计数和布尔状态；缓存前缀、键及业务结果均无可导出字段。
	text := fmt.Sprintf("%+v", stats)
	if strings.Contains(text, "customer") || strings.Contains(text, "secret-filter") || strings.Contains(text, c.prefix) {
		t.Fatalf("缓存指标泄漏业务键: %s", text)
	}
}

func TestUsageCacheStatsDoNotReportMissWhenRedisWasNotContacted(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
	var out map[string]int
	if err := c.DoJSON(context.Background(), "local-only", time.Minute, &out, func() (any, error) {
		return map[string]int{"requests": 1}, nil
	}); err != nil {
		t.Fatal(err)
	}
	stats := c.Stats(time.Now())
	if stats.RemoteConfigured || stats.RemoteMisses != 0 || stats.SourceFills != 1 {
		t.Fatalf("未访问 Redis 时不应伪报远端 miss: %+v", stats)
	}
}

func TestUsageResultCacheRemoteHitAcrossInstances(t *testing.T) {
	remote := newMemoryByteCacheStore()
	c1 := newUsageResultCacheForTest(remote, 32, 1<<20)
	c2 := newUsageResultCacheForTest(remote, 32, 1<<20)
	type payload struct {
		Requests int64 `json:"requests"`
	}
	var fills atomic.Int32
	fill := func() (any, error) {
		fills.Add(1)
		return &payload{Requests: 42}, nil
	}
	var first, second payload
	if err := c1.DoJSON(context.Background(), "shared", time.Minute, &first, fill); err != nil {
		t.Fatal(err)
	}
	if err := c2.DoJSON(context.Background(), "shared", time.Minute, &second, fill); err != nil {
		t.Fatal(err)
	}
	if first.Requests != 42 || second.Requests != 42 || fills.Load() != 1 || c2.remoteHits.Load() != 1 {
		t.Fatalf("远端复用失败: first=%+v second=%+v fills=%d remote_hits=%d", first, second, fills.Load(), c2.remoteHits.Load())
	}
}

func TestUsageResultCacheRemoteFailureFallsBack(t *testing.T) {
	remote := newMemoryByteCacheStore()
	remote.getErr = errors.New("redis unavailable")
	c := newUsageResultCacheForTest(remote, 32, 1<<20)
	var fills atomic.Int32
	fill := func() (any, error) {
		fills.Add(1)
		return map[string]int{"requests": 7}, nil
	}
	for i := 0; i < 2; i++ {
		var out map[string]int
		if err := c.DoJSON(context.Background(), "fallback", time.Minute, &out, fill); err != nil || out["requests"] != 7 {
			t.Fatalf("Redis 故障不应影响业务: out=%v err=%v", out, err)
		}
	}
	if fills.Load() != 1 || c.remoteErrors.Load() != 1 || c.localHits.Load() == 0 {
		t.Fatalf("应由本机短缓存承接降级: fills=%d remote_errors=%d local_hits=%d", fills.Load(), c.remoteErrors.Load(), c.localHits.Load())
	}
}

func TestUsageResultCacheUnavailableKeepsOnlineBaselineLocalTTL(t *testing.T) {
	remote := newMemoryByteCacheStore()
	remote.getErr = errors.New("redis unavailable")
	c := newUsageResultCacheForTest(remote, usageCacheLocalMaxEntries, usageCacheLocalMaxBytes)
	var out string
	if err := c.DoJSON(context.Background(), "baseline-ttl", usageAggregateHistoricalTTL, &out, func() (any, error) {
		return "source-result", nil
	}); err != nil || out != "source-result" {
		t.Fatalf("Redis 故障时应正常回源: value=%q err=%v", out, err)
	}

	// 旧版线上 Portal 的本机缓存是 60 秒；新版故障降级不得低于这个口径。
	now := time.Now()
	if _, ok := c.local.Get(c.fullKey("baseline-ttl"), now.Add(59*time.Second)); !ok {
		t.Fatal("Redis 故障时本机结果应至少保留 60 秒")
	}
	if _, ok := c.local.Get(c.fullKey("baseline-ttl"), now.Add(61*time.Second)); ok {
		t.Fatal("本机缓存不得超过 60 秒上限")
	}
}

func TestUsageResultCacheUnavailableBackoffAvoidsRepeatedRemoteWaits(t *testing.T) {
	remote := newMemoryByteCacheStore()
	remote.getErr = errors.New("redis unavailable")
	c := newUsageResultCacheForTest(remote, usageCacheLocalMaxEntries, usageCacheLocalMaxBytes)

	for _, key := range []string{"first", "second"} {
		var out string
		if err := c.DoJSON(context.Background(), key, time.Minute, &out, func() (any, error) {
			return key, nil
		}); err != nil || out != key {
			t.Fatalf("退避期内应正常回源: key=%s value=%q err=%v", key, out, err)
		}
	}
	remote.mu.Lock()
	getsDuringFailure := remote.gets
	remote.mu.Unlock()
	if getsDuringFailure != 1 {
		t.Fatalf("全局退避期内不得让不同键重复等待 Redis: gets=%d", getsDuringFailure)
	}
	now := time.Now()
	if stats := c.Stats(now.Add(29 * time.Second)); !stats.RemoteBackoffActive {
		t.Fatalf("30 秒退避应仍生效: %+v", stats)
	}
	if stats := c.Stats(now.Add(31 * time.Second)); stats.RemoteBackoffActive {
		t.Fatalf("30 秒后应允许自动恢复探测: %+v", stats)
	}

	// 不等真实 30 秒：把退避时钟推到过去，验证下一个新键会重新接入已恢复的 Redis。
	remote.mu.Lock()
	remote.getErr = nil
	remote.mu.Unlock()
	c.remoteBackoffUntil.Store(time.Now().Add(-time.Second).UnixNano())
	var recovered string
	if err := c.DoJSON(context.Background(), "recovered", time.Minute, &recovered, func() (any, error) {
		return "redis-is-back", nil
	}); err != nil || recovered != "redis-is-back" {
		t.Fatalf("Redis 恢复后应自动重新接入: value=%q err=%v", recovered, err)
	}
	remote.mu.Lock()
	getsAfterRecovery, setsAfterRecovery := remote.gets, remote.sets
	remote.mu.Unlock()
	if getsAfterRecovery != 2 || setsAfterRecovery != 1 {
		t.Fatalf("Redis 恢复后未重新读写: gets=%d sets=%d", getsAfterRecovery, setsAfterRecovery)
	}
}

func TestUsageResultCacheProductionLocalCapacityIsBounded(t *testing.T) {
	c := newUsageResultCache(Settings{})
	defer c.Close()
	now := time.Now()
	for i := 0; i < usageCacheLocalMaxEntries+1; i++ {
		c.local.Put(c.fullKey(fmt.Sprintf("capacity-%03d", i)), []byte(`1`), usageCacheLocalTTL, now)
	}
	entries, bytes := c.local.size()
	if entries != usageCacheLocalMaxEntries || bytes > usageCacheLocalMaxBytes {
		t.Fatalf("生产本机缓存上限失效: entries=%d/%d bytes=%d/%d",
			entries, usageCacheLocalMaxEntries, bytes, usageCacheLocalMaxBytes)
	}
	if _, ok := c.local.Get(c.fullKey("capacity-000"), now); ok {
		t.Fatal("超过条目上限时应淘汰最旧项")
	}
	if _, ok := c.local.Get(c.fullKey(fmt.Sprintf("capacity-%03d", usageCacheLocalMaxEntries)), now); !ok {
		t.Fatal("超过条目上限时不得误淘汰最新项")
	}
}

func TestUsageResultCacheRemoteTimeoutFallsBackQuickly(t *testing.T) {
	remote := newMemoryByteCacheStore()
	remote.blockGet = true
	c := newUsageResultCacheForTest(remote, 32, 1<<20)
	start := time.Now()
	var out string
	if err := c.DoJSON(context.Background(), "timeout", time.Minute, &out, func() (any, error) { return "ok", nil }); err != nil {
		t.Fatal(err)
	}
	if out != "ok" || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("远端超时应在 %s 左右降级: out=%q elapsed=%s", usageCacheRemoteTimeout, out, time.Since(start))
	}
}

func TestUsageResultCacheCorruptRemoteRebuilds(t *testing.T) {
	remote := newMemoryByteCacheStore()
	c := newUsageResultCacheForTest(remote, 32, 1<<20)
	remote.putRaw(c.fullKey("corrupt"), []byte(`{"requests":`), time.Minute)
	var fills atomic.Int32
	var out struct {
		Requests int `json:"requests"`
	}
	err := c.DoJSON(context.Background(), "corrupt", time.Minute, &out, func() (any, error) {
		fills.Add(1)
		return map[string]int{"requests": 9}, nil
	})
	if err != nil || out.Requests != 9 || fills.Load() != 1 || remote.deletes != 1 {
		t.Fatalf("损坏缓存应精确删除并重建: out=%+v fills=%d deletes=%d err=%v", out, fills.Load(), remote.deletes, err)
	}
}

func TestUsageResultCacheCanceledRequestDoesNotFill(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	var out string
	err := c.DoJSON(ctx, "canceled", time.Minute, &out, func() (any, error) {
		called = true
		return "wrong", nil
	})
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("已取消请求不应访问缓存或源库: called=%v err=%v", called, err)
	}
}

func TestUsageResultCacheDoesNotRetainOversizedPayload(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, usageCacheMaxPayloadBytes*2)
	var fills atomic.Int32
	fill := func() (any, error) {
		fills.Add(1)
		return strings.Repeat("x", usageCacheMaxPayloadBytes+1), nil
	}
	for i := 0; i < 2; i++ {
		var out string
		if err := c.DoJSON(context.Background(), "large", time.Minute, &out, fill); err != nil {
			t.Fatal(err)
		}
	}
	if fills.Load() != 2 {
		t.Fatalf("超过单项上限的结果不应驻留内存: fills=%d", fills.Load())
	}
}

func TestUsageResultCacheTTLExpires(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
	var fills atomic.Int32
	fill := func() (any, error) {
		return fills.Add(1), nil
	}
	var first, second int32
	if err := c.DoJSON(context.Background(), "short-ttl", 15*time.Millisecond, &first, fill); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := c.DoJSON(context.Background(), "short-ttl", 15*time.Millisecond, &second, fill); err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 2 || fills.Load() != 2 {
		t.Fatalf("TTL 后必须重新计算: first=%d second=%d fills=%d", first, second, fills.Load())
	}
}

func TestUsageAggregateTTLPolicy(t *testing.T) {
	now := time.Date(2026, 8, 3, 8, 30, 0, 0, usageCST)
	todayStart := time.Date(2026, 8, 3, 0, 0, 0, 0, usageCST).Unix()
	tests := []struct {
		name string
		toTs int64
		want time.Duration
	}{
		{name: "历史区间在今天零点闭合", toTs: todayStart, want: usageAggregateHistoricalTTL},
		{name: "包含今天", toTs: todayStart + 86400, want: usageAggregateLiveTTL},
		{name: "本周等未来上界仍属活跃区间", toTs: todayStart + 4*86400, want: usageAggregateLiveTTL},
		{name: "更早历史", toTs: todayStart - 86400, want: usageAggregateHistoricalTTL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := usageAggregateTTL(tt.toTs, now); got != tt.want {
				t.Fatalf("TTL=%s want=%s", got, tt.want)
			}
		})
	}
}

func TestUsageResultCacheRemoteRemainingTTLDoesNotExtendInL1(t *testing.T) {
	remote := newMemoryByteCacheStore()
	c := newUsageResultCacheForTest(remote, 32, 1<<20)
	expiresAt := time.Now().Add(60 * time.Millisecond)
	record, err := encodeUsageCacheRecord([]byte(`41`), expiresAt)
	if err != nil {
		t.Fatal(err)
	}
	// 故意让替身 Redis 键比记录中的绝对过期时间活得更久，
	// 验证 L1 不会把这个值另外延长 usageCacheLocalTTL。
	remote.putRaw(c.fullKey("remaining-ttl"), record, time.Minute)
	var first int
	if err := c.DoJSON(context.Background(), "remaining-ttl", time.Minute, &first, func() (any, error) {
		return 99, nil
	}); err != nil || first != 41 {
		t.Fatalf("首次应命中远端记录: value=%d err=%v", first, err)
	}

	time.Sleep(100 * time.Millisecond)
	var fills atomic.Int32
	var second int
	if err := c.DoJSON(context.Background(), "remaining-ttl", time.Minute, &second, func() (any, error) {
		fills.Add(1)
		return 42, nil
	}); err != nil {
		t.Fatal(err)
	}
	if second != 42 || fills.Load() != 1 {
		t.Fatalf("远端绝对过期后必须重算，不得被 L1 延长: value=%d fills=%d", second, fills.Load())
	}
}

func TestUsageResultCacheFreshBypassesAndReplacesExistingValue(t *testing.T) {
	c := newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)
	var source atomic.Int32
	source.Store(1)
	fill := func() (any, error) { return source.Load(), nil }

	var first, cached, refreshed, after int32
	if err := c.DoJSON(context.Background(), "fresh", time.Minute, &first, fill); err != nil {
		t.Fatal(err)
	}
	source.Store(2)
	if err := c.DoJSON(context.Background(), "fresh", time.Minute, &cached, fill); err != nil {
		t.Fatal(err)
	}
	if err := c.DoJSONFresh(context.Background(), "fresh", time.Minute, &refreshed, fill); err != nil {
		t.Fatal(err)
	}
	if err := c.DoJSON(context.Background(), "fresh", time.Minute, &after, fill); err != nil {
		t.Fatal(err)
	}
	if first != 1 || cached != 1 || refreshed != 2 || after != 2 {
		t.Fatalf("普通命中/主动刷新语义错误: first=%d cached=%d refreshed=%d after=%d", first, cached, refreshed, after)
	}
}

func TestUsageResultCacheFailedRemoteRefreshCannotFallBackToOldValue(t *testing.T) {
	remote := newMemoryByteCacheStore()
	c := newUsageResultCacheForTest(remote, 32, 1<<20)
	var warm string
	if err := c.DoJSON(context.Background(), "refresh-write-failure", time.Minute, &warm, func() (any, error) {
		return "old-remote", nil
	}); err != nil {
		t.Fatal(err)
	}

	remote.setErr = errors.New("redis set unavailable")
	c.remoteBackoffUntil.Store(0)
	var refreshed string
	if err := c.DoJSONFresh(context.Background(), "refresh-write-failure", time.Minute, &refreshed, func() (any, error) {
		return "fresh-result", nil
	}); err != nil || refreshed != "fresh-result" {
		t.Fatalf("刷新本身应成功并返回源结果: value=%q err=%v", refreshed, err)
	}

	// 模拟 L1 到期和 Redis 恢复。远端仍是刷新前的旧值，但逐键 bypass 必须让
	// 本次回源；新值成功写入后，下一台实例也应命中它。
	c.local.Delete(c.fullKey("refresh-write-failure"))
	remote.setErr = nil
	c.remoteBackoffUntil.Store(0)
	var fills atomic.Int32
	var after string
	if err := c.DoJSON(context.Background(), "refresh-write-failure", time.Minute, &after, func() (any, error) {
		fills.Add(1)
		return "new-source", nil
	}); err != nil {
		t.Fatal(err)
	}
	if after != "new-source" || fills.Load() != 1 {
		t.Fatalf("Redis 恢复后不得倒退到旧值: value=%q fills=%d", after, fills.Load())
	}

	c2 := newUsageResultCacheForTest(remote, 32, 1<<20)
	var shared string
	if err := c2.DoJSON(context.Background(), "refresh-write-failure", time.Minute, &shared, func() (any, error) {
		return "unexpected-refill", nil
	}); err != nil || shared != "new-source" || c2.remoteHits.Load() != 1 {
		t.Fatalf("恢复后的新值未正确共享: value=%q hits=%d err=%v", shared, c2.remoteHits.Load(), err)
	}
}

func TestUsageResultCacheConcurrentFreshCannotBeOverwrittenByOlderFill(t *testing.T) {
	c := newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)
	normalStarted := make(chan struct{})
	releaseNormal := make(chan struct{})
	normalDone := make(chan error, 1)
	var normalValue string
	go func() {
		normalDone <- c.DoJSON(context.Background(), "fresh-race", time.Minute, &normalValue, func() (any, error) {
			close(normalStarted)
			<-releaseNormal
			return "older-normal", nil
		})
	}()
	<-normalStarted

	freshDone := make(chan error, 1)
	var freshValue string
	go func() {
		freshDone <- c.DoJSONFresh(context.Background(), "fresh-race", time.Minute, &freshValue, func() (any, error) {
			return "newer-refresh", nil
		})
	}()

	// 旧实现允许刷新先写入、随后又被更早开始的普通查询覆盖；修复后刷新会等
	// 普通查询交接完再重算。无论刷新是否已经返回，都在这里放行旧查询。
	var earlyFreshErr error
	freshReturnedEarly := false
	select {
	case earlyFreshErr = <-freshDone:
		freshReturnedEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseNormal)
	if err := <-normalDone; err != nil {
		t.Fatal(err)
	}
	if !freshReturnedEarly {
		earlyFreshErr = <-freshDone
	}
	if earlyFreshErr != nil {
		t.Fatal(earlyFreshErr)
	}
	if normalValue != "older-normal" || freshValue != "newer-refresh" {
		t.Fatalf("并发请求自身结果错误: normal=%q fresh=%q", normalValue, freshValue)
	}

	var final string
	if err := c.DoJSON(context.Background(), "fresh-race", time.Minute, &final, func() (any, error) {
		return "unexpected-refill", nil
	}); err != nil {
		t.Fatal(err)
	}
	if final != "newer-refresh" {
		t.Fatalf("主动刷新完成后不得被更早开始的普通查询覆盖: final=%q", final)
	}
}

func TestUsageResultCacheNormalWaitsForFreshAndReusesIt(t *testing.T) {
	c := newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)
	freshStarted := make(chan struct{})
	releaseFresh := make(chan struct{})
	freshDone := make(chan error, 1)
	var freshValue string
	go func() {
		freshDone <- c.DoJSONFresh(context.Background(), "fresh-first", time.Minute, &freshValue, func() (any, error) {
			close(freshStarted)
			<-releaseFresh
			return "fresh-value", nil
		})
	}()
	<-freshStarted

	var normalFillCalls atomic.Int32
	normalDone := make(chan error, 1)
	var normalValue string
	go func() {
		normalDone <- c.DoJSON(context.Background(), "fresh-first", time.Minute, &normalValue, func() (any, error) {
			normalFillCalls.Add(1)
			return "stale-source", nil
		})
	}()
	close(releaseFresh)
	if err := <-freshDone; err != nil {
		t.Fatal(err)
	}
	if err := <-normalDone; err != nil {
		t.Fatal(err)
	}
	if freshValue != "fresh-value" || normalValue != "fresh-value" || normalFillCalls.Load() != 0 {
		t.Fatalf("刷新先完成时普通请求应复用刷新结果: fresh=%q normal=%q normal_fills=%d",
			freshValue, normalValue, normalFillCalls.Load())
	}
}

func TestUsageResultCacheKeyGateWaitIsCancelable(t *testing.T) {
	c := newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)
	normalStarted := make(chan struct{})
	releaseNormal := make(chan struct{})
	normalDone := make(chan error, 1)
	go func() {
		var out string
		normalDone <- c.DoJSON(context.Background(), "cancel-gate", time.Minute, &out, func() (any, error) {
			close(normalStarted)
			<-releaseNormal
			return "normal", nil
		})
	}()
	<-normalStarted

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var fillCalled atomic.Bool
	var out string
	waitDone := make(chan error, 1)
	start := time.Now()
	go func() {
		waitDone <- c.DoJSONFresh(ctx, "cancel-gate", time.Minute, &out, func() (any, error) {
			fillCalled.Store(true)
			return "wrong", nil
		})
	}()
	deadline := time.Now().Add(time.Second)
	for {
		c.fillGate.mu.Lock()
		gate := c.fillGate.gates[c.fullKey("cancel-gate")]
		waiting := gate != nil && gate.refs == 2
		c.fillGate.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("刷新请求未进入同键等待闸门")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	err := <-waitDone
	if !errors.Is(err, context.Canceled) || fillCalled.Load() || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("已取消的等待者必须快速退出且不执行源查询: err=%v fill=%v elapsed=%s",
			err, fillCalled.Load(), time.Since(start))
	}
	close(releaseNormal)
	if err := <-normalDone; err != nil {
		t.Fatal(err)
	}
}

func TestUsageResultCacheFreshFailureIsNotCached(t *testing.T) {
	c := newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)
	var warm string
	if err := c.DoJSON(context.Background(), "fresh-error", time.Minute, &warm, func() (any, error) {
		return "last-good", nil
	}); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("source unavailable")
	var failed string
	if err := c.DoJSONFresh(context.Background(), "fresh-error", time.Minute, &failed, func() (any, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("刷新失败应返回源错误: %v", err)
	}
	var after string
	if err := c.DoJSON(context.Background(), "fresh-error", time.Minute, &after, func() (any, error) {
		return "wrong", nil
	}); err != nil {
		t.Fatal(err)
	}
	if after != "last-good" {
		t.Fatalf("失败结果不得覆盖上一个成功缓存: %q", after)
	}
}

func TestUsageResultCacheOversizedFreshResultRemovesOldValue(t *testing.T) {
	c := newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, usageCacheMaxPayloadBytes*2)
	var warm string
	if err := c.DoJSON(context.Background(), "large-refresh", time.Minute, &warm, func() (any, error) {
		return "old", nil
	}); err != nil {
		t.Fatal(err)
	}
	largeValue := strings.Repeat("x", usageCacheMaxPayloadBytes+1)
	var refreshed string
	if err := c.DoJSONFresh(context.Background(), "large-refresh", time.Minute, &refreshed, func() (any, error) {
		return largeValue, nil
	}); err != nil || refreshed != largeValue {
		t.Fatalf("超大刷新结果仍应正常返回: len=%d err=%v", len(refreshed), err)
	}
	var calls atomic.Int32
	var after string
	if err := c.DoJSON(context.Background(), "large-refresh", time.Minute, &after, func() (any, error) {
		calls.Add(1)
		return "new-source", nil
	}); err != nil {
		t.Fatal(err)
	}
	if after != "new-source" || calls.Load() != 1 {
		t.Fatalf("超大刷新后不得回退到旧缓存: value=%q calls=%d", after, calls.Load())
	}
}

func TestUsageResultCacheFailedDeleteCannotRestoreOldRemoteValue(t *testing.T) {
	remote := newMemoryByteCacheStore()
	c := newUsageResultCacheForTest(remote, 32, usageCacheMaxPayloadBytes*2)
	var warm string
	if err := c.DoJSON(context.Background(), "large-delete-failure", time.Minute, &warm, func() (any, error) {
		return "old-remote", nil
	}); err != nil {
		t.Fatal(err)
	}

	remote.deleteErr = errors.New("redis delete unavailable")
	c.remoteBackoffUntil.Store(0)
	largeValue := strings.Repeat("x", usageCacheMaxPayloadBytes+1)
	var refreshed string
	if err := c.DoJSONFresh(context.Background(), "large-delete-failure", time.Minute, &refreshed, func() (any, error) {
		return largeValue, nil
	}); err != nil || refreshed != largeValue {
		t.Fatalf("超大刷新结果仍应正常返回: len=%d err=%v", len(refreshed), err)
	}

	remote.deleteErr = nil
	c.remoteBackoffUntil.Store(0)
	var fills atomic.Int32
	var after string
	if err := c.DoJSON(context.Background(), "large-delete-failure", time.Minute, &after, func() (any, error) {
		fills.Add(1)
		return "new-source", nil
	}); err != nil {
		t.Fatal(err)
	}
	if after != "new-source" || fills.Load() != 1 {
		t.Fatalf("删除失败恢复后不得读回旧 Redis 值: value=%q fills=%d", after, fills.Load())
	}
}

func TestUsageResultCacheDeleteWaitsForInFlightFill(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
	fillStarted := make(chan struct{})
	releaseFill := make(chan struct{})
	fillDone := make(chan error, 1)
	go func() {
		var out string
		fillDone <- c.DoJSON(context.Background(), "delete-race", time.Minute, &out, func() (any, error) {
			close(fillStarted)
			<-releaseFill
			return "in-flight-value", nil
		})
	}()
	<-fillStarted

	deleteDone := make(chan struct{})
	go func() {
		c.Delete(context.Background(), "delete-race")
		close(deleteDone)
	}()
	select {
	case <-deleteDone:
		t.Fatal("删除不得越过仍在执行的同键填充")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseFill)
	if err := <-fillDone; err != nil {
		t.Fatal(err)
	}
	<-deleteDone

	var fills atomic.Int32
	var after string
	if err := c.DoJSON(context.Background(), "delete-race", time.Minute, &after, func() (any, error) {
		fills.Add(1)
		return "after-delete", nil
	}); err != nil {
		t.Fatal(err)
	}
	if after != "after-delete" || fills.Load() != 1 {
		t.Fatalf("删除结束后不得残留并发填充值: value=%q fills=%d", after, fills.Load())
	}
}

func TestBoundedByteCacheEnforcesEntryAndByteLimits(t *testing.T) {
	c := newBoundedByteCache(2, 6)
	now := time.Now()
	c.Put("a", []byte("111"), time.Minute, now)
	c.Put("b", []byte("22"), time.Minute, now)
	c.Put("c", []byte("333"), time.Minute, now)
	entries, bytes := c.size()
	if entries > 2 || bytes > 6 {
		t.Fatalf("本机缓存越过硬上限: entries=%d bytes=%d", entries, bytes)
	}
	if _, ok := c.Get("a", now); ok {
		t.Fatal("最旧项应被 LRU 淘汰")
	}
}

// 本机 Docker Redis 集成测试。默认跳过；验收时显式传入独立测试 Redis，
// 使用随机键且只精确删除自己的键，绝不 FLUSH 数据库。
func TestUsageResultCacheRedisIntegration(t *testing.T) {
	addr := os.Getenv("MONITOR_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("MONITOR_TEST_REDIS_ADDR 未设置")
	}
	s := Settings{
		UsageRedisAddr:     addr,
		UsageRedisUsername: os.Getenv("MONITOR_TEST_REDIS_USERNAME"),
		UsageRedisPassword: os.Getenv("MONITOR_TEST_REDIS_PASSWORD"),
		UsageRedisPrefix:   "nxmon:test:integration:" + time.Now().Format("20060102150405.000000000"),
	}
	c1 := newUsageResultCache(s)
	defer c1.Close()
	c2 := newUsageResultCache(s)
	defer c2.Close()

	key := "roundtrip"
	defer c1.Delete(context.Background(), key)
	var fills atomic.Int32
	fill := func() (any, error) {
		fills.Add(1)
		return map[string]int64{"requests": 123}, nil
	}
	var first, second map[string]int64
	if err := c1.DoJSON(context.Background(), key, 5*time.Second, &first, fill); err != nil {
		t.Fatal(err)
	}
	if err := c2.DoJSON(context.Background(), key, 5*time.Second, &second, fill); err != nil {
		t.Fatal(err)
	}
	if first["requests"] != 123 || second["requests"] != 123 || fills.Load() != 1 || c2.remoteHits.Load() != 1 {
		t.Fatalf("真实 Redis 往返失败: first=%v second=%v fills=%d hits=%d", first, second, fills.Load(), c2.remoteHits.Load())
	}

	// 错误凭据也只能让缓存降级，不能让页面失败。
	badSettings := s
	badSettings.UsageRedisPassword = "intentionally-wrong-password"
	badSettings.UsageRedisPrefix += ":bad-auth"
	bad := newUsageResultCache(badSettings)
	bad.logRemoteErrors = false
	defer bad.Close()
	start := time.Now()
	var fallback string
	if err := bad.DoJSON(context.Background(), "fallback", time.Minute, &fallback, func() (any, error) { return "ok", nil }); err != nil {
		t.Fatalf("Redis 鉴权失败不应传给业务: %v", err)
	}
	if fallback != "ok" || bad.remoteErrors.Load() == 0 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("错误 Redis 凭据应快速降级: value=%q errors=%d elapsed=%s", fallback, bad.remoteErrors.Load(), time.Since(start))
	}
}

func TestUsageResultCacheRedisUnavailableIntegration(t *testing.T) {
	addr := os.Getenv("MONITOR_TEST_REDIS_UNAVAILABLE_ADDR")
	if addr == "" {
		t.Skip("MONITOR_TEST_REDIS_UNAVAILABLE_ADDR 未设置")
	}
	c := newUsageResultCache(Settings{UsageRedisAddr: addr, UsageRedisPrefix: "nxmon:test:unavailable"})
	c.logRemoteErrors = false
	defer c.Close()
	start := time.Now()
	var out string
	if err := c.DoJSON(context.Background(), "fallback", time.Minute, &out, func() (any, error) { return "source-result", nil }); err != nil {
		t.Fatal(err)
	}
	if out != "source-result" || c.remoteErrors.Load() == 0 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("Redis 断连应快速回源: value=%q errors=%d elapsed=%s", out, c.remoteErrors.Load(), time.Since(start))
	}
}
