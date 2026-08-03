package monitor

// cache.go:管理端与客户端用量聚合结果缓存。
//
// 设计边界:
//   - Redis 是可选的主缓存，只保存可重新计算的 JSON 聚合结果，不参与鉴权；
//   - 本机只保留最多 10 秒、32 项/8 MiB 的应急缓存，且绝不超过 Redis 记录的绝对过期时间；
//   - 同键并发由进程内 singleflight 合并，等待者取消不会影响正在执行的请求；
//   - Redis 超时、断连、鉴权失败一律自动降级，绝不能让业务接口因缓存返回 500；
//   - 所有远端键都带 TTL，删除只用精确键，禁止 KEYS/SCAN。

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	redis "github.com/redis/go-redis/v9"
)

const (
	usageCacheLocalTTL        = 10 * time.Second
	usageCacheLocalMaxEntries = 32
	usageCacheLocalMaxBytes   = 8 << 20 // 8 MiB，Monitor 256 MiB 容器内的硬上限
	usageCacheMaxPayloadBytes = 2 << 20 // 单个结果过大时直接返回但不缓存
	usageCacheRemoteTimeout   = 150 * time.Millisecond
	usageCacheRemoteBackoff   = 5 * time.Second
	usageCacheWarnInterval    = time.Minute
	usageCacheRedisPoolSize   = 8

	// 包含今天的报表是一分钟级准实时；已结束的历史日期基本不再变化，
	// 用更长 TTL 减少重复扫描。管理端主动重选日期时会绕过旧缓存。
	usageAggregateLiveTTL       = time.Minute
	usageAggregateHistoricalTTL = 10 * time.Minute
	usageAggregateKeyVersion    = "v2"
	usageCacheRecordVersion     = 1
)

var errUsageCacheMiss = errors.New("usage cache miss")

// byteCacheStore 是 Redis 的最小能力面。测试用内存替身实现同一接口，生产实现见下方。
type byteCacheStore interface {
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte, time.Duration) error
	Delete(context.Context, ...string) error
	Close() error
}

type redisByteCacheStore struct {
	client *redis.Client
}

func newRedisByteCacheStore(s Settings) *redisByteCacheStore {
	return &redisByteCacheStore{client: redis.NewClient(&redis.Options{
		Addr:         s.UsageRedisAddr,
		Username:     s.UsageRedisUsername,
		Password:     s.UsageRedisPassword,
		DB:           s.UsageRedisDB,
		DialTimeout:  usageCacheRemoteTimeout,
		ReadTimeout:  usageCacheRemoteTimeout,
		WriteTimeout: usageCacheRemoteTimeout,
		PoolSize:     usageCacheRedisPoolSize,
		MaxRetries:   0, // 业务层会快速降级，避免客户端内部重试把请求拖长
	})}
}

func (s *redisByteCacheStore) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, errUsageCacheMiss
	}
	return b, err
}

func (s *redisByteCacheStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.client.Set(ctx, key, value, ttl).Err()
}

func (s *redisByteCacheStore) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return s.client.Del(ctx, keys...).Err()
}

func (s *redisByteCacheStore) Close() error { return s.client.Close() }

type usageResultCache struct {
	prefix   string
	remote   byteCacheStore
	local    *boundedByteCache
	flight   cacheFlightGroup
	fillGate cacheKeyGateGroup

	remoteBackoffUntil atomic.Int64
	lastRemoteWarn     atomic.Int64
	remoteHits         atomic.Uint64
	remoteMisses       atomic.Uint64
	localHits          atomic.Uint64
	fills              atomic.Uint64
	remoteErrors       atomic.Uint64
	logRemoteErrors    bool
}

// usageCacheRecord 把绝对过期时间和业务 JSON 一起存入 Redis。
// Redis 命中后，本机 L1 只使用“剩余 TTL”，避免 60s 的远端结果被 L1
// 再延长 10s。它是内部格式，不会返回给前端。
type usageCacheRecord struct {
	Version           int             `json:"v"`
	ExpiresAtUnixNano int64           `json:"expires_at_unix_nano"`
	Payload           json.RawMessage `json:"payload"`
}

func newUsageResultCache(s Settings) *usageResultCache {
	prefix := strings.Trim(strings.TrimSpace(s.UsageRedisPrefix), ":")
	if prefix == "" {
		prefix = "nxmon:usage:v1"
	}
	c := &usageResultCache{
		prefix:          prefix,
		local:           newBoundedByteCache(usageCacheLocalMaxEntries, usageCacheLocalMaxBytes),
		logRemoteErrors: true,
	}
	if strings.TrimSpace(s.UsageRedisAddr) != "" {
		c.remote = newRedisByteCacheStore(s)
		slog.Info("用量 Redis 缓存已配置", "addr", s.UsageRedisAddr, "db", s.UsageRedisDB, "prefix", prefix)
	}
	return c
}

// newUsageResultCacheForTest 注入远端替身，并允许缩小本地上限以验证淘汰行为。
func newUsageResultCacheForTest(remote byteCacheStore, maxEntries, maxBytes int) *usageResultCache {
	return &usageResultCache{
		prefix: "nxmon:test:v1",
		remote: remote,
		local:  newBoundedByteCache(maxEntries, maxBytes),
	}
}

func (c *usageResultCache) Close() {
	if c != nil && c.remote != nil {
		if err := c.remote.Close(); err != nil {
			slog.Warn("关闭用量 Redis 缓存连接失败", "err", err)
		}
	}
}

func (c *usageResultCache) fullKey(key string) string {
	return c.prefix + ":" + strings.TrimLeft(key, ":")
}

// DoJSON 返回 dst 指向的具体类型。fill 的结果先序列化再解码，即使第一个请求也不会
// 把可变对象直接塞进共享缓存，避免后续 handler 修改切片时污染其他请求。
func (c *usageResultCache) DoJSON(
	ctx context.Context,
	key string,
	ttl time.Duration,
	dst any,
	fill func() (any, error),
) error {
	return c.doJSON(ctx, key, ttl, false, dst, fill)
}

// DoJSONFresh 用于用户明确发起的刷新：不读旧 L1/Redis，重新计算成功后
// 覆盖两级缓存。同键并发刷新仍合并为一次 fill；fill 失败不缓存错误。
func (c *usageResultCache) DoJSONFresh(
	ctx context.Context,
	key string,
	ttl time.Duration,
	dst any,
	fill func() (any, error),
) error {
	return c.doJSON(ctx, key, ttl, true, dst, fill)
}

func (c *usageResultCache) doJSON(
	ctx context.Context,
	key string,
	ttl time.Duration,
	forceFresh bool,
	dst any,
	fill func() (any, error),
) error {
	if err := validateJSONDestination(dst); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil {
		value, err := fill()
		if err != nil {
			return err
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return decodeJSON(data, dst)
	}

	fullKey := c.fullKey(key)
	for attempt := 0; attempt < 2; attempt++ {
		data, err := c.loadOrFill(ctx, fullKey, ttl, forceFresh || attempt > 0, fill)
		if err != nil {
			return err
		}
		if err := decodeJSON(data, dst); err == nil {
			return nil
		} else if attempt == 1 {
			return fmt.Errorf("用量缓存结果解码失败: %w", err)
		}
		// 远端出现截断/旧版本/异常内容时精确删除并重建；不把缓存损坏暴露给页面。
		c.Delete(ctx, key)
	}
	return errors.New("用量缓存结果解码失败")
}

func validateJSONDestination(dst any) error {
	rv := reflect.ValueOf(dst)
	if !rv.IsValid() || rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("用量缓存目标必须是非空指针")
	}
	return nil
}

// decodeJSON 先解码到同类型临时值，成功后再整体替换 dst，避免失败的 JSON 留下半截字段。
func decodeJSON(data []byte, dst any) error {
	rv := reflect.ValueOf(dst)
	tmp := reflect.New(rv.Elem().Type())
	if err := json.Unmarshal(data, tmp.Interface()); err != nil {
		return err
	}
	rv.Elem().Set(tmp.Elem())
	return nil
}

func (c *usageResultCache) loadOrFill(
	ctx context.Context,
	fullKey string,
	ttl time.Duration,
	skipExisting bool,
	fill func() (any, error),
) ([]byte, error) {
	if !skipExisting {
		if data, ok := c.local.Get(fullKey, time.Now()); ok {
			c.localHits.Add(1)
			return data, nil
		}
	}

	flightKey := fullKey
	if skipExisting {
		// 刷新请求只与其他刷新合并，不能跟一个正在读旧 Redis 值的普通请求合并。
		flightKey += "\x00fresh"
	}
	return c.flight.Do(ctx, flightKey, func() ([]byte, error) {
		// 普通冷查询与主动刷新不能并行回写同一个业务 key。否则较早开始、较晚结束的
		// 普通查询可能在刷新成功后把旧结果覆盖回来。该闸门只串行同 key，等待可取消；
		// 不同用户/范围/口径仍可并行，普通同键请求继续由上层 singleflight 合并。
		release, err := c.fillGate.Acquire(ctx, fullKey)
		if err != nil {
			return nil, err
		}
		defer release()

		// 排队期间其他请求可能刚写完，leader 取得执行权后必须再检查一次。
		if !skipExisting {
			if data, ok := c.local.Get(fullKey, time.Now()); ok {
				c.localHits.Add(1)
				return data, nil
			}
			if data, remaining, ok, err := c.remoteGet(ctx, fullKey); err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				// Redis 错误只意味着本次走源查询，不影响接口。
			} else if ok {
				c.remoteHits.Add(1)
				c.local.Put(fullKey, data, minDuration(remaining, usageCacheLocalTTL), time.Now())
				return data, nil
			} else {
				c.remoteMisses.Add(1)
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		value, err := fill()
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("序列化用量缓存失败: %w", err)
		}
		c.fills.Add(1)
		if len(data) <= usageCacheMaxPayloadBytes {
			now := time.Now()
			expiresAt := now.Add(ttl)
			c.local.Put(fullKey, data, minDuration(expiresAt.Sub(now), usageCacheLocalTTL), now)
			c.remoteSet(ctx, fullKey, data, expiresAt) // 失败自动降级，不覆盖业务结果
		} else if skipExisting {
			// 主动刷新已得到一份超过缓存上限的新结果：正常返回本次结果，
			// 但必须删除旧小结果，否则下一次普通请求会倒退到刷新前。
			c.Delete(ctx, strings.TrimPrefix(fullKey, c.prefix+":"))
		}
		return data, nil
	})
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (c *usageResultCache) remoteAllowed(now time.Time) bool {
	return c.remote != nil && now.UnixNano() >= c.remoteBackoffUntil.Load()
}

func (c *usageResultCache) remoteGet(ctx context.Context, key string) ([]byte, time.Duration, bool, error) {
	if !c.remoteAllowed(time.Now()) {
		return nil, 0, false, nil
	}
	opCtx, cancel := context.WithTimeout(ctx, usageCacheRemoteTimeout)
	defer cancel()
	data, err := c.remote.Get(opCtx, key)
	if errors.Is(err, errUsageCacheMiss) {
		c.remoteBackoffUntil.Store(0)
		return nil, 0, false, nil
	}
	if err != nil {
		c.noteRemoteError("GET", err)
		return nil, 0, false, err
	}
	c.remoteBackoffUntil.Store(0)
	payload, expiresAt, err := decodeUsageCacheRecord(data)
	if err != nil {
		// 旧版/损坏记录精确删除后当作 miss；禁止前缀扫描。
		if deleteErr := c.remote.Delete(opCtx, key); deleteErr != nil {
			c.noteRemoteError("DEL", deleteErr)
		}
		return nil, 0, false, nil
	}
	remaining := time.Until(expiresAt)
	if remaining <= 0 {
		if deleteErr := c.remote.Delete(opCtx, key); deleteErr != nil {
			c.noteRemoteError("DEL", deleteErr)
		}
		return nil, 0, false, nil
	}
	return payload, remaining, true, nil
}

func (c *usageResultCache) remoteSet(ctx context.Context, key string, data []byte, expiresAt time.Time) {
	ttl := time.Until(expiresAt)
	if ttl <= 0 || !c.remoteAllowed(time.Now()) {
		return
	}
	record, err := encodeUsageCacheRecord(data, expiresAt)
	if err != nil {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, usageCacheRemoteTimeout)
	defer cancel()
	if err := c.remote.Set(opCtx, key, record, ttl); err != nil {
		c.noteRemoteError("SET", err)
		return
	}
	c.remoteBackoffUntil.Store(0)
}

func encodeUsageCacheRecord(payload []byte, expiresAt time.Time) ([]byte, error) {
	return json.Marshal(usageCacheRecord{
		Version:           usageCacheRecordVersion,
		ExpiresAtUnixNano: expiresAt.UnixNano(),
		Payload:           append(json.RawMessage(nil), payload...),
	})
}

func decodeUsageCacheRecord(data []byte) ([]byte, time.Time, error) {
	var record usageCacheRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, time.Time{}, err
	}
	if record.Version != usageCacheRecordVersion || record.ExpiresAtUnixNano <= 0 || !json.Valid(record.Payload) {
		return nil, time.Time{}, errors.New("用量缓存记录格式无效")
	}
	return append([]byte(nil), record.Payload...), time.Unix(0, record.ExpiresAtUnixNano), nil
}

// usageAggregateTTL 以 CST 自然日判定数据是否还在增长。toTs 是不包含的上界：
// 结束在今天 00:00 或更早是已闭合历史区间，否则按一分钟级准实时处理。
func usageAggregateTTL(toTs int64, now time.Time) time.Duration {
	cstNow := now.In(usageCST)
	todayStart := time.Date(cstNow.Year(), cstNow.Month(), cstNow.Day(), 0, 0, 0, 0, usageCST).Unix()
	if toTs <= todayStart {
		return usageAggregateHistoricalTTL
	}
	return usageAggregateLiveTTL
}

func (c *usageResultCache) noteRemoteError(op string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	c.remoteErrors.Add(1)
	now := time.Now()
	c.remoteBackoffUntil.Store(now.Add(usageCacheRemoteBackoff).UnixNano())
	if !c.logRemoteErrors {
		return
	}
	prev := c.lastRemoteWarn.Load()
	if now.UnixNano()-prev < usageCacheWarnInterval.Nanoseconds() || !c.lastRemoteWarn.CompareAndSwap(prev, now.UnixNano()) {
		return
	}
	slog.Warn("用量 Redis 缓存暂不可用，已自动降级到本机有界缓存", "op", op, "err", err)
}

// Delete 只删除代码已知的精确键。远端不可用时旧键最多保留到自身 TTL；权限/成员变化
// 还会改变成员指纹，因此不会重新命中旧权限域的数据。
func (c *usageResultCache) Delete(ctx context.Context, keys ...string) {
	if c == nil || len(keys) == 0 {
		return
	}
	fullKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		full := c.fullKey(key)
		c.local.Delete(full)
		fullKeys = append(fullKeys, full)
	}
	if !c.remoteAllowed(time.Now()) {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, usageCacheRemoteTimeout)
	defer cancel()
	if err := c.remote.Delete(opCtx, fullKeys...); err != nil {
		c.noteRemoteError("DEL", err)
	}
}

// cacheKeyGateGroup 串行同一个业务缓存 key 的两种 leader（普通填充/主动刷新）。
// 使用可取消的 channel 信号量而不是 sync.Mutex，避免客户端已经断开后仍无期限排队。
// refs 归零即删除 gate，日期/筛选组合再多也不会形成永久增长的 map。
type cacheKeyGateGroup struct {
	mu    sync.Mutex
	gates map[string]*cacheKeyGate
}

type cacheKeyGate struct {
	sem  chan struct{}
	refs int
}

func (g *cacheKeyGateGroup) Acquire(ctx context.Context, key string) (func(), error) {
	g.mu.Lock()
	if g.gates == nil {
		g.gates = make(map[string]*cacheKeyGate)
	}
	gate := g.gates[key]
	if gate == nil {
		gate = &cacheKeyGate{sem: make(chan struct{}, 1)}
		g.gates[key] = gate
	}
	gate.refs++
	g.mu.Unlock()

	select {
	case gate.sem <- struct{}{}:
		return func() {
			<-gate.sem
			g.releaseRef(key, gate)
		}, nil
	case <-ctx.Done():
		g.releaseRef(key, gate)
		return nil, ctx.Err()
	}
}

func (g *cacheKeyGateGroup) releaseRef(key string, gate *cacheKeyGate) {
	g.mu.Lock()
	defer g.mu.Unlock()
	gate.refs--
	if gate.refs == 0 && g.gates[key] == gate {
		delete(g.gates, key)
	}
}

// cacheFlightGroup 仅保存正在执行的调用；调用完成立即删除，不承担结果缓存。
type cacheFlightGroup struct {
	mu sync.Mutex
	m  map[string]*cacheFlightCall
}

type cacheFlightCall struct {
	done chan struct{}
	data []byte
	err  error
}

func (g *cacheFlightGroup) Do(ctx context.Context, key string, fill func() ([]byte, error)) ([]byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g.mu.Lock()
		if g.m == nil {
			g.m = make(map[string]*cacheFlightCall)
		}
		if call := g.m[key]; call != nil {
			g.mu.Unlock()
			select {
			case <-call.done:
				// 首个请求被客户端取消时，正常等待者接手重建；等待者自身取消则返回。
				if errors.Is(call.err, context.Canceled) && ctx.Err() == nil {
					continue
				}
				return call.data, call.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		call := &cacheFlightCall{done: make(chan struct{})}
		g.m[key] = call
		g.mu.Unlock()

		call.data, call.err = fill()
		g.mu.Lock()
		if g.m[key] == call {
			delete(g.m, key)
		}
		close(call.done)
		g.mu.Unlock()
		return call.data, call.err
	}
}

type boundedByteCache struct {
	mu         sync.Mutex
	items      map[string]*list.Element
	lru        *list.List
	maxEntries int
	maxBytes   int
	usedBytes  int
}

type boundedByteEntry struct {
	key   string
	value []byte
	exp   time.Time
}

func newBoundedByteCache(maxEntries, maxBytes int) *boundedByteCache {
	if maxEntries < 1 {
		maxEntries = 1
	}
	if maxBytes < 1 {
		maxBytes = 1
	}
	return &boundedByteCache{
		items:      make(map[string]*list.Element),
		lru:        list.New(),
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
	}
}

func (c *boundedByteCache) Get(key string, now time.Time) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el := c.items[key]
	if el == nil {
		return nil, false
	}
	e := el.Value.(*boundedByteEntry)
	if !now.Before(e.exp) {
		c.removeElement(el)
		return nil, false
	}
	c.lru.MoveToFront(el)
	return append([]byte(nil), e.value...), true
}

func (c *boundedByteCache) Put(key string, value []byte, ttl time.Duration, now time.Time) {
	if ttl <= 0 || len(value) > c.maxBytes {
		return
	}
	copyValue := append([]byte(nil), value...)
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.items[key]; old != nil {
		c.removeElement(old)
	}
	el := c.lru.PushFront(&boundedByteEntry{key: key, value: copyValue, exp: now.Add(ttl)})
	c.items[key] = el
	c.usedBytes += len(copyValue)
	for len(c.items) > c.maxEntries || c.usedBytes > c.maxBytes {
		c.removeElement(c.lru.Back())
	}
}

func (c *boundedByteCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el := c.items[key]; el != nil {
		c.removeElement(el)
	}
}

func (c *boundedByteCache) removeElement(el *list.Element) {
	if el == nil {
		return
	}
	e := el.Value.(*boundedByteEntry)
	delete(c.items, e.key)
	c.usedBytes -= len(e.value)
	c.lru.Remove(el)
}

func (c *boundedByteCache) size() (entries, bytes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items), c.usedBytes
}
