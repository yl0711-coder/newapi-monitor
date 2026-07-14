package monitor

// cache.go:进程内 TTL 缓存 + 防击穿(singleflight),给客户端(portal)的组级查询用。
// 设计(与用户约定一致):
//   - 客户端【读】缓存:同一组+同一日期范围,TTL 内只真正查一次生产库——在线人数与库压力解耦;
//   - 管理端【不读】缓存(始终直查最新),但查询完成后按组切片【写】进来(写穿透预热,见 usage.go);
//   - 防击穿:缓存失效瞬间同键并发请求只放行第一个去查库,其余等它的结果——
//     数学上限死:生产库压力 ≤ 每个键每 TTL 一条查询。
// 失效策略:仅 TTL 自然过期,不做主动失效(报表场景,≤TTL 的旧数据无感;零失效 bug 面)。

import (
	"sync"
	"time"
)

type cacheEntry struct {
	val   any
	exp   time.Time     // 过期时刻
	ready chan struct{} // 关闭=填充完成(singleflight 等待点)
	err   error
}

type ttlCache struct {
	mu sync.Mutex
	m  map[string]*cacheEntry
}

func newTTLCache() *ttlCache { return &ttlCache{m: map[string]*cacheEntry{}} }

// Do 取 key 对应的值:命中且未过期直接返回;否则本协程(或等第一个到的协程)执行 fill 填充。
// fill 出错不缓存错误结果(下次重试),错误原样返回给本轮所有等待者。
func (c *ttlCache) Do(key string, ttl time.Duration, fill func() (any, error)) (any, error) {
	for {
		c.mu.Lock()
		e := c.m[key]
		now := time.Now()
		if e != nil {
			select {
			case <-e.ready: // 已填充完
				if e.err == nil && now.Before(e.exp) {
					c.mu.Unlock()
					return e.val, nil
				}
				// 过期或上次失败:走重建
			default: // 有人正在填充:等它
				c.mu.Unlock()
				<-e.ready
				continue // 重进循环取结果(或它失败后本协程接手重建)
			}
		}
		// 本协程负责填充
		ne := &cacheEntry{ready: make(chan struct{})}
		c.m[key] = ne
		c.mu.Unlock()

		val, err := fill()
		ne.val, ne.err = val, err
		if err == nil {
			ne.exp = time.Now().Add(ttl)
		}
		close(ne.ready)
		if err != nil { // 失败不留缓存,下个请求重试
			c.mu.Lock()
			if c.m[key] == ne {
				delete(c.m, key)
			}
			c.mu.Unlock()
		}
		return val, err
	}
}

// Put 直接写入(管理端写穿透预热用):无等待语义,覆盖旧值。
func (c *ttlCache) Put(key string, val any, ttl time.Duration) {
	e := &cacheEntry{val: val, exp: time.Now().Add(ttl), ready: make(chan struct{})}
	close(e.ready)
	c.mu.Lock()
	c.m[key] = e
	c.mu.Unlock()
}

// gc 清理过期项(portal 周期调用,防长期运行内存缓慢增长;条目本就少,粗扫即可)。
func (c *ttlCache) gc() {
	now := time.Now()
	c.mu.Lock()
	for k, e := range c.m {
		select {
		case <-e.ready:
			if now.After(e.exp) {
				delete(c.m, k)
			}
		default: // 填充中不动
		}
	}
	c.mu.Unlock()
}
