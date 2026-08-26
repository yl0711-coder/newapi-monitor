package monitor

// logchain_nostore_test.go：敏感排障接口禁止缓存（验收报告 RB-03）。
//
// 报告的复测通过标准要求 200/400/401/403/500 **全部**带禁止缓存头。
// 这一点决定了实现必须用中间件而不是在 handler 里逐个 c.Header：
// handler 有多条提前 return 的错误分支，逐个加必然漏，而漏掉的恰好是错误响应。

import (
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// serverGoSource 读 server.go 源码做路由注册断言。
// 用源码而非反射：gin 不暴露某路由挂了哪些中间件，反射拿不到。
func serverGoSource(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatalf("读 server.go 失败: %v", err)
	}
	return string(b)
}

// TestNoStoreSensitiveSetsHeadersBeforeHandler 中间件必须在 handler 写响应前设头。
func TestNoStoreSensitiveSetsHeadersBeforeHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// 逐个状态码验：报告要求 200/400/401/403/500 全部带头。
	// 这里用一个"故意在各状态码上提前返回"的假 handler，
	// 因为真 handler 的 500 分支需要生产库不可达，构造成本高且不稳定。
	for _, code := range []int{200, 400, 401, 403, 500} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			w := httptest.NewRecorder()
			c, engine := gin.CreateTestContext(w)
			engine.GET("/x", noStoreSensitive, func(c *gin.Context) {
				c.JSON(code, gin.H{"code": code})
			})
			c.Request = httptest.NewRequest("GET", "/x", nil)
			engine.ServeHTTP(w, c.Request)

			if w.Code != code {
				t.Fatalf("状态码应为 %d，got=%d", code, w.Code)
			}
			if got := w.Header().Get("Cache-Control"); got != "private, no-store" {
				t.Errorf("HTTP %d 缺禁止缓存头：Cache-Control=%q", code, got)
			}
			if got := w.Header().Get("Pragma"); got != "no-cache" {
				t.Errorf("HTTP %d 缺 Pragma: no-cache，got=%q", code, got)
			}
		})
	}
}

// TestLogChainRoutesUseNoStore 两个排障路由必须挂上中间件，前端也要声明 no-store。
//
// 路由注册用字符串断言：漏挂中间件不会让任何现有测试失败，
// 而它的后果（敏感诊断数据被中间层缓存）在功能测试里看不出来。
func TestLogChainRoutesUseNoStore(t *testing.T) {
	src := serverGoSource(t)
	for _, route := range []string{"/logchain/requests", "/logchain/filters"} {
		idx := strings.Index(src, `view.GET("`+route+`"`)
		if idx < 0 {
			t.Fatalf("找不到路由注册: %s", route)
		}
		line := src[idx:]
		if end := strings.Index(line, "\n"); end > 0 {
			line = line[:end]
		}
		if !strings.Contains(line, "noStoreSensitive") {
			t.Errorf("%s 未挂 noStoreSensitive 中间件：敏感诊断数据可能被中间层缓存\n注册行: %s", route, line)
		}
	}

	js := string(logChainJS)
	if strings.Count(js, `cache:'no-store'`) < 2 {
		t.Error("两个 logchain fetch 都必须声明 cache:'no-store'")
	}
}
