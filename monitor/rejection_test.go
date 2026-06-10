package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// rejection_test.go:被拒请求(前置拒绝)采集链路——验证 upsert 累加幂等 + 窗口聚合排序。

func TestRejectionsUpsertAndAggregate(t *testing.T) {
	m := newTestMonitor(t)
	const b = 1_900_000_000 / 60 * 60

	// 两个节点先各推一批;gpt-5.2/cq-codex-pro 在同节点同桶重复推(模拟分批),应累加。
	batch1 := []RejectionSample{
		{BucketTs: b, Node: "slave", Reason: "no_available_channel", Model: "gpt-5.2", Grp: "cq-codex-pro", Count: 3},
		{BucketTs: b, Node: "master", Reason: "no_available_channel", Model: "gpt-5.2", Grp: "cq-codex-pro", Count: 2},
		{BucketTs: b, Node: "slave", Reason: "no_available_channel", Model: "claude-opus-4-7", Grp: "claude-1.3x", Count: 1},
	}
	if err := m.upsertRejections(batch1); err != nil {
		t.Fatal(err)
	}
	// 同键再推(下一批),计数应在原基础上累加,而非覆盖。
	if err := m.upsertRejections([]RejectionSample{
		{BucketTs: b, Node: "slave", Reason: "no_available_channel", Model: "gpt-5.2", Grp: "cq-codex-pro", Count: 4},
	}); err != nil {
		t.Fatal(err)
	}

	rows := m.storeRejections(b - 60)
	if len(rows) != 2 {
		t.Fatalf("应聚合成 2 个(模型×分组),得 %d", len(rows))
	}
	// 按次数降序:gpt-5.2/cq-codex-pro = 3(slave) + 4(slave 累加) + 2(master) = 9
	top := rows[0]
	if top.Model != "gpt-5.2" || top.Group != "cq-codex-pro" || top.Count != 9 {
		t.Fatalf("Top 应为 gpt-5.2/cq-codex-pro=9(跨节点+累加),得 %s/%s=%d", top.Model, top.Group, top.Count)
	}
	if rows[1].Count != 1 {
		t.Fatalf("第二行应为 claude=1,得 %d", rows[1].Count)
	}

	// 窗口外取不到
	if got := m.storeRejections(b + 120); len(got) != 0 {
		t.Fatalf("窗口外不应有数据,得 %d", len(got))
	}
}

func TestIngestRejectionsHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `{"node":"slave","samples":[{"bucket_ts":1900000020,"reason":"no_available_channel","model":"gpt-5.2","group":"g1","count":3}]}`
	post := func(m *Monitor, auth string) *httptest.ResponseRecorder {
		r := gin.New()
		m.RegisterRoutes(r)
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/internal/rejections", strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		r.ServeHTTP(w, req)
		return w
	}

	// 未配置 token → 接口关闭 503
	if w := post(newTestMonitor(t), "Bearer x"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("未配置token应503,得%d", w.Code)
	}

	m := newTestMonitor(t)
	m.cfg.IngestToken = "secret123"
	// 无/错 token → 401
	if w := post(m, ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("无token应401,得%d", w.Code)
	}
	if w := post(m, "Bearer wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("错token应401,得%d", w.Code)
	}
	// 正确 token → 200 + 入库
	if w := post(m, "Bearer secret123"); w.Code != http.StatusOK {
		t.Fatalf("正确token应200,得%d: %s", w.Code, w.Body.String())
	}
	if rows := m.storeRejections(1900000020 - 60); len(rows) != 1 || rows[0].Count != 3 {
		t.Fatalf("应入库1行count=3,得%+v", rows)
	}
}
