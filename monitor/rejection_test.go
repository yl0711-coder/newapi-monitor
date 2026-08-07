package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
)

// rejection_test.go:被拒请求(前置拒绝)采集链路——验证不同批次累加、同批次重试幂等和窗口聚合。

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
	post := func(m *Monitor, auth, body string) *httptest.ResponseRecorder {
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
	body := `{"node":"slave","batch_id":"batch-0001","samples":[{"bucket_ts":1900000020,"reason":"no_available_channel","model":"gpt-5.2","group":"g1","count":3}]}`

	// 未配置 token → 接口关闭 503
	if w := post(newTestMonitor(t), "Bearer x", body); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("未配置token应503,得%d", w.Code)
	}

	m := newTestMonitor(t)
	m.cfg.IngestToken = "secret123"
	// 无/错 token → 401
	if w := post(m, "", body); w.Code != http.StatusUnauthorized {
		t.Fatalf("无token应401,得%d", w.Code)
	}
	if w := post(m, "Bearer wrong", body); w.Code != http.StatusUnauthorized {
		t.Fatalf("错token应401,得%d", w.Code)
	}
	// 正确 token → 200 + 入库
	if w := post(m, "Bearer secret123", body); w.Code != http.StatusOK {
		t.Fatalf("正确token应200,得%d: %s", w.Code, w.Body.String())
	}
	if rows := m.storeRejections(1900000020 - 60); len(rows) != 1 || rows[0].Count != 3 {
		t.Fatalf("应入库1行count=3,得%+v", rows)
	}
	// 响应在网络中丢失后，采集器会用相同 batch_id 重试；不得再次累加。
	if w := post(m, "Bearer secret123", body); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"duplicate":true`) {
		t.Fatalf("同批重试应返回200 duplicate=true,得%d: %s", w.Code, w.Body.String())
	}
	if rows := m.storeRejections(1900000020 - 60); len(rows) != 1 || rows[0].Count != 3 {
		t.Fatalf("同批重试后计数不得翻倍,得%+v", rows)
	}
	// 同一个 ID 携带不同内容是协议错误，必须拒绝且不污染原计数。
	conflict := strings.Replace(body, `"count":3`, `"count":4`, 1)
	if w := post(m, "Bearer secret123", conflict); w.Code != http.StatusConflict {
		t.Fatalf("复用batch_id但内容变化应409,得%d: %s", w.Code, w.Body.String())
	}
	if rows := m.storeRejections(1900000020 - 60); len(rows) != 1 || rows[0].Count != 3 {
		t.Fatalf("冲突批次不得写入,得%+v", rows)
	}
	// 不同批次落在同一分钟仍应累加。
	next := strings.Replace(strings.Replace(body, "batch-0001", "batch-0002", 1), `"count":3`, `"count":4`, 1)
	if w := post(m, "Bearer secret123", next); w.Code != http.StatusOK {
		t.Fatalf("新批次应200,得%d: %s", w.Code, w.Body.String())
	}
	if rows := m.storeRejections(1900000020 - 60); len(rows) != 1 || rows[0].Count != 7 {
		t.Fatalf("不同批次应累加到7,得%+v", rows)
	}
	missingID := `{"node":"slave","samples":[]}`
	if w := post(m, "Bearer secret123", missingID); w.Code != http.StatusBadRequest {
		t.Fatalf("缺batch_id应400,得%d: %s", w.Code, w.Body.String())
	}
}

func TestRejectionBatchHashIgnoresSampleOrder(t *testing.T) {
	rows := []RejectionSample{
		{BucketTs: 120, Node: "n", Reason: "b", Model: "m", Grp: "g", Count: 2},
		{BucketTs: 60, Node: "n", Reason: "a", Model: "m", Grp: "g", Count: 1},
	}
	reversed := []RejectionSample{rows[1], rows[0]}
	if rejectionBatchPayloadHash(rows) != rejectionBatchPayloadHash(reversed) {
		t.Fatal("同一批样本仅顺序变化时不应被误判为冲突")
	}
}

func TestConcurrentRejectionBatchRetriesAccumulateExactlyOnce(t *testing.T) {
	m := newTestMonitor(t)
	rows := []RejectionSample{{BucketTs: 120, Node: "master", Reason: "no_available_channel", Model: "m", Grp: "g", Count: 3}}
	const workers = 12
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	duplicates := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			duplicate, err := m.ingestRejectionBatch("master", "concurrent-batch", rows, 180)
			errs <- err
			duplicates <- duplicate
		}()
	}
	wg.Wait()
	close(errs)
	close(duplicates)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发重试不应入库失败: %v", err)
		}
	}
	duplicateCount := 0
	for duplicate := range duplicates {
		if duplicate {
			duplicateCount++
		}
	}
	if duplicateCount != workers-1 {
		t.Fatalf("应只有一个新批次，其余均识别为重复: duplicates=%d", duplicateCount)
	}
	got := m.storeRejections(60)
	if len(got) != 1 || got[0].Count != 3 {
		t.Fatalf("并发重试后计数必须恰好一次: %+v", got)
	}
}
