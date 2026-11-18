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

func TestIngestRejectionsV1IsGone(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.cfg.IngestToken = "secret123"
	r := gin.New()
	m.RegisterRoutes(r)
	body := `{"node":"slave","batch_id":"batch-0001","samples":[{"bucket_ts":1900000020,"reason":"no_available_channel","model":"gpt-5.2","group":"g1","count":3}]}`
	for _, auth := range []string{"", "Bearer wrong", "Bearer secret123"} {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/internal/rejections", strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		r.ServeHTTP(w, req)
		if w.Code != http.StatusGone || !strings.Contains(w.Body.String(), "/internal/rejections/v2") {
			t.Fatalf("auth=%q: v1 should answer 410 pointing to v2, got %d %s", auth, w.Code, w.Body.String())
		}
	}
	if rows := m.storeRejections(1900000020 - 60); len(rows) != 0 {
		t.Fatalf("retired v1 must not store samples: %+v", rows)
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

func TestRejectionBatchHashOrdersCustomerIdentity(t *testing.T) {
	rows := []RejectionSample{
		{BucketTs: 120, Node: "n", Reason: "r", Model: "m", Grp: "g", UserID: 7, Count: 1},
		{BucketTs: 120, Node: "n", Reason: "r", Model: "m", Grp: "g", UserID: 3, Count: 1},
	}
	reversed := []RejectionSample{rows[1], rows[0]}
	if rejectionBatchPayloadHash(rows) != rejectionBatchPayloadHash(reversed) {
		t.Fatal("同一批多客户样本仅顺序变化时不应被误判为冲突")
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
