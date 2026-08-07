package monitor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func postHost(t *testing.T, m *Monitor, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/internal/host", m.ingestHost)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/host", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestHostContainerSnapshotBackwardCompatibility(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.IngestToken = "secret"
	now := time.Now().Unix()
	body := fmt.Sprintf(`{"node":"Node-A","ts":%d,"mem_total_mb":2048,"containers":[{"name":"/new-api","state":"RUNNING","health":"healthy","restart_count":2}]}`, now+86400)
	if w := postHost(t, m, body); w.Code != http.StatusOK {
		t.Fatalf("新 agent 推送失败: %d %s", w.Code, w.Body.String())
	}
	var rows []HostContainerSnapshot
	if err := m.storeDB.Find(&rows).Error; err != nil || len(rows) != 1 || rows[0].Name != "new-api" || rows[0].State != "running" {
		t.Fatalf("容器快照错误: %+v err=%v", rows, err)
	}
	if rows[0].LastSeen < now-1 || rows[0].LastSeen > time.Now().Unix()+1 {
		t.Fatalf("容器新鲜度必须使用服务端接收时间，不能被客户端未来时间污染: %+v", rows[0])
	}
	// 旧 agent 不含 containers 字段，必须保留已有快照。
	legacy := fmt.Sprintf(`{"node":"Node-A","ts":%d,"load1":0.5}`, now+60)
	if w := postHost(t, m, legacy); w.Code != http.StatusOK {
		t.Fatalf("旧 agent 推送失败: %d %s", w.Code, w.Body.String())
	}
	rows = nil
	if err := m.storeDB.Find(&rows).Error; err != nil || len(rows) != 1 {
		t.Fatalf("旧 agent 不应清空快照: %+v err=%v", rows, err)
	}
	// 新 agent 明确空列表，代表白名单为空，应清空该节点快照。
	empty := fmt.Sprintf(`{"node":"Node-A","ts":%d,"containers":[]}`, now+120)
	if w := postHost(t, m, empty); w.Code != http.StatusOK {
		t.Fatalf("空白名单推送失败: %d %s", w.Code, w.Body.String())
	}
	var count int64
	m.storeDB.Model(&HostContainerSnapshot{}).Where("node = ?", "Node-A").Count(&count)
	if count != 0 {
		t.Fatalf("明确空列表应清空快照，剩余 %d", count)
	}
}

func TestHostContainerInvalidPayloadDoesNotPartiallyWriteMetrics(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.IngestToken = "secret"
	now := time.Now().Unix()
	body := fmt.Sprintf(`{"node":"Node-B","ts":%d,"load1":9,"containers":[{"name":"api","state":"running"},{"name":"api","state":"running"}]}`, now)
	if w := postHost(t, m, body); w.Code != http.StatusBadRequest {
		t.Fatalf("重复容器名应 400，得 %d %s", w.Code, w.Body.String())
	}
	var count int64
	m.storeDB.Model(&InfraSample{}).Where("resource = ?", "Node-B").Count(&count)
	if count != 0 {
		t.Fatalf("无效请求不应留下半套指标，得 %d 行", count)
	}
}

func TestInfraStatusIncludesContainerHealthAndStaleness(t *testing.T) {
	m := newTestMonitor(t)
	if got := containerStatus("running", "unknown"); got != "warn" {
		t.Fatalf("inspect 不完整的 running 容器不得假绿，得 %s", got)
	}
	if got := m.infraStatus(InfraResource{Type: "instance", Metrics: map[string]float64{"cpu": 1}, Containers: []InfraContainer{{Status: "bad"}}}); got != "bad" {
		t.Fatalf("容器 bad 应使实例 bad，得 %s", got)
	}
	if got := m.infraStatus(InfraResource{Type: "instance", Containers: []InfraContainer{{Status: "warn"}}}); got != "warn" {
		t.Fatalf("只有容器数据时也应反映 warn，得 %s", got)
	}
	if err := m.replaceHostContainerSnapshots("Node-C", []HostContainerSnapshot{{Node: "Node-C", Name: "api", State: "running", Health: "healthy", LastSeen: 100}}); err != nil {
		t.Fatal(err)
	}
	rows := m.hostContainerSnapshot("Node-C", 401)
	if len(rows) != 1 || rows[0].Status != "warn" {
		t.Fatalf("超过 5 分钟未更新不应继续显示绿色: %+v", rows)
	}
}
