package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestBrandName:站点名"取一次 → 落库 → 之后用存的 → 可手改"的完整行为。
func TestBrandName(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"system_name": "酷站"}})
	}))
	defer srv.Close()

	m := newTestMonitor(t)
	m.cfg.NewAPIBaseURL = srv.URL

	// 1) 首次:未存 → 从 new-api 取 system_name 并落库
	if got := m.brandName(); got != "酷站" {
		t.Fatalf("首次应取到 system_name '酷站',实际 %q", got)
	}
	if got := m.loadAlertConfig().SiteName; got != "酷站" {
		t.Errorf("应已落库,实际存的 %q", got)
	}

	// 2) 再次:用存的,不再请求 new-api
	before := hits
	if got := m.brandName(); got != "酷站" {
		t.Errorf("二次应返回存的,实际 %q", got)
	}
	if hits != before {
		t.Errorf("已存后不应再请求 new-api(hits %d→%d)", before, hits)
	}

	// 3) 手动覆盖:存了别的名 → 返回它,且不请求 new-api
	c := m.loadAlertConfig()
	c.SiteName = "自定义名"
	if err := m.saveAlertConfig(c); err != nil {
		t.Fatal(err)
	}
	before = hits
	if got := m.brandName(); got != "自定义名" || hits != before {
		t.Errorf("手改后应返回自定义名且不请求,实际 %q(hits +%d)", got, hits-before)
	}
}
