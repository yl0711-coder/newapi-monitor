package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBrandHandlerReturnsNewAPILogoCachedAndSafe(t *testing.T) {
	var calls int32
	logo := "/logo.png"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"system_name": "Site", "logo": logo}})
	}))
	defer srv.Close()
	m := newStabilityTestMonitor(t)
	m.cfg.NewAPIBaseURL = srv.URL
	get := func() map[string]string {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/api/brand", nil)
		m.brandHandler(c)
		var out map[string]string
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return out
	}
	out := get()
	if out["name"] != "Site" || out["logo"] != srv.URL+"/logo.png" {
		t.Fatalf("relative logo must be resolved against new-api: %v", out)
	}
	before := atomic.LoadInt32(&calls)
	if again := get(); again["logo"] != out["logo"] || atomic.LoadInt32(&calls) != before {
		t.Fatalf("logo must be cached, calls %d -> %d", before, atomic.LoadInt32(&calls))
	}
	for in, want := range map[string]string{
		"javascript:alert(1)":          "",
		"data:text/html,<b>x</b>":      "",
		"data:image/png;base64,AAAA":   "data:image/png;base64,AAAA",
		"https://cdn.example/logo.png": "https://cdn.example/logo.png",
	} {
		if got := safeBrandLogo(in); got != want {
			t.Fatalf("safeBrandLogo(%q)=%q want %q", in, got, want)
		}
	}
}
