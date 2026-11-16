package monitor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type usageLogShortcutTestReply struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    struct {
		Items []UsageLogShortcut `json:"items"`
		Max   int                `json:"max"`
	} `json:"data"`
}

func newUsageLogShortcutTestServer(t *testing.T) (*Monitor, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	m := &Monitor{cfg: Settings{SessionSecret: "test-secret"}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	r := gin.New()
	m.RegisterRoutes(r)
	return m, r
}

func usageLogShortcutCall(t *testing.T, m *Monitor, r *gin.Engine, who string, role int, method, path, body string) (int, usageLogShortcutTestReply) {
	t.Helper()
	w := httptest.NewRecorder()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if who != "" {
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession(who, role, time.Now().Unix())})
	}
	r.ServeHTTP(w, req)
	var out usageLogShortcutTestReply
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func TestUsageLogShortcutsArePerAdminAndValidated(t *testing.T) {
	m, r := newUsageLogShortcutTestServer(t)
	call := func(who, method, path, body string) (int, usageLogShortcutTestReply) {
		return usageLogShortcutCall(t, m, r, who, roleAdmin, method, path, body)
	}

	if code, _ := call("", http.MethodGet, "/usage-logs/shortcuts", ""); code != http.StatusUnauthorized {
		t.Fatalf("anonymous list = %d, want 401", code)
	}
	if code, _ := usageLogShortcutCall(t, m, r, "viewer", roleAdmin-1, http.MethodGet, "/usage-logs/shortcuts", ""); code != http.StatusForbidden {
		t.Fatalf("below-admin list = %d, want 403", code)
	}

	code, out := call("alice", http.MethodGet, "/usage-logs/shortcuts", "")
	if code != 200 || !out.Success || len(out.Data.Items) != 0 || out.Data.Max != usageLogShortcutMax {
		t.Fatalf("empty list = %d %+v", code, out)
	}

	// Empty label falls back to the username; labels are trimmed and clipped.
	_, out = call("alice", http.MethodPost, "/usage-logs/shortcuts", `{"username":"  yiche-api  ","label":""}`)
	if !out.Success || len(out.Data.Items) != 1 || out.Data.Items[0].Label != "yiche-api" || out.Data.Items[0].Username != "yiche-api" {
		t.Fatalf("create default label = %+v", out)
	}
	_, out = call("alice", http.MethodPost, "/usage-logs/shortcuts", `{"username":"gaotu","label":"高途高途高途高途高途高途高途高途高途"}`)
	if !out.Success || len(out.Data.Items) != 2 || []rune(out.Data.Items[1].Label)[0] != '高' || len([]rune(out.Data.Items[1].Label)) != usageLogShortcutLabelRunes {
		t.Fatalf("create clipped label = %+v", out)
	}
	if strings.Contains(fmt.Sprint(out.Data.Items), "alice") {
		t.Fatalf("owner must not be serialized: %+v", out.Data.Items)
	}

	for _, bad := range []struct{ body, want string }{
		{`{"username":"   "}`, "请填写用户名"},
		{`{"username":"gao%"}`, "通配符"},
		{`{"username":"yiche-api","label":"again"}`, "已在快捷标签中"},
		{`{"username":"` + strings.Repeat("u", 65) + `"}`, "过长"},
		{`not json`, "请求格式错误"},
	} {
		code, out := call("alice", http.MethodPost, "/usage-logs/shortcuts", bad.body)
		if code != http.StatusBadRequest || out.Success || !strings.Contains(out.Message, bad.want) {
			t.Fatalf("create %s = %d %+v, want message containing %q", bad.body, code, out, bad.want)
		}
	}

	// Bob has his own empty list and cannot touch Alice's rows.
	_, bob := call("bob", http.MethodGet, "/usage-logs/shortcuts", "")
	if len(bob.Data.Items) != 0 {
		t.Fatalf("bob sees alice's shortcuts: %+v", bob.Data.Items)
	}
	_, out = call("alice", http.MethodGet, "/usage-logs/shortcuts", "")
	aliceFirst := out.Data.Items[0].ID
	if code, rep := call("bob", http.MethodPost, fmt.Sprintf("/usage-logs/shortcuts/%d", aliceFirst), `{"username":"hijack"}`); code != http.StatusBadRequest || !strings.Contains(rep.Message, "不存在") {
		t.Fatalf("bob update alice row = %d %+v", code, rep)
	}
	if _, rep := call("bob", http.MethodPost, fmt.Sprintf("/usage-logs/shortcuts/%d/delete", aliceFirst), ""); !rep.Success || len(rep.Data.Items) != 0 {
		t.Fatalf("bob delete alice row = %+v", rep)
	}
	// The same username may exist independently for another admin.
	if _, rep := call("bob", http.MethodPost, "/usage-logs/shortcuts", `{"username":"yiche-api","label":"易车"}`); !rep.Success || len(rep.Data.Items) != 1 {
		t.Fatalf("bob create same username = %+v", rep)
	}
	_, out = call("alice", http.MethodGet, "/usage-logs/shortcuts", "")
	if len(out.Data.Items) != 2 || out.Data.Items[0].Username != "yiche-api" || out.Data.Items[0].Label != "yiche-api" {
		t.Fatalf("alice rows changed by bob: %+v", out.Data.Items)
	}

	// Update: rename + change username; cannot collide with another own row.
	second := out.Data.Items[1].ID
	if code, rep := call("alice", http.MethodPost, fmt.Sprintf("/usage-logs/shortcuts/%d", second), `{"username":"yiche-api"}`); code != http.StatusBadRequest || !strings.Contains(rep.Message, "已在快捷标签中") {
		t.Fatalf("update collision = %d %+v", code, rep)
	}
	_, out = call("alice", http.MethodPost, fmt.Sprintf("/usage-logs/shortcuts/%d", second), `{"username":"gaotu-2","label":"高途"}`)
	if !out.Success || out.Data.Items[1].Username != "gaotu-2" || out.Data.Items[1].Label != "高途" {
		t.Fatalf("update = %+v", out)
	}

	// Reorder ignores unknown/foreign ids and keeps unlisted rows after.
	_, out = call("alice", http.MethodPost, "/usage-logs/shortcuts", `{"username":"youku"}`)
	ids := []int64{out.Data.Items[0].ID, out.Data.Items[1].ID, out.Data.Items[2].ID}
	_, out = call("alice", http.MethodPost, "/usage-logs/shortcuts/order", fmt.Sprintf(`{"ids":[%d,999999,%d]}`, ids[2], ids[0]))
	got := []int64{out.Data.Items[0].ID, out.Data.Items[1].ID, out.Data.Items[2].ID}
	if !out.Success || got[0] != ids[2] || got[1] != ids[0] || got[2] != ids[1] {
		t.Fatalf("reorder = %v, want [%d %d %d]", got, ids[2], ids[0], ids[1])
	}

	// Delete own row.
	_, out = call("alice", http.MethodPost, fmt.Sprintf("/usage-logs/shortcuts/%d/delete", ids[0]), "")
	if !out.Success || len(out.Data.Items) != 2 {
		t.Fatalf("delete = %+v", out)
	}
	if code, _ := call("alice", http.MethodPost, "/usage-logs/shortcuts/abc/delete", ""); code != http.StatusBadRequest {
		t.Fatalf("delete bad id = %d", code)
	}
}

func TestUsageLogShortcutsCapPerAdmin(t *testing.T) {
	m, r := newUsageLogShortcutTestServer(t)
	for i := 0; i < usageLogShortcutMax; i++ {
		_, out := usageLogShortcutCall(t, m, r, "alice", roleAdmin, http.MethodPost, "/usage-logs/shortcuts", fmt.Sprintf(`{"username":"user-%d"}`, i))
		if !out.Success {
			t.Fatalf("create %d = %+v", i, out)
		}
	}
	code, out := usageLogShortcutCall(t, m, r, "alice", roleAdmin, http.MethodPost, "/usage-logs/shortcuts", `{"username":"one-more"}`)
	if code != http.StatusBadRequest || !strings.Contains(out.Message, "最多") {
		t.Fatalf("over cap = %d %+v", code, out)
	}
	// The cap is per admin.
	if _, out := usageLogShortcutCall(t, m, r, "bob", roleAdmin, http.MethodPost, "/usage-logs/shortcuts", `{"username":"one-more"}`); !out.Success {
		t.Fatalf("bob under own cap = %+v", out)
	}
}
