package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// HTTP contract tests intentionally use the original JSON shapes, including
// null versus omitted fields. Run them against the original handlers first.
func TestUsageLogsRefactorHTTPContract(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	// Keep the fixtures outside the rolling RPM/TPM window.
	const ts = int64(1794500000)
	insertUsageLogs(t, db,
		usageLogRow{1, 31, ts, 2, "kjl_Dev", "tok", "sample", 50, 2, 3, 7, "codex", "x", ""},
		usageLogRow{2, 31, ts, 5, "kjl_Dev", "tok", "sample", 999, 4, 5, 9, "codex", "y", ""},
	)
	m := &Monitor{prodDB: db}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m.RegisterRoutes(r)
	session := m.signSession("fixture-admin", roleAdmin, time.Now().Unix())

	for _, tc := range []struct{ name, path, want string }{
		{"empty", "list?channel=999&p=2&ps=3", `{"success":true,"message":"","data":{"page":2,"page_size":3,"total":0,"items":[],"next_cursor":null}}`},
		{"deferred-empty", "list?channel=999&defer_total=1", `{"success":true,"message":"","data":{"page":1,"page_size":10,"total":0,"total_pending":false,"has_more":false,"items":[],"next_cursor":null}}`},
		{"deferred-late", "list?channel=999&p=2&defer_total=1", `{"success":true,"message":"","data":{"page":2,"page_size":10,"total":null,"total_pending":true,"has_more":false,"items":[],"next_cursor":null}}`},
		{"count-ignores-cursor", "count?group=codex&before_id=not-a-cursor", `{"success":true,"message":"","data":{"total":2}}`},
		{"quota", "stat?type=5&request_id=y", `{"success":true,"message":"","data":{"quota":50,"quota_skipped":false,"rpm":0,"tpm":0}}`},
		{"skip-quota", "stat?skip_quota=1", `{"success":true,"message":"","data":{"quota":null,"quota_skipped":true,"rpm":0,"tpm":0}}`},
		{"invalid-cursor", "list?before_id=-1", `{"success":false,"message":"日志分页位置无效，请重新搜索"}`},
		{"filter-before-cursor", "list?username=%25a%25&before_id=-1", `{"success":false,"message":"使用模糊搜索时，关键词长度至少为 2 个字符"}`},
		{"missing-user", "user/9999", `{"success":false,"message":"用户不存在"}`},
		{"invalid-user", "user/0", `{"success":false,"message":"用户 ID 无效"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/usage-logs/"+tc.path, nil)
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: session})
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			var got, want any
			if w.Code != 200 || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") ||
				json.Unmarshal(w.Body.Bytes(), &got) != nil || json.Unmarshal([]byte(tc.want), &want) != nil ||
				!reflect.DeepEqual(got, want) {
				t.Fatalf("HTTP contract changed: status=%d body=%s want=%s", w.Code, w.Body.String(), tc.want)
			}
		})
	}
}
