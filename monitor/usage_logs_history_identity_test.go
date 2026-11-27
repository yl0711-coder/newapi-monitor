package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestUsageLogsExactUsernameSurvivesRenameCyclesAndReuse(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "renamed away and back", true: "name reused by another user"}[reuse], func(t *testing.T) {
			db := newUsageLogsFakeProd(t)
			now := time.Now().Unix()
			middleUser, middleName := int64(31), "temporary"
			wantCount, wantQuota := int64(2), int64(40)
			if reuse {
				middleUser, middleName = 40, "kjl_Dev"
				wantCount, wantQuota = 3, 60
			}
			insertUsageLogs(t, db,
				usageLogRow{id: 1, userID: 31, createdAt: now - 30, typ: 2, username: "kjl_Dev", quota: 10},
				usageLogRow{id: 2, userID: middleUser, createdAt: now - 20, typ: 2, username: middleName, quota: 20},
				usageLogRow{id: 3, userID: 31, createdAt: now - 10, typ: 2, username: "kjl_Dev", quota: 30})
			m := &Monitor{prodDB: db}
			query := "?username=kjl_Dev"
			w := usageLogsGET(t, m, m.serveUsageLogsCount, "/usage-logs/count"+query, nil)
			var count struct {
				Success bool
				Data    struct{ Total int64 }
			}
			if e := json.Unmarshal(w.Body.Bytes(), &count); e != nil || !count.Success || count.Data.Total != wantCount {
				t.Fatalf("historical username count: %s expected=%d", w.Body.String(), wantCount)
			}
			w = usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list"+query, nil)
			var list usageLogsListResp
			if e := json.Unmarshal(w.Body.Bytes(), &list); e != nil || !list.Success || list.Data.Total != wantCount {
				t.Fatalf("historical username list: %s", w.Body.String())
			}
			for _, row := range list.Data.Items {
				if row.Username != "kjl_Dev" {
					t.Fatal("different historical name entered exact-name result")
				}
			}
			w = usageLogsGET(t, m, m.serveUsageLogsStat, "/usage-logs/stat"+query, nil)
			var stat struct {
				Success bool
				Data    struct{ Quota, RPM int64 }
			}
			if e := json.Unmarshal(w.Body.Bytes(), &stat); e != nil || !stat.Success || stat.Data.Quota != wantQuota || stat.Data.RPM != wantCount {
				t.Fatalf("historical username amounts/rates: %s expected=%d", w.Body.String(), wantQuota)
			}
		})
	}
}

func TestUsageLogsLegacyCountListCannotBypassHeavyAdmission(t *testing.T) {
	db, counts := newCountedUsageLogsDB(t)
	m := &Monitor{prodDB: db}
	m.initUsageLogsGates()
	m.usageLogsBackgroundGate <- struct{}{}
	defer func() { <-m.usageLogsBackgroundGate }()
	counts.reset()
	w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?defer_total=1", nil)
	var response struct{ Success bool }
	if e := json.Unmarshal(w.Body.Bytes(), &response); e != nil || !response.Success {
		t.Fatalf("deferred list should retain foreground capacity: %s", w.Body.String())
	}
	counts.reset()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/list", nil).WithContext(ctx)
	m.serveUsageLogsList(c)
	if e := json.Unmarshal(recorder.Body.Bytes(), &response); e != nil || response.Success || counts.logs.Load() != 0 {
		t.Fatalf("legacy COUNT must wait for heavy slot without source SQL: %s queries=%d", recorder.Body.String(), counts.logs.Load())
	}
	if len(m.usageLogsGate) != 0 || len(m.usageLogsBackgroundGate) != 1 {
		t.Fatal("admission was leaked or foreground slot consumed")
	}
}
