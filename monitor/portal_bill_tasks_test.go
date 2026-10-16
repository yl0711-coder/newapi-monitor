package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/usagebill/exportjobs"
	"gorm.io/gorm"
)

func TestPortalBillTasksAuthorizationErrorClassification(t *testing.T) {
	for _, err := range []error{fmt.Errorf("%w: %w", errPortalAuthorizationInvalid, context.DeadlineExceeded), errors.New("database locked"), context.Canceled, gorm.ErrRecordNotFound} {
		if got := portalBillAuthorizationError(err); !errors.Is(got, errPortalBillAuthUnavailable) || errors.Is(got, exportjobs.ErrUnauthorized) {
			t.Fatal("transient error treated as revocation")
		}
	}
	for _, err := range []error{errPortalAuthorizationInvalid, fmt.Errorf("%w: %w", errPortalAuthorizationInvalid, gorm.ErrRecordNotFound)} {
		if !errors.Is(portalBillAuthorizationError(err), exportjobs.ErrUnauthorized) {
			t.Fatal("revocation not recognized")
		}
	}
}

func TestPortalBillTasksCapabilityOnlyAfterMount(t *testing.T) {
	m, h, ck, _ := billHTTPFixture(t)
	if m.portalBillTaskContract() != 0 {
		t.Fatal("default enabled")
	}
	s, err := m.initPortalBillTasks()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if m.portalBillTaskContract() != 0 {
		t.Fatal("advertised without routes")
	}
	s.mount(h.(*gin.Engine))
	if m.portalBillTaskContract() != 1 {
		t.Fatal("mounted capability missing")
	}
	w := billGet(h, ck, "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13&format=bill-v1")
	var d map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil || w.Code != 200 || d["bill_task_contract"] != float64(1) {
		t.Fatalf("prepare capability missing: %d %s", w.Code, w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if m.portalBillTaskContract() != 0 {
		t.Fatal("advertised after close")
	}
}

func TestPortalBillTasksCapabilityAbsentByDefault(t *testing.T) {
	_, h, ck, _ := billHTTPFixture(t)
	w := billGet(h, ck, "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13&format=bill-v1")
	if w.Code != 200 || strings.Contains(w.Body.String(), "bill_task_contract") {
		t.Fatalf("default behavior changed: %d %s", w.Code, w.Body.String())
	}
}

func billTasksFixture(t *testing.T) (*portalBillTasks, http.Handler, *http.Cookie, string) {
	t.Helper()
	m, h, ck, _ := billHTTPFixture(t)
	base := t.TempDir()
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := newPortalBillTasks(m, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	s.mount(h.(*gin.Engine))
	return s, h, ck, billPrepare(t, h, ck)
}

func billTaskRequest(h http.Handler, ck *http.Cookie, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
	if ck != nil {
		r.AddCookie(ck)
	}
	r.Header.Set("Origin", "http://127.0.0.1")
	r.Header.Set("X-Usage-Bill-Request", "1")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func billTaskID(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var response struct {
		Task struct {
			ID string `json:"id"`
		}
	}
	if w.Code != 202 || json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Task.ID) != 32 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	return response.Task.ID
}

func billTaskWait(t *testing.T, h http.Handler, ck *http.Cookie, id, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		w := billTaskRequest(h, ck, "GET", "/api/logs/bill/tasks/"+id, "")
		var response struct {
			Task struct {
				State string `json:"state"`
			}
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil {
			t.Fatalf("status: %d %s", w.Code, w.Body.String())
		}
		if response.Task.State == want {
			return
		}
		if response.Task.State != "queued" && response.Task.State != "running" {
			t.Fatalf("unexpected state: %s", w.Body.String())
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("task timeout")
}

func TestPortalBillTasksHTTPFlowAndDuplicate(t *testing.T) {
	s, h, ck, ticket := billTasksFixture(t)
	var calls atomic.Int64
	s.build = func(ctx context.Context, w io.Writer, scope portalBillScope, total int64, auth func(context.Context) error, p func(int64) error) (int64, error) {
		calls.Add(1)
		if len(scope.IDs) != 1 || scope.IDs[0] != 101 || total != 1 {
			t.Error("wrong source scope")
		}
		return portalBuildVerifiedBill(ctx, w, portalBillBuildPlan{1, "123", 101, 10}, func(ctx context.Context, c logPageCursor, n int) ([]LogRow, error) {
			return s.m.queryGroupLogsForExportCursor(ctx, scope.IDs, scope.From, scope.To, scope.Member, 2, scope.Model, scope.Group, scope.Token, scope.Detail, scope.RequestID, c, n)
		}, auth, p)
	}
	body := `{"ticket":"` + ticket + `"}`
	id := billTaskID(t, billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks", body))
	if id2 := billTaskID(t, billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks", body)); id2 != id {
		t.Fatal("duplicate job")
	}
	billTaskWait(t, h, ck, id, "ready")
	if calls.Load() != 1 {
		t.Fatal("duplicate generation")
	}
	w := billTaskRequest(h, ck, "GET", "/api/logs/bill/tasks/"+id+"/download", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "0.000246") || !strings.Contains(w.Body.String(), "Request ID") || w.Header().Get("Content-Length") == "" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("download: %d %s", w.Code, w.Body.String())
	}
	if w = billTaskRequest(h, ck, "GET", "/api/logs/bill/tasks/"+id+"/download", ""); w.Code != 429 {
		t.Fatal("download rate bypass")
	}
	// Same group cannot bypass the ordinary CSV download quota either.
	if w = billGet(h, ck, "/api/logs/bill/v1?ticket="+ticket); w.Code != 429 {
		t.Fatalf("existing route quota bypass: %d", w.Code)
	}
}

func TestPortalBillTasksRejectClientScopeAndCSRF(t *testing.T) {
	_, h, ck, ticket := billTasksFixture(t)
	for _, tc := range []struct {
		name, body, origin, header string
		cookie                     bool
		want                       int
	}{
		{"no-session", `{}`, "http://127.0.0.1", "1", false, 401},
		{"cross-site", `{}`, "https://evil.invalid", "1", true, 403},
		{"no-origin", `{}`, "", "1", true, 403},
		{"no-header", `{}`, "http://127.0.0.1", "", true, 403},
		{"bad-ticket", `{"ticket":"bad"}`, "http://127.0.0.1", "1", true, 400},
		{"client-scope", `{"ticket":"` + ticket + `","user_id":202}`, "http://127.0.0.1", "1", true, 400},
		{"trailing", `{"ticket":"` + ticket + `"}{}`, "http://127.0.0.1", "1", true, 400},
		{"large", `{"ticket":"` + strings.Repeat("x", 17000) + `"}`, "http://127.0.0.1", "1", true, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "http://127.0.0.1/api/logs/bill/tasks", strings.NewReader(tc.body))
			if tc.cookie {
				r.AddCookie(ck)
			}
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("X-Usage-Bill-Request", tc.header)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("got %d want %d", w.Code, tc.want)
			}
		})
	}
}

func TestPortalBillTasksFailureCancelAndScope(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "other-group", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			s, h, ck, ticket := billTasksFixture(t)
			started := make(chan struct{})
			s.build = func(ctx context.Context, w io.Writer, _ portalBillScope, _ int64, _ func(context.Context) error, _ func(int64) error) (int64, error) {
				close(started)
				if mode == "failure" {
					_, _ = io.WriteString(w, "private-partial")
					return 0, errors.New("raw secret must not escape")
				}
				<-ctx.Done()
				return 0, ctx.Err()
			}
			id := billTaskID(t, billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks", `{"ticket":"`+ticket+`"}`))
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("not started")
			}
			if mode == "failure" {
				billTaskWait(t, h, ck, id, "failed")
				w := billTaskRequest(h, ck, "GET", "/api/logs/bill/tasks/"+id+"/download", "")
				if w.Code != 409 || strings.Contains(w.Body.String(), "private-partial") || strings.Contains(w.Body.String(), "secret") {
					t.Fatal("partial leaked")
				}
				return
			}
			if mode == "cancel" {
				w := billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks/"+id+"/cancel", "{}")
				if w.Code != 200 {
					t.Fatalf("cancel: %d %s", w.Code, w.Body.String())
				}
				w = billTaskRequest(h, ck, "GET", "/api/logs/bill/tasks/"+id+"/download", "")
				if w.Code != 404 {
					t.Fatal("canceled file readable")
				}
				return
			}
			if mode == "other-group" {
				g := CustomerGroup{Name: "other", PortalEmail: "other@example.invalid"}
				if err := s.m.storeDB.Create(&g).Error; err != nil {
					t.Fatal(err)
				}
				ck = &http.Cookie{Name: portalSessionCookie, Value: s.m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
			} else {
				if err := s.m.storeDB.Model(&CustomerGroup{}).Where("portal_email = ?", "bill@example.invalid").Update("portal_auth_ver", 1).Error; err != nil {
					t.Fatal(err)
				}
			}
			for _, suffix := range []string{"", "/download"} {
				w := billTaskRequest(h, ck, "GET", "/api/logs/bill/tasks/"+id+suffix, "")
				want := 404
				if mode == "revoked" {
					want = 401
				}
				if w.Code != want {
					t.Fatalf("scope: %d %s", w.Code, w.Body.String())
				}
			}
		})
	}
}

func TestPortalBillTasksShareExistingGenerationLock(t *testing.T) {
	s, h, ck, ticket := billTasksFixture(t)
	f, lease, err := s.m.createPortalExportTempFile(portalExportMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	defer lease.release()
	var calls atomic.Int64
	s.build = func(context.Context, io.Writer, portalBillScope, int64, func(context.Context) error, func(int64) error) (int64, error) {
		calls.Add(1)
		return 1, nil
	}
	id := billTaskID(t, billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks", `{"ticket":"`+ticket+`"}`))
	billTaskWait(t, h, ck, id, "failed")
	if calls.Load() != 0 {
		t.Fatal("background task bypassed CSV generation lock")
	}
	lease.release()
	other, otherLease, err := s.m.createPortalExportTempFile(portalExportMaxBytes)
	if err != nil {
		t.Fatal("generation lock leaked")
	}
	other.Close()
	os.Remove(other.Name())
	otherLease.release()
}

func TestPortalBillTasksMemberChange(t *testing.T) {
	s, h, ck, ticket := billTasksFixture(t)
	s.build = func(ctx context.Context, w io.Writer, _ portalBillScope, _ int64, _ func(context.Context) error, p func(int64) error) (int64, error) {
		if _, err := io.WriteString(w, "completed-private-bill"); err != nil {
			return 0, err
		}
		if err := p(1); err != nil {
			return 0, err
		}
		return 1, nil
	}
	id := billTaskID(t, billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks", `{"ticket":"`+ticket+`"}`))
	billTaskWait(t, h, ck, id, "ready")
	if err := s.m.storeDB.Where("user_id = ?", 101).Delete(&TrackedUser{}).Error; err != nil {
		t.Fatal(err)
	}
	w := billTaskRequest(h, ck, "GET", "/api/logs/bill/tasks/"+id+"/download", "")
	if w.Code == 200 || strings.Contains(w.Body.String(), "completed-private-bill") {
		t.Fatal("removed member bill leaked")
	}
}

func TestPortalBillTasksMySQLIntegration(t *testing.T) {
	// fixture itself requires explicit local-only opt-in and a Unix socket.
	source, _ := billSnapshotMySQLFixture(t, 2100)
	s, h, ck, ticket := billTasksFixture(t)
	s.m.prodDB = source.prodDB
	claim, ok := s.m.verifyPortalExportClaim(ticket, time.Now().Unix())
	if !ok {
		t.Fatal("fixture ticket")
	}
	claim.FromTs = 1699999999
	claim.ToTs = 1700086400
	claim.Total = 2100
	claim.StartCursor = 2101
	ticket, err := s.m.signPortalExportClaim(claim)
	if err != nil {
		t.Fatal(err)
	}
	id := billTaskID(t, billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks", `{"ticket":"`+ticket+`"}`))
	billTaskWait(t, h, ck, id, "ready")
	w := billTaskRequest(h, ck, "GET", "/api/logs/bill/tasks/"+id+"/download", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "local-2100") || !strings.Contains(w.Body.String(), "local-1,") {
		t.Fatalf("download failed: %d", w.Code)
	}
	if source.prodDB.Stats().InUse != 0 {
		t.Fatal("source retained after download")
	}
}
