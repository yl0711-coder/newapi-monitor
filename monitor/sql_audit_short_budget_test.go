package monitor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
)

// Capture the real short-budget SELECT, but explain it only AFTER the handler
// returns on this isolated fixture. Never relax the product's 500ms budget or
// make its latency include a diagnostic EXPLAIN. Originals remain marked skipped.
func TestSQLAuditNginxShortBudgetReads(t *testing.T) {
	if activeSQLPlanCapture == nil {
		t.Skip("requires explicit isolated audit report")
	}
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	rawID := "synthetic-offline-plan-request"
	in := validEvidenceBatch()
	in.Events[0].OneAPIIDHMAC = nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", rawID)
	if w := postNginxEvidence(t, m, in); w.Code != 200 {
		t.Fatal("synthetic evidence ingest failed", w.Code)
	}
	type stmt struct {
		q    string
		args []any
	}
	var observed []stmt
	if err := m.nginxEvidenceDB.Callback().Query().Before("gorm:query").Register("test:short-budget-sql", func(tx *gorm.DB) {
		if tx.Statement.Table != "nginx_request_evidences" && tx.Statement.Table != "nginx_evidence_source_states" {
			return
		}
		if tx.Statement.SQL.Len() == 0 {
			callbacks.BuildQuerySQL(tx)
		}
		observed = append(observed, stmt{tx.Statement.SQL.String(), append([]any{}, tx.Statement.Vars...)})
	}); err != nil {
		t.Fatal(err)
	}
	if h := m.nginxEvidenceHealth(context.Background(), time.Now()); !h.StoreReachable {
		t.Fatal("real health read failed")
	}
	for _, previous := range []bool{false, true} {
		if previous {
			m.cfg.NginxEvidencePreviousHMACKeyID = m.cfg.NginxEvidenceHMACKeyID
			m.cfg.NginxEvidencePreviousHMACKey = m.cfg.NginxEvidenceHMACKey
			m.cfg.NginxEvidenceHMACKeyID = "next-key"
			m.cfg.NginxEvidenceHMACKey = strings.Repeat("z", 32)
		}
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/nginx/evidence/lookup", strings.NewReader(`{"request_id":"`+rawID+`"}`))
		c.Request.Header.Set("Content-Type", "application/json")
		m.serveNginxEvidenceLookup(c)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"found":true`) {
			t.Fatal("real current/previous key read failed", w.Code)
		}
	}
	if len(observed) != 3 {
		t.Fatal("did not observe actual health/current/previous source SELECTs", len(observed))
	}
	for _, q := range observed {
		h := fmt.Sprintf("%x", sha256.Sum256([]byte(q.q)))
		capture := activeSQLPlanCapture
		var origin capturedSQLPlan
		capture.mu.Lock()
		for _, item := range capture.items {
			if item.Fingerprint == h && item.Status == "not_run_short_or_cancelled_budget" && item.File == "monitor/nginx_evidence.go" {
				origin = item
				break
			}
		}
		capture.mu.Unlock()
		if origin.File == "" {
			t.Fatal("post-request plan has no matched original source call")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var rows []struct{ Detail string }
		err := m.nginxEvidenceDB.WithContext(ctx).Raw("EXPLAIN QUERY PLAN "+q.q, q.args...).Scan(&rows).Error
		cancel()
		if err != nil || len(rows) == 0 {
			t.Fatal("isolated post-request plan failed", err)
		}
		origin.Fixture = t.Name() + "/offline-plan-after-handler"
		origin.Status = "explained"
		origin.Plan = nil
		for _, row := range rows {
			origin.Plan = append(origin.Plan, row.Detail)
		}
		capture.mu.Lock()
		capture.items = append(capture.items, origin)
		capture.mu.Unlock()
	}
}
