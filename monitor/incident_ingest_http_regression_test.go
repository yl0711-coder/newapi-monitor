package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// 上轮 review 第 8 项：详情的各子查询失败也不能被显示成无影响/无修订。
func TestIncidentB3DetailSubqueryErrorsAreUnavailable(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	if err := m.commitIncidentWindowDecision(context.Background(), core, ingest, d, d.WindowToTs+1500); err != nil {
		t.Fatal(err)
	}
	row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	for _, tc := range []struct{ table, field string }{
		{"incident_impacts", "impacts"},
		{"incident_impact_windows", "impacts_archived"},
		{"incident_ingest_revisions", "revisions"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			const cb = "b3:detail_query_failure"
			if err := m.storeDB.Callback().Query().Before("gorm:query").Register(cb, func(tx *gorm.DB) {
				if tx.Statement.Table == tc.table {
					_ = tx.AddError(errors.New("injected detail query failure"))
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(cb) })
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodGet, "/incidents/detail?id="+row.IncidentID, nil)
			m.serveIncidentDetail(c)
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if rec.Code != http.StatusServiceUnavailable || body["data_status"] != "unavailable" || body["failed_field"] != tc.field {
				t.Fatalf("failed query must not masquerade as an empty result: status=%d body=%s", rec.Code, rec.Body)
			}
		})
	}
}

func TestIncidentB3PendingQueryErrorIsNotFalse(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	if err := m.commitIncidentWindowDecision(context.Background(), core, ingest, d, d.WindowToTs+1500); err != nil {
		t.Fatal(err)
	}
	const cb = "b3:pending_summary_query_failure"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(cb, func(tx *gorm.DB) {
		if tx.Statement.Table == "incident_pending_summaries" {
			_ = tx.AddError(errors.New("injected pending summary query failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(cb) })
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/incidents", nil)
	m.serveIncidentList(c)
	var body struct {
		Incidents []map[string]any `json:"incidents"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || len(body.Incidents) != 1 {
		t.Fatalf("unexpected list response: status=%d body=%s", rec.Code, rec.Body)
	}
	item := body.Incidents[0]
	if item["scenario_pending"] != nil || item["scenario_pending_status"] != "unavailable" {
		t.Fatalf("query failure must remain unknown instead of false: %+v", item)
	}
}
