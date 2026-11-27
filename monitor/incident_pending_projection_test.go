package monitor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Frozen complete-row reads, independent of the new projection helper.
func legacyIncidentPendingRead(m *Monitor, ctx context.Context, id string) (bool, string, error) {
	var summary IncidentPendingSummary
	res := m.storeDB.WithContext(ctx).Where("incident_id = ?", id).Limit(1).Find(&summary)
	if res.Error != nil {
		return false, "", fmt.Errorf("读事故待确认摘要失败: %w", res.Error)
	}
	if res.RowsAffected > 0 && summary.ScenarioPending {
		return true, summary.PendingReason, nil
	}
	var row IncidentIngestWindowState
	res = m.storeDB.WithContext(ctx).Where("incident_id = ? AND scenario_pending = ?", id, true).Limit(1).Find(&row)
	if res.Error != nil {
		return false, "", fmt.Errorf("读窗口待确认状态失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return false, "", nil
	}
	return true, row.PendingReason, nil
}

func TestSQLAuditIncidentPendingProjection(t *testing.T) {
	db, _ := sqlScaleDB(t, &IncidentPendingSummary{}, &IncidentIngestWindowState{})
	m := &Monitor{storeDB: db}
	for _, row := range []IncidentPendingSummary{
		{IncidentID: "summary", ScenarioPending: true, PendingReason: "summary-reason"},
		{IncidentID: "fallback", ScenarioPending: false, PendingReason: "must-not-win"},
		{IncidentID: "clear", ScenarioPending: false},
	} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i, row := range []struct {
		id      string
		pending bool
		reason  string
	}{
		{"summary", true, "window-must-not-win"}, {"fallback", true, "window-reason"},
		{"window-only", true, "window-only-reason"}, {"clear", false, "unused"},
		{"different-incident", true, "must-not-leak"},
	} {
		if err := db.Create(&IncidentIngestWindowState{DedupKey: fmt.Sprint(i), WindowToTs: int64(i + 1), IncidentID: row.id,
			ScenarioPending: row.pending, PendingReason: row.reason, CoverageProof: strings.Repeat("proof", 40)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	type observation struct {
		Case, Variant string
		US            []int64
	}
	var results []observation
	for _, id := range []string{"summary", "fallback", "window-only", "clear", "absent"} {
		want, reason, err := legacyIncidentPendingRead(m, context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		start := len(results)
		for _, name := range []string{"legacy_complete_rows", "current_projection"} {
			results = append(results, observation{Case: id, Variant: name})
		}
		for round := 0; round < 7; round++ {
			for j := 0; j < 2; j++ {
				i := j
				if round%2 != 0 {
					i = 1 - j
				}
				read := legacyIncidentPendingRead
				if i == 1 {
					read = func(m *Monitor, ctx context.Context, id string) (bool, string, error) {
						return m.incidentScenarioPendingByID(ctx, id)
					}
				}
				at := time.Now()
				got, why, err := read(m, context.Background(), id)
				us := time.Since(at).Microseconds()
				if err != nil || got != want || why != reason {
					t.Fatal("pending state or exact reason changed", id, got, why, err)
				}
				results[start+i].US = append(results[start+i].US, us)
			}
		}
	}
	// Missing window table must not affect an authoritative positive summary.
	// It must still fail for a member that needs the fallback, not become false.
	attachSQLAuditFixture(t, m)
	if err := db.Exec("DROP TABLE incident_ingest_window_states").Error; err != nil {
		t.Fatal(err)
	}
	if got, reason, err := m.incidentScenarioPendingByID(context.Background(), "summary"); err != nil || !got || reason != "summary-reason" {
		t.Fatal("authoritative summary incorrectly needed windows", err)
	}
	if _, _, err := m.incidentScenarioPendingByID(context.Background(), "fallback"); err == nil {
		t.Fatal("missing fallback became clear")
	}
	if err := db.Exec("DROP TABLE incident_pending_summaries").Error; err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.incidentScenarioPendingByID(context.Background(), "summary"); err == nil {
		t.Fatal("missing summary became clear")
	}
	source, err := os.ReadFile("incident_http.go")
	if err != nil {
		t.Fatal(err)
	}
	sqlScaleWrite(t, "incident-pending-projection-20261127.json", struct {
		Warning, SourceSHA256 string
		Results               []observation
	}{
		"Small synthetic state fixture, 7 alternating memory-SQLite samples, complete bool/reason equality. Not a large-data or production latency claim; N+1 and index choice remain separate review items.", fmt.Sprintf("%x", sha256.Sum256(source)), results})
}
