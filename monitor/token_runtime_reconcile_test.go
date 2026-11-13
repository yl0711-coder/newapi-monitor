package monitor

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// Reuse an archived, bounded production read against the private retained
// fixture. This helper never connects to the source or alters coverage.
func tokenRuntimeReconcileArchivedSource(t *testing.T, m *Monitor) {
	t.Helper()
	path := os.Getenv("TOKEN_TEST_FIXED_SOURCE")
	if path == "" {
		return
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	type sourceRow struct {
		ID        int64 `json:"id"`
		CreatedAt int64 `json:"created_at"`
		UserID    int64 `json:"user_id"`
		ChannelID int64 `json:"channel_id"`
		Model     string
		Group     string
		Input     sql.NullString
		Output    sql.NullString
		Quota     sql.NullString
	}
	var source struct {
		From, To int64
		ReadOnly bool `json:"read_only"`
		Success  bool
		Stage    string
		Rows     []sourceRow
	}
	if err := json.Unmarshal(body, &source); err != nil || !source.ReadOnly || !source.Success || source.Stage != "complete" || source.From <= 0 || source.To <= source.From || source.To-source.From > 300 || len(source.Rows) == 0 || len(source.Rows) > 5000 {
		t.Fatal("invalid bounded archived source evidence", err)
	}
	var facts []TokenMeteringFact
	if err := m.tokenReadStore().Where("source_epoch=? AND created_at>=? AND created_at<?", m.cfg.TokenFactsSourceEpoch, source.From, source.To).Order("created_at,log_id").Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != len(source.Rows) {
		t.Fatalf("source/facts population differs: source=%d facts=%d", len(source.Rows), len(facts))
	}
	byID := map[int64]sourceRow{}
	for _, row := range source.Rows {
		if _, duplicate := byID[row.ID]; duplicate || row.CreatedAt < source.From || row.CreatedAt >= source.To {
			t.Fatal("source evidence has duplicate or out-of-range identity")
		}
		byID[row.ID] = row
	}
	for _, fact := range facts {
		row, found := byID[fact.LogID]
		if !found || row.CreatedAt != fact.CreatedAt || row.UserID != fact.UserID || row.ChannelID != fact.ChannelID || row.Model != fact.Model || row.Group != fact.UserGroup {
			t.Fatal("source/facts identity or dimensions differ", fact.LogID)
		}
		var parsed tokenMeteringParsed
		if err := json.Unmarshal([]byte(fact.SourceFacts), &parsed); err != nil {
			t.Fatal(err)
		}
		for name, expected := range map[string]sql.NullString{"prompt_tokens": row.Input, "completion_tokens": row.Output, "quota": row.Quota} {
			field := parsed.Fields[name]
			if expected.Valid && (field.State != "valid" || field.Value != expected.String) || !expected.Valid && field.State != "null" {
				t.Fatal("source/facts numeric operand differs", fact.LogID, name)
			}
		}
	}
	policy, err := parseTokenDetectionPolicy(m.cfg.TokenDetectionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	var counts struct{ Heads, Pending int64 }
	// Independent base-table join, without using the materialized pending
	// groups or the production predicate builder under test.
	if err := m.tokenReadStore().Raw(`SELECT COUNT(c.log_id) AS heads,
		COALESCE(SUM(c.log_id IS NULL OR c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR c.policy_hash!=?
		OR c.reference_generation!=COALESCE(b.generation,0)
		OR c.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END),0) AS pending
		FROM token_metering_facts f LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id
		LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket
		LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=f.created_at/300*300
		WHERE f.source_epoch=? AND f.created_at>=? AND f.created_at<?`, tokenDetectionPolicyHash(policy), m.cfg.TokenFactsSourceEpoch, source.From, source.To).Scan(&counts).Error; err != nil {
		t.Fatal(err)
	}
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: source.From, To: source.To, Limit: 100}
	seen := map[int64]bool{}
	var summary tokenDetectionSummary
	for pages := 0; ; pages++ {
		if pages > 50 {
			t.Fatal("bounded API pagination did not terminate")
		}
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/anomalies/token/detection/query", strings.NewReader(tokenDetectionJSON(q)))
		c.Request.Header.Set("Content-Type", "application/json")
		m.serveTokenDetectionQuery(c)
		var response struct {
			Available bool
			HasMore   bool `json:"has_more"`
			Summary   tokenDetectionSummary
			Items     []struct {
				Request TokenDetectionCurrent
				Result  tokenDetectionResult
				Display tokenDetectionDisplay
			}
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 || !response.Available {
			t.Fatal("fixed-window detection API unavailable", w.Code, err)
		}
		summary = response.Summary
		for _, item := range response.Items {
			h := item.Request
			src, found := byID[h.LogID]
			if !found || seen[h.LogID] || src.CreatedAt != h.CreatedAt || src.UserID != h.UserID || src.ChannelID != h.ChannelID || src.Model != h.Model || src.Group != h.UserGroup {
				t.Fatal("fixed-window API identity mismatch", h.LogID)
			}
			if src.Quota.Valid && (item.Result.ActualQuota == nil || *item.Result.ActualQuota != src.Quota.String) || !src.Quota.Valid && item.Result.ActualQuota != nil {
				t.Fatal("API quota differs from source", h.LogID)
			}
			if item.Display.Assessment == nil || item.Display.Assessment.UpstreamConfirmed {
				t.Fatal("missing assessment or invented upstream evidence", h.LogID)
			}
			seen[h.LogID] = true
			q.AfterAt, q.AfterID = h.CreatedAt, h.LogID
		}
		if !response.HasMore {
			break
		}
	}
	if int64(len(seen)) != counts.Heads || summary.Observed != counts.Heads || summary.Unprocessed != counts.Pending {
		t.Fatal("fixed-window API/base-table reconciliation differs", len(seen), counts, summary)
	}
	for _, gap := range []struct {
		needed bool
		name   string
	}{{counts.Pending > 0, "detection_pending_or_stale"}, {summary.Insufficient > 0, "evidence_insufficient"}, {!summary.SourceComplete, "source_range_incomplete"}} {
		if gap.needed != slices.Contains(summary.Gaps, gap.name) {
			t.Fatal("fixed-window gap misreported", gap.name, summary)
		}
	}
	t.Logf("archived_source_reconciliation sha256=%x from=%d to=%d source=%d facts=%d api_results=%d no_result=%d pending_or_stale=%d candidates=%d insufficient=%d source_complete=%t gaps=%v", sha256.Sum256(body), source.From, source.To, len(source.Rows), len(facts), len(seen), int64(len(facts))-counts.Heads, counts.Pending, summary.Candidates, summary.Insufficient, summary.SourceComplete, summary.Gaps)
}
