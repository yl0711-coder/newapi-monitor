package monitor

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type tokenAcquisitionCapturedRow struct {
	ID        int64           `json:"id"`
	CreatedAt int64           `json:"created_at"`
	UserID    int64           `json:"user_id"`
	ChannelID int64           `json:"channel_id"`
	RequestID string          `json:"request_id"`
	Model     string          `json:"model"`
	Group     string          `json:"group"`
	Input     string          `json:"input"`
	Output    string          `json:"output"`
	Quota     string          `json:"quota"`
	Stream    string          `json:"stream"`
	UseTime   string          `json:"use_time"`
	Other     json.RawMessage `json:"other"`
}

// Replays a bounded production capture in isolated SQLite databases. This
// proves the real operands and durable repair path, not source-window coverage
// or runtime latency on 8204.
func TestTokenAcquisitionCapturedRepairUpdatesReportAndDetail(t *testing.T) {
	file := os.Getenv("TOKEN_ACQUISITION_CAPTURE")
	if file == "" {
		t.Skip("bounded production source and old-fact capture not supplied")
	}
	read := func(name string, target any) {
		t.Helper()
		body, err := os.ReadFile(filepath.Join(filepath.Dir(file), name))
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(body, target); err != nil {
			t.Fatal(err)
		}
	}
	var captured struct {
		From int64                         `json:"from"`
		To   int64                         `json:"to"`
		Rows []tokenAcquisitionCapturedRow `json:"rows"`
	}
	read(filepath.Base(file), &captured)
	var policy tokenDetectionPolicy
	read("policy.json", &policy)
	if policy.SourceContract == nil || len(captured.Rows) == 0 {
		t.Fatal("capture policy or rows absent")
	}
	var prior []struct {
		TokenMeteringFact
		Facts    string `json:"source_facts"`
		Hash     string `json:"fact_hash"`
		Bytes    int64  `json:"stored_bytes"`
		Evidence string `json:"evidence_json"`
	}
	read("local-sample.json", &prior)
	m, _, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenFactsSourceEpoch, m.cfg.TokenDetectionPolicy = policy.SourceContract.Epoch, tokenDetectionJSON(policy)
	db := m.tokenStore()
	start, now := captured.From/300*300, captured.To+600
	if err := db.Create(&TokenMeteringState{SourceEpoch: m.cfg.TokenFactsSourceEpoch, SourceMode: "complete", StartAt: start, ScanThrough: start + 300, CompleteThrough: start + 300}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: start, EndAt: start + 300, Done: true, SourceComplete: true, NextAt: now + 1800}).Error; err != nil {
		t.Fatal(err)
	}
	for _, r := range captured.Rows {
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,created_at,type,user_id,channel_id,model_name,`group`,prompt_tokens,completion_tokens,quota,is_stream,use_time,other,request_id) VALUES(?,?,2,?,?,?,?,?,?,?,?,?,?,?)", r.ID, r.CreatedAt, r.UserID, r.ChannelID, r.Model, r.Group, r.Input, r.Output, r.Quota, r.Stream, r.UseTime, string(r.Other), r.RequestID); err != nil {
			t.Fatal(err)
		}
	}
	affected := map[int64]TokenMeteringFact{}
	for _, old := range prior {
		f := old.TokenMeteringFact
		f.SourceFacts, f.FactHash, f.StoredBytes, f.EvidenceJSON = old.Facts, old.Hash, old.Bytes, old.Evidence
		p := tokenMeteringDecode(t, f)
		if !tokenMeteringNeedsSourceReplay(f, p) || p.Fields["expr_b64"].State != "invalid" {
			continue
		}
		if err := db.Create(&f).Error; err != nil {
			t.Fatal(err)
		}
		affected[f.LogID] = f
	}
	if len(affected) < 2 {
		t.Fatal("affected captured facts not loaded", len(affected))
	}
	detectionRun(t, m, now)
	before := map[int64]TokenDetectionCurrent{}
	for id := range affected {
		h, result := detectionHead(t, m, id)
		if result.Price.Supported {
			t.Fatal("old missing operands already calculated", id)
		}
		before[id] = h
	}
	var requested int64
	if err := db.Table("token_metering_source_repairs_v1").Where("source_epoch=? AND start_at=?", m.cfg.TokenFactsSourceEpoch, start).Count(&requested).Error; err != nil || requested != 1 {
		t.Fatal("detection did not durably schedule actual source reread", requested, err)
	}
	for round := int64(0); round < 4; round++ {
		if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+round*10, 0)); err != nil {
			t.Fatal(err)
		}
	}
	for id, old := range affected {
		fact := tokenMeteringFactRead(t, m, id)
		if fact.ParserVersion != tokenMeteringParserVersion || fact.Revision <= old.Revision || tokenMeteringNeedsSourceReplay(fact, tokenMeteringDecode(t, fact)) {
			t.Fatal("actual reread failed to publish newer fields", id, fact.ParserVersion, fact.Revision)
		}
	}
	for round := int64(0); round < 3; round++ {
		detectionRun(t, m, now+40+round)
	}
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: captured.From, To: captured.To, Limit: 50}
	report, err := m.tokenDetectionReport(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	summary := report["summary"].(tokenDetectionSummary)
	if summary.Unprocessed != 0 || summary.RuleEligible["historical_price_difference"] != int64(len(captured.Rows)) {
		t.Fatal("repaired fields missing from latest summary", summary)
	}
	items := report["items"].([]map[string]any)
	proof := []map[string]any{}
	for _, item := range items {
		head := item["request"].(TokenDetectionCurrent)
		old, ok := before[head.LogID]
		if !ok {
			continue
		}
		result := item["result"].(tokenDetectionResult)
		display := item["display"].(tokenDetectionDisplay)
		if !result.Price.Supported || result.ActualQuota == nil || result.Price.Quota != *result.ActualQuota || head.Revision <= old.Revision || display.Stale {
			t.Fatal("list kept old decision", head.LogID, result.Price)
		}
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/detail", strings.NewReader(tokenDetectionJSON(map[string]any{"source_epoch": head.SourceEpoch, "log_id": head.LogID})))
		c.Request.Header.Set("Content-Type", "application/json")
		m.serveTokenDetectionDetail(c)
		var detail struct {
			Revision int64                 `json:"revision"`
			Display  tokenDetectionDisplay `json:"display"`
			Evidence tokenDetectionFrozen  `json:"evidence"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &detail) != nil || detail.Revision != head.Revision || detail.Evidence.Result.Price.Quota != result.Price.Quota || !reflect.DeepEqual(detail.Display.MissingFields, display.MissingFields) || !reflect.DeepEqual(detail.Display.DerivedOperands, display.DerivedOperands) {
			t.Fatal("list/detail revision, price or effective gaps disagree", head.LogID, w.Code)
		}
		proof = append(proof, map[string]any{"log_id": head.LogID, "request_id": display.RequestID, "before": old, "after": head, "price": result.Price, "missing": display.MissingFields, "acquisition": display.Acquisition})
	}
	if len(proof) != len(affected) {
		t.Fatal("repaired identities omitted", len(proof), len(affected))
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(file), "repair-pipeline-replay.json"), []byte(tokenDetectionJSON(map[string]any{"environment": "isolated_sqlite_replay_of_bounded_production_capture", "rows": proof, "summary": summary})), 0600); err != nil {
		t.Fatal(err)
	}
}
