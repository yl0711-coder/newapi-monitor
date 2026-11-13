package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Optional private acceptance fixture. Only the caller's read-only archive is
// read; no customer payload is copied into the repository or test output.
type tokenCaseRow struct {
	ID        int64           `json:"log_id"`
	RequestID string          `json:"request_id"`
	CreatedAt int64           `json:"created_at"`
	ChannelID int64           `json:"channel_id"`
	Model     string          `json:"model"`
	Group     string          `json:"group"`
	Input     json.Number     `json:"input_tokens"`
	Output    json.Number     `json:"output_tokens"`
	Quota     json.Number     `json:"quota"`
	Stream    bool            `json:"is_stream"`
	UseTime   json.Number     `json:"use_time"`
	Other     json.RawMessage `json:"other"`
}

func tokenCaseRead(t *testing.T, root, name string, dst any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatal("invalid private fixture JSON", err)
	}
}

func TestTokenCaseArchiveReplay(t *testing.T) {
	root := os.Getenv("TOKEN_CASE_ARCHIVE_ROOT")
	if root == "" {
		t.Skip("requires privately mounted, read-only 2026-10-12 case archive")
	}
	base := filepath.Join(root, "核算及历史日志")
	evidenceRoot := filepath.Join(base, "Zmxt_dev-Token损失核算-2026-10-12-证据")
	var history, controls struct {
		UserID int64          `json:"user_id"`
		Rows   []tokenCaseRow `json:"rows"`
	}
	tokenCaseRead(t, evidenceRoot, "history.json", &history)
	tokenCaseRead(t, evidenceRoot, "controls.json", &controls)
	var labels struct {
		Candidates []tokenCaseRow `json:"four_s_line_items"`
		A6         tokenCaseRow   `json:"a6_paired_line_item"`
	}
	tokenCaseRead(t, base, "Zmxt_dev-Token异常与损失核算-2026-10-12.json", &labels)
	if len(history.Rows) != 9873 || len(controls.Rows) != 1088 || len(labels.Candidates) != 548 || history.UserID != controls.UserID {
		t.Fatal("unexpected archive scope")
	}
	m, _, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenFactsUsageAdapter = "unknown"
	policy := detectionPolicy()
	policy.Version, policy.SizeTolerance = "case-offline-v1", 64
	// These are archived parameters, not new production thresholds.
	rows := slices.Concat(history.Rows, controls.Rows)
	facts := make([]TokenMeteringFact, 0, len(rows))
	refs := make(map[int64][]TokenMeteringRef, len(rows))
	owners := map[string][]TokenMeteringRef{}
	seen := map[int64]bool{}
	for _, row := range rows {
		if row.ID <= 0 || seen[row.ID] {
			t.Fatal("duplicate or absent archive identity")
		}
		seen[row.ID] = true
		column := func(v string) sql.NullString { return sql.NullString{String: v, Valid: v != ""} }
		stream := "0"
		if row.Stream {
			stream = "1"
		}
		raw := tokenMeteringRaw{ID: row.ID, CreatedAt: row.CreatedAt, UserID: history.UserID, ChannelID: row.ChannelID,
			Model: row.Model, Group: row.Group, Input: column(row.Input.String()), Output: column(row.Output.String()), Quota: column(row.Quota.String()),
			Stream: column(stream), UseTime: column(row.UseTime.String()), Other: column(string(row.Other)), OtherBytes: int64(len(row.Other)), RequestID: row.RequestID}
		fact, ownRefs := parseTokenMetering(m.cfg, raw, row.CreatedAt+1)
		facts, refs[row.ID] = append(facts, fact), ownRefs
		for _, ref := range ownRefs {
			key := tokenMeteringRefKey(ref.KeyID, ref.Hash)
			owners[key] = append(owners[key], ref)
		}
	}
	events := map[string][]NginxRequestEvidence{}
	seenEvents := map[string]bool{}
	for _, file := range []string{"nginx-candidates.json", "more-nginx-candidates.json", "cached-nginx-candidates.json", "remaining-nginx-candidates.json", "controls-nginx-candidates.json"} {
		var archive struct {
			Rows []map[string]string `json:"rows"`
		}
		tokenCaseRead(t, evidenceRoot, file, &archive)
		for _, row := range archive.Rows {
			id := tokenDetectionHash(row)
			if seenEvents[id] {
				continue
			}
			seenEvents[id] = true
			at, err := time.Parse("2006-01-02 15:04:05", row["@timestamp"])
			if err != nil {
				t.Fatal("invalid archived event time")
			}
			length, err := strconv.ParseInt(row["request_length"], 10, 64)
			if err != nil || length <= 0 {
				t.Fatal("invalid archived request size")
			}
			status, err := strconv.Atoi(row["status"])
			if err != nil {
				t.Fatal("invalid archived status")
			}
			hash := nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", row["oneapi_request_id"])
			key := tokenMeteringRefKey(m.cfg.NginxEvidenceHMACKeyID, hash)
			events[key] = append(events[key], NginxRequestEvidence{EventID: id, EventMS: at.UnixMilli(), Node: "private-archive", Route: row["uri"],
				Method: row["request_method"], Status: status, HMACKeyID: m.cfg.NginxEvidenceHMACKeyID, OneAPIIDHMAC: hash,
				RequestLength: &length, RequestLengthState: "valid", RequestLengthVersion: "request-length-v1"})
		}
	}
	byID := map[int64]TokenMeteringFact{}
	linked, normalized := 0, 0
	for i, fact := range facts {
		facts[i] = resolveTokenMeteringLink(fact, refs[fact.LogID], events, owners, false, "verified", fact.CreatedAt+3600)
		if facts[i].LinkState == "unique" {
			linked++
		}
		if tokenMeteringDecode(t, facts[i]).TotalInput != nil {
			normalized++
		}
		byID[fact.LogID] = facts[i]
	}
	if err := m.tokenStore().CreateInBatches(facts, 128).Error; err != nil {
		t.Fatal(err)
	}
	type summary struct {
		Rows       int            `json:"rows"`
		Comparable int            `json:"comparable"`
		Candidates int            `json:"input_candidates"`
		Reasons    map[string]int `json:"reference_reasons"`
	}
	witnesses := map[int64]string{}
	witnessCounts := map[string]int{}
	evaluate := func(labelRows []tokenCaseRow, stage int) summary {
		t.Helper()
		s := summary{Rows: len(labelRows), Reasons: map[string]int{}}
		for _, row := range labelRows {
			fact, ok := byID[row.ID]
			if !ok {
				t.Fatal("label missing from original history")
			}
			in, err := tokenDetectionInputOf(fact)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := tokenReferenceFrom(m.tokenStore(), in, policy, stage, row.CreatedAt+3600)
			if err != nil {
				t.Fatal(err)
			}
			result := tokenDetectionEvaluate(in, policy, ref, 0, false)
			if result.SourceComplete || result.EstimatedLoss != nil || in.Usage.Fields["quota_per_unit"].State != "missing" {
				t.Fatal("partial archive or absent monetary evidence became complete")
			}
			s.Reasons[ref.Reason]++
			if result.Rules[1].Eligible {
				s.Comparable++
			}
			if result.Rules[1].Hit {
				s.Candidates++
			}
			kind := ""
			if len(labelRows) == 548 && stage == 0 && result.Rules[1].Hit {
				kind = "input_candidate"
			} else if len(labelRows) == 548 && stage == 3 && !result.Rules[1].Eligible {
				kind = "reference_insufficient"
			} else if len(labelRows) == 1088 && result.Rules[1].Eligible && !result.Rules[1].Hit {
				kind = "control"
			}
			if kind != "" && witnessCounts[kind] < 4 && witnesses[row.ID] == "" {
				witnesses[row.ID] = kind
				witnessCounts[kind]++
			}
		}
		return s
	}
	result := map[string]any{"facts": len(facts), "linked": linked, "normalized": normalized,
		"source_coverage": "partial_archive", "money_fields": "unavailable_without_historical_binding"}
	for _, stage := range []int{0, 1, 2, 3} {
		result["candidates_stage_"+strconv.Itoa(stage)] = evaluate(labels.Candidates, stage)
	}
	result["controls_stage_2"] = evaluate(controls.Rows, 2)
	result["a6_stage_3"] = evaluate([]tokenCaseRow{labels.A6}, 3)
	t.Log("ARCHIVE_REPLAY " + tokenDetectionJSON(result))
	if result["candidates_stage_3"].(summary).Candidates == 0 {
		t.Fatal("known input inflation cases were all excluded")
	}
	if result["controls_stage_2"].(summary).Candidates != 0 {
		t.Fatal("archived controls became input inflation candidates")
	}
	witnesses[labels.A6.ID] = "a6_candidate"
	tokenCaseWorkerAndAPI(t, m, policy, byID, refs, witnesses)
}

// Run representative originals through the same fenced transaction used by the
// background worker, then read persisted list, summaries and frozen details.
// The entire partial archive remains available for reference selection. This
// test does not mark any of the other archived requests as already processed.
func tokenCaseWorkerAndAPI(t *testing.T, m *Monitor, p tokenDetectionPolicy, facts map[int64]TokenMeteringFact, refs map[int64][]TokenMeteringRef, witnesses map[int64]string) {
	t.Helper()
	ctx := context.Background()
	m.prodDB = nil // All API reads must use local facts and persisted decisions.
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(p)
	if len(witnesses) != 13 {
		t.Fatal("representative case categories are incomplete", len(witnesses))
	}
	if err := m.ensureTokenDetectionCapacity(ctx); err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Update("source_mode", "partial_archive").Error; err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 0, len(witnesses))
	for id := range witnesses {
		ids = append(ids, id)
		if len(refs[id]) > 0 {
			if err := m.tokenStore().Create(refs[id]).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return facts[ids[i]].CreatedAt < facts[ids[j]].CreatedAt })
	for _, delay := range []int64{1, 301, 601, 3601} {
		for _, id := range ids {
			at := facts[id].CreatedAt + delay
			state := TokenDetectionState{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LeaseToken: "private-case-worker", LeaseUntil: at + 30}
			if err := m.tokenStore().Save(&state).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.processTokenDetection(ctx, state.SourceEpoch, id, p, state.LeaseToken, at); err != nil {
				t.Fatal("case worker persistence", err)
			}
		}
	}
	wantCandidates := 0
	for _, id := range ids {
		_, r := detectionHead(t, m, id)
		if r.SourceComplete || r.EstimatedLoss != nil || r.Price.Supported {
			t.Fatal("partial case produced complete coverage or money")
		}
		if (witnesses[id] == "input_candidate" || witnesses[id] == "a6_candidate") && !r.Rules[1].Hit {
			t.Fatal("known candidate lost between evaluation and persisted worker result", witnesses[id], r.Rules[1], r.Reference.Reason, r.Reference.SampleCount)
		}
		if witnesses[id] == "control" && r.Rules[1].Hit {
			t.Fatal("persisted control became an input inflation candidate")
		}
		if r.Candidate {
			wantCandidates++
		}
	}
	router := gin.New()
	router.POST("/query", m.serveTokenDetectionQuery)
	router.POST("/detail", m.serveTokenDetectionDetail)
	request := func(route string, payload, dst any) json.RawMessage {
		t.Helper()
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", route, strings.NewReader(tokenDetectionJSON(payload)))
		r.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal("local case API failed", route, w.Code)
		}
		if err := json.Unmarshal(w.Body.Bytes(), dst); err != nil {
			t.Fatal(err)
		}
		return append(json.RawMessage{}, w.Body.Bytes()...)
	}
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: facts[ids[0]].CreatedAt - 1, To: facts[ids[len(ids)-1]].CreatedAt + 1, Limit: 2, Status: "candidate"}
	pages, details := []json.RawMessage{}, []json.RawMessage{}
	seen := map[int64]bool{}
	for page := 0; page < len(ids); page++ {
		var response struct {
			Summary tokenDetectionSummary `json:"summary"`
			HasMore bool                  `json:"has_more"`
			Items   []struct {
				Request TokenDetectionCurrent `json:"request"`
				Result  tokenDetectionResult  `json:"result"`
			} `json:"items"`
		}
		pages = append(pages, request("/query", q, &response))
		if response.Summary.Candidates != int64(wantCandidates) || response.Summary.SourceComplete || response.Summary.Unprocessed == 0 || len(response.Summary.Amounts) != 0 {
			t.Fatal("persisted API summary changed counts, coverage or absent monetary identity")
		}
		for _, item := range response.Items {
			if seen[item.Request.LogID] || !item.Result.Candidate {
				t.Fatal("candidate pagination duplicated or misclassified a request")
			}
			seen[item.Request.LogID] = true
			q.AfterAt, q.AfterID = item.Request.CreatedAt, item.Request.LogID
			var detail struct {
				Evidence tokenDetectionFrozen `json:"evidence"`
			}
			details = append(details, request("/detail", map[string]any{"source_epoch": q.SourceEpoch, "log_id": item.Request.LogID, "revision": item.Request.Revision}, &detail))
			if !detail.Evidence.Result.Candidate || detail.Evidence.Input.Fact.LogID != item.Request.LogID || detail.Evidence.Input.Usage.Fields["quota_per_unit"].State != "missing" {
				t.Fatal("frozen detail does not reproduce its listed decision")
			}
		}
		if !response.HasMore {
			break
		}
	}
	if len(seen) != wantCandidates {
		t.Fatal("candidate page total disagrees with stored worker results")
	}
	if output := os.Getenv("TOKEN_CASE_RESULT_PATH"); output != "" {
		data := tokenDetectionJSON(map[string]any{"scope": "private partial archive through production worker and read APIs", "pages": pages, "details": details})
		if err := os.WriteFile(output, []byte(data+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("ARCHIVE_WORKER_API " + tokenDetectionJSON(map[string]any{"persisted_requests": len(witnesses), "candidate_requests": wantCandidates, "pages": len(pages), "details": len(details), "coverage": "partial_archive", "source_access": false}))
}
