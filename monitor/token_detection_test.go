package monitor

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/metering"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func detectionPolicy() tokenDetectionPolicy {
	return tokenDetectionPolicy{Version: "synthetic-calibration-v1", MinSamples: 3, MinChannels: 2, LookbackSeconds: 3600, SizeTolerance: 16, SizeRelative: "0.01", InputRatio: "5", MinInput: 20000, CacheShareDrop: "0.5", CacheIntensityRatio: "2", CacheMinSustained: 2, PricingAdapter: "newapi-text-v1"}
}
func detectionFixture(t *testing.T) (*Monitor, int64, string) {
	t.Helper()
	m, now, path := tokenMeteringFixture(t)
	if err := m.storeDB.AutoMigrate(&TokenDetectionState{}, &TokenDetectionRetry{}, &TokenDetectionBucket{}, &TokenDetectionCurrent{}, &TokenDetectionRevision{}, &TokenDetectionReference{}); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenDetectionEnabled = true
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(detectionPolicy())
	if err := initTokenDetectionReportIndex(m.storeDB); err != nil {
		t.Fatal(err)
	}
	return m, now, path
}
func detectionFact(t *testing.T, m *Monitor, id, at, ch, input, read, delta int64) TokenMeteringFact {
	t.Helper()
	other := map[string]any{"claude": true, "cache_tokens": read, "cache_creation_tokens": 0, "cache_creation_tokens_5m": 0, "cache_creation_tokens_1h": 0, "cache_ratio": "0.1", "cache_creation_ratio": "1.25", "cache_creation_ratio_5m": "1.25", "cache_creation_ratio_1h": "2", "model_ratio": "0.5", "model_price": 0, "completion_ratio": 2, "effective_group_ratio": 1, "group_ratio": 3, "user_group_ratio": 7, "billing_mode": "ratio", "billing_source": "wallet", "currency": "USD", "quota_per_unit": 500000, "rounding_mode": "half_up", "image": false, "is_task": false, "request_rules": []any{}, "request_path": "/v1/messages", "is_model_mapped": false}
	other["usage_semantic"] = "anthropic"
	raw := tokenMeteringRawFixture(tokenDetectionJSON(other))
	raw.ID = id
	raw.CreatedAt = at
	raw.ChannelID = ch
	raw.UserID = 2
	raw.Model = "fixture-model"
	raw.Group = "actual-group"
	raw.Input = sql.NullString{String: strconv.FormatInt(input, 10), Valid: true}
	raw.Output = sql.NullString{String: "2", Valid: true}
	raw.RequestID = fmt.Sprintf("fixture-%d", id)
	f, refs := parseTokenMetering(m.cfg, raw, at+1)
	u := tokenMeteringDecode(t, f)
	price := tokenPrice(u, detectionPolicy(), big.NewRat(1, 1))
	if !price.Supported {
		t.Fatalf("fixture price: %+v", price)
	}
	q, _ := strconv.ParseInt(price.Quota, 10, 64)
	raw.Quota = sql.NullString{String: strconv.FormatInt(q+delta, 10), Valid: true}
	f, refs = parseTokenMetering(m.cfg, raw, at+1)
	length := int64(1000)
	f.RequestLength = &length
	f.LengthState = "valid"
	f.LinkState = "unique"
	f.EvidenceLevel = "exact"
	f.EvidenceJSON = tokenDetectionJSON([]tokenMeteringCandidate{{EventID: fmt.Sprintf("fixture-entry-%d", id), Node: "synthetic", KeyID: "key-1", EventMS: at * 1000, Status: 200, Route: "/v1/responses", Length: &length, LengthState: "valid", LengthVersion: "request-length-v1"}})
	f.LinkRevision = 1
	f.LinkNextAt = 0
	if err := m.tokenStore().Create(&f).Error; err != nil {
		t.Fatal(err)
	}
	if len(refs) > 0 {
		if err := m.tokenStore().Create(&refs).Error; err != nil {
			t.Fatal(err)
		}
	}
	return f
}
func detectionRun(t *testing.T, m *Monitor, now int64) {
	t.Helper()
	if _, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal(err)
	}
}
func detectionHead(t *testing.T, m *Monitor, id int64) (TokenDetectionCurrent, tokenDetectionResult) {
	t.Helper()
	var h TokenDetectionCurrent
	if err := m.storeDB.First(&h, "source_epoch = ? AND log_id = ?", m.cfg.TokenFactsSourceEpoch, id).Error; err != nil {
		t.Fatal(err)
	}
	var r tokenDetectionResult
	if err := json.Unmarshal([]byte(h.ResultJSON), &r); err != nil {
		t.Fatal(err)
	}
	return h, r
}
func detectionSeed(t *testing.T, m *Monitor, now int64) {
	t.Helper()
	for i := int64(1); i <= 3; i++ {
		detectionFact(t, m, i, now-600+i, 1+i%2, 10000, 0, 0)
	}
}
func TestTokenDetectionHistoricalFormulaPrecisionAndUnsupported(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-20, 1, 9007199254740993, 0, 0)
	u := tokenMeteringDecode(t, f)
	p := detectionPolicy()
	r := tokenPrice(u, p, big.NewRat(1, 1))
	if !r.Supported || r.Quota != "4503599627370499" {
		t.Fatalf("integer precision or duplicated group ratio: %+v", r)
	}
	for _, field := range []string{"effective_group_ratio", "quota_per_unit", "rounding_mode", "completion_ratio", "model_ratio", "cache_creation_ratio_1h"} {
		copy := u
		copy.Fields = map[string]metering.Field{}
		for k, v := range u.Fields {
			copy.Fields[k] = v
		}
		delete(copy.Fields, field)
		if got := tokenPrice(copy, p, big.NewRat(1, 1)); got.Supported {
			t.Fatalf("missing %s calculated", field)
		}
	}
	u.Fields["billing_source"] = metering.Field{State: "valid", Value: "subscription"}
	r = tokenPrice(u, p, big.NewRat(1, 1))
	if r.Supported || r.Subject == "customer_wallet" {
		t.Fatalf("subject mixed %+v", r)
	}
}
func TestTokenDetectionTierCrossingAndOpenAIContainment(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-20, 1, 200, 0, 0)
	u := tokenMeteringDecode(t, f)
	expr := `len <= 100 ? tier("short",p*1+c*2) : tier("long",p*3+c*2)`
	encoded, _ := json.Marshal(base64.StdEncoding.EncodeToString([]byte(expr)))
	u.Fields["expr_b64"] = tokenMeteringExpression(encoded)
	u.Fields["billing_mode"] = metering.Field{State: "valid", Value: "tiered_expr"}
	u.Fields["matched_tier"] = metering.Field{State: "valid", Value: "long"}
	full := tokenPrice(u, detectionPolicy(), big.NewRat(1, 1))
	ref := tokenPrice(u, detectionPolicy(), big.NewRat(1, 2))
	if !full.Supported || !ref.Supported || full.Quota != "302" || ref.Quota != "52" || ref.Tier != "short" {
		t.Fatalf("crossing: %+v %+v", full, ref)
	}
	u.Semantic = "openai_inclusive"
	u.TotalInput = ptrTokenMetering(200)
	u.UnassignedCacheWrite = nil
	u.Fields["cache_tokens"] = metering.Field{State: "valid", Value: "50"}
	encoded, _ = json.Marshal(base64.StdEncoding.EncodeToString([]byte(`tier("base",p+cr*0.1+c*2)`)))
	u.Fields["expr_b64"] = tokenMeteringExpression(encoded)
	u.Fields["matched_tier"] = metering.Field{State: "valid", Value: "base"}
	if r := tokenPrice(u, detectionPolicy(), big.NewRat(1, 1)); !r.Supported || r.Quota != "80" {
		t.Fatalf("inclusive cache counted twice %+v", r)
	}
	u.Fields["cache_tokens"] = metering.Field{State: "valid", Value: "500"}
	if r := tokenPrice(u, detectionPolicy(), big.NewRat(1, 1)); r.Supported {
		t.Fatal("overlap must not be fabricated")
	}
}
func TestTokenDetectionReferenceNoFutureLeakAndRevision(t *testing.T) {
	m, now, _ := detectionFixture(t)
	target := detectionFact(t, m, 10, now-200, 3, 100000, 0, 0)
	for i := int64(1); i <= 3; i++ {
		detectionFact(t, m, i, now-190+i, 1+i%2, 10000, 0, 0)
	}
	detectionRun(t, m, now)
	h, r := detectionHead(t, m, target.LogID)
	if r.Rules[1].Eligible || r.Candidate || h.Stage != 0 {
		t.Fatalf("future reference leaked %+v", r)
	}
	detectionRun(t, m, now+301)
	h, r = detectionHead(t, m, 10)
	if h.Revision < 2 || !r.Rules[1].Hit || r.Rules[1].EvidenceLevel != "inferred" || r.EstimatedLoss == nil {
		t.Fatalf("late reference did not revise %+v %+v", h, r)
	}
	before := h.Revision
	detectionRun(t, m, now+302)
	h, _ = detectionHead(t, m, 10)
	if h.Revision != before {
		t.Fatal("identical round appended revision")
	}
}
func TestTokenDetectionCaseNormalDedupAndFrozenRecompute(t *testing.T) {
	m, now, path := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-200, 3, 100000, 0, 0)
	detectionFact(t, m, 11, now-190, 3, 100000, 0, 5)
	detectionFact(t, m, 12, now-180, 3, 10000, 0, 0)
	detectionRun(t, m, now)
	h, r := detectionHead(t, m, 10)
	_, normal := detectionHead(t, m, 12)
	if !r.Candidate || normal.Candidate || r.EstimatedLoss == nil || *r.EstimatedLoss != "45000" {
		t.Fatalf("case/normal %+v %+v", r, normal)
	}
	before := h.Revision
	detectionRun(t, m, now+1)
	h, _ = detectionHead(t, m, 10)
	if h.Revision != before {
		t.Fatal("duplicate revisions")
	}
	var rows []TokenDetectionCurrent
	if err := m.storeDB.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	sum, err := tokenDetectionSummarize(rows)
	if err != nil || sum.Candidates != 2 {
		t.Fatalf("dedup %+v %v", sum, err)
	}
	actual := int64(0)
	for _, a := range sum.Amounts {
		if a.Method == "observed_consumption_not_loss" {
			actual, _ = strconv.ParseInt(a.Quota, 10, 64)
		}
	}
	if actual != 100009 {
		t.Fatalf("actual quota %d", actual)
	}
	var rev TokenDetectionRevision
	if err := m.storeDB.First(&rev, "source_epoch = ? AND log_id = ? AND revision = ?", h.SourceEpoch, h.LogID, h.Revision).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Where("source_epoch = ?", h.SourceEpoch).Delete(&TokenMeteringFact{}).Error; err != nil {
		t.Fatal(err)
	}
	reopened, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := reopened.DB()
	defer func() { _ = conn.Close() }()
	var saved TokenDetectionRevision
	if err := reopened.First(&saved, "source_epoch = ? AND log_id = ? AND revision = ?", h.SourceEpoch, h.LogID, h.Revision).Error; err != nil {
		t.Fatal(err)
	}
	var frozen tokenDetectionFrozen
	if err := json.Unmarshal([]byte(saved.FrozenJSON), &frozen); err != nil {
		t.Fatal(err)
	}
	again := tokenDetectionEvaluate(frozen.Input, frozen.Policy, frozen.Result.Reference, frozen.CachePriorSignals, frozen.Result.SourceComplete)
	if tokenDetectionJSON(again) != tokenDetectionJSON(frozen.Result) {
		t.Fatal("cannot recompute after source retention")
	}
}
func TestTokenDetectionCommonShiftCannotReseedAnchor(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-200, 3, 10000, 0, 0)
	detectionRun(t, m, now)
	// All new channels shift; the original anchor must survive the rolling window.
	for i := int64(20); i < 24; i++ {
		detectionFact(t, m, i, now+4000+i, 1+i%3, 100000, 0, 0)
	}
	detectionRun(t, m, now+4300)
	_, r := detectionHead(t, m, 23)
	if !r.Rules[1].Hit || r.Reference.Upper == nil || *r.Reference.Upper != 10000 {
		t.Fatalf("shift learned as normal %+v", r)
	}
}
func TestTokenDetectionReferenceConflictAndLowSamples(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-200, 3, 10000, 0, 0)
	detectionRun(t, m, now)
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id = ?", 1).Updates(map[string]any{"link_state": "conflict", "link_revision": 2}).Error; err != nil {
		t.Fatal(err)
	}
	detectionFact(t, m, 11, now-100, 3, 100000, 0, 0)
	detectionRun(t, m, now+1)
	_, r := detectionHead(t, m, 11)
	if r.Rules[1].Eligible || r.Reference.Reason != "anchor_evidence_changed_requires_review" {
		t.Fatalf("conflict reference trusted %+v", r.Reference)
	}
}
func TestTokenDetectionCacheShiftSustainedAndNoFakeLoss(t *testing.T) {
	m, now, _ := detectionFixture(t)
	for i := int64(1); i <= 3; i++ {
		detectionFact(t, m, i, now-600+i, 1+i%2, 1000, 9000, 0)
	}
	detectionFact(t, m, 10, now-200, 3, 10000, 0, 0)
	detectionFact(t, m, 11, now-100, 3, 10000, 0, 0)
	detectionRun(t, m, now)
	// Newest-first work can observe the second request before the first signal
	// commits. Scope generation must requeue it and converge without new facts.
	detectionRun(t, m, now+1)
	detectionRun(t, m, now+2)
	_, first := detectionHead(t, m, 10)
	_, second := detectionHead(t, m, 11)
	if !first.CacheSignal || first.Rules[2].Hit || !second.Rules[2].Hit || second.EstimatedLoss != nil {
		t.Fatalf("cache changes: %+v %+v", first, second)
	}
}
func TestTokenDetectionMissingInvalidUnsetAndDisabled(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	u := tokenMeteringDecode(t, f)
	for _, state := range []string{"missing", "null", "invalid", "sentinel"} {
		v := u
		v.Fields = map[string]metering.Field{}
		for k, f := range u.Fields {
			v.Fields[k] = f
		}
		v.Fields["prompt_tokens"] = metering.Field{State: state}
		v.TotalInput = nil
		r := tokenFormatRule(v)
		if r.Hit != (state == "invalid" || state == "sentinel") {
			t.Fatalf("%s %+v", state, r)
		}
	}
	m.cfg.TokenDetectionEnabled = false
	detectionRun(t, m, now)
	var count int64
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("disabled worker ran")
	}
	p := detectionPolicy()
	p.InputRatio = ""
	in, _ := tokenDetectionInputOf(f)
	r := tokenDetectionEvaluate(in, p, tokenReference{}, 0, false)
	if r.Rules[1].Reason != "threshold_unconfigured" {
		t.Fatal("made up threshold")
	}
}
func TestTokenDetectionWriteFailureRollbackAndRestart(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	if err := m.storeDB.Exec("CREATE TRIGGER fail_detection BEFORE INSERT ON token_detection_revisions BEGIN SELECT RAISE(ABORT,'fixture'); END").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenDetectionRound(context.Background(), now); err == nil {
		t.Fatal("failure swallowed")
	}
	var count int64
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("partial commit")
	}
	if err := m.storeDB.Exec("DROP TRIGGER fail_detection").Error; err != nil {
		t.Fatal(err)
	}
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB}
	detectionRun(t, restarted, now+1)
	h, _ := detectionHead(t, m, 1)
	if h.Revision != 1 {
		t.Fatal("restart duplicated revision")
	}
}
func TestTokenDetectionConcurrentRoundsAndStaleLease(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := m.runTokenDetectionRound(context.Background(), now); errs <- e }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	h, _ := detectionHead(t, m, 1)
	if h.Revision != 1 {
		t.Fatal("concurrent duplicate")
	}
	if e := m.processTokenDetection(context.Background(), m.cfg.TokenFactsSourceEpoch, 1, detectionPolicy(), "stale", now); !errors.Is(e, errTokenDetectionLease) {
		t.Fatalf("stale writer accepted %v", e)
	}
}
func TestTokenDetectionReadOnlyQueriesAndPagination(t *testing.T) {
	m, now, _ := detectionFixture(t)
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-100, 3, 100000, 0, 0)
	detectionRun(t, m, now)
	m.cfg.TokenDetectionEnabled = false
	m.prodDB = nil
	r := gin.New()
	r.POST("/query", m.serveTokenDetectionQuery)
	r.POST("/detail", m.serveTokenDetectionDetail)
	r.GET("/health", m.serveTokenDetectionHealth)
	for _, tc := range []struct {
		path, body string
		status     int
	}{{"/query", tokenDetectionJSON(tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 1000, To: now, Limit: 2}), 200}, {"/query", `{"from":1,"to":99999999}`, 400}, {"/detail", `{"log_id":10}`, 200}, {"/detail", `{"log_id":999}`, 404}, {"/health", "", 200}} {
		method := http.MethodPost
		if tc.path == "/health" {
			method = http.MethodGet
		}
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Fatalf("%s %d %s", tc.path, w.Code, w.Body.String())
		}
		if tc.path == "/query" && tc.status == 200 && !strings.Contains(w.Body.String(), `"has_more":true`) {
			t.Fatal("pagination missing")
		}
	}
}
func TestTokenDetectionPolicyBounds(t *testing.T) {
	p := detectionPolicy()
	if _, err := parseTokenDetectionPolicy(tokenDetectionJSON(p)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"{}", `{"version":"a","version":"b"}`, strings.Replace(tokenDetectionJSON(p), `"min_samples":3`, `"min_samples":1`, 1), strings.Replace(tokenDetectionJSON(p), `"input_ratio":"5"`, `"input_ratio":"NaN"`, 1)} {
		if _, err := parseTokenDetectionPolicy(raw); err == nil {
			t.Fatalf("invalid policy accepted %s", raw)
		}
	}
}
