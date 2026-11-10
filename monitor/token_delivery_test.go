package monitor

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func tokenDeliveryFixture(t *testing.T) (*Monitor, int64, string) {
	t.Helper()
	m, now, path := detectionFixture(t)
	if err := m.storeDB.AutoMigrate(append(tokenDeliveryTables(), &AlertConfig{})...); err != nil {
		t.Fatal(err)
	}
	m.cfg.SessionSecret = "synthetic-token-delivery-session"
	m.cfg.TokenExportsEnabled = true
	m.cfg.TokenExportPolicy = tokenDetectionJSON(tokenExportPolicy{Version: "fixture-v1", Targets: []tokenExportTarget{{ID: "supplier-fixture", SourceEpoch: m.cfg.TokenFactsSourceEpoch, ChannelIDs: []int64{3}, From: now - 86400, To: now + 86400}}})
	m.cfg.TokenNotificationEvaluationEnabled = true
	m.cfg.TokenNotificationPolicy = tokenDetectionJSON(tokenNotifyPolicy{Version: "fixture-v1", WindowSeconds: 3600, CooldownSeconds: 3600, MaxAgeSeconds: 3600, DailyBudget: 10, EscalationFactor: 2, Rules: []tokenNotifyRule{{Name: "historical_price_difference", MinRequests: 1, Evidence: "exact", Severity: "SEV3"}}})
	m.cfg.TokenNotificationRecipients = "test@example.invalid"
	m.cfg.TokenNotificationBaseURL = "https://monitor.example.invalid"
	if err := m.storeDB.Create(&AlertConfig{ID: 1, Enabled: true, SMTPHost: "smtp.example.invalid"}).Error; err != nil {
		t.Fatal(err)
	}
	return m, now, path
}
func tokenDeliveryCandidate(t *testing.T, m *Monitor, now int64) TokenDetectionCurrent {
	t.Helper()
	detectionFact(t, m, 10, now-100, 3, 10000, 0, 1)
	detectionRun(t, m, now)
	h, r := detectionHead(t, m, 10)
	if !r.Candidate {
		t.Fatal("fixture candidate absent")
	}
	return h
}
func tokenDeliveryExportRequest(m *Monitor, h TokenDetectionCurrent, now int64) tokenExportRequest {
	return tokenExportRequest{RequestID: "fixture-request", TargetID: "supplier-fixture", SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 600, To: now + 1, Selection: []tokenExportSelection{{LogID: h.LogID, Revision: h.Revision, DecisionHash: h.DecisionHash}}}
}
func tokenDeliveryUnzip(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	z, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, file := range z.File {
		r, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, e := io.ReadAll(r)
		r.Close()
		if e != nil {
			t.Fatal(e)
		}
		files[file.Name] = string(b)
	}
	return files
}
func TestTokenDeliverySettingsDefaultsAndStrictness(t *testing.T) {
	if err := validateTokenDeliverySettings(Settings{}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []Settings{{TokenExportsEnabled: true}, {TokenNotificationsEnabled: true}, {TokenNotificationEvaluationEnabled: true}} {
		if validateTokenDeliverySettings(s) == nil {
			t.Fatal("enabled without prerequisites")
		}
	}
	for _, raw := range []string{`{"version":"v1"} trailing`, `{"version":"v1"} {}`, `{"unknown":1}`} {
		if _, err := parseTokenExportPolicy(raw); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
}
func TestTokenDeliveryReportFiltersAndAmountsAgree(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	for _, q := range []tokenDetectionQuery{
		{Status: "candidate"}, {Rule: "historical_price_difference", Evidence: "exact"}, {Method: "historical_formula_v1", Subject: "customer_wallet", Unit: "USD", QuotaPerUnit: "500000"},
	} {
		q.SourceEpoch = h.SourceEpoch
		q.From = now - 600
		q.To = now + 1
		q.Limit = 1
		got, err := m.tokenDetectionReport(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		s := got["summary"].(tokenDetectionSummary)
		items := got["items"].([]map[string]any)
		if s.Candidates != 1 || len(items) != 1 || s.Amounts[tokenDetectionHash([]string{"customer_wallet", "USD", "500000", "historical_formula_v1"})].Quota != "1" {
			t.Fatalf("scope or exact money mismatch %+v", s)
		}
		a := got["analysis"].(tokenReportAnalysis)
		if a.Calculable["historical_formula_v1"] != 1 || a.StatusCounts["candidate"] != 1 {
			t.Fatal(a)
		}
	}
	q := tokenDetectionQuery{SourceEpoch: h.SourceEpoch, From: now - 600, To: now + 1, Limit: 50, Status: "candidate", Method: "input_upper_reference_v1", Unavailable: true}
	got, err := m.tokenDetectionReport(context.Background(), q)
	if err != nil || got["summary"].(tokenDetectionSummary).Candidates != 1 {
		t.Fatal(got, err)
	}
	q.AfterAt = h.CreatedAt
	q.AfterID = h.LogID
	got, err = m.tokenDetectionReport(context.Background(), q)
	if err != nil || len(got["items"].([]map[string]any)) != 0 || got["summary"].(tokenDetectionSummary).Candidates != 1 {
		t.Fatal("pagination changed summary", err)
	}
	if tokenReportFiltersValid(tokenDetectionQuery{Method: "arbitrary"}) || tokenReportFiltersValid(tokenDetectionQuery{Subject: "wallet"}) {
		t.Fatal("invalid filters accepted")
	}
}
func TestTokenDeliveryExportRedactsAndRetainsFrozenAfterFactCleanup(t *testing.T) {
	m, now, path := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	q := tokenDeliveryExportRequest(m, h, now)
	if err := m.storeDB.Where("source_epoch = ?", h.SourceEpoch).Delete(&TokenMeteringFact{}).Error; err != nil {
		t.Fatal(err)
	}
	out, err := m.createTokenExport(context.Background(), "owner", q, now)
	if err != nil {
		t.Fatal(err)
	}
	files := tokenDeliveryUnzip(t, out.Payload)
	if len(files) != 5 {
		t.Fatal("incomplete package", files)
	}
	for _, forbidden := range []string{`"user_id"`, `"log_id"`, `"hmac_refs"`, `"quota_per_unit"`, `"model_ratio"`, `"actual_quota"`} {
		if strings.Contains(files["evidence.json"], forbidden) {
			t.Fatal("private field exported", forbidden)
		}
	}
	if !strings.Contains(files["evidence.json"], "not_collected_cannot_reverse_hmac") || !strings.Contains(files["summary.csv"], "pricing_withheld") {
		t.Fatal("missing disclosure")
	}
	digest := sha256.Sum256(out.Payload)
	if out.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatal("outer checksum")
	}
	var manifest struct {
		Checks map[string]string `json:"files_sha256"`
	}
	if err := json.Unmarshal([]byte(files["manifest.json"]), &manifest); err != nil {
		t.Fatal(err)
	}
	for name, want := range manifest.Checks {
		hash := sha256.Sum256([]byte(files[name]))
		if hex.EncodeToString(hash[:]) != want {
			t.Fatal("inner checksum", name)
		}
	}
	m = detectionReviewReopen(t, m, path)
	again, err := m.createTokenExport(context.Background(), "owner", q, now+1)
	if err != nil || !bytes.Equal(out.Payload, again.Payload) {
		t.Fatal("restart/idempotence", err)
	}
	var count int64
	m.storeDB.Model(&TokenEvidenceExport{}).Count(&count)
	if count != 1 {
		t.Fatal("duplicate package")
	}
}
func TestTokenDeliveryExportRejectsScopeOwnerAndExpiredRevision(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	base := tokenDeliveryExportRequest(m, h, now)
	for _, change := range []func(*tokenExportRequest){
		func(q *tokenExportRequest) { q.TargetID = "other-supplier" },
		func(q *tokenExportRequest) { q.IncludeCustomerPricing = true },
		func(q *tokenExportRequest) { q.From = now - 100000 },
		func(q *tokenExportRequest) { q.Selection[0].DecisionHash = strings.Repeat("a", 64) },
	} {
		q := base
		q.Selection = append([]tokenExportSelection(nil), base.Selection...)
		change(&q)
		if _, err := m.createTokenExport(context.Background(), "owner", q, now); err == nil {
			t.Fatal("unsafe scope succeeded")
		}
	}
	out, err := m.createTokenExport(context.Background(), "owner", base, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.readTokenExport(context.Background(), "another-owner", out.ID, now); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("owner isolation", err)
	}
	if _, err = m.readTokenExport(context.Background(), "owner", out.ID, now+tokenExportTTL); !errors.Is(err, errTokenExportConflict) {
		t.Fatal("expired download", err)
	}
	if err = m.cleanupTokenDelivery(context.Background(), now+tokenExportTTL); err != nil {
		t.Fatal(err)
	}
	var saved TokenEvidenceExport
	m.storeDB.First(&saved, "id = ?", out.ID)
	if len(saved.Payload) != 0 {
		t.Fatal("expired bytes retained")
	}
	if len(base.Selection) != 1 || tokenExportRequestValid(tokenExportRequest{RequestID: "x"}) {
		t.Fatal("selection validation")
	}
}
func TestTokenDeliveryExportPricingWhitelistCSVAndAtomicAudit(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	q := tokenDeliveryExportRequest(m, h, now)
	p, _ := parseTokenExportPolicy(m.cfg.TokenExportPolicy)
	p.Targets[0].AllowCustomerPricing = true
	m.cfg.TokenExportPolicy = tokenDetectionJSON(p)
	q.IncludeCustomerPricing = true
	if err := m.storeDB.Exec("CREATE TRIGGER block_export_audit BEFORE INSERT ON token_delivery_audits BEGIN SELECT RAISE(ABORT,'fixture'); END").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.createTokenExport(context.Background(), "owner", q, now); err == nil {
		t.Fatal("audit failed open")
	}
	var n int64
	m.storeDB.Model(&TokenEvidenceExport{}).Count(&n)
	if n != 0 {
		t.Fatal("unaudited payload retained")
	}
	m.storeDB.Exec("DROP TRIGGER block_export_audit")
	out, err := m.createTokenExport(context.Background(), "owner", q, now)
	if err != nil {
		t.Fatal(err)
	}
	files := tokenDeliveryUnzip(t, out.Payload)
	if !strings.Contains(files["evidence.json"], "model_ratio") || !strings.Contains(files["summary.csv"], "customer_wallet") {
		t.Fatal("authorized price absent")
	}
	for _, s := range []string{"=x", " +x", "-2", "@cmd", "\t=payload", "line\nbreak"} {
		if !strings.HasPrefix(tokenExportCell(s), "'") {
			t.Fatal("CSV formula/control not escaped", s)
		}
	}
}
func TestTokenDeliveryHTTPAuthorizationCSRFAndLocalQuery(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m.RegisterRoutes(r)
	request := func(method, path, name string, role int, body, csrf, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		if name != "" {
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession(name, role, time.Now().Unix())})
		}
		req.Header.Set("X-Monitor-Token-CSRF", csrf)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	for _, role := range []int{0, roleAdmin} {
		name := ""
		if role > 0 {
			name = "reader"
		}
		w := request("POST", "/anomalies/token/exports", name, role, "{}", "", "")
		if w.Code != 401 && w.Code != 403 {
			t.Fatal("write authorization", w.Code)
		}
	}
	w := request("GET", "/anomalies/token/access", "owner", roleRoot, "", "", "")
	var access map[string]any
	if json.Unmarshal(w.Body.Bytes(), &access) != nil || w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	csrf := access["csrf_token"].(string)
	if err := m.storeDB.Model(&TokenDetectionRevision{}).Where("source_epoch=? AND log_id=?", h.SourceEpoch, h.LogID).Update("expires_at", time.Now().Unix()+3600).Error; err != nil {
		t.Fatal(err)
	}
	body := tokenDetectionJSON(tokenDeliveryExportRequest(m, h, now))
	for _, pair := range [][2]string{{"", ""}, {csrf, "https://outside.invalid"}} {
		w = request("POST", "/anomalies/token/exports", "owner", roleRoot, body, pair[0], pair[1])
		if w.Code != 403 {
			t.Fatal("CSRF", w.Code)
		}
	}
	w = request("POST", "/anomalies/token/exports", "owner", roleRoot, body, csrf, "http://example.com")
	if w.Code != 200 {
		t.Fatal("authorized create", w.Code, w.Body.String())
	}
	var created TokenEvidenceExport
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || created.ID == "" {
		t.Fatal("create response", err, w.Body.String())
	}
	w = request("GET", "/anomalies/token/exports/"+created.ID, "another-owner", roleRoot, "", "", "")
	if w.Code != 404 {
		t.Fatal("HTTP owner isolation", w.Code)
	}
	w = request("POST", "/anomalies/token/exports/"+created.ID+"/download", "owner", roleRoot, "{}", csrf, "http://example.com")
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/zip" || w.Header().Get("X-Content-SHA256") != created.SHA256 {
		t.Fatal("HTTP download", w.Code, w.Body.String())
	}
	tokenDeliveryUnzip(t, w.Body.Bytes())
	w = request("GET", "/anomalies/token/notifications/health", "reader", roleAdmin, "", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "delivery_enabled") {
		t.Fatal("notification health", w.Code, w.Body.String())
	}
	w = request("POST", "/anomalies/token/detection/query", "reader", roleAdmin, tokenDetectionJSON(tokenDetectionQuery{SourceEpoch: h.SourceEpoch, From: now - 600, To: now + 1, Limit: 50, Status: "candidate"}), "", "")
	if w.Code != 200 || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatal("local query or cache boundary", w.Code)
	}
}
func tokenDeliveryOutbox(t *testing.T, m *Monitor) TokenNotifyOutbox {
	t.Helper()
	var row TokenNotifyOutbox
	if err := m.storeDB.Order("created_at DESC,id").First(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}
func TestTokenDeliveryNotifyShadowFailureBackoffRestartAndSuccess(t *testing.T) {
	m, now, path := tokenDeliveryFixture(t)
	tokenDeliveryCandidate(t, m, now)
	calls := 0
	sender := func(context.Context, AlertConfig, string, string, string) error {
		calls++
		return errors.New("synthetic transport failure")
	}
	if err := m.runTokenNotificationRound(context.Background(), now, sender); err != nil {
		t.Fatal(err)
	}
	row := tokenDeliveryOutbox(t, m)
	if row.State != "paused" || calls != 0 {
		t.Fatal("shadow delivered", row.State, calls)
	}
	m.cfg.TokenNotificationsEnabled = true
	if err := m.runTokenNotificationRound(context.Background(), now+1, sender); err != nil {
		t.Fatal(err)
	}
	row = tokenDeliveryOutbox(t, m)
	if row.State != "uncertain" || row.Attempts != 1 || calls != 1 {
		t.Fatal(row, calls)
	}
	var target tokenNotifyTarget
	m.storeDB.First(&target, "id = ?", row.TargetKey)
	if target.LastSentAt != 0 {
		t.Fatal("failure entered cooldown")
	}
	m = detectionReviewReopen(t, m, path)
	if err := m.runTokenNotificationRound(context.Background(), now+2, sender); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("backoff ignored")
	}
	stableID := row.ID + "@monitor.local"
	sender = func(_ context.Context, _ AlertConfig, subject, body, id string) error {
		calls++
		if id != stableID || !strings.Contains(subject, "待核查") || strings.Contains(body, "fixture-10") {
			t.Fatal("identity or privacy", id)
		}
		return nil
	}
	if err := m.runTokenNotificationRound(context.Background(), now+61, sender); err != nil {
		t.Fatal(err)
	}
	row = tokenDeliveryOutbox(t, m)
	if row.State != "sent" || row.Attempts != 2 || calls != 2 {
		t.Fatal(row, calls)
	}
	if err := m.runTokenNotificationRound(context.Background(), now+62, sender); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("duplicate send")
	}
}
func TestTokenDeliveryNotifyPauseExpiryBudgetAndDisable(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	tokenDeliveryCandidate(t, m, now)
	sender := func(context.Context, AlertConfig, string, string, string) error {
		t.Fatal("unexpected send")
		return nil
	}
	if err := m.runTokenNotificationRound(context.Background(), now, sender); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenNotificationsEnabled = true
	if err := m.storeDB.Model(&tokenDeliveryState{}).Where("id = ?", "notify").Updates(map[string]any{"budget_day": (now + 28800) / 86400, "budget_used": 10}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.runTokenNotificationRound(context.Background(), now+1, sender); !errors.Is(err, errTokenDeliveryCapacity) {
		t.Fatal("daily budget", err)
	}
	if err := m.runTokenNotificationRound(context.Background(), now+3601, sender); err != nil {
		t.Fatal(err)
	}
	if row := tokenDeliveryOutbox(t, m); row.State != "expired" || row.SentAt != 0 {
		t.Fatal("stale paused flood", row)
	}
	m.cfg.TokenNotificationEvaluationEnabled = false
	if err := m.runTokenNotificationRound(context.Background(), now+3602, sender); err != nil {
		t.Fatal(err)
	}
}
func TestTokenDeliveryNotifyCrashLeaseAndConcurrentRounds(t *testing.T) {
	m, now, path := tokenDeliveryFixture(t)
	tokenDeliveryCandidate(t, m, now)
	if err := m.runTokenNotificationRound(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	row := tokenDeliveryOutbox(t, m)
	if err := m.storeDB.Model(&TokenNotifyOutbox{}).Where("id = ?", row.ID).Updates(map[string]any{"state": "sending", "attempts": 1, "lease_until": now + 1, "lease_token": "lost"}).Error; err != nil {
		t.Fatal(err)
	}
	m = detectionReviewReopen(t, m, path)
	m.cfg.TokenNotificationsEnabled = true
	var calls atomic.Int32
	sender := func(context.Context, AlertConfig, string, string, string) error { calls.Add(1); return nil }
	if err := m.runTokenNotificationRound(context.Background(), now+2, sender); err != nil {
		t.Fatal(err)
	}
	if row = tokenDeliveryOutbox(t, m); row.State != "uncertain" || calls.Load() != 0 {
		t.Fatal("crash recovery", row)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- m.runTokenNotificationRound(context.Background(), now+62, sender) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("concurrent duplicate", calls.Load())
	}
}
func TestTokenDeliveryNotifySeverityUpgradeAndTargetsIndependent(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	tokenDeliveryCandidate(t, m, now)
	m.cfg.TokenNotificationsEnabled = true
	calls := 0
	sender := func(context.Context, AlertConfig, string, string, string) error { calls++; return nil }
	if err := m.runTokenNotificationRound(context.Background(), now, sender); err != nil {
		t.Fatal(err)
	}
	p, _ := parseTokenNotifyPolicy(m.cfg.TokenNotificationPolicy)
	p.Version = "fixture-v2"
	p.Rules[0].Severity = "SEV2"
	m.cfg.TokenNotificationPolicy = tokenDetectionJSON(p)
	if err := m.runTokenNotificationRound(context.Background(), now+1, sender); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("severity upgrade suppressed", calls)
	}
	rows := []TokenDetectionCurrent{}
	m.storeDB.Find(&rows)
	clone := rows[0]
	clone.ChannelID = 4
	clone.Model = "other-model"
	clone.LogID = 20
	rows = append(rows, clone)
	groups, err := tokenNotifyGroups(rows, p, now)
	if err != nil || len(groups) != 2 {
		t.Fatal("targets merged", len(groups), err)
	}
}
func TestTokenDeliveryNotifyAuditFailureLeavesRecoverableAttempt(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	tokenDeliveryCandidate(t, m, now)
	m.cfg.TokenNotificationsEnabled = true
	sender := func(context.Context, AlertConfig, string, string, string) error {
		if err := m.storeDB.Exec("CREATE TRIGGER fail_notify_result BEFORE INSERT ON token_delivery_audits WHEN NEW.action='notification_result' BEGIN SELECT RAISE(ABORT,'fixture'); END").Error; err != nil {
			t.Fatal(err)
		}
		return nil
	}
	if err := m.runTokenNotificationRound(context.Background(), now, sender); err == nil {
		t.Fatal("post SMTP persistence failure hidden")
	}
	if row := tokenDeliveryOutbox(t, m); row.State != "sending" || row.SentAt != 0 {
		t.Fatal("failed transaction advanced cooldown", row)
	}
}
