package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestTokenDeliveryDownloadConcurrentBudgetAndAudit(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	out, err := m.createTokenExport(context.Background(), "owner", tokenDeliveryExportRequest(m, h, now), now)
	if err != nil {
		t.Fatal(err)
	}
	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.downloadTokenExport(context.Background(), "owner", out.ID, now+1)
			if err == nil {
				ok.Add(1)
			} else if !errors.Is(err, errTokenDeliveryCapacity) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var count int64
	m.storeDB.Model(&TokenDeliveryAudit{}).Where("action = ?", "export_downloaded").Count(&count)
	if ok.Load() != 10 || count != 10 {
		t.Fatalf("concurrent cap/audit: %d / %d", ok.Load(), count)
	}
	if _, err = m.downloadTokenExport(context.Background(), "owner", out.ID, now+62); err != nil {
		t.Fatal("budget did not recover", err)
	}
	if err = m.storeDB.Exec("CREATE TRIGGER fail_download_audit BEFORE INSERT ON token_delivery_audits WHEN NEW.action='export_downloaded' BEGIN SELECT RAISE(ABORT,'fixture'); END").Error; err != nil {
		t.Fatal(err)
	}
	if _, err = m.downloadTokenExport(context.Background(), "owner", out.ID, now+63); err == nil {
		t.Fatal("unaudited download succeeded")
	}
}

func TestTokenDeliveryMoneyUnknownIdentityDegradesCountAndDrilldown(t *testing.T) {
	for _, field := range []string{"subject", "unit", "quota_per_unit"} {
		t.Run(field, func(t *testing.T) {
			m, now, _ := tokenDeliveryFixture(t)
			h := tokenDeliveryCandidate(t, m, now)
			var r tokenDetectionResult
			if err := json.Unmarshal([]byte(h.ResultJSON), &r); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "subject":
				r.Price.Subject = ""
			case "unit":
				r.Price.Unit = ""
			case "quota_per_unit":
				r.Price.QuotaPerUnit = "0"
			}
			if err := m.storeDB.Model(&h).Update("result_json", tokenDetectionJSON(r)).Error; err != nil {
				t.Fatal(err)
			}
			q := tokenDetectionQuery{SourceEpoch: h.SourceEpoch, From: now - 600, To: now + 1, Limit: 50, Method: "historical_formula_v1"}
			got, err := m.tokenDetectionReport(context.Background(), q)
			if err != nil || len(got["items"].([]map[string]any)) != 0 {
				t.Fatal("unavailable money counted", got, err)
			}
			q.Unavailable = true
			got, err = m.tokenDetectionReport(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			if len(got["items"].([]map[string]any)) != 1 || got["analysis"].(tokenReportAnalysis).Calculable[q.Method] != 0 || len(got["summary"].(tokenDetectionSummary).Amounts) != 0 {
				t.Fatal("coverage, drilldown and amount diverged", got)
			}
		})
	}
}
func TestTokenDeliveryExportWhitelistUnknownPayloadAndComparisonPrivacy(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	var rev TokenDetectionRevision
	if err := m.storeDB.First(&rev, "source_epoch=? AND log_id=? AND revision=?", h.SourceEpoch, h.LogID, h.Revision).Error; err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(rev.FrozenJSON), &raw); err != nil {
		t.Fatal(err)
	}
	input := raw["input"].(map[string]any)
	input["customer_body"] = "private-text-marker"
	input["authorization"] = "private-secret-marker"
	input["usage"].(map[string]any)["fields"].(map[string]any)["unknown_secret"] = map[string]any{"state": "valid", "value": "private-extension-marker"}
	ref := raw["result"].(map[string]any)["reference"].(map[string]any)
	ref["samples"] = []map[string]any{{"source_epoch": "private-other-source", "log_id": 98765, "channel_id": 87654, "created_at": now - 200, "request_length": 1000, "input": 100, "price": "private-other-price"}}
	if err := m.storeDB.Model(&rev).Update("frozen_json", tokenDetectionJSON(raw)).Error; err != nil {
		t.Fatal(err)
	}
	out, err := m.createTokenExport(context.Background(), "owner", tokenDeliveryExportRequest(m, h, now), now)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range tokenDeliveryUnzip(t, out.Payload) {
		for _, marker := range []string{"private-text-marker", "private-secret-marker", "private-extension-marker", "private-other-source", "private-other-price", "98765", "87654"} {
			if strings.Contains(value, marker) {
				t.Fatalf("%s leaked %s", name, marker)
			}
		}
	}
	p, _ := parseTokenExportPolicy(m.cfg.TokenExportPolicy)
	p.Version = "revoked-version"
	m.cfg.TokenExportPolicy = tokenDetectionJSON(p)
	if _, err = m.readTokenExport(context.Background(), "owner", out.ID, now+1); !errors.Is(err, errTokenExportScope) {
		t.Fatal("changed authorization did not revoke package", err)
	}
}
func TestTokenDeliveryCleanupDisabledRecoversExpiredSending(t *testing.T) {
	for _, scenario := range []string{"ordinary", "expired_audit_capacity"} {
		t.Run(scenario, func(t *testing.T) {
			m, now, path := tokenDeliveryFixture(t)
			tokenDeliveryCandidate(t, m, now)
			if err := m.runTokenNotificationRound(context.Background(), now, nil); err != nil {
				t.Fatal(err)
			}
			row := tokenDeliveryOutbox(t, m)
			if err := m.storeDB.Model(&row).Updates(map[string]any{"state": "sending", "lease_until": now + 30, "lease_token": "crashed", "attempts": 1}).Error; err != nil {
				t.Fatal(err)
			}
			if scenario == "expired_audit_capacity" {
				var count int64
				if err := m.storeDB.Model(&TokenDeliveryAudit{}).Count(&count).Error; err != nil {
					t.Fatal(err)
				}
				old := make([]TokenDeliveryAudit, 10000-count)
				for i := range old {
					old[i] = TokenDeliveryAudit{At: now - tokenDeliveryRetention - 1, Actor: "worker", Action: "expired_fixture"}
				}
				if err := m.storeDB.CreateInBatches(old, 200).Error; err != nil {
					t.Fatal(err)
				}
			}
			m = detectionReviewReopen(t, m, path)
			m.cfg.TokenNotificationEvaluationEnabled = false
			if err := m.cleanupTokenDelivery(context.Background(), now+3601); err != nil {
				t.Fatal(err)
			}
			row = tokenDeliveryOutbox(t, m)
			if row.State != "expired" || row.Payload != "" || row.LeaseToken != "" || row.SentAt != 0 {
				t.Fatal("disabled cleanup leaked crashed attempt", row)
			}
			var count, recovered int64
			if err := m.storeDB.Model(&TokenDeliveryAudit{}).Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			if scenario == "expired_audit_capacity" && count != 9801 {
				t.Fatalf("bounded audit cleanup did not release capacity: %d", count)
			}
			if err := m.cleanupTokenDelivery(context.Background(), now+3602); err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Model(&TokenDeliveryAudit{}).Where("action = ?", "notification_recovered").Count(&recovered).Error; err != nil {
				t.Fatal(err)
			}
			if recovered != 1 {
				t.Fatalf("recovery audit duplicated: %d", recovered)
			}
		})
	}
}

func TestTokenDeliveryNotificationAttemptsExhaustedAndBudgetAcrossDay(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	tokenDeliveryCandidate(t, m, now)
	m.cfg.TokenNotificationsEnabled = true
	n := 0
	sender := func(context.Context, AlertConfig, string, string, string) error { n++; return errors.New("fixture") }
	for _, offset := range []int64{0, 60, 240, 840, 2640, 2700} {
		if err := m.runTokenNotificationRound(context.Background(), now+offset, sender); err != nil {
			t.Fatal(err)
		}
	}
	row := tokenDeliveryOutbox(t, m)
	if n != 5 || row.State != "failed" || row.Attempts != 5 || row.SentAt != 0 {
		t.Fatal("retry bound", n, row)
	}
	// Resetting the calendar budget is independent of successful sending and is durable.
	newer := now + 86400
	detectionFact(t, m, 11, newer-100, 4, 10000, 0, 1)
	detectionRun(t, m, newer)
	if err := m.storeDB.Model(&tokenDeliveryState{}).Where("id=?", "notify").Update("budget_used", 10).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.runTokenNotificationRound(context.Background(), newer, sender); err != nil {
		t.Fatal(err)
	}
	var state tokenDeliveryState
	m.storeDB.First(&state, "id=?", "notify")
	if state.BudgetUsed != 1 || state.BudgetDay != (newer+28800)/86400 || n != 6 {
		t.Fatal("day reset", state, n)
	}
}

func TestTokenDeliveryNotificationExhaustedFailureRequeuesAfterRecovery(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	m.cfg.TokenNotificationsEnabled = true
	calls := 0
	failing := func(context.Context, AlertConfig, string, string, string) error {
		calls++
		return errors.New("smtp unavailable")
	}
	// Follow the production backoff schedule through the fifth and final
	// attempt. The final terminal row must retain a future retry boundary.
	for _, offset := range []int64{0, 60, 240, 840, 2640} {
		if err := m.runTokenNotificationRound(context.Background(), now+offset, failing); err != nil {
			t.Fatal(err)
		}
	}
	row := tokenDeliveryOutbox(t, m)
	if calls != 5 || row.State != "failed" || row.Attempts != 5 || row.SentAt != 0 || row.NextAt <= now+2640 {
		t.Fatalf("failure exhaustion state: calls=%d row=%+v", calls, row)
	}
	// Requeue must use the frozen outbox identity as well as the target row;
	// clearing a target fingerprint must not make the old primary key swallow
	// the recovered notification.
	if err := m.storeDB.Model(&tokenNotifyTarget{}).Where("id = ?", row.TargetKey).Update("fingerprint", "").Error; err != nil {
		t.Fatal(err)
	}
	// The same anomaly remains current when the transport recovers. Move the
	// synthetic fact into the current evaluation window without changing its
	// revision or decision fingerprint.
	retryAt := row.NextAt
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("source_epoch = ? AND log_id = ?", h.SourceEpoch, h.LogID).Update("created_at", retryAt-100).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("source_epoch = ? AND log_id = ?", h.SourceEpoch, h.LogID).Update("created_at", retryAt-100).Error; err != nil {
		t.Fatal(err)
	}
	var sentCalls int
	working := func(context.Context, AlertConfig, string, string, string) error {
		sentCalls++
		return nil
	}
	if err := m.runTokenNotificationRound(context.Background(), retryAt, working); err != nil {
		t.Fatal(err)
	}
	row = tokenDeliveryOutbox(t, m)
	if sentCalls != 1 || calls != 5 || row.State != "sent" || row.Attempts != 1 || row.SentAt != retryAt {
		t.Fatalf("requeue after recovery: sent=%d failed=%d row=%+v", sentCalls, calls, row)
	}
	var outboxCount int64
	if err := m.storeDB.Model(&TokenNotifyOutbox{}).Count(&outboxCount).Error; err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("terminal row was duplicated instead of safely reused: %d", outboxCount)
	}
	if err := m.runTokenNotificationRound(context.Background(), retryAt+1, working); err != nil {
		t.Fatal(err)
	}
	if sentCalls != 1 {
		t.Fatal("successful fingerprint escaped cooldown", sentCalls)
	}
}

func TestTokenDeliveryNotificationExhaustedFailureRequeuesWithTargetFingerprint(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	m.cfg.TokenNotificationsEnabled = true
	calls := 0
	failing := func(context.Context, AlertConfig, string, string, string) error {
		calls++
		return errors.New("smtp unavailable")
	}
	for _, offset := range []int64{0, 60, 240, 840, 2640} {
		if err := m.runTokenNotificationRound(context.Background(), now+offset, failing); err != nil {
			t.Fatal(err)
		}
	}
	row := tokenDeliveryOutbox(t, m)
	var target tokenNotifyTarget
	if err := m.storeDB.First(&target, "id = ?", row.TargetKey).Error; err != nil {
		t.Fatal(err)
	}
	if target.Fingerprint == "" || target.Fingerprint != row.Fingerprint {
		t.Fatalf("failure changed target fingerprint: target=%q row=%q", target.Fingerprint, row.Fingerprint)
	}
	retryAt := row.NextAt
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("source_epoch = ? AND log_id = ?", h.SourceEpoch, h.LogID).Update("created_at", retryAt-100).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("source_epoch = ? AND log_id = ?", h.SourceEpoch, h.LogID).Update("created_at", retryAt-100).Error; err != nil {
		t.Fatal(err)
	}
	sent := 0
	working := func(context.Context, AlertConfig, string, string, string) error {
		sent++
		return nil
	}
	if err := m.runTokenNotificationRound(context.Background(), retryAt, working); err != nil {
		t.Fatal(err)
	}
	row = tokenDeliveryOutbox(t, m)
	if sent != 1 || calls != 5 || row.State != "sent" || row.Attempts != 1 || row.SentAt != retryAt {
		t.Fatalf("same fingerprint did not recover: sent=%d failed=%d row=%+v", sent, calls, row)
	}
	if err := m.storeDB.First(&target, "id = ?", row.TargetKey).Error; err != nil {
		t.Fatal(err)
	}
	if target.Fingerprint != row.Fingerprint {
		t.Fatalf("successful retry changed fingerprint identity: target=%q row=%q", target.Fingerprint, row.Fingerprint)
	}
	if err := m.runTokenNotificationRound(context.Background(), retryAt+1, working); err != nil {
		t.Fatal(err)
	}
	if sent != 1 {
		t.Fatalf("successful fingerprint escaped cooldown: %d", sent)
	}
}

func TestTokenDeliveryNotificationExpiredPayloadReusesOriginalOutbox(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	// Evaluation remains on, while delivery is off, so the candidate is
	// durably queued without invoking SMTP.
	if err := m.runTokenNotificationRound(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	row := tokenDeliveryOutbox(t, m)
	if row.State != "paused" || row.Payload == "" {
		t.Fatalf("expected paused frozen outbox: %+v", row)
	}
	originalID := row.ID
	expiredAt := now + 3601
	if err := m.cleanupTokenDelivery(context.Background(), expiredAt); err != nil {
		t.Fatal(err)
	}
	row = tokenDeliveryOutbox(t, m)
	if row.State != "expired" || row.Payload != "" || row.ID != originalID {
		t.Fatalf("cleanup did not expire and redact original row: %+v", row)
	}

	// Keep the same frozen candidate in the current evaluation window. Its
	// expired row must be replenished from the local fact, not recreated under
	// a new primary key.
	retryAt := expiredAt + 60
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("source_epoch = ? AND log_id = ?", h.SourceEpoch, h.LogID).Update("created_at", retryAt-100).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("source_epoch = ? AND log_id = ?", h.SourceEpoch, h.LogID).Update("created_at", retryAt-100).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenNotificationsEnabled = true
	sent := 0
	working := func(context.Context, AlertConfig, string, string, string) error {
		sent++
		return nil
	}
	if err := m.runTokenNotificationRound(context.Background(), retryAt, working); err != nil {
		t.Fatal(err)
	}
	row = tokenDeliveryOutbox(t, m)
	if sent != 1 || row.ID != originalID || row.State != "sent" || row.Payload == "" || row.Attempts != 1 {
		t.Fatalf("expired row was not reused for delivery: sent=%d row=%+v", sent, row)
	}
	var count int64
	if err := m.storeDB.Model(&TokenNotifyOutbox{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expired retry created a duplicate outbox: %d", count)
	}
}

func TestTokenDeliveryEvaluationCapacityDoesNotBlockDurableOutbox(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	if err := m.runTokenNotificationRound(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	var fact TokenMeteringFact
	m.storeDB.First(&fact, "source_epoch=? AND log_id=?", h.SourceEpoch, h.LogID)
	var heads []TokenDetectionCurrent
	var facts []TokenMeteringFact
	for i := int64(100); i < 201; i++ {
		head := h
		head.LogID = i
		head.ChannelID = i
		f := fact
		f.LogID = i
		f.ChannelID = i
		heads = append(heads, head)
		facts = append(facts, f)
	}
	if err := m.storeDB.CreateInBatches(&facts, 50).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.CreateInBatches(&heads, 50).Error; err != nil {
		t.Fatal(err)
	}
	calls := 0
	m.cfg.TokenNotificationsEnabled = true
	err := m.runTokenNotificationRound(context.Background(), now+1, func(context.Context, AlertConfig, string, string, string) error { calls++; return nil })
	if !errors.Is(err, errTokenDeliveryCapacity) || calls != 1 {
		t.Fatal("evaluation capacity blocked drain", calls, err)
	}
	var sent int64
	m.storeDB.Model(&TokenNotifyOutbox{}).Where("state=?", "sent").Count(&sent)
	if sent != 1 {
		t.Fatal("durable work lost", sent)
	}
	var state tokenDeliveryState
	m.storeDB.First(&state, "id=?", "notify")
	if state.LastReason != "notification_capacity_or_budget_limited" || state.LastEvaluatedAt != now {
		t.Fatal("partial evaluation claimed complete", state)
	}
}
func TestTokenDeliveryExportLimitsAndRecovery(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	for i := 0; i < 10; i++ {
		q := tokenDeliveryExportRequest(m, h, now)
		q.RequestID = fmt.Sprint("request-", i)
		if _, err := m.createTokenExport(context.Background(), "owner", q, now); err != nil {
			t.Fatal(err)
		}
	}
	q := tokenDeliveryExportRequest(m, h, now)
	q.RequestID = "over-budget"
	if _, err := m.createTokenExport(context.Background(), "owner", q, now+1); !errors.Is(err, errTokenDeliveryCapacity) {
		t.Fatal("creation budget", err)
	}
	if err := m.cleanupTokenDelivery(context.Background(), now+3601); err != nil {
		t.Fatal(err)
	}
	if _, err := m.createTokenExport(context.Background(), "owner", q, now+3601); err != nil {
		t.Fatal("budget did not release", err)
	}
	q.Selection = append(q.Selection, q.Selection[0])
	if tokenExportRequestValid(q) {
		t.Fatal("duplicate request counted twice")
	}
}
func TestTokenDeliveryNotificationStaleResultsAreExcluded(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=?", h.SourceEpoch, h.LogID).Update("link_revision", gorm.Expr("link_revision+1")).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.runTokenNotificationRound(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	var n int64
	m.storeDB.Model(&TokenNotifyOutbox{}).Count(&n)
	if n != 0 {
		t.Fatal("stale evidence enqueued", n)
	}
}

func TestTokenDeliveryEmptyDimensionDrilldownKeepsExactScope(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	clone := h
	clone.LogID = 11
	clone.UserGroup = ""
	clone.Model = ""
	clone.UserID = 0
	clone.ChannelID = 0
	if err := m.storeDB.Create(&clone).Error; err != nil {
		t.Fatal(err)
	}
	for _, q := range []tokenDetectionQuery{{GroupSet: true}, {ModelSet: true}, {UserSet: true}, {ChannelSet: true}} {
		q.SourceEpoch = h.SourceEpoch
		q.From = now - 600
		q.To = now + 1
		q.Limit = 50
		got, err := m.tokenDetectionReport(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		items := got["items"].([]map[string]any)
		if len(items) != 1 || items[0]["request"].(TokenDetectionCurrent).LogID != 11 || got["summary"].(tokenDetectionSummary).Candidates != 1 {
			t.Fatal("unknown dimension widened scope", q, got)
		}
	}
	q := tokenDetectionQuery{SourceEpoch: h.SourceEpoch, From: now - 600, To: now + 1, Limit: 50}
	got, err := m.tokenDetectionReport(context.Background(), q)
	if err != nil || got["summary"].(tokenDetectionSummary).Candidates != 2 {
		t.Fatal("legacy empty filter changed", err)
	}
}

func TestTokenDeliveryRejectedExportConcurrentAuditCapacity(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	h := tokenDeliveryCandidate(t, m, now)
	q := tokenDeliveryExportRequest(m, h, now)
	q.TargetID = "unauthorized-target"
	body := tokenDetectionJSON(q)
	if err := m.storeDB.Exec("WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<9999) INSERT INTO token_delivery_audits (at,actor,action,object_id,reason) SELECT ?, 'fixture', 'fixture', '', '' FROM fixture", now).Error; err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m.RegisterRoutes(r)
	cookie := &http.Cookie{Name: sessionCookie, Value: m.signSession("owner", roleRoot, time.Now().Unix())}
	access := httptest.NewRequest("GET", "/anomalies/token/access", nil)
	access.AddCookie(cookie)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, access)
	var info map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil || w.Code != 200 {
		t.Fatal(w.Code, err)
	}
	csrf := info["csrf_token"].(string)
	var rejected, capacity atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/anomalies/token/exports", strings.NewReader(body))
			req.AddCookie(cookie)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Monitor-Token-CSRF", csrf)
			out := httptest.NewRecorder()
			r.ServeHTTP(out, req)
			switch out.Code {
			case http.StatusForbidden:
				rejected.Add(1)
			case http.StatusTooManyRequests:
				capacity.Add(1)
			default:
				t.Errorf("unexpected rejection status %d", out.Code)
			}
		}()
	}
	wg.Wait()
	var audits, exports int64
	if err := m.storeDB.Model(&TokenDeliveryAudit{}).Count(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenEvidenceExport{}).Count(&exports).Error; err != nil {
		t.Fatal(err)
	}
	if audits != 10000 || exports != 0 || rejected.Load() != 1 || capacity.Load() != 23 {
		t.Fatalf("audit capacity not atomic: audits=%d exports=%d rejected=%d limited=%d", audits, exports, rejected.Load(), capacity.Load())
	}
}
