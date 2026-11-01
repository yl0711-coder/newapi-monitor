package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func monthlyStatisticsReviewed(t *testing.T, input MonthlyPreparationInput) (MonthlyBillEvidence, []byte) {
	t.Helper()
	raw, meta := monthlyBundleRaw(t, input, monthlyBundleFixture(input))
	now := time.Unix(preparationAt("2026-10-21"), 0)
	b, err := PrepareMonthlyCandidateBundle(raw, meta, now)
	if err != nil || len(b.Entries) == 0 || b.Entries[0].Status != "needs_review" {
		t.Fatalf("candidate failed: %+v, %v", b.Entries, err)
	}
	b.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
	b.Reconciliation.SourceIntegrity = b.SourceIntegrity
	bundle := monthlyReviewJSON(t, b)
	proof, err := ReviewMonthlyCandidate(raw, meta, bundle, monthlyReviewJSON(t, monthlyReviewApproval(bundle, b.Entries[0], now)), now)
	if err != nil {
		t.Fatal(err)
	}
	e, err := decodeMonthlyBillEvidence(proof)
	if err != nil {
		t.Fatal(err)
	}
	return e, proof
}

func TestMonthlyStatisticsUnknownDoesNotBlockVerifiedMoney(t *testing.T) {
	for _, wallet := range []bool{false, true} {
		for _, missing := range []string{"requests", "tokens", "both", "none"} {
			t.Run(stringBool(wallet)+"-"+missing, func(t *testing.T) {
				input := monthlyWalletFixture()
				a := &input.Accounts[0]
				if !wallet {
					a.WalletMonths = nil
					a.Spans[0].ConsumeQuota = 200000000
				}
				a.Spans[0].Requests, a.Spans[0].Tokens = preparationInt(0), preparationInt(0)
				requestsUnknown, tokensUnknown := missing == "requests" || missing == "both", missing == "tokens" || missing == "both"
				if requestsUnknown {
					a.Spans[0].Requests = nil
				}
				if tokensUnknown {
					a.Spans[0].Tokens = nil
				}
				e, _ := monthlyStatisticsReviewed(t, input)
				p, _, err := buildPublishedMonthlyBill(e, time.Now())
				if err != nil || !monthlyStatementAmountsComplete(p) {
					t.Fatal("money blocked by stats", err)
				}
				if err := monthlyBatchBindNames(&p, []monthlyBatchMember{{ID: 1, Username: "synthetic-wallet"}}); err != nil {
					t.Fatal(err)
				}
				if p.Consumption.Amount != "400.000000" || *p.MonthOpeningBalance != "100.000000" || *p.MonthEndBalance != "200.000000" || p.Consumption.RequestsUnknown != requestsUnknown || p.Consumption.TokensUnknown != tokensUnknown {
					t.Fatal("amount/coverage changed", p)
				}
				if missing == "none" {
					if e.Version != 2 || p.MemberDetailsVersion != 1 || e.Users[0].Statistics != nil {
						t.Fatal("known zero changed legacy format")
					}
				} else if e.Version != 3 || p.MemberDetailsVersion != 2 || e.Users[0].Statistics == nil {
					t.Fatal("unknown stats lost version protection")
				}
				body := monthlyReviewJSON(t, p)
				var shape struct {
					Consumption map[string]json.RawMessage `json:"consumption"`
				}
				if err := json.Unmarshal(body, &shape); err != nil {
					t.Fatal(err)
				}
				for name, unknown := range map[string]bool{"requests": requestsUnknown, "tokens": tokensUnknown} {
					want := "0"
					if unknown {
						want = "null"
					}
					if string(shape.Consumption[name]) != want {
						t.Fatal("unknown became zero", name, string(shape.Consumption[name]))
					}
				}
				var decoded monthlyStatement
				if err := json.Unmarshal(body, &decoded); err != nil || !reflect.DeepEqual(p, decoded) {
					t.Fatal("nullable round trip changed bill", err)
				}
				xlsx, err := monthlyStatementXLSX(p)
				if err != nil {
					t.Fatal(err)
				}
				monthlyAuditProjection(t, p, e, xlsx)
				monthlyBatchCheckMemberSheet(t, xlsx, p.Members)
			})
		}
	}
}

func TestMonthlyStatisticsOrganizationCoverageNotPartialTotals(t *testing.T) {
	p := monthlyMembersFixture(t)
	// Keep complete per-account financial data unchanged; only one token total
	// is unavailable. Known counts from other members are not a full total.
	p.MemberDetailsVersion = 2
	p.Members[0].Consumption.TokensUnknown = true
	p.Members[0].Consumption.Tokens = 0
	p.Consumption.TokensUnknown, p.Consumption.Tokens = true, 0
	if !monthlyStatementAmountsComplete(p) {
		t.Fatal("valid incomplete stats blocked money")
	}
	if p.Consumption.Requests != 3 {
		t.Fatal("known request total lost")
	}
	for name, mutate := range map[string]func(*monthlyStatement){
		"partial sum":                      func(p *monthlyStatement) { p.Consumption.Tokens = 30 },
		"missingness lost":                 func(p *monthlyStatement) { p.Consumption.TokensUnknown = false },
		"member partial value":             func(p *monthlyStatement) { p.Members[0].Consumption.Tokens = 1 },
		"downgraded public schema":         func(p *monthlyStatement) { p.MemberDetailsVersion = 1 },
		"known requests changed":           func(p *monthlyStatement) { p.Consumption.Requests++ },
		"missing stats cannot waive money": func(p *monthlyStatement) { p.Members[0].Closing = "0.000000" },
	} {
		t.Run(name, func(t *testing.T) {
			q := p
			q.Members = append([]monthlyMemberStatement(nil), p.Members...)
			c := *p.Consumption
			q.Consumption = &c
			mutate(&q)
			if monthlyStatementAmountsComplete(q) {
				t.Fatal("corrupt coverage/money accepted")
			}
		})
	}
	// Aggregate coverage must not depend on which member is visited first.
	a, err := monthlyAggregateStatistics(p.Members)
	if err != nil {
		t.Fatal(err)
	}
	p.Members[0], p.Members[2] = p.Members[2], p.Members[0]
	b, err := monthlyAggregateStatistics(p.Members)
	if err != nil || a != b {
		t.Fatal("member order changed coverage", err)
	}
}

func TestMonthlyStatisticsStrictEvidenceAndJSON(t *testing.T) {
	input := monthlyWalletFixture()
	input.Accounts[0].Spans[0].Tokens = nil
	e, proof := monthlyStatisticsReviewed(t, input)
	for name, mutate := range map[string]func(*MonthlyBillEvidence){
		"version downgrade":       func(e *MonthlyBillEvidence) { e.Version = 2 },
		"hidden partial total":    func(e *MonthlyBillEvidence) { e.Users[0].Tokens = 123 },
		"redundant known flags":   func(e *MonthlyBillEvidence) { e.Users[0].Statistics.TokensKnown = true },
		"known statistic invalid": func(e *MonthlyBillEvidence) { e.Users[0].Requests = -1 },
		"money mismatch":          func(e *MonthlyBillEvidence) { e.Users[0].ClosingQuota++ },
	} {
		t.Run(name, func(t *testing.T) {
			var q MonthlyBillEvidence
			if err := json.Unmarshal(proof, &q); err != nil {
				t.Fatal(err)
			}
			mutate(&q)
			if _, _, err := buildPublishedMonthlyBill(q, time.Now()); err == nil {
				t.Fatal("invalid nullable evidence accepted")
			}
		})
	}
	for name, raw := range map[string]string{
		"omitted token flag":       strings.Replace(string(proof), `,"tokens_known":false`, "", 1),
		"null token flag":          strings.Replace(string(proof), `"tokens_known":false`, `"tokens_known":null`, 1),
		"null coverage":            strings.Replace(string(proof), `"statistics":{"requests_known":true,"tokens_known":false}`, `"statistics":null`, 1),
		"null numeric placeholder": strings.Replace(string(proof), `"tokens":0`, `"tokens":null`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if raw == string(proof) {
				t.Fatal("bad test mutation")
			}
			if _, err := decodeMonthlyBillEvidence([]byte(raw)); err == nil {
				t.Fatal("missing internal coverage accepted")
			}
		})
	}
	c := monthlyUserConsumption(e.Users[0])
	raw := monthlyReviewJSON(t, c)
	for _, bad := range []string{
		strings.Replace(string(raw), `"tokens":null,`, "", 1),
		strings.Replace(string(raw), `"tokens":null`, `"tokens":null,"Tokens":0`, 1),
		strings.Replace(string(raw), `"tokens":null`, `"tokens":0.5`, 1),
		strings.Replace(string(raw), `"tokens":null`, `"tokens":"0"`, 1),
	} {
		var c monthlyConsumption
		if err := json.Unmarshal([]byte(bad), &c); err == nil {
			t.Fatal("malformed customer statistics accepted", bad)
		}
	}
}

func TestMonthlyStatisticsLegacyKnownJSONByteCompatibility(t *testing.T) {
	c := monthlyConsumption{Requests: 0, Tokens: 123456, Amount: "1.000000", Refund: "0.000000", Net: "1.000000"}
	want := []byte(`{"requests":0,"tokens":123456,"amount":"1.000000","refund":"0.000000","net":"1.000000"}`)
	if got := monthlyReviewJSON(t, c); !bytes.Equal(got, want) {
		t.Fatal("legacy JSON changed", string(got))
	}
	var again monthlyConsumption
	if err := json.Unmarshal(want, &again); err != nil || again != c {
		t.Fatal("legacy JSON no longer readable", err)
	}
	input := monthlyWalletFixture()
	input.Accounts[0].Spans[0].Tokens = nil
	e, _ := monthlyStatisticsReviewed(t, input)
	p, _, err := buildPublishedMonthlyBill(e, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// This is the prior reader's explicit version gate, not a claim that the
	// old binary can display nullable bills. Old bills stay version 1/readable.
	if p.MemberDetailsVersion == 0 || p.MemberDetailsVersion == 1 {
		t.Fatal("old reader could misread null as zero")
	}
	if e.Version == 2 {
		t.Fatal("old importer could silently reinterpret new evidence")
	}
}

func TestMonthlyStatisticsNullableCustomerPublishDownloadWithdrawJourney(t *testing.T) {
	input := monthlyWalletFixture()
	input.Accounts[0].Spans[0].Tokens = nil
	input.Accounts[0].Spans[0].Requests = nil
	e, proof := monthlyStatisticsReviewed(t, input)
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.PortalMonthlyStatementEnabled, m.cfg.LocalSnapshotOnly, m.cfg.AlertsDisabled = true, true, true
	g := CustomerGroup{ID: 1, Name: "synthetic stats", PortalEmail: "stats@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 1, GroupID: 1, Username: "synthetic-wallet"}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ctx := context.Background()
	if _, err := ImportMonthlyBill(ctx, m.storeDB, proof, true, now); err != nil {
		t.Fatal(err)
	}
	version, err := MonthlyBillPublicationVersion(ctx, m.storeDB, 1, e.Month, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ImportMonthlyBill(ctx, m.storeDB, proof, true, now); err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	m.RegisterPortalRoutes(r)
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, now.Unix())}
	read := func(path, capability string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.AddCookie(cookie)
		req.Header.Set("X-Monthly-Statistics", capability)
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	download := "/api/monthly-statement/download?month=" + e.Month + "&publication=" + version
	for _, path := range []string{"/api/monthly-statements?month=" + e.Month, "/api/monthly-statement?month=" + e.Month, download} {
		for _, capability := range []string{"", "invalid"} {
			w := read(path, capability)
			if w.Code != 409 || !strings.Contains(w.Body.String(), "请刷新页面") || strings.Contains(w.Body.String(), `"requests"`) {
				t.Fatal("old browser received nullable statistics", path, w.Code)
			}
		}
	}
	for _, path := range []string{"/api/monthly-statements?month=" + e.Month, "/api/monthly-statement?month=" + e.Month} {
		w := read(path, "nullable-v1")
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"requests":null,"tokens":null`) || !strings.Contains(w.Body.String(), `"amount":"400.000000"`) || strings.Contains(w.Body.String(), "400.018") {
			t.Fatal("customer nullable/amount contract failed", path, w.Code, w.Body.String())
		}
	}
	file := read(download, "nullable-v1")
	if file.Code != 200 {
		t.Fatal("missing stats blocked download", file.Code)
	}
	var record PortalMonthlyBill
	if err := m.storeDB.First(&record, "group_id = ? AND month = ?", 1, e.Month).Error; err != nil {
		t.Fatal(err)
	}
	p, err := readPublishedMonthlyBill(record)
	if err != nil {
		t.Fatal(err)
	}
	monthlyAuditProjection(t, p, e, file.Body.Bytes())
	monthlyBatchCheckMemberSheet(t, file.Body.Bytes(), p.Members)
	// Recovering a statistic is a revision, not an in-place change to an issued
	// bill. Preserve money and audit history; the stale download stops working.
	e.Users[0].Statistics = nil
	e.Users[0].Requests, e.Users[0].Tokens = 10, 100
	e.Version = 2
	updated := monthlyReviewJSON(t, e)
	if _, err := ImportMonthlyBill(ctx, m.storeDB, updated, true, now); err == nil {
		t.Fatal("published bill overwritten")
	}
	if err := WithdrawMonthlyBill(ctx, m.storeDB, 1, e.Month, version, "synthetic-local-only", "synthetic statistics recovered", true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ImportMonthlyBill(ctx, m.storeDB, updated, true, now); err != nil {
		t.Fatal(err)
	}
	newVersion, err := MonthlyBillPublicationVersion(ctx, m.storeDB, 1, e.Month, now)
	if err != nil || version == newVersion {
		t.Fatal("statistics revision reused old version", err)
	}
	if w := portalDo(r, "GET", "/api/monthly-statement/download?month="+e.Month+"&publication="+version, "", cookie); w.Code != 409 {
		t.Fatal("stale nullable version downloaded", w.Code)
	}
}
