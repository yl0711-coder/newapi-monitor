package monthlydraft

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
)

// Actual customer login and generation endpoints -> explicit synthetic worker
// step -> existing policy/import -> customer DTO and XLSX. No production scope,
// online source, real approval or hidden application goroutine is introduced.
func TestTokenGenerationCustomerAutomaticSavedPageAndDownload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	auth, ro, path := usageAuthorityFixture(t)
	m, err := monitor.New(monitor.Settings{StorePath: path, LocalSnapshotOnly: true, AlertsDisabled: true, PortalMonthlyStatementEnabled: true, SessionSecret: "synthetic-token-generation-only"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	const password = "SyntheticLocalOnly!2026"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id IN ?", []int64{1, 2}).Update("portal_pw_admin", string(hash)).Error; err != nil {
		t.Fatal(err)
	}
	f := selectionFixture(t)
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	addTokenArchives(t, &f)
	applyTokenInput(t, &f)
	stageFixture(t, f, "")
	selector, err := GenerationSelector(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewUsageGenerationGuard(ro, selector)
	if err != nil {
		t.Fatal(err)
	}
	f.g.verifyCurrent = guard
	service, err := NewUsageHTTPService(f.g, ro)
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	if err := m.RegisterPortalRoutesWithGeneration(router, service); err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://127.0.0.1")
		req.Header.Set("X-Usage-Bill-Request", "1")
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		for _, c := range cookies {
			req.AddCookie(c)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	login := func(email string) []*http.Cookie {
		w := request("POST", "/login", `{"email":"`+email+`","password":"`+password+`"}`, nil)
		if w.Code != 200 || len(w.Result().Cookies()) == 0 {
			t.Fatal("synthetic customer login failed", w.Code, w.Body.String())
		}
		return w.Result().Cookies()
	}
	own, foreign := login("owner@example.invalid"), login("other@example.invalid")
	if w := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); w.Code != 202 {
		t.Fatal("token generation request rejected", w.Code, w.Body.String())
	}
	if w := request("GET", "/api/monthly-statement?month=2026-09", "", own); w.Code != 404 {
		t.Fatal("unreviewed draft exposed", w.Code)
	}
	scope := automaticScopeFixture(t, f)
	metadata, err := NewObservedAutomaticContext(f.g, func(context.Context, monthlytasks.Request) (AutomaticScope, error) { return scope, nil })
	if err != nil {
		t.Fatal(err)
	}
	published := 0
	publish := func(ctx context.Context, req monthlytasks.Request, raw []byte) error {
		published++
		var evidence monitor.MonthlyBillEvidence
		if json.Unmarshal(raw, &evidence) != nil || evidence.Version != 5 || len(evidence.Users) != 2 || evidence.Users[0].ConsumeQuota != 1500000 || evidence.Users[1].ConsumeQuota != 1500000 {
			t.Fatal("token policy changed existing consumption")
		}
		_, err := m.PublishMonthlyBillForAuthorization(ctx, raw, monitor.MonthlyBillExpectedAuthorization{GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest})
		return err
	}
	worker, err := NewAutomaticLocalWorker(f.g, metadata, publish)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.step(context.Background()); err != nil || !worked || published != 1 {
		t.Fatal("token automatic worker failed", worked, err)
	}
	w := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	var reply struct {
		Data struct {
			Publication  string `json:"publication"`
			Opening      string `json:"month_opening_balance"`
			Closing      string `json:"month_end_balance"`
			TokenVersion int    `json:"token_details_version"`
			Consumption  struct {
				Amount string `json:"amount"`
				Refund string `json:"refund"`
			} `json:"consumption"`
			Members []struct {
				UserID int64 `json:"user_id"`
				Tokens []struct {
					ID     int64  `json:"token_id"`
					Name   string `json:"name"`
					Amount string `json:"amount"`
					Refund string `json:"refund"`
					Net    string `json:"net"`
				} `json:"token_consumption"`
			} `json:"members"`
		} `json:"data"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil || reply.Data.TokenVersion != 1 || reply.Data.Opening != "2000.000000" || reply.Data.Closing != "2006.000000" || reply.Data.Consumption.Amount != "6.000000" || reply.Data.Consumption.Refund != "2.000000" || len(reply.Data.Members) != 2 {
		t.Fatal("customer token bill money differs", w.Code, w.Body.String())
	}
	for _, member := range reply.Data.Members {
		if len(member.Tokens) != 3 {
			t.Fatal("member token rows missing")
		}
		for i, token := range member.Tokens {
			wantID := int64(0)
			wantName, wantAmount, wantRefund, wantNet := "历史未记录令牌", "0.000002", "0.000000", "0.000002"
			if i == 0 {
				wantID = member.UserID*100 + 1
				wantName, wantAmount, wantRefund, wantNet = "renamed", "2.000000", "0.000000", "2.000000"
			}
			if i == 1 {
				wantID = member.UserID*100 + 2
				wantName, wantAmount, wantRefund, wantNet = "same name", "0.999998", "1.000000", "-0.000002"
			}
			if token.ID != wantID || token.Name != wantName || token.Amount != wantAmount || token.Refund != wantRefund || token.Net != wantNet {
				t.Fatal("token source/page amount or ownership mismatch", member)
			}
		}
	}
	downloadPath := "/api/monthly-statement/download?month=2026-09&publication=" + reply.Data.Publication
	file := request("GET", downloadPath, "", own)
	if file.Code != 200 {
		t.Fatal("customer token download unavailable", file.Code)
	}
	book, err := zip.NewReader(bytes.NewReader(file.Body.Bytes()), int64(file.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, part := range book.File {
		if part.Name != "xl/worksheets/sheet3.xml" {
			continue
		}
		found = true
		r, err := part.Open()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(io.LimitReader(r, 2<<20))
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		var sheet struct {
			Rows []struct {
				Number int `xml:"r,attr"`
				Cells  []struct {
					Ref   string `xml:"r,attr"`
					Value string `xml:"v"`
					Text  string `xml:"is>t"`
				} `xml:"c"`
			} `xml:"sheetData>row"`
		}
		if err := xml.Unmarshal(raw, &sheet); err != nil {
			t.Fatal(err)
		}
		if len(sheet.Rows) != 9 {
			t.Fatal("workbook token rows missing")
		}
		for i, row := range sheet.Rows[3:] {
			member := reply.Data.Members[i/3]
			token := member.Tokens[i%3]
			id := fmt.Sprint(token.ID)
			if token.ID == 0 {
				id = "—"
			}
			if len(row.Cells) != 7 || row.Cells[1].Text != fmt.Sprint(member.UserID) || row.Cells[2].Text != token.Name || row.Cells[3].Text != id || row.Cells[4].Value != customerMoneyCents(t, token.Amount) || row.Cells[5].Value != customerMoneyCents(t, token.Refund) || row.Cells[6].Value != customerMoneyCents(t, token.Net) {
				t.Fatal("source/page/workbook token amounts differ", row)
			}
		}
	}
	if !found {
		t.Fatal("token worksheet missing")
	}
	for _, private := range []string{"selection_sha256", "token_review_sha256", "token-month:", "source_reference"} {
		if strings.Contains(w.Body.String(), private) {
			t.Fatal("private token evidence leaked", private)
		}
	}
	if bad := request("GET", downloadPath, "", foreign); bad.Code != 404 {
		t.Fatal("other organization downloaded token bill", bad.Code)
	}
	// Stop generation by making private inputs unavailable. Frozen customer
	// reads/downloads must keep their exact bytes, without a source re-read.
	for _, selected := range f.plan.TokenUsageSources {
		s, _ := tokenSource(f.input, selected.SourceID)
		if err := os.Remove(s.Path); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
		again := request("GET", downloadPath, "", own)
		if page.Code != 200 || again.Code != 200 || !bytes.Equal(page.Body.Bytes(), w.Body.Bytes()) || !bytes.Equal(again.Body.Bytes(), file.Body.Bytes()) {
			t.Fatal("saved customer bill changed when generation input unavailable")
		}
	}
	if worked, err := worker.step(context.Background()); err != nil || worked || published != 1 {
		t.Fatal("saved token bill was generated/published again", err)
	}
}

func TestTokenGenerationSelectedHistoricalReconstructionCombination(t *testing.T) {
	f, _ := reconstructedGenerationFixture(t)
	addTokenArchives(t, &f)
	applyTokenInput(t, &f)
	job := generationSubmit(t, f)
	a, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil || a.ScopeReview == nil || !generationHex(a.TokenSHA256, 32) {
		t.Fatal("historical/token combination rejected", err)
	}
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata(context.Background(), job.Request)
	if err != nil {
		t.Fatal(err)
	}
	handoff, err := f.g.PrepareReviewHandoff(context.Background(), job.Request.Owner, job.ID, meta.BundleContext)
	if err != nil || len(handoff.Bundle) == 0 {
		t.Fatal("historical token handoff failed", err)
	}
}
