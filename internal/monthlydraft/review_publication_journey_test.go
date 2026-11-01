package monthlydraft

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

// Invoked only by the synthetic real-login/automatic-worker journey. An
// explicit fixture review is not a production review or automatic publication.
func testGenerationReviewedPublication(t *testing.T, g *Generation, f generationFixture, job monthlytasks.Task, db *gorm.DB,
	request func(string, string, string, []*http.Cookie) *httptest.ResponseRecorder, own, foreign []*http.Cookie, selectedContexts ...[]byte) {
	t.Helper()
	ctx := context.Background()
	if len(selectedContexts) > 1 {
		t.Fatal("ambiguous synthetic handoff context")
	}
	selectedContext := handoffContext(t, f)
	if len(selectedContexts) == 1 {
		selectedContext = selectedContexts[0]
	}
	handoff, err := testWaitReviewHandoff(ctx, g, job, selectedContext)
	if err != nil {
		t.Fatal(err)
	}
	var bundle monitor.MonthlyPreparationBundle
	if err := json.Unmarshal(handoff.Bundle, &bundle); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	approval := monitor.MonthlyCandidateReview{Version: 1, GroupID: 1, Month: "2026-09", BundleSHA256: generationHash(handoff.Bundle),
		CandidateSHA256: bundle.Entries[0].CandidateSHA256, Decision: "approve", ReviewedBy: "synthetic-test-reviewer-only",
		ReviewedAt: now.Unix(), SourcesReviewed: true, MemberScopeReviewed: true, WalletBasisReviewed: true}
	approval.ReconstructionReviewed = len(bundle.Entries[0].Candidate.ReconstructedUsers) != 0 // Explicit synthetic fixture review only.
	approvalRaw, err := json.Marshal(approval)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := monitor.ReviewMonthlyCandidate(handoff.Input, handoff.Context, handoff.Bundle, approvalRaw, now)
	if err != nil {
		t.Fatal(err)
	}
	const detail = "/api/monthly-statement?month=2026-09"
	if _, err := monitor.ImportMonthlyBill(ctx, db, raw, false, now); err != nil {
		t.Fatal(err)
	}
	if w := request("GET", detail, "", own); w.Code != 404 {
		t.Fatal("dry-run published", w.Code)
	}
	for range 2 {
		if _, err := monitor.ImportMonthlyBill(ctx, db, raw, true, now); err != nil {
			t.Fatal(err)
		}
	}
	w := request("GET", detail, "", own)
	var reply struct {
		Data struct {
			Published   bool   `json:"published"`
			Publication string `json:"publication"`
			Opening     string `json:"month_opening_balance"`
			Closing     string `json:"month_end_balance"`
			Credits     struct {
				Known      string `json:"known_total"`
				Adjustment string `json:"adjustment"`
				Rows       []struct {
					UserID int64  `json:"user_id"`
					Amount string `json:"amount"`
				} `json:"rows"`
			} `json:"credits"`
			Consumption struct {
				Amount   string `json:"amount"`
				Refund   string `json:"refund"`
				Net      string `json:"net"`
				Requests *int64 `json:"requests"`
				Tokens   *int64 `json:"tokens"`
			} `json:"consumption"`
			Members []struct {
				UserID      int64  `json:"user_id"`
				Opening     string `json:"opening"`
				Credit      string `json:"credit_total"`
				Adjustment  string `json:"adjustment"`
				Closing     string `json:"closing"`
				Consumption struct {
					Amount string `json:"amount"`
					Refund string `json:"refund"`
				} `json:"consumption"`
			} `json:"members"`
		} `json:"data"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil {
		t.Fatal("published detail unavailable", w.Code, w.Body.String())
	}
	p := reply.Data
	if !p.Published || len(p.Publication) != 64 || p.Opening != "2000.000000" || p.Closing != "2006.000000" || p.Credits.Known != "810.000000" || p.Credits.Adjustment != "-800.000000" || p.Consumption.Amount != "6.000000" || p.Consumption.Refund != "2.000000" || p.Consumption.Net != "4.000000" || p.Consumption.Requests != nil || p.Consumption.Tokens != nil {
		t.Fatal("published summary differs from reconciled source", w.Body.String())
	}
	for _, private := range []string{"synthetic-test-reviewer-only", "source_reference", "fixture#", "plan_sha256", "funding_review_sha256"} {
		if strings.Contains(w.Body.String(), private) {
			t.Fatal("private review material leaked", private)
		}
	}
	if len(p.Members) != 2 || len(p.Credits.Rows) != 3 {
		t.Fatal("members or movements lost")
	}
	seen := map[string]bool{}
	for _, row := range p.Credits.Rows {
		seen[fmt.Sprintf("%d:%s", row.UserID, row.Amount)] = true
	}
	for _, want := range []string{"201:-800.000000", "201:10.000000", "202:800.000000"} {
		if !seen[want] {
			t.Fatal("transfer or topup lost", want)
		}
	}
	const download = "/api/monthly-statement/download?month=2026-09&publication="
	xlsx := request("GET", download+p.Publication, "", own)
	if xlsx.Code != 200 || xlsx.Header().Get("Content-Type") != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" {
		t.Fatal("download failed", xlsx.Code)
	}
	book, err := zip.NewReader(bytes.NewReader(xlsx.Body.Bytes()), int64(xlsx.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	wantMembers := [][]string{{"1000.000000", "10.000000", "-800.000000", "3.000000", "1.000000", "208.000000"}, {"1000.000000", "800.000000", "0.000000", "3.000000", "1.000000", "1798.000000"}}
	found, summaryFound := false, false
	for _, file := range book.File {
		if file.Name != "xl/worksheets/sheet2.xml" && file.Name != "xl/worksheets/sheet1.xml" {
			continue
		}
		reader, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(reader, 2<<20))
		_ = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		var sheet struct {
			Rows []struct {
				Number int `xml:"r,attr"`
				Cells  []struct {
					Value string `xml:"v"`
					Text  string `xml:"is>t"`
				} `xml:"c"`
			} `xml:"sheetData>row"`
		}
		if err := xml.Unmarshal(body, &sheet); err != nil {
			t.Fatal(err)
		}
		if file.Name == "xl/worksheets/sheet1.xml" {
			summaryFound = true
			want := map[string]string{"充值合计": "810.000000", "赠送合计": "0.000000", "到账合计": p.Credits.Known,
				"退款金额（元）": p.Consumption.Refund, "净消费（元）": p.Consumption.Net, "余额扣减调整（元）": p.Credits.Adjustment,
				"月初余额（元）": p.Opening, "月末余额（元）": p.Closing}
			metrics := false
			for i, row := range sheet.Rows {
				if len(row.Cells) == 0 {
					continue
				}
				if amount, ok := want[row.Cells[0].Text]; ok {
					if len(row.Cells) < 2 || row.Cells[1].Value != customerMoneyCents(t, amount) {
						t.Fatal("summary workbook differs from page", row.Cells[0].Text)
					}
					delete(want, row.Cells[0].Text)
				}
				if row.Cells[0].Text == "总消费（元）" {
					if i+1 >= len(sheet.Rows) {
						t.Fatal("summary metrics absent")
					}
					cells := sheet.Rows[i+1].Cells
					if len(cells) != 3 || cells[0].Value != customerMoneyCents(t, p.Consumption.Amount) || cells[1].Text != "—" || cells[2].Text != "—" {
						t.Fatal("summary cost or unknown stats changed")
					}
					metrics = true
				}
			}
			if len(want) != 0 || !metrics {
				t.Fatal("summary workbook incomplete", want)
			}
			continue
		}
		found = true
		count, footerCount := 0, 0
		for _, row := range sheet.Rows {
			if row.Number < 4 {
				continue
			}
			if len(row.Cells) == 7 && row.Cells[0].Text == "组织合计" {
				footerCount++
				for i, amount := range []string{p.Opening, p.Credits.Known, p.Credits.Adjustment, p.Consumption.Amount, p.Consumption.Refund, p.Closing} {
					if row.Cells[i+1].Value != customerMoneyCents(t, amount) {
						t.Fatal("organization footer money differs from source", i, row.Cells[i+1].Value)
					}
				}
				continue
			}
			index := row.Number - 4
			if index >= 2 || len(row.Cells) != 7 {
				t.Fatal("unexpected member sheet row")
			}
			count++
			member := p.Members[index]
			if member.UserID != int64(201+index) || !strings.Contains(row.Cells[0].Text, fmt.Sprintf("ID %d", member.UserID)) {
				t.Fatal("member sheet identity changed")
			}
			page := []string{member.Opening, member.Credit, member.Adjustment, member.Consumption.Amount, member.Consumption.Refund, member.Closing}
			values := []string{}
			for _, cell := range row.Cells[1:] {
				values = append(values, cell.Value)
			}
			centValues := make([]string, len(page))
			for i, amount := range page {
				centValues[i] = customerMoneyCents(t, amount)
			}
			if !reflect.DeepEqual(page, wantMembers[index]) || !reflect.DeepEqual(values, centValues) {
				t.Fatal("per-account source/page/workbook money differs", page, values)
			}
		}
		if count != 2 || footerCount != 1 {
			t.Fatal("member sheet incomplete")
		}
	}
	if !found || !summaryFound {
		t.Fatal("summary/member sheet absent")
	}
	for _, path := range []string{detail, download + p.Publication} {
		if w := request("GET", path, "", foreign); w.Code != 404 {
			t.Fatal("other organization accessed bill", w.Code)
		}
	}
	if w := request("GET", download+strings.Repeat("0", 64), "", own); w.Code != 409 {
		t.Fatal("wrong publication accepted", w.Code)
	}
	if w := request("GET", detail, "", own); w.Code != 200 || !bytes.Equal(w.Body.Bytes(), request("GET", detail, "", own).Body.Bytes()) {
		t.Fatal("repeat detail changed")
	}
}

// DraftReady is a durable marker, not a join of the worker goroutine: Run still
// performs its final authority check before releasing generationSlots. Only a
// transient busy response may be retried by this synthetic operator journey.
// No evidence/permission failure is swallowed and no production retry changes.
func testWaitReviewHandoff(parent context.Context, g *Generation, job monthlytasks.Task, raw []byte) (ReviewHandoff, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return ReviewHandoff{}, err
		}
		handoff, err := g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, raw)
		if !errors.Is(err, ErrGenerationBusy) {
			return handoff, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ReviewHandoff{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func TestSyntheticReviewWaitOnlyRetriesBusy(t *testing.T) {
	f := generationFixtureNew(t)
	g := f.g
	job, err := g.Submit(context.Background(), f.plan, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Run(context.Background(), job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	// Deterministically reproduce the busy tail; never release a guard owned
	// by another worker or hide an actual non-busy error.
	generationSlots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err = testWaitReviewHandoff(ctx, g, job, handoffContext(t, f))
	cancel()
	<-generationSlots
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("busy wait did not honor cancellation", err)
	}
	if _, err := testWaitReviewHandoff(context.Background(), g, job, nil); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("non-busy error swallowed", err)
	}
	if _, err := testWaitReviewHandoff(context.Background(), g, job, handoffContext(t, f)); err != nil {
		t.Fatal("handoff failed after busy owner exited", err)
	}
}
