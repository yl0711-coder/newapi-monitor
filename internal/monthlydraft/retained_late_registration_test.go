package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Original synthetic facts: user 201 is active in September; user 202 is born
// in October. Construct the record before sealing, never edit captured slots.
func retainedLateFixture(t *testing.T, keepActivity, zeroPrice bool) (capturedHistoryFixture, *sourcePreparationDriver, int64) {
	t.Helper()
	f := newCapturedHistoryFixture(t, func(o *monthlysource.TokenUsageWindowObservation) {
		if o.Request.UserID != 202 {
			return
		}
		if !keepActivity {
			o.Rows = []monthlysource.TokenUsageWindowRow{}
			o.Totals = []monthlysource.UsageWindowRow{}
			return
		}
		if zeroPrice {
			for i := range o.Rows {
				o.Rows[i].Quota, o.Rows[i].MinQuota, o.Rows[i].MaxQuota = 0, 0, 0
			}
			for i := range o.Totals {
				o.Totals[i].Quota, o.Totals[i].MinQuota, o.Totals[i].MaxQuota = 0, 0, 0
			}
		}
	})
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	start, _ := time.ParseInLocation("2006-01", f.req.Month, zone)
	end := start.AddDate(0, 1, 0).Unix()
	registered := end + 10*86400
	driver := f.wallet.funding.source.Driver().(*walletPreparationDriver).base
	driver.registrationAt = map[int64]int64{201: start.Unix(), 202: registered}
	log := monthlysource.FundingLog{ID: 700, UserID: 202, CreatedAt: registered, Type: 4, Content: "新用户注册赠送 $1.000000 额度", Other: ""}
	body := materialPreparationJSON(t, map[string]any{"row": log})
	path := filepath.Join(f.cfg.EvidenceRoot, "original-later-signup.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	assemblyHistoryUpdate(t, &f, func(input *monitor.MonthlyPreparationInput) {
		zero := int64(0)
		for i := range input.Accounts {
			a := &input.Accounts[i]
			if a.UserID != 202 {
				continue
			}
			a.Months = []string{"2026-10"}
			a.Baseline = monitor.MonthlyPreparationBalance{UserID: a.UserID, At: end, Source: "fixture#original-later-zero-origin"}
			a.Spans = []monitor.MonthlyPreparationSpan{{UserID: a.UserID, From: end, To: registered + 86400, Requests: &zero, Tokens: &zero, Source: "fixture#later-empty-span"}}
			a.Movements = []monitor.MonthlyPreparationMovement{{UserID: a.UserID, ID: log.ID, At: log.CreatedAt, DeltaQuota: 500000, Source: "later-signup#/row"}}
			a.MovementCoverage = []monitor.MonthlyPreparationCoverage{{UserID: a.UserID, From: end, To: registered + 86400, Source: "fixture#later-funding"}}
			a.Snapshots = []monitor.MonthlyPreparationBalance{{UserID: a.UserID, At: registered + 86400, BalanceQuota: 500000, Source: "fixture#later-wallet"}}
			a.WalletMonths, a.TokenMonths, a.MonthStatistics = nil, nil, nil
		}
		// Independently created fixture proof agrees with the original fields.
		for i := range input.Sources {
			if input.Sources[i].ID != "fixture" {
				continue
			}
			basis := materialPreparationJSON(t, input.Accounts)
			if err := os.WriteFile(input.Sources[i].Path, basis, 0600); err != nil {
				t.Fatal(err)
			}
			input.Sources[i].SHA256 = generationHash(basis)
		}
		input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: "later-signup", Path: path, SHA256: generationHash(body)})
	})
	return f, driver, registered
}

func TestRetainedLateMemberActualGenerationDownloadAndRestart(t *testing.T) {
	f, driver, _ := retainedLateFixture(t, false, false)
	original, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	d, cfg := openHistoryAssembly(t, f, false, nil)
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	calendar := request("GET", "/api/monthly-statement/calendar", "", own)
	first := request("GET", "/api/monthly-statement/generation?month=2026-09", "", own)
	if calendar.Code != 200 || first.Code != 200 {
		t.Fatal(calendar.Code, first.Code)
	}
	method, path, body := capturedHistoryFirstButtonRequest(t, first.Body.Bytes(), calendar.Body.Bytes())
	if out := request(method, path, body, own); out.Code != 202 {
		t.Fatal(out.Code, out.Body.String())
	}
	record := waitHistoryAssemblyBill(t, f, d)
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("missing user evidence")
	}
	active, later := evidence.Users[0], evidence.Users[1]
	if active.UserID != 201 || active.OpeningQuota != 500000000 || active.ConsumeQuota != 1500000 || active.RefundQuota != 500000 || active.ClosingQuota != 499000000 {
		t.Fatal("active user's money changed")
	}
	if later.UserID != 202 || later.OpeningQuota != 0 || later.ClosingQuota != 0 || later.ConsumeQuota != 0 || later.RefundQuota != 0 || len(later.Events) != 0 || later.TokenUsage == nil || len(later.TokenUsage.Rows) != 0 {
		t.Fatal("future gift/activity booked in the earlier month")
	}
	closeDeploymentTest(t, d)
	page, book := assertRetainedLateCustomerDownload(t, f, request, own, foreign)
	if current, err := os.ReadFile(f.sources.History.Path); err != nil || !bytes.Equal(current, original) {
		t.Fatal("original zero origin or future gift was changed", err)
	}
	reopened := f
	reopened.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	d2 := reopened.open(t)
	if err := d2.enableCapturedSourcePreparation(t.Context(), "captured-scope", writeHistoryAssemblyConfig(t, f, cfg), capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	read, session, other := capturedHistoryHTTP(t, f, d2)
	if err := d2.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	closeDeploymentTest(t, d2)
	after, afterBook := assertRetainedLateCustomerDownload(t, f, read, session, other)
	if !bytes.Equal(page, after) || !bytes.Equal(book, afterBook) {
		t.Fatal("restart changed saved bill/download")
	}
	if driver.writes.Load() != 0 {
		t.Fatal("source writes occurred")
	}
}

// Independent source oracle: only 201 existed in September. Read that user's
// ORIGINAL synthetic token archive, not the generated bill, for token totals.
// The older helper intentionally expects TWO active users and stays unchanged.
func assertRetainedLateCustomerDownload(t *testing.T, f capturedHistoryFixture, request func(string, string, string, []*http.Cookie) *httptest.ResponseRecorder, own, foreign []*http.Cookie) ([]byte, []byte) {
	t.Helper()
	page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	var view struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &view) != nil || view.Data.Publication == "" {
		t.Fatal("saved customer statement unavailable", page.Code)
	}
	if out := request("GET", "/api/monthly-statement?month=2026-09", "", foreign); out.Code == 200 {
		t.Fatal("foreign organization read another bill")
	}
	book := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", own)
	if book.Code != 200 || !strings.Contains(book.Header().Get("Content-Type"), "spreadsheetml.sheet") {
		t.Fatal("saved bill download failed", book.Code)
	}
	if out := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", foreign); out.Code == 200 {
		t.Fatal("foreign organization downloaded another bill")
	}
	raw, err := os.ReadFile(filepath.Join(f.f.root, "tokens-201.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, report, err := monthlysource.DecodeTokenUsageMonthArchive(t.Context(), raw, time.Now())
	if err != nil || report.UserID != 201 || report.Month != f.req.Month {
		t.Fatal("original token oracle missing", err)
	}
	want := retainedSeriesView{Month: f.req.Month, Opening: "1000.000000", Closing: "998.000000", Consumption: retainedSeriesConsumption{Amount: "3.000000", Refund: "1.000000", Net: "2.000000"}}
	want.Credits.Total = "0.000000"
	var history monitor.MonthlyPreparationInput
	historyRaw, err := os.ReadFile(f.sources.History.Path)
	if err != nil || json.Unmarshal(historyRaw, &history) != nil {
		t.Fatal("original account oracle missing", err)
	}
	for _, account := range history.Accounts {
		m := retainedSeriesMember{UserID: account.UserID, Username: account.Username, Opening: "0.000000", Closing: "0.000000", CreditTotal: "0.000000", Tokens: []retainedSeriesToken{}, Consumption: retainedSeriesConsumption{Amount: "0.000000", Refund: "0.000000", Net: "0.000000"}}
		if account.UserID == 201 {
			m.Opening, m.Closing, m.Consumption = want.Opening, want.Closing, want.Consumption
			money := func(q int64) string { return new(big.Rat).SetFrac(big.NewInt(q), big.NewInt(500000)).FloatString(6) }
			for _, row := range report.Rows {
				name := row.Name
				if row.TokenID == 0 {
					name = "历史未记录令牌"
				}
				m.Tokens = append(m.Tokens, retainedSeriesToken{TokenID: row.TokenID, Name: name, Amount: money(row.ConsumeQuota), Refund: money(row.RefundQuota), Net: money(row.ConsumeQuota - row.RefundQuota)})
			}
		}
		want.Members = append(want.Members, m)
	}
	oracle := materialPreparationJSON(t, map[string]any{"ok": true, "data": want})
	retainedSeriesWorkbookMatchesSource(t, oracle, page.Body.Bytes(), book.Body.Bytes())
	parts := retainedWorkbookParts(t, book.Body.Bytes())
	for _, private := range []string{"captured-scope", "captured-reconstruction", "observation_database", "reconstructions", "source_identity", "candidate_sha256"} {
		if bytes.Contains(page.Body.Bytes(), []byte(private)) {
			t.Fatal("private proof leaked into customer page", private)
		}
		for _, raw := range parts {
			if bytes.Contains(raw, []byte(private)) {
				t.Fatal("private proof leaked into customer workbook", private)
			}
		}
	}
	return bytes.Clone(page.Body.Bytes()), bytes.Clone(book.Body.Bytes())
}

func TestRetainedLateMemberRejectsUnknownOrContradictoryOriginals(t *testing.T) {
	for _, scenario := range []string{"nonzero_origin", "nonzero_used", "earlier_registration", "unknown_registration", "missing_event", "wrong_user", "wrong_delta", "not_signup", "two_signups", "bad_pin"} {
		t.Run(scenario, func(t *testing.T) {
			f, driver, registered := retainedLateFixture(t, false, false)
			assemblyHistoryUpdate(t, &f, func(input *monitor.MonthlyPreparationInput) {
				a := &input.Accounts[1]
				switch scenario {
				case "nonzero_origin":
					a.Baseline.BalanceQuota = 1
				case "nonzero_used":
					a.Baseline.UsedQuota = 1
				case "earlier_registration":
					driver.registrationAt[202] = a.Baseline.At - 1
				case "unknown_registration":
					driver.registrationAt[202] = 0
				case "missing_event":
					a.Movements = []monitor.MonthlyPreparationMovement{}
				case "wrong_delta":
					a.Movements[0].DeltaQuota++
				case "bad_pin":
					input.Sources[len(input.Sources)-1].SHA256 = generationHash([]byte("changed original"))
				case "wrong_user", "not_signup", "two_signups":
					log := monthlysource.FundingLog{ID: 700, UserID: 202, CreatedAt: registered, Type: 4, Content: "新用户注册赠送 $1.000000 额度", Other: ""}
					if scenario == "wrong_user" {
						log.UserID++
					}
					if scenario == "not_signup" {
						log.Type, log.Content = 1, "使用在线充值成功，充值金额: $1.000000，支付金额：1.00"
					}
					object := map[string]any{"row": log}
					if scenario == "two_signups" {
						log.ID++
						object["second"] = log
						a.Movements = append(a.Movements, monitor.MonthlyPreparationMovement{UserID: 202, ID: log.ID, At: registered, DeltaQuota: 500000, Source: "later-signup#/second"})
					}
					body := materialPreparationJSON(t, object)
					ref := &input.Sources[len(input.Sources)-1]
					if err := os.WriteFile(ref.Path, body, 0600); err != nil {
						t.Fatal(err)
					}
					ref.SHA256 = generationHash(body)
				}
			})
			d, _ := openHistoryAssembly(t, f, false, nil)
			files, _, err := f.spool.Advance(t.Context(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			if _, proof, err := d.assembleRetainedHistoryScope(t.Context(), f.spool, f.req, files, f.sources.History); err == nil || proof != nil {
				t.Fatal("unproven registration accepted")
			}
			generationAdmissionCounts(t, d.receiver, 0, 0)
		})
	}
}

func TestRetainedLateMemberAnyObservedRequestCannotBecomeZero(t *testing.T) {
	for _, zeroPrice := range []bool{false, true} {
		t.Run(map[bool]string{false: "charged", true: "zero_price"}[zeroPrice], func(t *testing.T) {
			f, _, _ := retainedLateFixture(t, true, zeroPrice)
			d, _ := openHistoryAssembly(t, f, false, nil)
			files, _, err := f.spool.Advance(t.Context(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			next, err := newCatalogueCapturedHistoryPreparation(d, f.spool, "captured-scope", f.sources.History, monthlysource.MonthlyObservationFile{}, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if ready, err := next(t.Context(), f.req, files); err == nil || ready {
				t.Fatal("observed request was erased", ready, err)
			}
			generationAdmissionCounts(t, d.receiver, 0, 0)
			closeDeploymentTest(t, d)
		})
	}
}

func TestRetainedLateMemberMetadataFailureRecoversWithoutInventingOrigin(t *testing.T) {
	f, driver, _ := retainedLateFixture(t, false, false)
	d, _ := openHistoryAssembly(t, f, false, nil)
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	calendar := d.runtime.service.(*registrationCalendarService)
	if _, err := calendar.calendar(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion); err != nil {
		t.Fatal(err)
	}
	driver.onQuery = func() error { return errors.New("synthetic metadata outage") }
	if _, proof, err := d.assembleRetainedHistoryScope(t.Context(), f.spool, f.req, files, f.sources.History); err == nil || proof != nil {
		t.Fatal("metadata outage became a registration")
	}
	driver.onQuery = nil
	if scope, proof, err := d.assembleRetainedHistoryScope(t.Context(), f.spool, f.req, files, f.sources.History); err != nil || proof == nil || scope.Scope.Accounts[1].Registration == nil {
		t.Fatal("original metadata did not recover", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, proof, err := d.assembleRetainedHistoryScope(ctx, f.spool, f.req, files, f.sources.History); err == nil || proof != nil {
		t.Fatal("cancelled assembly returned proof")
	}
}

func TestRetainedLateMemberOrdinaryHistoryNeedsNoExtraSourceLookup(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d, _ := openHistoryAssembly(t, f, false, nil)
	raw, err := os.ReadFile(f.sources.History.Path)
	if err != nil {
		t.Fatal(err)
	}
	history, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	month, err := time.ParseInLocation("2006-01", f.req.Month, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		t.Fatal(err)
	}
	driver := f.wallet.funding.source.Driver().(*walletPreparationDriver).base
	before := driver.queries.Load()
	result, err := d.g.retainedLateMemberRegistrations(t.Context(), f.wallet.funding, f.req, history, month.Unix(), month.AddDate(0, 1, 0).Unix())
	if err != nil || len(result) != 0 || driver.queries.Load() != before || driver.writes.Load() != 0 {
		t.Fatal("ordinary retained history incurred a source read or invented registration", err)
	}
}
