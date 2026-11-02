package monthlydraft

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Complete synthetic ORIGINAL token observations and historical evidence.
// Setup is bulk-local only; not 12,000 measured source queries or production.
func partitionedCapturedScaleFixture(t *testing.T, accounts int) capturedHistoryFixture {
	t.Helper()
	if accounts < 2 || accounts > 200 {
		t.Fatal("synthetic scale must remain within the real 200-account limit")
	}
	f := newCapturedHistoryFixture(t)
	db := f.wallet.funding.capture
	prototypes := [2][]monthlysource.TokenUsageWindowObservation{}
	for phase := 0; phase < 2; phase++ {
		for day := 0; day < 30; day++ {
			var raw []byte
			if err := db.QueryRow(`SELECT payload FROM portal_monthly_token_capture_slots WHERE job_key=? AND ordinal=?`, f.req.Key(), phase*60+day*2).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var o monthlysource.TokenUsageWindowObservation
			if err := json.Unmarshal(raw, &o); err != nil {
				t.Fatal(err)
			}
			prototypes[phase] = append(prototypes[phase], o)
		}
	}
	for id := int64(203); id <= int64(200+accounts); id++ {
		if err := f.auth.Create(&monitor.TrackedUser{UserID: id, GroupID: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	owner, ids, err := LoadUsageGenerationOwner(t.Context(), f.auth, 1, 1)
	if err != nil || len(ids) != accounts {
		t.Fatal("large current roster", len(ids), err)
	}
	f.req.Owner = owner
	scope, err := f.wallet.funding.scope(t.Context(), f.req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(t.Context(), db, f.req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-time.Hour).UnixMilli()
	for i, scan := range plan.Scans {
		at := base + int64(i%30)*1000 + int64(i/30)*600000
		p := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100000000, EndReached: true, Rows: []monthlysource.FundingLog{}, StartedAtMS: at, CompletedAtMS: at + 1}
		if _, err := monthlysource.SaveFundingPage(t.Context(), db, p); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for phase := 0; phase < 2; phase++ {
		for day := 0; day < 30; day++ {
			for member, id := range ids {
				var o monthlysource.TokenUsageWindowObservation
				body := materialPreparationJSON(t, prototypes[phase][day])
				if err := json.Unmarshal(body, &o); err != nil {
					t.Fatal(err)
				}
				o.Request.UserID, o.Wallet.UserID = id, id
				change := func(r *monthlysource.UsageWindowRow) {
					r.UserID = id
					r.MinID += int64(member) * 10000
					r.MaxID += int64(member) * 10000
				}
				for i := range o.Totals {
					change(&o.Totals[i])
				}
				for i := range o.Rows {
					change(&o.Rows[i].UsageWindowRow)
					if o.Rows[i].TokenID > 0 {
						o.Rows[i].TokenID += int64(member) * 1000
					}
				}
				u, err := monthlysource.UsageObservationFromToken(o)
				if err != nil {
					t.Fatal(err)
				}
				projection, raw := materialPreparationJSON(t, u), materialPreparationJSON(t, o)
				uid := generationHash(projection)
				if _, err := tx.Exec(`INSERT INTO portal_monthly_usage_observations VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, uid, u.Request.SourceIdentity, id, u.Request.From, u.Request.To, u.StartedAtMS, u.CompletedAtMS, projection); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(`INSERT INTO portal_monthly_token_capture_slots VALUES(?,?,?,?,?,?,?,?)`, f.req.Key(), phase*30*accounts+day*accounts+member, id, generationHash(raw), uid, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for phase := 0; phase < 2; phase++ {
		at := base + int64(phase)*600000
		o := monthlysource.WalletObservation{Version: 1, Basis: "database_observation_not_month_boundary", SourceIdentity: scope.Funding.SourceIdentity, StartedAtMS: at, CompletedAtMS: at + 1, Users: []monthlysource.WalletUser{}}
		for _, id := range ids {
			balance, used := int64(499000000), int64(1001000000)
			o.Users = append(o.Users, monthlysource.WalletUser{UserID: id, Exists: true, BalanceQuota: &balance, UsedQuota: &used})
		}
		id, err := monthlysource.SaveWalletObservation(t.Context(), db, o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO portal_monthly_wallet_capture_slots VALUES(?,?,?)`, f.req.Key(), phase, id); err != nil {
			t.Fatal(err)
		}
	}
	history := draftCopy(t, f.f.input)
	prototype := materialPreparationJSON(t, history.Accounts[0])
	history.Accounts = nil
	for _, id := range ids {
		var a monitor.MonthlyPreparationAccount
		if err := json.Unmarshal(prototype, &a); err != nil {
			t.Fatal(err)
		}
		a.UserID, a.Username, a.Baseline.UserID = id, fmt.Sprintf("synthetic-%d", id), id
		for i := range a.Snapshots {
			a.Snapshots[i].UserID = id
		}
		for i := range a.WalletMonths {
			a.WalletMonths[i].UserID = id
		}
		for i := range a.MovementCoverage {
			a.MovementCoverage[i].UserID = id
		}
		history.Accounts = append(history.Accounts, a)
	}
	basis := materialPreparationJSON(t, history.Accounts)
	if err := os.WriteFile(history.Sources[0].Path, basis, 0600); err != nil {
		t.Fatal(err)
	}
	history.Sources[0].SHA256 = generationHash(basis)
	raw := draftJSON(t, history)
	if err := os.WriteFile(f.sources.History.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.sources.History.SHA256 = generationHash(raw)
	f.f.input, f.f.raw, f.f.plan.Owner, f.f.plan.Funding.UserIDs = history, raw, owner, ids
	document := RetainedAutomaticScope{Version: 1, Owner: owner, Month: f.req.Month, SourceIdentity: scope.Funding.SourceIdentity, Scope: automaticScopeFixture(t, f.f)}
	raw = materialPreparationJSON(t, document)
	if err := os.WriteFile(f.sources.Scope.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.sources.Scope.SHA256 = generationHash(raw)
	f.wallet.funding.partitionUsage = true
	return f
}

// The exact same end-to-end body and money/ownership/restart/download checks
// run at 200 accounts in the separate native performance gate. Race runs use
// this bounded scale so detector overhead is not mistaken for a product SLA.
func TestPartitionedCapturedGenerationBoundedRaceMonth(t *testing.T) {
	testPartitionedCapturedGenerationScale(t, 12)
}

func testPartitionedCapturedGenerationScale(t *testing.T, accounts int) {
	t.Helper()
	f := partitionedCapturedScaleFixture(t, accounts)
	d := f.open(t)
	start := time.Now()
	files, _, err := f.spool.Advance(t.Context(), f.req)
	if err != nil {
		t.Fatal("large source seal", err)
	}
	t.Logf("%d accounts / %d pairs complete seal: %s (production deadlines unchanged)", accounts, accounts*30, time.Since(start))
	prepared, err := d.g.prepareCapturedHistoryInput(t.Context(), f.req, files, f.sources, "captured-scope")
	if err != nil {
		t.Fatal("large complete normalization", err)
	}
	var p GenerationPlan
	if json.Unmarshal(prepared.Plan, &p) != nil || len(p.TokenUsageSources) != accounts || len(p.UsageWindows) != 0 || p.CapturedUsageSourceID == "" {
		t.Fatal("partial or legacy large plan")
	}
	d.runtime.worker.pollMin, d.runtime.worker.pollMax = 5*time.Millisecond, 20*time.Millisecond
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if result, err := d.PrepareMonthlyInput(t.Context(), prepared.Plan, prepared.Preparation, "", true); err != nil || !result.Applied {
		t.Fatal("large checked input selection", err)
	}
	request, own, foreign := capturedHistoryHTTP(t, f, d)
	if out := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); out.Code != 202 {
		t.Fatal("large customer click", out.Code, out.Body.String())
	}
	var saved monitor.PortalMonthlyBill
	deadline := time.Now().Add(40 * time.Second)
	for f.auth.First(&saved, "group_id=1 AND month='2026-09'").Error != nil {
		if time.Now().After(deadline) {
			t.Fatal("large bill not published", d.runtime.worker.Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeDeploymentTest(t, d)
	var e monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(saved.Evidence), &e) != nil || len(e.Users) != accounts {
		t.Fatal("partial large saved evidence")
	}
	for _, u := range e.Users {
		if u.OpeningQuota != 500000000 || u.ClosingQuota != 499000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("large per-account/token money changed", u.UserID)
		}
	}
	page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	var view struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &view) != nil || view.Data.Publication == "" {
		t.Fatal("large saved page unavailable")
	}
	if out := request("GET", "/api/monthly-statement?month=2026-09", "", foreign); out.Code == 200 {
		t.Fatal("foreign account read large bill")
	}
	book := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", own)
	if book.Code != 200 {
		t.Fatal("large workbook unavailable", book.Code)
	}
	z, err := zip.NewReader(bytes.NewReader(book.Body.Bytes()), int64(book.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	for _, file := range z.File {
		if file.Name == "xl/worksheets/sheet1.xml" {
			r, err := file.Open()
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(io.LimitReader(r, 2<<20))
			closeErr := r.Close()
			if err != nil || closeErr != nil || !strings.Contains(string(raw), fmt.Sprintf("%d.00", accounts*1000)) || !strings.Contains(string(raw), fmt.Sprintf("%d.00", accounts*998)) {
				t.Fatal("large workbook summary differs from saved per-account money", err, closeErr)
			}
			checked = true
		}
	}
	if !checked {
		t.Fatal("missing large summary sheet")
	}
}
