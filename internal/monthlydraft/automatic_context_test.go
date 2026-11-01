package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Only wallet/directory scope comes from the synthetic trusted producer. No
// event classifications are copied into the new adapter's input.
func automaticScopeFixture(t *testing.T, f generationFixture) AutomaticScope {
	t.Helper()
	var c monitor.MonthlyBundleContext
	if err := json.Unmarshal(handoffContext(t, f), &c); err != nil {
		t.Fatal(err)
	}
	s := AutomaticScope{Group: c.Groups[0], Accounts: []AutomaticWalletScope{}}
	for _, a := range c.Accounts {
		s.Accounts = append(s.Accounts, AutomaticWalletScope{UserID: a.UserID, WalletOnly: a.WalletOnly, WalletSource: a.WalletSource})
	}
	return s
}

func TestObservedAutomaticContextOriginalEventsAndReadOnly(t *testing.T) {
	f := generationFixtureNew(t)
	job := generationSubmit(t, f)
	scope := automaticScopeFixture(t, f)
	load, err := NewObservedAutomaticContext(f.g, func(_ context.Context, req monthlytasks.Request) (AutomaticScope, error) {
		if req != job.Request {
			t.Fatal("loader received another customer request")
		}
		return scope, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	before := map[string]string{}
	for _, path := range []string{f.receiverPath, f.path, f.input.Sources[0].Path} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = generationHash(raw)
	}
	out, err := load(context.Background(), job.Request)
	if err != nil {
		t.Fatal(err)
	}
	var c monitor.MonthlyBundleContext
	if err := json.Unmarshal(out.BundleContext, &c); err != nil || len(c.Accounts) != 2 || len(c.Accounts[0].Events) != 2 || len(c.Accounts[1].Events) != 1 {
		t.Fatal("events missing or assigned to operator", c, err)
	}
	a, b := c.Accounts[0], c.Accounts[1]
	if a.UserID != 201 || b.UserID != 202 || a.Events[0].ID != 1 || a.Events[0].DeltaQuota != -400000000 || a.Events[0].Kind != "adjustment" || a.Events[0].Method != "余额扣减" ||
		a.Events[1].ID != 3 || a.Events[1].DeltaQuota != 5000000 || a.Events[1].Kind != "recharge" || a.Events[1].Method != "在线充值" ||
		b.Events[0].ID != 2 || b.Events[0].DeltaQuota != 400000000 || b.Events[0].Method != "后台充值" {
		t.Fatal("original normalization/payment-vs-credit/transfer changed", c.Accounts)
	}
	for _, marker := range []string{"reviewed_by", "admin_info", "sources_reviewed", "published", "server_uuid"} {
		if bytes.Contains(out.BundleContext, []byte(marker)) {
			t.Fatal("raw/admin/publication metadata leaked", marker)
		}
	}
	// Reordering a trusted directory must not cause a different evidence hash.
	scope.Group.Members[0], scope.Group.Members[1] = scope.Group.Members[1], scope.Group.Members[0]
	again, err := load(context.Background(), job.Request)
	if err != nil || !bytes.Equal(again.BundleContext, out.BundleContext) {
		t.Fatal("metadata changed for equivalent member ordering", err)
	}
	scope.Group.Name = "caller changed after return"
	if bytes.Contains(out.BundleContext, []byte(scope.Group.Name)) {
		t.Fatal("returned context aliases loader state")
	}
	for path, digest := range before {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != digest {
			t.Fatal("metadata preparation wrote receiver or evidence", err)
		}
	}
	after, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || after != job || after.State != monthlytasks.Queued {
		t.Fatal("metadata preparation started/changed task", after, err)
	}
}

func TestObservedAutomaticContextRejectsScopeOrMoneyMismatch(t *testing.T) {
	f := generationFixtureNew(t)
	windows, err := f.g.auditWindows(context.Background(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"one_point", "wrong_id", "wrong_time", "wrong_target", "missing_event", "duplicate_event", "extra_event", "payment_not_credit", "missing_account", "missing_member", "foreign_member", "wrong_username", "duplicate_metadata", "foreign_metadata", "wallet_unknown", "missing_wallet_source", "foreign_source", "foreign_group", "wrong_month", "missing_observation"} {
		t.Run(scenario, func(t *testing.T) {
			input := draftCopy(t, f.input)
			scope := automaticScopeFixture(t, f)
			selected := windows
			switch scenario {
			case "one_point":
				input.Accounts[0].Movements[0].DeltaQuota++
			case "wrong_id":
				input.Accounts[0].Movements[0].ID = 999
			case "wrong_time":
				input.Accounts[0].Movements[0].At++
			case "wrong_target":
				input.Accounts[0].Movements[0].UserID = 202
			case "missing_event":
				input.Accounts[0].Movements = input.Accounts[0].Movements[1:]
			case "duplicate_event":
				input.Accounts[0].Movements = append(input.Accounts[0].Movements, input.Accounts[0].Movements[0])
			case "extra_event":
				m := input.Accounts[0].Movements[0]
				m.ID = 999
				input.Accounts[0].Movements = append(input.Accounts[0].Movements, m)
			case "payment_not_credit":
				input.Accounts[0].Movements[1].DeltaQuota = 500000 // payment 1 != credited 10
			case "missing_account":
				scope.Accounts = scope.Accounts[:1]
			case "missing_member":
				scope.Group.Members = scope.Group.Members[:1]
			case "foreign_member":
				scope.Group.Members[0].UserID = 999
			case "wrong_username":
				scope.Group.Members[0].Username = "another account"
			case "duplicate_metadata":
				scope.Accounts[1] = scope.Accounts[0]
			case "foreign_metadata":
				scope.Accounts[0].UserID = 999
			case "wallet_unknown":
				scope.Accounts[0].WalletOnly = false
			case "missing_wallet_source":
				scope.Accounts[0].WalletSource = ""
			case "foreign_source":
				scope.Accounts[0].WalletSource = "not-retained#wallet"
			case "foreign_group":
				scope.Group.GroupID = 2
			case "wrong_month":
				scope.Group.Months = []string{"2026-08"}
			case "missing_observation":
				selected = windows[:len(windows)-1]
			}
			raw := draftJSON(t, input)
			plan := f.plan
			plan.PreparationSHA256 = generationHash(raw)
			out, err := observedAutomaticContext(context.Background(), plan, raw, selected, scope)
			if err == nil || len(out.BundleContext) != 0 || len(out.Corrections) != 0 {
				t.Fatal("bad metadata produced partial context", scenario, err)
			}
		})
	}
}

func TestObservedAutomaticContextFactoryFailureBoundaries(t *testing.T) {
	for _, scenario := range []string{"loader_error", "cancel", "source_changed", "authority_changed", "funding_damaged", "foreign_request"} {
		t.Run(scenario, func(t *testing.T) {
			f := generationFixtureNew(t)
			job := generationSubmit(t, f)
			scope := automaticScopeFixture(t, f)
			load, err := NewObservedAutomaticContext(f.g, func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
				switch scenario {
				case "loader_error":
					return AutomaticScope{}, errors.New("synthetic trusted-source timeout")
				case "source_changed":
					if err := os.WriteFile(f.input.Sources[0].Path, []byte("changed synthetic source"), 0600); err != nil {
						t.Fatal(err)
					}
				case "authority_changed":
					f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
				}
				return scope, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "cancel":
				cancel()
			case "funding_damaged":
				// Damage only this new synthetic source, never a retained/live DB.
				db := generationDB(t, f.path, false)
				if _, err := db.Exec(`DELETE FROM portal_monthly_funding_records WHERE scan_id=?`, f.plan.Windows[0].NewerScanID); err == nil {
					t.Fatal("fixture unexpectedly allowed source deletion")
				}
				if _, err := db.Exec(`DROP TRIGGER monthly_funding_records_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`DELETE FROM portal_monthly_funding_records WHERE scan_id=?`, f.plan.Windows[0].NewerScanID); err != nil {
					t.Fatal(err)
				}
			case "foreign_request":
				job.Request.InputSHA256 = strings.Repeat("a", 64)
			}
			out, err := load(ctx, job.Request)
			if err == nil || len(out.BundleContext) != 0 {
				t.Fatal("failed source produced usable metadata", err)
			}
		})
	}
	if _, err := NewObservedAutomaticContext(nil, nil); err == nil {
		t.Fatal("nil dependencies accepted")
	}
}

func TestObservedAutomaticContextMissingHourAndCancellation(t *testing.T) {
	f := generationFixtureNew(t)
	// Existing fixture has no money events after the first hour. Missing that
	// hour is NOT an empty month, even if the remaining observations are empty.
	windows, err := f.g.auditWindows(context.Background(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
	out, err := observedAutomaticContext(context.Background(), f.plan, f.raw, windows[1:], automaticScopeFixture(t, f))
	if err == nil || len(out.BundleContext) != 0 {
		t.Fatal("unobserved hour treated as empty month")
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if out, err := observedAutomaticContext(ctx, f.plan, f.raw, windows, automaticScopeFixture(t, f)); err == nil || len(out.BundleContext) != 0 {
		t.Fatal("expired preparation succeeded")
	}
}
func TestObservedAutomaticContextEmptyEventsAreExplicit(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	db := generationDB(t, filepath.Join(t.TempDir(), "empty-funding.sqlite"), false)
	if err := monthlysource.InitFundingPageStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	var windows []monthlysource.FundingWindowObservation
	plan := f.plan
	plan.Windows = nil
	base := (f.plan.Windows[len(f.plan.Windows)-1].To + 86400) * 1000
	for i, w := range f.plan.Windows {
		var scans []string
		for pass := int64(0); pass < 2; pass++ {
			scan, err := monthlysource.NewFundingScan(plan.Funding.SourceIdentity, plan.Funding.Rules, plan.Funding.UserIDs, w.From, w.To)
			if err != nil {
				t.Fatal(err)
			}
			at := base + int64(i)*20 + pass*300000
			if _, err := monthlysource.SaveFundingPage(ctx, db, monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100,
				EndReached: true, StartedAtMS: at, CompletedAtMS: at + 10, Rows: []monthlysource.FundingLog{}}); err != nil {
				t.Fatal(err)
			}
			scans = append(scans, scan.ID)
		}
		observation, err := monthlysource.ReviewFundingWindow(ctx, db, scans[0], scans[1])
		if err != nil {
			t.Fatal(err)
		}
		windows = append(windows, observation)
		c := observation.Comparison()
		plan.Windows = append(plan.Windows, monthlysource.FundingMonthWindowVersion{From: c.From, To: c.To,
			OlderScanID: c.OlderScanID, NewerScanID: c.NewerScanID, OlderHeadID: c.OlderHeadID, NewerHeadID: c.NewerHeadID})
	}
	input := draftCopy(t, f.input)
	for i := range input.Accounts {
		a := &input.Accounts[i]
		a.Movements = []monitor.MonthlyPreparationMovement{}
		a.Snapshots[0].BalanceQuota = 499000000 // 1000 - 3 + 1 = 998, no funding.
	}
	raw := draftJSON(t, input)
	plan.PreparationSHA256 = generationHash(raw)
	out, err := observedAutomaticContext(ctx, plan, raw, windows, automaticScopeFixture(t, f))
	if err != nil {
		t.Fatal(err)
	}
	var result monitor.MonthlyBundleContext
	if err := json.Unmarshal(out.BundleContext, &result); err != nil || len(result.Accounts) != 2 {
		t.Fatal("empty observed month missing accounts", err)
	}
	for _, a := range result.Accounts {
		if a.Events == nil || len(a.Events) != 0 {
			t.Fatal("proved empty events must be explicit, not unknown", a.UserID)
		}
	}
}
