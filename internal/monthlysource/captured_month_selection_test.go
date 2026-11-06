package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

func capturedSelectionFixture(t *testing.T, changedFunding bool) (*sql.DB, string, string, OrganizationMonthScope) {
	t.Helper()
	receiver, path, key, scope, plan, _ := retainedTokenMonthCapture(t)
	if err := InitWalletMonthCaptureStore(context.Background(), receiver); err != nil {
		t.Fatal(err)
	}
	fillCapturedSelectionFundsAndWallets(t, receiver, scope, plan, changedFunding)
	return receiver, path, key, scope
}

func fillCapturedSelectionFundsAndWallets(t *testing.T, receiver *sql.DB, scope OrganizationMonthScope, plan FundingMonthCapturePlan, changedFunding bool) {
	t.Helper()
	base := time.Now().Add(-time.Hour)
	for i, scan := range plan.Scans {
		at := base.Add(time.Duration(i) * time.Second).UnixMilli()
		// The same synthetic source also contains consumption logs. MAX(id)
		// is global even when this funding-only traversal observes no rows.
		p := FundingPage{Version: 1, Scan: scan, FenceID: 2000000, EndReached: true, StartedAtMS: at, CompletedAtMS: at + 1, Rows: []FundingLog{}}
		if changedFunding && i == 30 {
			p.Rows = []FundingLog{{ID: 1, UserID: 201, CreatedAt: scan.From + 10, Type: 4, Content: "新用户注册赠送 1 点额度"}}
			p.Next = FundingCursor{ID: 1, At: scan.From + 10}
		}
		if _, err := SaveFundingPage(context.Background(), receiver, p); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		at := time.Now().Add(-time.Duration(10-6*i) * time.Minute).UnixMilli()
		users := []WalletUser{}
		for n, id := range scope.Funding.UserIDs {
			balance, used := int64(7000000+i+n), int64(9007199254740993)+int64(i+n)
			users = append(users, WalletUser{UserID: id, Exists: true, BalanceQuota: &balance, UsedQuota: &used})
		}
		o := WalletObservation{Version: 1, Basis: walletObservationBasis, SourceIdentity: scope.Funding.SourceIdentity,
			StartedAtMS: at, CompletedAtMS: at + 1, Users: users}
		if err := saveWalletCaptureSlot(context.Background(), receiver, plan, i, o); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCapturedMonthSelectionStableReadOnlyOriginalMoneyAndNoHistoryClaims(t *testing.T) {
	receiver, path, key, scope := capturedSelectionFixture(t, false)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := BuildCapturedMonthSelection(context.Background(), receiver, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	var index CapturedMonthSelection
	if json.Unmarshal(first.Selection, &index) != nil || index.Mode != CapturedMonthSelectionMode || len(index.Funding) != 30 || len(index.WalletIDs) != 2 || len(index.Tokens) != 1 || len(index.Tokens[0].Usage) != 30 || len(index.Tokens[0].Parents) != 60 {
		t.Fatal("incomplete selection", index)
	}
	if len(first.Tokens) != 1 {
		t.Fatal(first.Tokens)
	}
	archive, report, err := DecodeTokenUsageMonthArchive(context.Background(), first.Tokens[0].Raw, time.Now())
	if err != nil || archive.SelectionSHA256 != digest(first.Selection) || report.ConsumeQuota != 1000006 || report.RefundQuota != 2 || len(report.Rows) != 3 {
		t.Fatal("source amounts/IDs changed", report, err)
	}
	var generic map[string]any
	if err := json.Unmarshal(first.Selection, &generic); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"opening_balance", "closing_balance", "database", "published", "approved", "complete"} {
		if _, exists := generic[field]; exists {
			t.Fatal("invented history/sealed-file claim", field)
		}
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, err = sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	receiver.SetMaxOpenConns(1)
	second, err := BuildCapturedMonthSelection(context.Background(), receiver, key, scope)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("restart changed selection/archives", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("assembly wrote original captures", err)
	}
}

func TestCapturedMonthSelectionRejectsMissingChangedAndMixedScope(t *testing.T) {
	for _, scenario := range []string{"funding_change", "wallet_missing", "empty_day_missing", "projection_missing", "scope"} {
		t.Run(scenario, func(t *testing.T) {
			receiver, _, key, scope := capturedSelectionFixture(t, scenario == "funding_change")
			switch scenario {
			case "wallet_missing":
				if _, err := receiver.Exec(`DROP TRIGGER monthly_wallet_capture_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := receiver.Exec(`DELETE FROM portal_monthly_wallet_capture_slots WHERE ordinal=1`); err != nil {
					t.Fatal(err)
				}
			case "empty_day_missing":
				if _, err := receiver.Exec(`DROP TRIGGER monthly_token_capture_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := receiver.Exec(`DELETE FROM portal_monthly_token_capture_slots WHERE ordinal=10`); err != nil {
					t.Fatal(err)
				}
			case "projection_missing":
				if _, err := receiver.Exec(`DROP TRIGGER monthly_usage_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := receiver.Exec(`DELETE FROM portal_monthly_usage_observations WHERE id=(SELECT usage_id FROM portal_monthly_token_capture_slots WHERE ordinal=0)`); err != nil {
					t.Fatal(err)
				}
			case "scope":
				scope.GroupID++
			}
			out, err := BuildCapturedMonthSelection(context.Background(), receiver, key, scope)
			if err == nil || len(out.Selection) != 0 || len(out.Tokens) != 0 {
				t.Fatal("bad sources returned usable partial material", scenario, err)
			}
		})
	}
}

func TestCapturedMonthSelectionMultipleAccountsRemainIndependent(t *testing.T) {
	receiver, _, oldKey, oldScope := capturedSelectionFixture(t, false)
	pairs, _, err := LoadCapturedTokenMonth(context.Background(), receiver, oldKey, oldScope, 201)
	if err != nil {
		t.Fatal(err)
	}
	scope := oldScope
	scope.Funding.UserIDs = []int64{201, 202}
	scope.MemberFingerprint = digest([]byte("201,202,"))[:32]
	key := digest([]byte("synthetic two-account assembly"))
	plan, err := EnsureFundingMonthCapture(context.Background(), receiver, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	for phase := 0; phase < 2; phase++ {
		for day, pair := range pairs {
			for member, uid := range scope.Funding.UserIDs {
				original := pair.Older
				if phase == 1 {
					original = pair.Newer
				}
				raw, err := json.Marshal(original)
				if err != nil {
					t.Fatal(err)
				}
				var o TokenUsageWindowObservation
				if err := json.Unmarshal(raw, &o); err != nil {
					t.Fatal(err)
				}
				o.Request.UserID = uid
				o.Wallet.UserID = uid
				o.StartedAtMS += int64(member) * 500
				o.CompletedAtMS += int64(member) * 500
				factor := int64(member + 1)
				offset := int64(member) * 1000000
				change := func(r *UsageWindowRow) {
					r.UserID = uid
					r.Quota *= factor
					r.MinQuota *= factor
					r.MaxQuota *= factor
					r.MinID += offset
					r.MaxID += offset
				}
				for i := range o.Totals {
					change(&o.Totals[i])
				}
				for i := range o.Rows {
					change(&o.Rows[i].UsageWindowRow)
					if o.Rows[i].TokenID != 0 {
						o.Rows[i].TokenID += int64(member) * 100
					}
				}
				ordinal := phase*len(pairs)*len(scope.Funding.UserIDs) + day*len(scope.Funding.UserIDs) + member
				if err := saveTokenCaptureSlot(context.Background(), receiver, plan, ordinal, o); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	fillCapturedSelectionFundsAndWallets(t, receiver, scope, plan, false)
	assembled, err := BuildCapturedMonthSelection(context.Background(), receiver, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(assembled.Tokens) != 2 {
		t.Fatal("missing account", len(assembled.Tokens))
	}
	for i, token := range assembled.Tokens {
		archive, report, err := DecodeTokenUsageMonthArchive(context.Background(), token.Raw, time.Now())
		factor := int64(i + 1)
		if err != nil || token.UserID != scope.Funding.UserIDs[i] || report.UserID != token.UserID || report.ConsumeQuota != 1000006*factor || report.RefundQuota != 2*factor || archive.SelectionSHA256 != digest(assembled.Selection) {
			t.Fatal("accounts netted/mixed", report, err)
		}
	}
}
