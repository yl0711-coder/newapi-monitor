package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func usageMonthCapture(r FundingMonthRequest, user, from, to int64, rows []UsageWindowRow) UsageWindowObservation {
	balance, used := int64(-1), int64(9007199254740993)
	return UsageWindowObservation{Version: 1, Basis: usageWindowBasis,
		Request:     UsageWindowRequest{SourceIdentity: r.SourceIdentity, Rules: r.Rules, UserID: user, From: from, To: to},
		StartedAtMS: fundingMonthTestNow.Add(-time.Hour).UnixMilli(), CompletedAtMS: fundingMonthTestNow.Add(-time.Hour).UnixMilli() + 1,
		Wallet: WalletUser{UserID: user, Exists: true, BalanceQuota: &balance, UsedQuota: &used}, Rows: rows}
}

func usageMonthRow(user, at int64, kind int, source string, quota int64) UsageWindowRow {
	return UsageWindowRow{UserID: user, Type: kind, BillingSource: source, Records: 1, Quota: quota,
		MinQuota: quota, MaxQuota: quota, MinID: at, MaxID: at, FirstAt: at, LastAt: at}
}

func usageMonthSavePair(t *testing.T, db *sql.DB, a, b UsageWindowObservation) UsageWindowVersion {
	t.Helper()
	oldID, err := SaveUsageWindowObservation(context.Background(), db, a)
	if err != nil {
		t.Fatal(err)
	}
	newID, err := SaveUsageWindowObservation(context.Background(), db, b)
	if err != nil {
		t.Fatal(err)
	}
	return UsageWindowVersion{OlderID: oldID, NewerID: newID}
}

func usageMonthNewer(a UsageWindowObservation) UsageWindowObservation {
	b := a
	b.StartedAtMS += 300001
	b.CompletedAtMS += 300001
	b.Rows = slices.Clone(a.Rows)
	// Ongoing live wallet activity must not make a closed window look changed.
	changedBalance, changedUsed := int64(1000000), int64(9007199255740993)
	b.Wallet.BalanceQuota, b.Wallet.UsedQuota = &changedBalance, &changedUsed
	return b
}

func usageMonthDays(t *testing.T, db *sql.DB, r FundingMonthRequest, user, from, to int64, empty bool) []UsageWindowVersion {
	t.Helper()
	versions := []UsageWindowVersion{}
	for at := from; at < to; at += 86400 {
		rows := []UsageWindowRow{}
		if !empty {
			rows = []UsageWindowRow{
				usageMonthRow(user, at+1, 2, "subscription", 7),
				usageMonthRow(user, at+2, 2, "unspecified", 11),
				usageMonthRow(user, at+3, 2, "wallet", 9007199254740993),
				usageMonthRow(user, at+4, 6, "wallet", 17),
			}
		}
		a := usageMonthCapture(r, user, at, at+86400, rows)
		versions = append(versions, usageMonthSavePair(t, db, a, usageMonthNewer(a)))
	}
	return versions
}

func TestUsageMonthExactTotalsSourcesAccountIsolationAndDetachedReport(t *testing.T) {
	db, _ := usageTestStore(t)
	r, from, to := fundingMonthFixture(t, "2026-09", []int64{202, 201, 203})
	versions := usageMonthDays(t, db, r, 201, from, to, false)
	versions = append(versions, usageMonthDays(t, db, r, 202, from, to, true)...)
	slices.Reverse(versions)
	before := slices.Clone(versions)
	o, err := reviewUsageMonth(context.Background(), db, r, versions, fundingMonthTestNow)
	if err != nil {
		t.Fatal(err)
	}
	got := o.Report()
	if !reflect.DeepEqual(before, versions) || !reflect.DeepEqual(r.UserIDs, []int64{202, 201, 203}) ||
		got.Basis != usageMonthBasis || got.Timezone != "UTC+08:00" || got.From != from || got.To != to {
		t.Fatal("mutated input or month scope changed", got)
	}
	a, b, c := got.Accounts[0], got.Accounts[1], got.Accounts[2]
	want := []UsageMonthSourceTotal{
		{BillingSource: "subscription", ConsumeQuota: 210, ConsumeRecords: 30},
		{BillingSource: "unspecified", ConsumeQuota: 330, ConsumeRecords: 30},
		{BillingSource: "wallet", ConsumeQuota: 270215977642229790, RefundQuota: 510, ConsumeRecords: 30, RefundRecords: 30},
	}
	if a.UserID != 201 || a.ObservedSeconds != to-from || a.MatchingSeconds != to-from || a.Totals == nil || !reflect.DeepEqual(a.Totals.Sources, want) ||
		b.Totals == nil || b.Totals.Sources == nil || len(b.Totals.Sources) != 0 || c.Totals != nil || !reflect.DeepEqual(c.MissingWindows, []FundingInterval{{From: from, To: to}}) {
		t.Fatal("precision, refund, unknown source, empty or missing account totals wrong", got)
	}
	if !slices.Contains(a.Windows[0].Issues, "billing_source_unspecified") {
		t.Fatal("unknown source was silently declared wallet")
	}
	got.Accounts[0].Windows[0].Rows[0].Quota = 0
	got.Accounts[0].Windows[0].Issues[0] = "changed"
	got.Accounts[0].Totals.Sources[0].ConsumeQuota = 0
	got.Accounts[2].MissingWindows[0].From = 0
	if again := o.Report(); again.Accounts[0].Totals.Sources[0].ConsumeQuota != 210 || again.Accounts[0].Windows[0].Rows[0].Quota != 7 || again.Accounts[0].Windows[0].Issues[0] != "billing_source_unspecified" || again.Accounts[2].MissingWindows[0].From != from {
		t.Fatal("caller mutated private audit state")
	}
	if _, err := json.Marshal(o); err == nil {
		t.Fatal("opaque observation serialized as proof")
	}
	raw, err := json.Marshal(o.Report())
	if err != nil || json.Unmarshal(raw, &o) == nil || o.report != nil {
		t.Fatal("editable report reconstructed audit observation", err)
	}
}

func TestUsageMonthGapsChangesMissingAccountsAndZeroCharges(t *testing.T) {
	for _, scenario := range []string{"gap", "changed_quota", "changed_count", "changed_bounds", "missing_account", "zero_charge"} {
		t.Run(scenario, func(t *testing.T) {
			db, _ := usageTestStore(t)
			r, from, to := fundingMonthFixture(t, "2026-09", []int64{201})
			versions := usageMonthDays(t, db, r, 201, from, to, true)
			a := usageMonthCapture(r, 201, from, from+86400, []UsageWindowRow{usageMonthRow(201, from+1, 2, "wallet", 5)})
			b := usageMonthNewer(a)
			switch scenario {
			case "gap":
				versions = versions[1:]
			case "changed_quota":
				b.Rows[0] = usageMonthRow(201, from+1, 2, "wallet", 6)
			case "changed_count":
				b.Rows[0].Records, b.Rows[0].MaxID, b.Rows[0].Quota = 2, from+2, 10
			case "changed_bounds":
				b.Rows[0].LastAt++
			case "missing_account":
				b.Wallet = WalletUser{UserID: 201}
			case "zero_charge":
				a.Rows[0] = usageMonthRow(201, from+1, 2, "wallet", 0)
				b = usageMonthNewer(a)
			}
			if scenario != "gap" {
				versions[0] = usageMonthSavePair(t, db, a, b)
			}
			o, err := reviewUsageMonth(context.Background(), db, r, versions, fundingMonthTestNow)
			if err != nil {
				t.Fatal(err)
			}
			account := o.Report().Accounts[0]
			if scenario == "zero_charge" {
				if account.Totals == nil || account.Totals.Sources[0].ConsumeQuota != 0 || account.Totals.Sources[0].ConsumeRecords != 1 {
					t.Fatal("zero-charge record lost", account)
				}
			} else if account.Totals != nil {
				t.Fatal("incomplete/changed observations returned monthly money", account)
			}
			if scenario == "gap" && !reflect.DeepEqual(account.MissingWindows, []FundingInterval{{From: from, To: from + 86400}}) {
				t.Fatal("gap hidden", account)
			}
			if strings.HasPrefix(scenario, "changed_") && (account.Windows[0].Matching || len(account.Windows[0].Rows) != 0) {
				t.Fatal("changed source selected silently")
			}
		})
	}
}

func TestUsageMonthRejectsWrongScopeOrderOverlapAndCorruption(t *testing.T) {
	for _, scenario := range []string{"wrong_user", "wrong_source", "wrong_rules", "wrong_window", "too_close", "reversed", "future", "cross_month", "overlap", "reuse", "missing_version", "corrupt", "overflow"} {
		t.Run(scenario, func(t *testing.T) {
			db, _ := usageTestStore(t)
			r, from, _ := fundingMonthFixture(t, "2026-09", []int64{201})
			a := usageMonthCapture(r, 201, from, from+86400, []UsageWindowRow{usageMonthRow(201, from+1, 2, "wallet", 5)})
			b := usageMonthNewer(a)
			switch scenario {
			case "wrong_user":
				r.UserIDs = []int64{202}
			case "wrong_source":
				r.SourceIdentity = strings.Repeat("a", 64)
			case "wrong_rules":
				r.Rules.QuotaPerUnit = 1
			case "wrong_window":
				b.Request.To--
			case "too_close":
				b.StartedAtMS--
				b.CompletedAtMS--
			case "reversed":
				a, b = b, a
			case "future":
				b.StartedAtMS = fundingMonthTestNow.UnixMilli()
				b.CompletedAtMS = b.StartedAtMS + 1
			case "cross_month":
				a.Request.From--
				b.Request.From--
				a.Request.To--
				b.Request.To--
			case "overflow":
				a.Rows[0] = usageMonthRow(201, from+1, 2, "wallet", math.MaxInt64)
				b = usageMonthNewer(a)
			}
			versions := []UsageWindowVersion{usageMonthSavePair(t, db, a, b)}
			switch scenario {
			case "reuse":
				versions = append(versions, versions[0])
			case "missing_version":
				versions[0].NewerID = strings.Repeat("b", 64)
			case "corrupt":
				if _, err := db.Exec(`DROP TRIGGER monthly_usage_no_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`UPDATE portal_monthly_usage_observations SET window_to=window_to+1 WHERE id=?`, versions[0].NewerID); err != nil {
					t.Fatal(err)
				}
			case "overlap", "overflow":
				c := usageMonthCapture(r, 201, from+86400, from+2*86400, []UsageWindowRow{usageMonthRow(201, from+86401, 2, "wallet", 1)})
				if scenario == "overlap" {
					c.Request.From--
					c.Request.To--
				}
				versions = append(versions, usageMonthSavePair(t, db, c, usageMonthNewer(c)))
			}
			o, err := reviewUsageMonth(context.Background(), db, r, versions, fundingMonthTestNow)
			if err == nil || o.report != nil {
				t.Fatal("unsafe month accepted or partial result leaked", scenario, err)
			}
			if db.Stats().InUse != 0 {
				t.Fatal("receiver connection leaked")
			}
		})
	}
}

func TestUsageMonthCalendarCancellationReadOnlyAndBudget(t *testing.T) {
	db, path := usageTestStore(t)
	ctx := context.Background()
	for _, tc := range []struct {
		month string
		days  int64
	}{{"2024-02", 29}, {"2026-02", 28}, {"2026-09", 30}, {"2026-08", 31}, {"2025-12", 31}} {
		r, from, to := fundingMonthFixture(t, tc.month, []int64{201})
		o, err := reviewUsageMonth(ctx, db, r, nil, fundingMonthTestNow)
		if err != nil || o.Report().To-o.Report().From != tc.days*86400 || !reflect.DeepEqual(o.Report().Accounts[0].MissingWindows, []FundingInterval{{From: from, To: to}}) {
			t.Fatal("calendar gap wrong", err)
		}
	}
	r, from, _ := fundingMonthFixture(t, "2026-09", []int64{201})
	for _, month := range []string{"2026-9", "2026-10", "2026-11", "2019-12", ""} {
		r.Month = month
		if o, err := reviewUsageMonth(ctx, db, r, nil, fundingMonthTestNow); err == nil || o.report != nil {
			t.Fatal("invalid/current month accepted", month)
		}
	}
	r.Month = "2026-09"
	boundary := time.Date(2026, 9, 30, 16, 0, 0, 0, time.UTC)
	if _, err := reviewUsageMonth(ctx, db, r, nil, boundary); err != nil {
		t.Fatal(err)
	}
	if _, err := reviewUsageMonth(ctx, db, r, nil, boundary.Add(-time.Nanosecond)); err == nil {
		t.Fatal("UTC boundary used instead of UTC+8")
	}
	a := usageMonthCapture(r, 201, from, from+86400, []UsageWindowRow{})
	v := usageMonthSavePair(t, db, a, usageMonthNewer(a))
	for _, scenario := range []string{"nil_context", "nil_db", "canceled", "busy", "too_many", "bad_id", "duplicate_user", "bad_source"} {
		candidate, candidateDB, candidateContext, versions := r, db, ctx, []UsageWindowVersion{v}
		switch scenario {
		case "nil_context":
			candidateContext = nil
		case "nil_db":
			candidateDB = nil
		case "canceled":
			var cancel context.CancelFunc
			candidateContext, cancel = context.WithCancel(ctx)
			cancel()
		case "busy":
			usageMonthSlots <- struct{}{}
		case "too_many":
			versions = make([]UsageWindowVersion, usageMonthWindowLimit+1)
		case "bad_id":
			versions[0].OlderID = "invalid"
		case "duplicate_user":
			candidate.UserIDs = []int64{201, 201}
		case "bad_source":
			candidate.SourceIdentity = "invalid"
		}
		o, err := reviewUsageMonth(candidateContext, candidateDB, candidate, versions, fundingMonthTestNow)
		if scenario == "busy" {
			<-usageMonthSlots
			if !errors.Is(err, ErrUsageMonthBusy) {
				t.Fatal("busy audit queued", err)
			}
		}
		if err == nil || o.report != nil {
			t.Fatal("invalid audit accepted", scenario, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := ReviewUsageMonth(ctx, reader, r, []UsageWindowVersion{v}); err != nil {
		t.Fatal("readonly receiver audit failed", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || digest(before) != digest(after) {
		t.Fatal("audit modified receiver", err)
	}
}

func TestUsageMonthHourlyMonthAndReceiverTimeoutRecovery(t *testing.T) {
	db, _ := usageTestStore(t)
	r, from, to := fundingMonthFixture(t, "2026-08", []int64{201})
	ctx := context.Background()
	// Batch insertion is confined to this newly-created synthetic receiver.
	// The audit must still re-read/canonicalize all 1,488 individual captures.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	versions := []UsageWindowVersion{}
	for at := from; at < to; at += 3600 {
		a := usageMonthCapture(r, 201, at, at+3600, []UsageWindowRow{usageMonthRow(201, at+1, 2, "wallet", 500000), usageMonthRow(201, at+2, 6, "wallet", 1)})
		v := UsageWindowVersion{}
		for i, observation := range []UsageWindowObservation{a, usageMonthNewer(a)} {
			raw, err := usageWindowBytes(observation)
			if err != nil {
				t.Fatal(err)
			}
			id := digest(raw)
			if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_usage_observations
(id,source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?)`, id,
				observation.Request.SourceIdentity, observation.Request.UserID, observation.Request.From, observation.Request.To,
				observation.StartedAtMS, observation.CompletedAtMS, raw); err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				v.OlderID = id
			} else {
				v.NewerID = id
			}
		}
		versions = append(versions, v)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	o, err := reviewUsageMonth(ctx, db, r, versions, fundingMonthTestNow)
	if err != nil {
		t.Fatal(err)
	}
	a := o.Report().Accounts[0]
	if len(a.Windows) != 744 || a.Totals == nil || a.Totals.Sources[0].ConsumeQuota != 372000000 || a.Totals.Sources[0].RefundQuota != 744 ||
		a.ObservedSeconds != to-from || a.Totals.Sources[0].ConsumeRecords != 744 || a.Totals.Sources[0].RefundRecords != 744 {
		t.Fatal("hourly month aggregation changed", a.Totals)
	}
	t.Logf("744 hourly pairs / 1,488 captures audited in %s (synthetic local receiver only)", time.Since(started))
	// Occupy the only connection: cancellation must release the global slot,
	// return no partial month, and permit the next read after pool recovery.
	connection, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	blocked, err := reviewUsageMonth(short, db, r, versions, fundingMonthTestNow)
	if !errors.Is(err, context.DeadlineExceeded) || blocked.report != nil {
		t.Fatal("receiver timeout returned partial data or ignored deadline", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reviewUsageMonth(ctx, db, r, versions[:1], fundingMonthTestNow); err != nil {
		t.Fatal("receiver did not recover after cancellation", err)
	}
	if db.Stats().InUse != 0 || len(usageMonthSlots) != 0 {
		t.Fatal("audit leaked a connection or concurrency slot")
	}
}

func TestUsageMonthNoImplicitReceiverInitialization(t *testing.T) {
	db, _ := walletTestStore(t) // Deliberately has no usage tables.
	r, from, _ := fundingMonthFixture(t, "2026-09", []int64{201})
	a := usageMonthCapture(r, 201, from, from+86400, []UsageWindowRow{})
	b := usageMonthNewer(a)
	oldRaw, err := usageWindowBytes(a)
	if err != nil {
		t.Fatal(err)
	}
	newRaw, err := usageWindowBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	versions := []UsageWindowVersion{{OlderID: digest(oldRaw), NewerID: digest(newRaw)}}
	if o, err := reviewUsageMonth(context.Background(), db, r, versions, fundingMonthTestNow); err == nil || o.report != nil {
		t.Fatal("uninitialized receiver silently accepted")
	}
	var tables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='portal_monthly_usage_observations'`).Scan(&tables); err != nil || tables != 0 {
		t.Fatal("audit implicitly changed receiver schema", err)
	}
}
