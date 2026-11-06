package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Real data is explicitly supplied outside Git. No credentials or source
// connections are used; all writes are confined to a new test-local database.
func TestObservationRetainedWholeMonthUsageReadOnly(t *testing.T) {
	root := os.Getenv("MONTHLY_CAPTURE_RETAINED_MONTH")
	archive := os.Getenv("MONTHLY_CAPTURE_RETAINED_ACCOUNT")
	if root == "" || archive == "" {
		t.Skip("explicit retained closed-month captures and independent account archive required")
	}
	if _, err := privateInfo(root, true); err != nil {
		t.Fatal("private original month directory required", err)
	}
	originals := map[string]string{}
	read := func(name string, limit int64) []byte {
		t.Helper()
		st, err := os.Lstat(name)
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() <= 0 || st.Size() > limit {
			t.Fatal("invalid private retained file", filepath.Base(name))
		}
		raw, err := os.ReadFile(name)
		if err != nil || int64(len(raw)) != st.Size() {
			t.Fatal("read original", err)
		}
		originals[name] = hash(raw)
		return raw
	}
	t.Cleanup(func() {
		for name, want := range originals {
			raw, err := os.ReadFile(name)
			if err != nil || hash(raw) != want {
				t.Error("original file changed during offline audit", filepath.Base(name))
			}
		}
	})
	ctx := context.Background()
	receiverPath := filepath.Join(t.TempDir(), "monthly-usage.sqlite")
	if err := exclusiveFile(receiverPath, nil); err != nil {
		t.Fatal(err)
	}
	db, err := openReceiver(ctx, receiverPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := monthlysource.InitUsageWindowStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	const from, to int64 = 1788192000, 1790784000
	const pin = "f3cd458aac20374cf8b52dcdf0c4baf8d3e90c966ae42229365cc30399968937"
	request := monthlysource.FundingMonthRequest{Month: "2026-09", SourceIdentity: pin,
		Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}, UserIDs: []int64{148}}
	versions := make([]monthlysource.UsageWindowVersion, 0, 30)
	newer := make([]monthlysource.UsageWindowObservation, 0, 30)
	emptyDays := 0
	for day := range 30 {
		var pair [2]monthlysource.UsageWindowObservation
		var ids [2]string
		for index, phase := range []string{"a", "b"} {
			dir := filepath.Join(root, fmt.Sprintf("sep-d%02d-%s", day+1, phase))
			if _, err := privateInfo(dir, true); err != nil {
				t.Fatal("missing/private day directory", day+1, phase)
			}
			requestRaw := read(filepath.Join(dir, "request.json"), 4096)
			raw := read(filepath.Join(dir, "observation.json"), 65536)
			var req observationRequest
			var marker observationReceipt
			if json.Unmarshal(requestRaw, &req) != nil || req.validate(time.Now()) != nil ||
				json.Unmarshal(read(filepath.Join(dir, "manifest.json"), 4096), &marker) != nil ||
				marker.Version != 1 || !marker.Complete || marker.Directory != dir || marker.Mode != "retained_private_observation_not_bill_approval" ||
				marker.RequestSHA256 != hash(requestRaw) || marker.ObservationSHA256 != hash(raw) || validateObservation(raw, req) != nil {
				t.Fatal("day capture seal or canonical payload differs", day+1, phase)
			}
			if req.Kind != "observe-usage" || req.SourceIdentity != pin || !slices.Equal(req.UserIDs, request.UserIDs) ||
				req.From != from+int64(day)*86400 || req.To != from+int64(day+1)*86400 || json.Unmarshal(raw, &pair[index]) != nil || pair[index].Request.Rules != request.Rules {
				t.Fatal("source, account, unit or closed-day range differs")
			}
			id, err := monthlysource.SaveUsageWindowObservation(ctx, db, pair[index])
			if err != nil || id != hash(raw) {
				t.Fatal("original day payload not retained unchanged", err)
			}
			loaded, err := monthlysource.LoadUsageWindowObservation(ctx, db, id)
			if err != nil || !reflect.DeepEqual(loaded, pair[index]) {
				t.Fatal("real stored day changed", err)
			}
			ids[index] = id
		}
		if !slices.Equal(pair[0].Rows, pair[1].Rows) || pair[1].StartedAtMS-pair[0].CompletedAtMS < 300000 {
			t.Fatal("independent historical daily captures differ or too close", day+1)
		}
		if len(pair[0].Rows) == 0 {
			emptyDays++
		}
		versions = append(versions, monthlysource.UsageWindowVersion{OlderID: ids[0], NewerID: ids[1]})
		newer = append(newer, pair[1])
	}
	observed, err := monthlysource.ReviewUsageMonth(ctx, db, request, versions)
	if err != nil {
		t.Fatal("whole month audit", err)
	}
	report := observed.Report()
	if len(report.Accounts) != 1 || report.From != from || report.To != to || report.Accounts[0].UserID != 148 {
		t.Fatal("month or account scope changed")
	}
	account := report.Accounts[0]
	if account.ObservedSeconds != to-from || account.MatchingSeconds != to-from || len(account.MissingWindows) != 0 ||
		account.Totals == nil || len(account.Totals.Sources) != 1 {
		t.Fatal("missing, duplicate, unknown or inconsistent month usage")
	}
	total := account.Totals.Sources[0]
	if total.BillingSource != "wallet" || total.ConsumeQuota != 7480870 || total.ConsumeRecords != 221 || total.RefundQuota != 0 || total.RefundRecords != 0 {
		t.Fatal("real month consumption or refund differs from independently retained result", total)
	}
	archivedRaw := read(archive, 1<<20)
	if hash(archivedRaw) != "4745bf552bce626da74cf17187eef8f868467913adb811f2b9e19d508adaafa2" {
		t.Fatal("original independent account archive changed")
	}
	var archived struct {
		September []struct {
			Type          int    `json:"type"`
			BillingSource string `json:"billing_source"`
			Quota         string `json:"quota"`
			Records       int64  `json:"records"`
		} `json:"month_2026-09"`
	}
	if json.Unmarshal(archivedRaw, &archived) != nil || len(archived.September) != 1 {
		t.Fatal("independent monthly source missing")
	}
	old := archived.September[0]
	q, err := strconv.ParseInt(old.Quota, 10, 64)
	if err != nil || old.Type != 2 || old.BillingSource != total.BillingSource || q != total.ConsumeQuota || old.Records != total.ConsumeRecords {
		t.Fatal("fresh observed totals do not match original archive")
	}
	// Omitting a genuinely empty day changes coverage, not money. The month
	// must still become unknown, rather than using equal net sums as evidence.
	gap, err := monthlysource.ReviewUsageMonth(ctx, db, request, versions[1:])
	if err != nil || gap.Report().Accounts[0].Totals != nil || len(gap.Report().Accounts[0].MissingWindows) == 0 {
		t.Fatal("omitted first day hidden by equal monetary totals", err)
	}
	duplicate := append(slices.Clone(versions), versions[0])
	if _, err := monthlysource.ReviewUsageMonth(ctx, db, request, duplicate); err == nil {
		t.Fatal("duplicated day counted twice")
	}
	other := request
	other.UserIDs = []int64{149}
	if _, err := monthlysource.ReviewUsageMonth(ctx, db, other, versions); err == nil {
		t.Fatal("another account borrowed real customer's observations")
	}
	// Alter only a NEW test-local copy, never a captured file. Even one quota
	// of disagreement between independent observations must prevent month totals.
	changed := newer[27]
	changed.Rows = slices.Clone(changed.Rows)
	if len(changed.Rows) != 1 || changed.Rows[0].Records <= 1 {
		t.Fatal("expected populated retained day for corruption test")
	}
	changed.Rows[0].Quota++
	changedID, err := monthlysource.SaveUsageWindowObservation(ctx, db, changed)
	if err != nil {
		t.Fatal("store test-local mutation", err)
	}
	changedVersions := slices.Clone(versions)
	changedVersions[27].NewerID = changedID
	unmatched, err := monthlysource.ReviewUsageMonth(ctx, db, request, changedVersions)
	if err != nil || unmatched.Report().Accounts[0].Totals != nil || unmatched.Report().Accounts[0].Windows[27].Matching {
		t.Fatal("one-quota daily disagreement hidden by monthly aggregation", err)
	}
	slices.Reverse(versions)
	again, err := monthlysource.ReviewUsageMonth(ctx, db, request, versions)
	if err != nil || !reflect.DeepEqual(report, again.Report()) {
		t.Fatal("input order changed monthly amounts", err)
	}
	t.Logf("real September usage: 30 days, 60 original observations, %d explicit empty days; 221 wallet consumption logs = 7,480,870 quota = 14.961740 CNY; refunds 0; matches independent archived source; no balance/funding completeness or bill publication claim", emptyDays)
}
