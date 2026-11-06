package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Only retained private originals: no credentials, network, original writes or
// customer task/publication. Missing coverage must not be converted into zero.
func TestDailyFundingRetainedRealCompleteMonthReadOnly(t *testing.T) {
	root, expected, archive := os.Getenv("MONTHLY_FUNDING_RETAINED_MONTH"), os.Getenv("MONTHLY_FUNDING_RETAINED_PLAN_SHA"), os.Getenv("MONTHLY_CAPTURE_RETAINED_FUNDING")
	if root == "" || expected == "" || archive == "" {
		t.Skip("explicit private month originals, externally retained plan digest and independent funding archive required")
	}
	if _, err := privateInfo(root, true); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dir := filepath.Join(root, "september")
	batch, err := readMonthBatch(ctx, dir, "0ca7b9c063030327a998ca65795be28a710ba2f5718c79a7e65da7ff540dc10d")
	if err != nil || batch.Version != 2 || batch.GroupID != 27 || len(batch.Windows) != 30 || !slices.Equal(batch.Funding.UserIDs, []int64{148}) {
		t.Fatal("original capture batch differs", err)
	}
	plan, _, err := readMonthPlan(filepath.Join(dir, "month-plan.json"), expected)
	if err != nil || plan.Version != 2 || plan.GroupID != 27 || plan.Funding.Month != "2026-09" ||
		plan.Funding.SourceIdentity != "f3cd458aac20374cf8b52dcdf0c4baf8d3e90c966ae42229365cc30399968937" || !reflect.DeepEqual(plan.Funding, batch.Funding) {
		t.Fatal("real retained month plan differs", err)
	}
	originals := map[string]string{}
	for _, w := range plan.Windows {
		for _, name := range []string{"manifest.json", "funding.sqlite"} {
			p := filepath.Join(dir, w.Job, name)
			if _, err := privateInfo(p, false); err != nil {
				t.Fatal(err)
			}
			originals[p] = fileHash(t, p)
		}
	}
	for _, p := range []string{filepath.Join(dir, "batch.json"), filepath.Join(dir, "month-plan.json"), archive} {
		originals[p] = fileHash(t, p)
	}
	assemblyRaw, err := privateRead(filepath.Join(root, "assembly-receipt.json"), 4096)
	var assembly monthAssemblyReceipt
	if err != nil || json.Unmarshal(assemblyRaw, &assembly) != nil || assembly.Mode != "assembled_local_evidence_not_bill_approval" || assembly.PlanSHA256 != expected || assembly.GroupID != 27 || assembly.Month != "2026-09" || assembly.Pages != 62 || assembly.Rows != 1732 {
		t.Fatal("real assembled evidence receipt differs", err)
	}
	assembledPath := filepath.Join(dir, "observation-assembly", "funding.sqlite")
	if fileHash(t, assembledPath) != assembly.DatabaseSHA256 {
		t.Fatal("assembled real evidence digest mismatch")
	}
	originals[assembledPath] = assembly.DatabaseSHA256
	originals[filepath.Join(root, "assembly-receipt.json")] = hash(assemblyRaw)
	t.Cleanup(func() {
		for p, want := range originals {
			if fileHash(t, p) != want {
				t.Error("original evidence changed during offline review", filepath.Base(p))
			}
		}
	})
	review, err := auditMonth(ctx, dir, plan)
	if err != nil {
		t.Fatal("full original month reaudit", err)
	}
	if len(review.Accounts) != 1 || len(review.Windows.Accounts) != 1 || review.Accounts[0].UserID != 148 ||
		review.Windows.From != 1788192000 || review.Windows.To != 1790784000 || len(review.Windows.MissingWindows) != 0 || len(review.Windows.Conflicts) != 0 {
		t.Fatal("wrong month/account or missing/overlapping source coverage")
	}
	account, coverage := review.Accounts[0], review.Windows.Accounts[0]
	if account.Status != "observed_month" || account.Totals == nil || coverage.SelectedSeconds != 2592000 || coverage.MatchingSeconds != 2592000 {
		for _, w := range coverage.Windows {
			if len(w.Issues) > 0 {
				t.Logf("unresolved real day index %d: issues=%v review_ids=%v unscoped_ids=%v", w.WindowIndex, w.Issues, w.ReviewLogIDs, w.UnscopedLogIDs)
			}
		}
		t.Fatalf("complete financial observation not established: status=%s selected=%d matching=%d", account.Status, coverage.SelectedSeconds, coverage.MatchingSeconds)
	}
	if account.Totals.IncreaseQuota != 500000000 || account.Totals.DecreaseQuota != 0 || len(account.Movements) != 1 ||
		account.Movements[0].LogID != 3715551 || account.Movements[0].DeltaQuota != 500000000 {
		t.Fatal("real month credited amount differs from independent retained source")
	}
	oldRaw, err := privateRead(archive, 1<<20)
	if err != nil || hash(oldRaw) != "de27c057994b1a379f091dd59f05a5ad849580ffe9bea232acc7423e01e70525" {
		t.Fatal("independent archive changed", err)
	}
	var old map[string]json.RawMessage
	if json.Unmarshal(oldRaw, &old) != nil {
		t.Fatal("independent original JSON")
	}
	matches := 0
	for _, b := range old {
		var rows []monthlysource.FundingLog
		if json.Unmarshal(b, &rows) != nil {
			continue
		}
		for _, r := range rows {
			if r.ID == 3715551 && r.CreatedAt == account.Movements[0].At {
				normal := monthlysource.NormalizeFundingLog(r, plan.Funding.Rules)
				if normal.Status != monthlysource.FundingMovement || normal.TargetUserID != 148 || normal.DeltaQuota == nil || *normal.DeltaQuota != 500000000 || normal.SourceDigest != account.Movements[0].SourceDigest || normal.Kind != account.Movements[0].Kind || normal.Method != account.Movements[0].Method {
					t.Fatal("fresh whole-month source fields differ from independent retained event")
				}
				matches++
			}
		}
	}
	if matches != 1 {
		t.Fatal("independent source event missing or duplicated")
	}
	assembled, err := openReceiver(ctx, assembledPath, false)
	if err != nil {
		t.Fatal(err)
	}
	again, err := reviewAssembledMonth(ctx, assembled, plan, plan.Windows, assembly.Pages, assembly.Rows)
	closeErr := assembled.Close()
	if err != nil || closeErr != nil || !reflect.DeepEqual(review, again) {
		t.Fatal("sealed real source changed per-account money or source identity", err, closeErr)
	}
	rawReview, err := json.Marshal(again)
	if err != nil || hash(rawReview) != assembly.ReviewSHA256 {
		t.Fatal("assembled review changed after sealing", err)
	}
	// Metadata-only counterexamples. No original is edited or relabelled.
	gap := plan
	gap.Windows = slices.Clone(plan.Windows[1:])
	if _, err := validateMonthAuditPlan(ctx, gap); err == nil {
		t.Fatal("missing first day became zero")
	}
	gap = plan
	gap.Windows = append(slices.Clone(plan.Windows[:3]), plan.Windows[4:]...)
	if _, err := validateMonthAuditPlan(ctx, gap); err == nil {
		t.Fatal("omitting a genuinely empty September 4 became an observed zero day")
	}
	duplicate := plan
	duplicate.Windows = append(slices.Clone(plan.Windows), plan.Windows[0])
	if _, err := validateMonthAuditPlan(ctx, duplicate); err == nil {
		t.Fatal("duplicate day accepted")
	}
	other := plan
	other.Funding.UserIDs = []int64{149}
	if _, err := auditMonth(ctx, dir, other); err == nil {
		t.Fatal("other account borrowed real customer's funding")
	}
	t.Log("real September funding: 30 ordered independent daily pairs, 2592000 matched and selected seconds; credited 500000000 quota = 1000 CNY, decrease 0, one original event; sealed SQLite retains 62 pages/1732 observation rows and identical review; all original source bytes unchanged; not wallet/month-boundary or bill publication proof")
}
