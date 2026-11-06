package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Explicit private originals only. This joins independently re-audited usage
// and funding to TWO retained balance views. A needs_review candidate is not
// proof of original midnight balances, historical membership or atomic writes,
// and this test never approves, imports, publishes or changes source records.
func TestRetainedRealMonthBalanceLinkReadOnly(t *testing.T) {
	names := []string{"MONTHLY_BALANCE_RETAINED_INPUT", "MONTHLY_BALANCE_RETAINED_SCOPE", "MONTHLY_BALANCE_RETAINED_SCOPE_SQL",
		"MONTHLY_CAPTURE_RETAINED_MONTH", "MONTHLY_CAPTURE_RETAINED_ACCOUNT", "MONTHLY_FUNDING_RETAINED_MONTH",
		"MONTHLY_FUNDING_RETAINED_PLAN_SHA", "MONTHLY_CAPTURE_RETAINED_FUNDING"}
	for _, name := range names {
		if os.Getenv(name) == "" {
			t.Skip("explicit complete private observations and independent balance archives required")
		}
	}
	// Reuse the actual source-page, receipt, independent-pair, zero-day and
	// sealed-store checks. Do not trust a hand-entered total or a JSON report.
	TestObservationRetainedWholeMonthUsageReadOnly(t)
	TestDailyFundingRetainedRealCompleteMonthReadOnly(t)
	originals := map[string]string{}
	read := func(path string) []byte {
		t.Helper()
		raw, err := privateRead(path, monitor.MonthlyPreparationInputLimit)
		if err != nil {
			t.Fatal(err)
		}
		originals[path] = hash(raw)
		return raw
	}
	t.Cleanup(func() {
		for path, want := range originals {
			if fileHash(t, path) != want {
				t.Error("balance-link original changed", filepath.Base(path))
			}
		}
	})
	input, err := monitor.DecodeMonthlyPreparationInput(read(os.Getenv(names[0])))
	if err != nil {
		t.Fatal(err)
	}
	var account monitor.MonthlyPreparationAccount
	for _, a := range input.Accounts {
		if a.UserID == 148 {
			account = a
		}
	}
	if account.UserID != 148 || account.GroupID != 27 || account.CounterBasis != "net_consumption" || account.Baseline.BalanceQuota != 0 || account.Baseline.UsedQuota != 0 {
		t.Fatal("retained account or opening basis differs")
	}
	account.Months = []string{"2026-09"}
	account.Spans = slices.Clone(account.Spans)
	account.Movements = slices.Clone(account.Movements)
	account.Snapshots = slices.Clone(account.Snapshots)
	// Ordinary observations remain observations, never exact settled cutoffs.
	for i := range account.Snapshots {
		if account.Snapshots[i].TimeBasis != "" && account.Snapshots[i].TimeBasis != "collection_observation" {
			t.Fatal("reviewed historical cutoff requires a separate retained selection")
		}
		account.Snapshots[i].TimeBasis = "collection_observation"
	}
	const from, to int64 = 1788192000, 1790784000
	var before []monitor.MonthlyPreparationSpan
	for _, span := range account.Spans {
		if span.To <= from {
			before = append(before, span)
		}
	}
	account.Spans = before
	var monthGross, monthRefund, monthRequests int64
	for day := range 30 {
		path := filepath.Join(os.Getenv("MONTHLY_CAPTURE_RETAINED_MONTH"), fmt.Sprintf("sep-d%02d-b", day+1), "observation.json")
		raw := read(path)
		var observation monthlysource.UsageWindowObservation
		if json.Unmarshal(raw, &observation) != nil || monthlysource.ValidateUsageWindowObservation(observation) != nil ||
			observation.Request.UserID != account.UserID || observation.Request.From != from+int64(day)*86400 || observation.Request.To != from+int64(day+1)*86400 {
			t.Fatal("missing or incorrectly scoped real daily usage")
		}
		var consume, refund, requests int64
		for _, row := range observation.Rows {
			if row.BillingSource != "wallet" {
				t.Fatal("unknown/subscription usage cannot become wallet consumption")
			}
			if row.Type == 2 {
				consume += row.Quota
				requests += row.Records
			} else {
				refund += row.Quota
			}
		}
		id := fmt.Sprintf("real_usage_d%02d", day+1)
		input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: id, Path: path, SHA256: hash(raw)})
		// These captures did not retain token aggregates. Unknown is not zero.
		account.Spans = append(account.Spans, monitor.MonthlyPreparationSpan{UserID: account.UserID, From: observation.Request.From,
			To: observation.Request.To, ConsumeQuota: consume, RefundQuota: refund, Requests: &requests, Source: id + "#rows"})
		monthGross += consume
		monthRefund += refund
		monthRequests += requests
	}
	if monthGross != 7480870 || monthRefund != 0 || monthRequests != 221 {
		t.Fatal("re-audited real month money changed")
	}
	fundingRoot := filepath.Join(os.Getenv("MONTHLY_FUNDING_RETAINED_MONTH"), "september")
	plan, _, err := readMonthPlan(filepath.Join(fundingRoot, "month-plan.json"), os.Getenv("MONTHLY_FUNDING_RETAINED_PLAN_SHA"))
	if err != nil {
		t.Fatal(err)
	}
	funding, err := auditMonth(context.Background(), fundingRoot, plan)
	if err != nil || len(funding.Accounts) != 1 || len(funding.Accounts[0].Movements) != 1 {
		t.Fatal("original monthly funding not available", err)
	}
	m := funding.Accounts[0].Movements[0]
	// Exact raw record equality was independently checked above, not merely a
	// matching quota sum. Keep the original movement's provenance unchanged.
	found := 0
	var movementSource string
	for _, movement := range account.Movements {
		if movement.At >= from && movement.At < to {
			if movement.ID != m.LogID || movement.At != m.At || movement.DeltaQuota != m.DeltaQuota {
				t.Fatal("preparation movement differs from fresh full-month source")
			}
			found++
			movementSource = movement.Source
		}
	}
	if found != 1 {
		t.Fatal("missing or duplicated September funding")
	}

	var first map[string]json.RawMessage
	firstPath := os.Getenv("MONTHLY_CAPTURE_RETAINED_ACCOUNT")
	if json.Unmarshal(read(firstPath), &first) != nil {
		t.Fatal("first retained snapshot")
	}
	var second map[string]json.RawMessage
	secondPath := os.Getenv("MONTHLY_BALANCE_RETAINED_SCOPE")
	secondRaw := read(secondPath)
	if json.Unmarshal(secondRaw, &second) != nil {
		t.Fatal("second retained snapshot")
	}
	matchedSource := false
	for _, source := range input.Sources {
		if source.ID == "s002" && source.Path == secondPath && source.SHA256 == hash(secondRaw) {
			matchedSource = true
		}
	}
	if !matchedSource {
		t.Fatal("second snapshot differs from independently retained preparation source digest")
	}
	var queries map[string]string
	queryPath := os.Getenv("MONTHLY_BALANCE_RETAINED_SCOPE_SQL")
	if json.Unmarshal(read(queryPath), &queries) != nil {
		t.Fatal("second retained query definitions")
	}
	firstQueryPath := strings.TrimSuffix(firstPath, ".json") + ".sql.json"
	var firstQueries map[string]string
	if json.Unmarshal(read(firstQueryPath), &firstQueries) != nil || firstQueries["00_user"] != "SELECT id,username,quota,used_quota,request_count,UNIX_TIMESTAMP() AS snapshot_at FROM users WHERE id=148" {
		t.Fatal("first account query differs")
	}
	type snapshot struct {
		ID        int64           `json:"id"`
		Quota     int64           `json:"quota"`
		Used      int64           `json:"used_quota"`
		Timestamp json.RawMessage `json:"snapshot_at"`
	}
	var firstUsers, secondUsers []snapshot
	if json.Unmarshal(first["00_user"], &firstUsers) != nil || len(firstUsers) != 1 || firstUsers[0].ID != account.UserID || json.Unmarshal(second["00_users"], &secondUsers) != nil {
		t.Fatal("independent account snapshots missing")
	}
	var selected snapshot
	var ids []string
	for _, u := range secondUsers {
		ids = append(ids, strconv.FormatInt(u.ID, 10))
		if u.ID == account.UserID {
			if selected.ID != 0 {
				t.Fatal("duplicate snapshot account")
			}
			selected = u
		}
	}
	if selected.ID == 0 || queries["00_users"] != "SELECT id,username,quota,used_quota,request_count,UNIX_TIMESTAMP(NOW(6)) AS snapshot_at FROM users WHERE id IN ("+strings.Join(ids, ",")+") ORDER BY id" {
		t.Fatal("second query does not bind source accounts")
	}
	for _, s := range []struct {
		id, path, queryPath string
		data                map[string]json.RawMessage
		user                snapshot
		bridgeKey           string
	}{
		{"real_balance_early", firstPath, firstQueryPath, first, firstUsers[0], "month_2026-10"},
		{"real_balance_later", secondPath, queryPath, second, selected, "agg_148_1790784000_1792484737"},
	} {
		t.Run(s.id, func(t *testing.T) {
			inputCopy := input
			inputCopy.Sources = slices.Clone(input.Sources)
			for _, ref := range []struct{ id, path string }{{s.id, s.path}, {s.id + "_sql", s.queryPath}} {
				inputCopy.Sources = append(inputCopy.Sources, monitor.MonthlyPreparationSource{ID: ref.id, Path: ref.path, SHA256: hash(read(ref.path))})
			}
			// Decimal observation timestamps are rounded UP only as metadata.
			// Neither this timestamp nor the aggregate's SQL end is declared an
			// original month boundary or settled application/log watermark.
			n := strings.Trim(string(s.user.Timestamp), "\"")
			rat, ok := new(big.Rat).SetString(n)
			if !ok || rat.Sign() <= 0 {
				t.Fatal("invalid retained timestamp")
			}
			observed, rem := new(big.Int).QuoRem(rat.Num(), rat.Denom(), new(big.Int))
			if rem.Sign() > 0 {
				observed.Add(observed, big.NewInt(1))
			}
			if !observed.IsInt64() {
				t.Fatal("retained timestamp overflows")
			}
			bridge := retainedWalletAggregate(t, s.data[s.bridgeKey])
			month := retainedWalletAggregate(t, first["month_2026-09"])
			if month != monthGross {
				t.Fatal("independent archived month differs from daily originals")
			}
			for key, want := range map[string]string{
				"month_2026-09": retainedAggregateSQL(148, from, to),
				"month_2026-10": retainedAggregateSQL(148, to, 1793462400),
			} {
				if firstQueries[key] != want {
					t.Fatal("first retained aggregate scope differs", key)
				}
			}
			if queries["agg_148_1790784000_1792484737"] != retainedAggregateSQL(148, to, 1792484737) {
				t.Fatal("second bridge scope differs")
			}
			zero := int64(0)
			r := monitor.MonthlySnapshotReconstruction{UserID: account.UserID, Month: "2026-09", ObservedAt: observed.Int64(),
				SnapshotSource: s.id + "#account148", ScopeSource: s.id + "_sql#retained-view-semantics-still-require-review",
				MonthSource: "s041#month_2026-09", BridgeSource: s.id + "#" + s.bridgeKey, FundingSource: "s006#admin_9+original-wallet-rows",
				OpeningBasisSource: account.Baseline.Source, BalanceQuota: &s.user.Quota, UsedQuota: &s.user.Used,
				MonthConsumeQuota: &monthGross, MonthRefundQuota: &zero, BridgeConsumeQuota: &bridge, BridgeRefundQuota: &zero,
				FundingEventIDs: []int64{1141993, 1142002, m.LogID}}
			inputCopy.Accounts = []monitor.MonthlyPreparationAccount{account}
			raw, err := json.Marshal(inputCopy)
			if err != nil {
				t.Fatal(err)
			}
			metadata := monitor.MonthlyBundleContext{Version: 1, InputSHA256: hash(raw), Groups: []monitor.MonthlyBundleGroup{{GroupID: account.GroupID,
				Name: "retained-local-review-only", Source: account.BasisSource, Months: account.Months,
				Members: []monitor.MonthlyBundleMember{{UserID: account.UserID, Username: account.Username}}}},
				Accounts: []monitor.MonthlyBundleAccountContext{{UserID: account.UserID, WalletOnly: true, WalletSource: "s041#01_subscription/pending-historical-scope-review",
					Events: []monitor.MonthlyBundleEvent{{ID: m.LogID, At: m.At, DeltaQuota: m.DeltaQuota, Kind: m.Kind, Method: m.Method,
						Source: movementSource}}, Reconstructions: []monitor.MonthlySnapshotReconstruction{r}}}}
			check := func(t *testing.T, meta monitor.MonthlyBundleContext, wantReady bool) {
				t.Helper()
				contextRaw, marshalErr := json.Marshal(meta)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				bundle, bundleErr := monitor.PrepareMonthlyCandidateBundle(raw, contextRaw, time.Now())
				if !wantReady && bundleErr != nil && bundle.ReadyForReview == 0 {
					return // Missing/invalid structure must fail before any candidate.
				}
				if bundleErr != nil || (bundle.ReadyForReview == 1) != wantReady || (bundle.Blocked == 0) != wantReady {
					t.Fatal("unexpected real balance candidate result", bundleErr, bundle.ReadyForReview, bundle.Blocked)
				}
				if wantReady {
					u := bundle.Entries[0].Candidate.Users[0]
					if u.OpeningQuota != 96011849 || u.ClosingQuota != 588530979 || u.ReverseClosingQuota != u.ClosingQuota || u.ConsumeQuota != 7480870 || u.RefundQuota != 0 || u.Requests != 221 ||
						len(u.Events) != 1 || u.Events[0].DeltaQuota != 500000000 || u.Statistics == nil || u.Statistics.TokensKnown || len(bundle.Entries[0].Candidate.ReconstructedUsers) != 1 {
						t.Fatal("real customer money or unknown token coverage changed")
					}
				}
			}
			check(t, metadata, true)
			for _, field := range []string{"wallet", "used", "bridge", "funding", "refund", "account"} {
				t.Run("reject_"+field, func(t *testing.T) {
					bad := r
					oneMore := int64(1)
					switch field {
					case "wallet":
						oneMore += *r.BalanceQuota
						bad.BalanceQuota = &oneMore
					case "used":
						oneMore += *r.UsedQuota
						bad.UsedQuota = &oneMore
					case "bridge":
						bad.BridgeConsumeQuota = nil
					case "funding":
						bad.FundingEventIDs = []int64{m.LogID}
					case "refund":
						bad.BridgeRefundQuota = &oneMore
					case "account":
						bad.UserID++
					}
					meta := metadata
					meta.Accounts = slices.Clone(metadata.Accounts)
					meta.Accounts[0].Reconstructions = []monitor.MonthlySnapshotReconstruction{bad}
					check(t, meta, false)
				})
			}
		})
	}
	t.Log("real account September: two independent retained views reconcile opening 192.023698 + credits 1000 - consumption 14.961740 = closing 1177.061958 CNY; 221 consumption logs unchanged; only needs_review candidates, no historical scope attestation or publication")
}

func retainedAggregateSQL(id, from, to int64) string {
	return fmt.Sprintf("SELECT type,CASE WHEN JSON_VALID(other) THEN COALESCE(JSON_UNQUOTE(JSON_EXTRACT(other,'$.billing_source')),'unspecified') ELSE 'invalid_json' END AS billing_source,COUNT(*) AS records,COALESCE(SUM(quota),0) AS quota,COALESCE(SUM(prompt_tokens),0) AS input_tokens,COALESCE(SUM(completion_tokens),0) AS output_tokens FROM logs FORCE INDEX(idx_user_created_type) WHERE user_id=%d AND created_at>=%d AND created_at<%d AND type IN (2,6) GROUP BY type,billing_source", id, from, to)
}

func retainedWalletAggregate(t *testing.T, raw json.RawMessage) int64 {
	t.Helper()
	var rows []struct {
		Type    int    `json:"type"`
		Source  string `json:"billing_source"`
		Quota   string `json:"quota"`
		Records int64  `json:"records"`
	}
	if json.Unmarshal(raw, &rows) != nil || len(rows) != 1 || rows[0].Type != 2 || rows[0].Source != "wallet" || rows[0].Records <= 0 {
		t.Fatal("missing, ambiguous, refund or non-wallet retained aggregate")
	}
	q, err := strconv.ParseInt(rows[0].Quota, 10, 64)
	if err != nil || q < 0 || strconv.FormatInt(q, 10) != rows[0].Quota {
		t.Fatal("invalid exact retained quota")
	}
	return q
}
