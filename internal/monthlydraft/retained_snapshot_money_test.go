package monthlydraft

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// An additional retained capture, independent of the preparation input. This
// checks reconstruction arithmetic, NOT original midnight balances, database
// transaction provenance, atomic application writes, or publication eligibility.
// Customer IDs, amounts and private sources are supplied outside the repository.
func TestRetainedMonthlySnapshotMoneyReconstruction(t *testing.T) {
	_, _ = retainedMonthlySnapshotMoneyReconstruction(t)
}

// Shared by the opt-in local acceptance harness. Returning this private
// reconstruction does not approve, import or publish a customer bill.
func retainedMonthlySnapshotMoneyReconstruction(t *testing.T) (monitor.MonthlyPreparationInput, monitor.MonthlyBundleContext) {
	t.Helper()
	paths := []string{os.Getenv("MONTHLY_MONEY_INPUT"), os.Getenv("MONTHLY_SNAPSHOT_DATA"), os.Getenv("MONTHLY_SNAPSHOT_SQL")}
	for _, path := range paths {
		if path == "" {
			t.Skip("retained input, independent snapshot and query definitions not supplied")
		}
	}
	bodies := make([][]byte, len(paths))
	for i, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		bodies[i] = body
		t.Logf("retained input %d sha256=%s", i, generationHash(body))
	}
	input, err := monitor.DecodeMonthlyPreparationInput(bodies[0])
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]json.RawMessage
	var queries map[string]string
	if err := json.Unmarshal(bodies[1], &data); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bodies[2], &queries); err != nil {
		t.Fatal(err)
	}
	var users []struct {
		ID         int64 `json:"id"`
		Quota      int64 `json:"quota"`
		UsedQuota  int64 `json:"used_quota"`
		SnapshotAt int64 `json:"snapshot_at"`
	}
	if err := json.Unmarshal(data["00_user"], &users); err != nil || len(users) != 1 {
		t.Fatal("expected one independent snapshot account", err)
	}
	u := users[0]
	if queries["00_user"] != fmt.Sprintf("SELECT id,username,quota,used_quota,request_count,UNIX_TIMESTAMP() AS snapshot_at FROM users WHERE id=%d", u.ID) {
		t.Fatal("snapshot account query mismatch")
	}
	var account *monitor.MonthlyPreparationAccount
	for i := range input.Accounts {
		if input.Accounts[i].UserID == u.ID {
			account = &input.Accounts[i]
		}
	}
	if account == nil || len(account.Months) != 1 || account.CounterBasis != "net_consumption" {
		t.Fatal("retained check requires one closed month and net counter basis")
	}
	zone := time.FixedZone("audit-UTC+8", 8*3600)
	start, err := time.ParseInLocation("2006-01", account.Months[0], zone)
	if err != nil || start.Unix() != account.Baseline.At || u.SnapshotAt <= start.AddDate(0, 1, 0).Unix() {
		t.Fatal("independent reconstruction scope mismatch", err)
	}
	monthEnd := start.AddDate(0, 1, 0).Unix()
	var subscriptions []json.RawMessage
	if err := json.Unmarshal(data["01_subscriptions"], &subscriptions); err != nil || subscriptions == nil || len(subscriptions) != 0 || queries["01_subscriptions"] != fmt.Sprintf("SELECT id,user_id,status,start_time,end_time FROM user_subscriptions WHERE user_id=%d LIMIT 1001", u.ID) {
		t.Fatal("wallet-only capture required", err)
	}
	monthNet, bridgeNet := new(big.Int), new(big.Int)
	monthGross, monthRefund, bridgeGross, bridgeRefund := new(big.Int), new(big.Int), new(big.Int), new(big.Int)
	monthRequests := int64(0)
	dayQuery := "SELECT type,CASE WHEN JSON_VALID(other) THEN COALESCE(JSON_UNQUOTE(JSON_EXTRACT(other,'$.billing_source')),'unspecified') ELSE 'invalid_json' END AS billing_source,COUNT(*) AS records,COALESCE(SUM(quota),0) AS quota,MIN(id) AS min_id,MAX(id) AS max_id FROM logs FORCE INDEX(idx_user_created_type) WHERE user_id=%d AND created_at>=%d AND created_at<%d AND type IN (2,6) GROUP BY type,billing_source"
	days := 0
	for day := start; day.Unix() <= u.SnapshotAt; day = day.AddDate(0, 0, 1) {
		key := "day_" + day.Format("2006-01-02")
		if queries[key] != fmt.Sprintf(dayQuery, u.ID, day.Unix(), day.AddDate(0, 0, 1).Unix()) {
			t.Fatal("missing, overlapping or differently scoped daily query", key)
		}
		var rows []struct {
			Type   int    `json:"type"`
			Source string `json:"billing_source"`
			Quota  string `json:"quota"`
			Count  int64  `json:"records"`
		}
		if err := json.Unmarshal(data[key], &rows); err != nil || rows == nil {
			t.Fatal("missing daily result (not a zero day)", key, err)
		}
		for _, row := range rows {
			q, ok := new(big.Int).SetString(row.Quota, 10)
			if !ok || q.Sign() < 0 || row.Source != "wallet" || (row.Type != 2 && row.Type != 6) || row.Count <= 0 {
				t.Fatal("invalid wallet aggregate", key)
			}
			if row.Type == 6 {
				if day.Unix() < monthEnd {
					monthRefund.Add(monthRefund, q)
				} else {
					bridgeRefund.Add(bridgeRefund, q)
				}
				q.Neg(q)
			} else {
				if day.Unix() < monthEnd {
					monthGross.Add(monthGross, q)
				} else {
					bridgeGross.Add(bridgeGross, q)
				}
			}
			if day.Unix() < monthEnd {
				monthNet.Add(monthNet, q)
				if row.Type == 2 {
					monthRequests += row.Count
				}
			} else {
				bridgeNet.Add(bridgeNet, q)
			}
		}
		days++
	}
	// Normalize original money events, including audits owned by the operator.
	// Never treat profile changes as credits or infer the recipient from owner.
	monthFunding, bridgeFunding := new(big.Int), new(big.Int)
	seen := map[int64]monthlysource.FundingLog{}
	wantMovements := map[int64]monitor.MonthlyPreparationMovement{}
	for _, m := range account.Movements {
		if m.At < u.SnapshotAt {
			wantMovements[m.ID] = m
		}
	}
	movements := 0
	independentIDs := []int64{}
	events := []monitor.MonthlyBundleEvent{}
	rules := monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}
	for _, key := range []string{"03_wallet_events", "04_target_audits"} {
		query := queries[key]
		prefix := fmt.Sprintf("SELECT id,user_id,created_at,type,content,other FROM logs FORCE INDEX(idx_user_created_type) WHERE user_id=%d AND created_at>=%d AND type IN (1,3,4,6) ORDER BY created_at,id LIMIT 2001", u.ID, start.Unix())
		if key == "04_target_audits" {
			prefix = fmt.Sprintf("SELECT id,user_id,created_at,type,content,other FROM logs FORCE INDEX(idx_created_at_type) WHERE created_at>=%d AND created_at<UNIX_TIMESTAMP() AND type=3 AND JSON_VALID(other) AND JSON_UNQUOTE(JSON_EXTRACT(other,'$.op.params.target_user_id'))='%d' ORDER BY created_at,id LIMIT 2001", start.Unix(), u.ID)
		}
		if query != prefix {
			t.Fatal("funding query scope mismatch", key)
		}
		var rows []monthlysource.FundingLog
		if err := json.Unmarshal(data[key], &rows); err != nil || rows == nil || len(rows) >= 2001 {
			t.Fatal("missing or truncated funding result", key, err)
		}
		for _, row := range rows {
			if prior, ok := seen[row.ID]; ok {
				if prior != row {
					t.Fatal("conflicting duplicate event", row.ID)
				}
				continue
			}
			seen[row.ID] = row
			if row.CreatedAt < start.Unix() || row.CreatedAt >= u.SnapshotAt {
				t.Fatal("funding event falls outside observed capture scope", row.ID)
			}
			r := monthlysource.NormalizeFundingLog(row, rules)
			if r.Status == monthlysource.FundingNonWallet {
				continue
			}
			if r.Status != monthlysource.FundingMovement || r.TargetUserID != u.ID || r.DeltaQuota == nil {
				t.Fatal("unresolved funding event", row.ID, r.Status, r.Reason)
			}
			m, ok := wantMovements[row.ID]
			if !ok || m.DeltaQuota != *r.DeltaQuota || m.At != row.CreatedAt {
				t.Fatal("independent funding differs from preparation", row.ID)
			}
			movements++
			independentIDs = append(independentIDs, row.ID)
			events = append(events, monitor.MonthlyBundleEvent{ID: row.ID, At: row.CreatedAt, DeltaQuota: *r.DeltaQuota, Kind: r.Kind, Method: r.Method, Source: m.Source})
			if row.CreatedAt < monthEnd {
				monthFunding.Add(monthFunding, big.NewInt(*r.DeltaQuota))
			} else {
				bridgeFunding.Add(bridgeFunding, big.NewInt(*r.DeltaQuota))
			}
		}
	}
	if movements != len(wantMovements) {
		t.Fatal("independent capture missing funding events")
	}
	// Compare the month to the current preparation, without changing its anchor.
	preparedNet, preparedRequests, through := new(big.Int), int64(0), start.Unix()
	for _, span := range account.Spans {
		if span.From >= monthEnd {
			continue
		}
		if span.From != through || span.To > monthEnd || span.Requests == nil {
			t.Fatal("preparation month has gaps or unretained request counts")
		}
		preparedNet.Add(preparedNet, big.NewInt(span.ConsumeQuota))
		preparedNet.Sub(preparedNet, big.NewInt(span.RefundQuota))
		preparedRequests += *span.Requests
		through = span.To
	}
	if through != monthEnd || preparedNet.Cmp(monthNet) != 0 || preparedRequests != monthRequests {
		t.Fatal("independent closed month differs from preparation")
	}
	forward := new(big.Int).Add(big.NewInt(account.Baseline.BalanceQuota), monthFunding)
	forward.Sub(forward, monthNet)
	reverse := new(big.Int).Sub(big.NewInt(u.Quota), bridgeFunding)
	reverse.Add(reverse, bridgeNet)
	used := new(big.Int).Add(big.NewInt(account.Baseline.UsedQuota), monthNet)
	used.Add(used, bridgeNet)
	if forward.Cmp(reverse) != 0 || used.Cmp(big.NewInt(u.UsedQuota)) != 0 {
		t.Fatalf("retained snapshot reconstruction differs: forward=%s reverse=%s counter_gap=%s", forward, reverse, new(big.Int).Sub(big.NewInt(u.UsedQuota), used))
	}
	t.Logf("independent reconstruction matches: account=%d month=%s days=%d events=%d requests=%d closing_quota=%s; not an original month-end observation or publication approval", u.ID, account.Months[0], days, movements, monthRequests, forward)
	// Exercise the real candidate path with a new in-memory input/context. The
	// original timing classifications and source bytes are never rewritten.
	selected := *account
	selected.Snapshots = append([]monitor.MonthlyPreparationBalance(nil), account.Snapshots...)
	for i := range selected.Snapshots {
		selected.Snapshots[i].TimeBasis = "collection_observation"
		selected.Snapshots[i].CutoffSource = ""
	}
	input.Accounts = []monitor.MonthlyPreparationAccount{selected}
	for i, id := range []string{"independent_snapshot", "independent_sql"} {
		input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: id, Path: paths[i+1], SHA256: generationHash(bodies[i+1])})
	}
	toInt := func(q *big.Int) *int64 {
		t.Helper()
		if !q.IsInt64() {
			t.Fatal("amount outside int64")
		}
		n := q.Int64()
		return &n
	}
	reconstruction := monitor.MonthlySnapshotReconstruction{
		UserID: u.ID, Month: selected.Months[0], ObservedAt: u.SnapshotAt,
		SnapshotSource: "independent_snapshot#00_user", ScopeSource: "independent_sql#retained-query-view-requires-review", MonthSource: "independent_snapshot#closed-month-days", BridgeSource: "independent_snapshot#later-visible-days", FundingSource: "independent_snapshot#03_wallet_events+04_target_audits", OpeningBasisSource: selected.Baseline.Source,
		BalanceQuota: &u.Quota, UsedQuota: &u.UsedQuota, MonthConsumeQuota: toInt(monthGross), MonthRefundQuota: toInt(monthRefund), BridgeConsumeQuota: toInt(bridgeGross), BridgeRefundQuota: toInt(bridgeRefund), FundingEventIDs: independentIDs,
	}
	inputRaw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	context := monitor.MonthlyBundleContext{Version: 1, InputSHA256: generationHash(inputRaw), Groups: []monitor.MonthlyBundleGroup{{GroupID: selected.GroupID, Name: "retained-local-scope", Source: selected.BasisSource, Months: selected.Months, Members: []monitor.MonthlyBundleMember{{UserID: u.ID, Username: selected.Username}}}}, Accounts: []monitor.MonthlyBundleAccountContext{{UserID: u.ID, WalletOnly: true, WalletSource: "independent_snapshot#01_subscriptions", Events: events, Reconstructions: []monitor.MonthlySnapshotReconstruction{reconstruction}}}}
	contextRaw, err := json.Marshal(context)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := monitor.PrepareMonthlyCandidateBundle(inputRaw, contextRaw, time.Unix(u.SnapshotAt+86400, 0))
	if err != nil || bundle.ReadyForReview != 1 || bundle.Blocked != 0 {
		t.Fatalf("retained candidate failed: %+v %v", bundle, err)
	}
	candidate := bundle.Entries[0].Candidate
	if len(candidate.ReconstructedUsers) != 1 || candidate.ReconstructedUsers[0] != u.ID || len(candidate.Users) != 1 || candidate.Users[0].ClosingQuota != forward.Int64() || candidate.Users[0].ConsumeQuota != monthGross.Int64() || candidate.Users[0].RefundQuota != monthRefund.Int64() || candidate.Users[0].Requests != monthRequests {
		t.Fatal("retained candidate altered original money")
	}
	t.Log("retained reconstruction reached needs_review candidate; no actual review, import or publication executed")
	for i, path := range paths {
		after, err := os.ReadFile(path)
		if err != nil || generationHash(after) != generationHash(bodies[i]) {
			t.Fatal("retained source changed", err)
		}
	}
	return input, context
}
