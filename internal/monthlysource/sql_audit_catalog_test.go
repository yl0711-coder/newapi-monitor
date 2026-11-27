package monthlysource

// Render the actual private query builders with synthetic parameters. This
// test has no database handle or network path and makes no completeness claim
// for the capture functions' other SQL or all parameter combinations.
import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSQLAuditMonthlySourceCatalog(t *testing.T) {
	out := os.Getenv("SQL_AUDIT_MONTHLY_CATALOG")
	if out == "" {
		t.Skip("explicit local catalog output required")
	}
	root, err := filepath.Abs("../../docs/sql-audit-20261126")
	if err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(out)
	if err != nil || !strings.HasPrefix(abs, root+string(os.PathSeparator)) {
		t.Fatal("catalog must remain inside this task's report directory")
	}
	type item struct {
		ID, Owner, Variant string
		SQL                string
		Args               []any
	}
	var cases []item
	add := func(owner, variant, q string, args ...any) {
		if strings.TrimSpace(q) == "" {
			t.Fatal("empty builder output")
		}
		cases = append(cases, item{fmt.Sprintf("monthly-%02d", len(cases)), owner, variant, q, args})
	}
	const from int64 = 1790784000 // Synthetic October day, no source read.
	add("usageWindowQuery", "closed-day", usageWindowQuery, int64(201), from, from+86400)
	add("tokenUsageWindowQuery", "closed-day", tokenUsageWindowQuery, int64(201), from, from+86400)
	add("walletTailUsageQuery", "month-to-view", walletTailUsageQuery, int64(201), from)
	add("walletTailFundingQuery", "initial", walletTailFundingQuery, from, int64(0), int64(0), int64(0), from, int64(0), int64(0), int64(0), int64(201))
	add("walletTailFundingQuery", "resume", walletTailFundingQuery, from, from+60, from+60, int64(100), from, from+60, from+60, int64(100), int64(201))
	for _, members := range []int{1, 200} {
		ids := make([]int64, members)
		for i := range ids {
			ids[i] = int64(201 + i)
		}
		for _, version := range []int{1, 2} {
			for _, resumed := range []bool{false, true} {
				s := FundingScan{Version: version, UserIDs: ids, From: from, To: from + 3600}
				if version == 2 {
					s.To = from + 86400
				}
				after := FundingCursor{}
				if resumed {
					after = FundingCursor{At: from + 60, ID: 100}
				}
				q, args := fundingQuery(s, 1000000, after)
				add("fundingQuery", fmt.Sprintf("v%d/members%d/resume%t", version, members, resumed), q, args...)
			}
		}
	}
	for _, kind := range []string{"user_usage", "global_funding"} {
		for _, resumed := range []bool{false, true} {
			r := WalletTailPageRequest{Kind: kind, Wallet: WalletTailRequest{UserID: 201, Boundary: from}}
			if resumed {
				r.After = WalletTailPageCursor{At: from + 60, Type: 2, ID: 100}
			}
			q, args := tailPageIndexQuery(r)
			add("tailPageIndexQuery", fmt.Sprintf("%s/resume%t", kind, resumed), q, args...)
			if kind == "user_usage" {
				q, args, err = serverTailUsageQuery(r)
				if err != nil {
					t.Fatal(err)
				}
				add("serverTailUsageQuery", fmt.Sprintf("resume%t", resumed), q, args...)
			}
		}
		for _, n := range []int{1, 200, walletTailPageRows} {
			if kind == "global_funding" && n > FundingPageLimit {
				continue
			}
			ids := make([]any, n)
			for i := range ids {
				ids[i] = int64(i + 1)
			}
			q, args, err := tailPageBodyQuery(WalletTailPageRequest{Kind: kind, Wallet: WalletTailRequest{UserID: 201, Boundary: from}}, ids)
			if err != nil {
				t.Fatal(err)
			}
			add("tailPageBodyQuery", fmt.Sprintf("%s/keys%d", kind, n), q, args...)
		}
	}
	for _, after := range []FundingCursor{{}, {At: from + 60, ID: 100}} {
		q, args, err := selectiveTailFundingQuery(WalletTailRequest{UserID: 201, Boundary: from}, after)
		if err != nil {
			t.Fatal(err)
		}
		add("selectiveTailFundingQuery", fmt.Sprintf("resume%t", after.ID != 0), q, args...)
	}
	// Capture the actual successful entry points, including their fmt-built
	// SQL. These source doubles cannot open a network connection. MySQL
	// EXPLAIN and results are a separate explicit isolated step.
	for _, count := range []int{1, WalletBatchLimit} {
		for _, operation := range []string{"CaptureWallet", "CaptureRegisteredWallet", "ReadRegistrationTimes"} {
			ids := make([]int64, count)
			data := make([][]driver.Value, count)
			at := time.Now().Add(-time.Hour).Unix()
			for n := range ids {
				id := int64(201 + n)
				ids[n] = id
				switch operation {
				case "CaptureWallet":
					data[n] = []driver.Value{id, int64(10), int64(20)}
				case "CaptureRegisteredWallet":
					data[n] = []driver.Value{id, at, int64(10), int64(20)}
				default:
					data[n] = []driver.Value{id, at}
				}
			}
			db, state, pin := walletTestSource(t, data)
			var err error
			switch operation {
			case "CaptureWallet":
				_, err = CaptureWallet(t.Context(), db, pin, ids)
			case "CaptureRegisteredWallet":
				_, err = CaptureRegisteredWallet(t.Context(), db, pin, ids)
			default:
				_, err = ReadRegistrationTimes(t.Context(), db, pin, ids)
			}
			if err != nil {
				t.Fatal("actual metadata entry point", operation, err)
			}
			state.mu.Lock()
			if len(state.queries) != 2 || !state.committed {
				state.mu.Unlock()
				t.Fatal("metadata entry point did not complete")
			}
			a := make([]any, len(state.args[1]))
			for n, v := range state.args[1] {
				a[n] = v.Value
			}
			add(operation, fmt.Sprintf("actual-call/members%d", count), state.queries[1], a...)
			state.mu.Unlock()
		}
	}
	raw, err := json.MarshalIndent(struct {
		Warning string
		Cases   []item
	}{"Actual query builder output; synthetic parameters; not runtime coverage, a production query or a performance result.", cases}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("rendered %d synthetic source cases", len(cases))
}
