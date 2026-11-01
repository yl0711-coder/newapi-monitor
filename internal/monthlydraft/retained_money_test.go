package monthlydraft

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Actual private records are opt-in, never copied into the repository. Check
// every retained movement with the production normalizer, not only the few
// customers/differences that initially motivated the monthly bill feature.
func TestRetainedMonthlyFundingMoneyMatchesEverySourceEvent(t *testing.T) {
	path := os.Getenv("MONTHLY_MONEY_INPUT")
	if path == "" {
		t.Skip("retained local money input not supplied")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	sources := make(map[string]monitor.MonthlyPreparationSource)
	for _, s := range input.Sources {
		sources[s.ID] = s
	}
	files := make(map[string]map[string]json.RawMessage)
	events, monthEvents := 0, 0
	owners := make(map[int64]int64)
	rules := monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}
	zone := time.FixedZone("audit-UTC+8", 8*3600)
	for _, account := range input.Accounts {
		for _, movement := range account.Movements {
			if owner, ok := owners[movement.ID]; ok {
				t.Fatalf("duplicate source event %d in accounts %d and %d", movement.ID, owner, account.UserID)
			}
			owners[movement.ID] = account.UserID
			id, selector, ok := strings.Cut(movement.Source, "#")
			if !ok {
				t.Fatal("missing retained source selector")
			}
			query, recordID, ok := strings.Cut(selector, "/id=")
			if !ok || recordID != strconv.FormatInt(movement.ID, 10) {
				t.Fatal("unexpected retained source event selector")
			}
			if files[id] == nil {
				source, ok := sources[id]
				if !ok {
					t.Fatal("missing source manifest entry")
				}
				body, err := os.ReadFile(source.Path)
				if err != nil || generationHash(body) != source.SHA256 {
					t.Fatal("source changed or unavailable", err)
				}
				var data map[string]json.RawMessage
				if err := json.Unmarshal(body, &data); err != nil {
					t.Fatal(err)
				}
				files[id] = data
			}
			var rows []monthlysource.FundingLog
			if err := json.Unmarshal(files[id][query], &rows); err != nil {
				t.Fatal(err)
			}
			found := 0
			for _, row := range rows {
				if row.ID != movement.ID {
					continue
				}
				found++
				got := monthlysource.NormalizeFundingLog(row, rules)
				if got.Status != monthlysource.FundingMovement || got.TargetUserID != account.UserID || got.At != movement.At || got.DeltaQuota == nil || *got.DeltaQuota != movement.DeltaQuota {
					t.Fatalf("funding mismatch account=%d event=%d status=%s reason=%s", account.UserID, row.ID, got.Status, got.Reason)
				}
			}
			if found != 1 {
				t.Fatalf("retained event matched %d rows", found)
			}
			events++
			// The retained input also includes later bridge events, which must
			// be verified but never included in a closed month's totals twice.
			for _, month := range account.Months {
				if time.Unix(movement.At, 0).In(zone).Format("2006-01") == month {
					monthEvents++
				}
			}
		}
	}
	if events == 0 {
		t.Fatal("empty retained audit")
	}
	t.Logf("source money exact: accounts=%d all_events=%d closed_month_events=%d source_files=%d", len(input.Accounts), events, monthEvents, len(files))
	// Reverse check: a normalized input could otherwise silently omit a funding
	// row and still pass the forward per-row checks above. Scan only retained
	// funding-query results, never source databases or current customer data.
	accounts := make(map[int64]monitor.MonthlyPreparationAccount)
	ends := make(map[int64]int64)
	movements := make(map[int64]monitor.MonthlyPreparationMovement)
	for _, account := range input.Accounts {
		accounts[account.UserID] = account
		for _, span := range account.Spans {
			ends[account.UserID] = max(ends[account.UserID], span.To)
		}
		for _, movement := range account.Movements {
			movements[movement.ID] = movement
		}
	}
	covered := make(map[int64]bool)
	for _, source := range input.Sources {
		if !strings.HasSuffix(source.Path, ".json") || strings.HasSuffix(source.Path, ".sql.json") {
			continue
		}
		body, err := os.ReadFile(source.Path)
		if err != nil || generationHash(body) != source.SHA256 {
			t.Fatal("retained funding source changed", err)
		}
		var data map[string]json.RawMessage
		if err := json.Unmarshal(body, &data); err != nil {
			t.Fatal(err)
		}
		for key, value := range data {
			if key != "02_wallet" && key != "02_admin_today" && !strings.HasPrefix(key, "admin_") && !strings.HasPrefix(key, "funds_") {
				continue
			}
			var rows []monthlysource.FundingLog
			if err := json.Unmarshal(value, &rows); err != nil {
				t.Fatal("invalid funding query result", err)
			}
			for _, row := range rows {
				var metadata struct {
					Op struct {
						Params struct {
							TargetUserID int64 `json:"target_user_id"`
						} `json:"params"`
					} `json:"op"`
				}
				if row.Other != "" {
					if err := json.Unmarshal([]byte(row.Other), &metadata); err != nil {
						t.Fatal("invalid retained funding metadata", err)
					}
				}
				target := row.UserID
				if metadata.Op.Params.TargetUserID > 0 {
					target = metadata.Op.Params.TargetUserID
				}
				account, ok := accounts[target]
				if !ok || row.CreatedAt < account.Baseline.At || row.CreatedAt >= ends[target] {
					continue
				}
				got := monthlysource.NormalizeFundingLog(row, rules)
				if got.Status == monthlysource.FundingReview {
					t.Fatalf("unreviewed money event account=%d event=%d reason=%s", target, row.ID, got.Reason)
				}
				if got.Status != monthlysource.FundingMovement || got.DeltaQuota != nil && *got.DeltaQuota == 0 {
					continue
				}
				movement, ok := movements[row.ID]
				if !ok || got.TargetUserID != target || movement.UserID != target || got.DeltaQuota == nil || *got.DeltaQuota != movement.DeltaQuota || row.CreatedAt != movement.At {
					t.Fatalf("omitted or changed money event account=%d event=%d", target, row.ID)
				}
				covered[row.ID] = true
			}
		}
	}
	if len(covered) != len(movements) {
		t.Fatalf("funding reverse coverage got=%d want=%d", len(covered), len(movements))
	}
	t.Logf("reverse retained funding coverage exact: %d unique events; duplicate captures not counted twice", len(covered))
}
