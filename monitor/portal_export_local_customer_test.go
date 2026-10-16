package monitor

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Saved, bounded production sample; only temporary local databases are used.
// Not a full monthly export and not a production database load test.
func TestPortalExportLocalCustomerSample(t *testing.T) {
	path := os.Getenv("USAGE_EXPORT_DX01_SAMPLE")
	if path == "" {
		t.Skip("private local customer sample not supplied")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var sample struct {
		UserID int64 `json:"user_id"`
		From   int64 `json:"from"`
		To     int64 `json:"to"`
		Facts  []struct {
			ID         int64         `json:"id"`
			Created    int64         `json:"created_at"`
			Username   string        `json:"username"`
			Token      string        `json:"token_name"`
			Group      string        `json:"group"`
			Model      string        `json:"model"`
			RequestID  string        `json:"request_id"`
			Prompt     sql.NullInt64 `json:"prompt"`
			Completion sql.NullInt64 `json:"completion"`
			Quota      sql.NullInt64 `json:"quota"`
			Other      string        `json:"other"`
		} `json:"facts"`
	}
	if err := json.Unmarshal(b, &sample); err != nil {
		t.Fatal(err)
	}
	if sample.UserID != 102 || len(sample.Facts) == 0 || len(sample.Facts) > 1000 || sample.To-sample.From != 3600 {
		t.Fatal("unexpected bounded sample")
	}
	// Amplify only in the disposable local fixture; never query production for
	// load testing. Preserve all original fields except the synthetic primary ID.
	repeat := 1
	if raw := os.Getenv("USAGE_EXPORT_DX01_REPEAT"); raw != "" {
		var err error
		repeat, err = strconv.Atoi(raw)
		if err != nil || repeat < 1 || repeat > 50 {
			t.Fatal("local replay repeat must be 1..50")
		}
	}
	if repeat > 1 {
		original := sample.Facts
		sample.Facts = nil
		for _, fact := range original {
			if fact.ID < 1 || fact.ID > (1<<63-1)/100 {
				t.Fatal("sample ID outside amplification range")
			}
			for copyIndex := 0; copyIndex < repeat; copyIndex++ {
				copyFact := fact
				copyFact.ID = fact.ID*100 + int64(copyIndex)
				sample.Facts = append(sample.Facts, copyFact)
			}
		}
	}
	sort.Slice(sample.Facts, func(i, j int) bool {
		a, b := sample.Facts[i], sample.Facts[j]
		if a.Created != b.Created {
			return a.Created > b.Created
		}
		return a.ID > b.ID
	})
	for _, format := range []string{"logs", "bill"} {
		t.Run(format, func(t *testing.T) {
			m, handler, cookie, _ := billHTTPFixture(t)
			if _, err := m.prodDB.Exec("DELETE FROM logs"); err != nil {
				t.Fatal(err)
			}
			if _, err := m.prodDB.Exec("CREATE INDEX customer_sample_user_time ON logs(user_id,created_at,type,id)"); err != nil {
				t.Fatal(err)
			}
			tx, err := m.prodDB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			stmt, err := tx.Prepare("INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,username,model_name,token_name,`group`,other,request_id) VALUES(?,101,?,2,?,?,?,?,?,?,?,?,?)")
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			var quotaSum int64
			for _, f := range sample.Facts {
				if !f.Quota.Valid || f.Quota.Int64 < 0 || f.Created < sample.From || f.Created >= sample.To {
					t.Fatal("invalid source fact")
				}
				_, err := stmt.Exec(f.ID, f.Created, f.Quota, f.Prompt, f.Completion, f.Username, f.Model, f.Token, f.Group, f.Other, f.RequestID)
				if err != nil {
					t.Fatal(err)
				}
				quotaSum += f.Quota.Int64
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			day := time.Unix(sample.From, 0).In(usageCST).Format("2006-01-02")
			q := "/api/logs/export/prepare?from=" + day + "&to=" + day
			path := "/api/logs/export/v1?ticket="
			if format == "bill" {
				q += "&format=bill-v1"
				path = "/api/logs/bill/v1?ticket="
			}
			started := time.Now()
			prep := billGet(handler, cookie, q)
			var reply struct {
				Ticket string `json:"ticket"`
				Total  int    `json:"total"`
			}
			if err := json.Unmarshal(prep.Body.Bytes(), &reply); err != nil || prep.Code != 200 || reply.Total != len(sample.Facts) {
				t.Fatalf("preflight code=%d", prep.Code)
			}
			w := billGet(handler, cookie, path+reply.Ticket)
			elapsed := time.Since(started)
			if w.Code != 200 {
				t.Fatalf("download code=%d: %s", w.Code, w.Body.String())
			}
			rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(w.Body.String(), "\ufeff"))).ReadAll()
			if err != nil || len(rows) != len(sample.Facts)+1 {
				t.Fatal("wrong row count")
			}
			amountColumn, requestColumn, modelColumn := 9, 11, 5
			if format == "bill" {
				amountColumn, requestColumn, modelColumn = 13, 5, 4
			}
			sum := new(big.Rat)
			missingInput, missingWrite, missingReference := 0, 0, 0
			tokenBlockers := map[string]int{}
			for i, row := range rows[1:] {
				if format == "bill" {
					if row[7] == "" {
						missingInput++
					}
					if row[10] == "" {
						missingWrite++
					}
					if strings.TrimPrefix(row[12], "¥") == "" {
						missingReference++
					}
				}
				fact := sample.Facts[i]
				if format == "bill" {
					wantReference := portalBillPinnedSampleReference(t, fact.Model, fact.Other, fact.Prompt.Int64, fact.Completion.Int64)
					wantDiscount := ""
					if wantReference == nil {
						if strings.TrimPrefix(row[12], "¥") != "" {
							t.Fatal("redacted tool facts unexpectedly priced")
						}
					} else {
						if strings.TrimPrefix(row[12], "¥") != wantReference.FloatString(6) {
							t.Fatalf("reference arithmetic differs at sample row %d", i)
						}
						if wantReference.Sign() > 0 {
							wantDiscount = strings.TrimRight(strings.TrimRight(new(big.Rat).Quo(big.NewRat(fact.Quota.Int64, 500000), wantReference).FloatString(3), "0"), ".")
						}
					}
					if row[14] != wantDiscount {
						t.Fatalf("discount arithmetic differs at sample row %d", i)
					}
				}
				if format == "bill" && row[7] != "" {
					// Independent expected values from this pinned default-OpenAI
					// source sample, not the bill formatter or its evidence helper.
					var raw struct {
						Read  int64 `json:"cache_tokens"`
						Write int64 `json:"cache_creation_tokens"`
					}
					if err := json.Unmarshal([]byte(fact.Other), &raw); err != nil {
						t.Fatal(err)
					}
					if row[7] != strconv.FormatInt(fact.Prompt.Int64-raw.Read-raw.Write, 10) || row[8] != strconv.FormatInt(fact.Completion.Int64, 10) || row[9] != strconv.FormatInt(raw.Read, 10) || row[10] != strconv.FormatInt(raw.Write, 10) {
						t.Fatal("recorded token conservation failed")
					}
				}
				if format == "bill" && row[7] == "" {
					r := LogRow{Type: 2, PromptTokens: fact.Prompt.Int64, CompletionTokens: fact.Completion.Int64, billingQuota: fact.Quota.Int64, billingFactsValid: fact.Prompt.Valid && fact.Completion.Valid && fact.Quota.Valid}
					e := parsePortalLogEvidence(fact.Other)
					check := checkPortalBilling(r, e)
					tokenBlockers[check.State+": "+check.Message]++
				}
				want := big.NewRat(fact.Quota.Int64, 500000)
				if strings.TrimPrefix(row[amountColumn], "¥") != want.FloatString(6) || row[requestColumn] != fact.RequestID || row[modelColumn] != fact.Model || row[0] != time.Unix(fact.Created, 0).In(usageCST).Format("2006-01-02 15:04:05") {
					t.Fatalf("source row %d changed or reordered", i)
				}
				v, ok := new(big.Rat).SetString(strings.TrimPrefix(row[amountColumn], "¥"))
				if !ok {
					t.Fatal("invalid amount")
				}
				sum.Add(sum, v)
			}
			if sum.Cmp(big.NewRat(quotaSum, 500000)) != 0 {
				t.Fatal("amount conservation failed")
			}
			t.Logf("local customer sample rows=%d replay_factor=%d bytes=%d total=%s row_order_and_amount=PASS", len(sample.Facts), repeat, w.Body.Len(), elapsed)
			if format == "bill" {
				if len(sample.Facts) == 1000*repeat && (missingInput != 8*repeat || missingWrite != 8*repeat || missingReference != 8*repeat) {
					t.Fatalf("fixed sample coverage regressed: input=%d write=%d reference=%d", missingInput, missingWrite, missingReference)
				}
				t.Logf("coverage gaps: ordinary_input=%d cache_write=%d reference=%d; successful download is NOT complete field coverage", missingInput, missingWrite, missingReference)
				t.Logf("token verification blockers: %v", tokenBlockers)
			}
		})
	}
}

// Independent integer oracle for this pinned local sample, NOT a production
// price catalogue. Prices below are tenths of USD/1M from its saved formulas.
// No production parser, normalization or reference helpers are used here.
func portalBillPinnedSampleReference(t *testing.T, model, raw string, prompt, completion int64) *big.Rat {
	t.Helper()
	var e struct {
		Read  int64           `json:"cache_tokens"`
		Write int64           `json:"cache_creation_tokens"`
		Tools json.RawMessage `json:"tool_surcharges"`
		Group float64         `json:"group_ratio"`
	}
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	if len(e.Tools) != 0 {
		return nil
	} // saved fixture deliberately redacted these
	if e.Group != 0.35 || prompt < e.Read+e.Write {
		t.Fatal("pinned sample contract changed")
	}
	prices := map[string][2][4]int64{
		"gpt-6.1-sol": {{20, 1, 25, 100}, {40, 2, 50, 150}},
		"gpt-6-sol":   {{20, 2, 25, 100}, {40, 4, 50, 150}},
		"gpt-6-astra": {{100, 10, 125, 500}, {200, 20, 250, 750}},
		"gpt-5.6-sol": {{40, 4, 50, 200}, {80, 8, 100, 300}},
		"gpt-5.5":     {{50, 5, 0, 300}, {100, 10, 10, 450}},
	}
	modelPrices, ok := prices[model]
	if !ok {
		t.Fatal("unexpected pinned model")
	}
	tier := 0
	if prompt > 272000 {
		tier = 1
	}
	p := modelPrices[tier]
	weighted := (prompt-e.Read-e.Write)*p[0] + e.Read*p[1] + e.Write*p[2] + completion*p[3]
	return big.NewRat(weighted*7, 10000000)
}
