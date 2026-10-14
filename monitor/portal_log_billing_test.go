package monitor

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestPortalBillingExactPlainAndPerCall(t *testing.T) {
	for _, tc := range []struct {
		name, raw       string
		p, c, quota     int64
		state, fragment string
	}{
		{"plain", `{"model_ratio":2,"group_ratio":1.5,"completion_ratio":3,"model_price":-1,"user_group_ratio":-1}`, 100, 20, 480, "matched", "$0.000960"},
		{"wallet", `{"model_ratio":2,"group_ratio":1.5,"completion_ratio":3,"billing_source":"wallet"}`, 100, 20, 480, "matched", "$0.000960"},
		{"personal already applied", `{"model_ratio":2,"group_ratio":1.5,"completion_ratio":3,"user_group_ratio":1.5}`, 100, 20, 480, "matched", "$0.000960"},
		{"free personal", `{"model_ratio":2,"group_ratio":0,"completion_ratio":3,"user_group_ratio":0}`, 100, 20, 0, "matched", "$0.000000"},
		{"half", `{"model_ratio":0.5,"group_ratio":1,"completion_ratio":1}`, 1, 0, 1, "matched", "四舍五入"},
		{"minimum", `{"model_ratio":0.49,"group_ratio":1,"completion_ratio":1}`, 1, 0, 1, "matched", "最小扣费"},
		{"exact decimal", `{"model_ratio":0.1,"group_ratio":1,"completion_ratio":0.2}`, 13, 1, 1, "matched", "$0.000002"},
		{"free model", `{"model_ratio":0,"group_ratio":1,"completion_ratio":1}`, 100, 20, 0, "matched", "$0.000000"},
		{"per call", `{"model_price":0.2,"model_ratio":1,"group_ratio":1.5,"request_path":"/v1/responses"}`, 100, 20, 150000, "matched", "$0.300000"},
		{"per call no type", `{"model_price":0.2,"model_ratio":1,"group_ratio":1.5}`, 100, 20, 150000, "incomplete", ""},
		{"missing rate", `{"model_ratio":1,"group_ratio":1}`, 100, 20, 120, "incomplete", ""},
		{"disagree", `{"model_ratio":2,"group_ratio":1.5,"completion_ratio":3}`, 100, 20, 481, "mismatch", ""},
		{"different personal", `{"model_ratio":2,"group_ratio":1.5,"completion_ratio":3,"user_group_ratio":2}`, 100, 20, 640, "incomplete", ""},
		{"zero tokens", `{"model_ratio":1,"group_ratio":1,"completion_ratio":1}`, 0, 0, 0, "incomplete", ""},
		{"cache missing rate", `{"model_ratio":1,"group_ratio":1,"completion_ratio":1,"cache_tokens":10}`, 100, 20, 120, "incomplete", ""},
		{"tool", `{"model_ratio":1,"group_ratio":1,"completion_ratio":1,"tool_surcharges":[{"name":"web_search","count":1,"price":10}]}`, 100, 20, 120, "unsupported", ""},
		{"other rate", `{"model_ratio":1,"group_ratio":1,"completion_ratio":1,"other_ratios":{"secret":2}}`, 100, 20, 120, "unsupported", ""},
		{"request rules", `{"model_ratio":1,"group_ratio":1,"completion_ratio":1,"request_rules":[]}`, 100, 20, 120, "unsupported", ""},
		{"subscription", `{"model_ratio":1,"group_ratio":1,"completion_ratio":1,"billing_source":"subscription"}`, 100, 20, 120, "unsupported", ""},
		{"saturated", `{"model_ratio":1e308,"group_ratio":1,"completion_ratio":1}`, 100, 20, 2147483647, "unsupported", ""},
		{"negative count", `{"model_ratio":1,"group_ratio":1,"completion_ratio":1}`, -1, 20, 19, "incomplete", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := LogRow{Type: 2, PromptTokens: tc.p, CompletionTokens: tc.c, billingQuota: tc.quota, billingFactsValid: true, CostUSD: float64(tc.quota) / quotaPerUSD}
			before := r
			got := checkPortalBilling(r, parsePortalLogEvidence(tc.raw))
			if got.State != tc.state || (tc.fragment != "" && !strings.Contains(got.Formula, tc.fragment)) {
				t.Fatalf("%+v", got)
			}
			if tc.state != "matched" && got.Formula != "" {
				t.Fatal("unverified formula exposed")
			}
			if !reflect.DeepEqual(r, before) {
				t.Fatal("modified charge")
			}
			populatePortalLogEvidence(&r, tc.raw)
			if r.UsageDetails.Billing.State != got.State || r.BillingProcess != got.Formula || portalCSVRecord(r)[portalCSVBillingColumn] != portalCSVReadableBilling(r) {
				t.Fatal("API/export formula differs")
			}
			if got.State != "matched" && !strings.Contains(portalCSVRecord(r)[portalCSVBillingColumn], got.Message) {
				t.Fatal("CSV missing verification status")
			}
		})
	}
}

func TestPortalBillingQueryNullFactsNeverVerifyZero(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	raw := `{"model_ratio":0,"group_ratio":1,"completion_ratio":1}`
	for id, values := range []string{"NULL,10,2", "0,NULL,2", "0,10,NULL", "0,10,2"} {
		_, err := m.prodDB.Exec(fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,other) VALUES (%d,10,1000,2,%s,?)", id+1, values), raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{10}, 0, 2000, 0, 2, "", "", "", "", "", 0, 100)
	if err != nil || len(rows) != 4 {
		t.Fatalf("%v, %d", err, len(rows))
	}
	for _, r := range rows {
		want := "incomplete"
		if r.ID == 4 {
			want = "matched"
		}
		if r.UsageDetails.Billing.State != want {
			t.Fatalf("id %d: %+v", r.ID, r.UsageDetails.Billing)
		}
		if r.SourceFacts == nil || r.SourceFacts.QuotaPresent != (r.ID != 1) || r.SourceFacts.PromptPresent != (r.ID != 2) || r.SourceFacts.CompletionPresent != (r.ID != 3) {
			t.Fatalf("source NULL presence lost: id=%d facts=%+v", r.ID, r.SourceFacts)
		}
		record := portalCSVRecord(r)
		if (record[9] == "") != (r.ID == 1) || (record[7] == "") != (r.ID == 2) || (record[8] == "") != (r.ID == 3) {
			t.Fatalf("missing source became CSV zero: id=%d record=%v", r.ID, record)
		}
		if r.ID == 4 && record[9] != "0.000000" {
			t.Fatal("real zero charge lost")
		}
	}
}

func TestPortalBillingDoesNotDropLegacyTaskExplanation(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	_, err := m.prodDB.Exec(`INSERT INTO logs (id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,content,other) VALUES (1,10,1000,2,1,10,2,?,?)`, "历史任务预扣说明", `{"is_task":true,"task_id":"task-123","model_price":-1}`)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{10}, 0, 2000, 0, 2, "", "", "", "", "", 0, 100)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v rows=%d", err, len(rows))
	}
	if rows[0].OtherContent != "历史任务预扣说明" || rows[0].BillingProcess != "" {
		t.Fatalf("task explanation lost: %+v", rows[0])
	}
}

func TestPortalBillingIntegerRoundingAgainstIndependentArithmetic(t *testing.T) {
	// Decimal tenths, independently evaluate exact integer numerators, including
	// half boundaries and minimum units. No float or production helper as oracle.
	for p := int64(1); p <= 50; p++ {
		for tenth := int64(0); tenth <= 20; tenth++ {
			q := (p*tenth + 5) / 10
			if q == 0 && tenth > 0 {
				q = 1
			}
			raw := fmt.Sprintf(`{"model_ratio":%d.%d,"group_ratio":1,"completion_ratio":1}`, tenth/10, tenth%10)
			got := checkPortalBilling(LogRow{PromptTokens: p, billingQuota: q, billingFactsValid: true}, parsePortalLogEvidence(raw))
			if got.State != "matched" {
				t.Fatalf("p=%d ratio=%d/10 q=%d %+v", p, tenth, q, got)
			}
		}
	}
}
