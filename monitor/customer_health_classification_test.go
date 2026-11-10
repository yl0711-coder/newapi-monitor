package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestCustomerHealthDiagnosticCatalogueParity(t *testing.T) {
	var fixture struct{ Rows []struct{ Content string } }
	b, err := os.ReadFile("testdata/logchain_diagnostic_originals.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Rows) != 300 {
		t.Fatal(len(fixture.Rows))
	}
	for i, s := range fixture.Rows {
		r := customerHealthDiagnosticRow(LogChainRow{Type: 5, Content: s.Content}, `{}`)
		included, got := customerHealthDiagnosticFailure(r)
		want := logChainAttributeFaultWithEvidence(r, nil)
		if !reflect.DeepEqual(got, want) || included != (want.Fault != faultDownstream) {
			t.Fatalf("row %d attribution drift", i+1)
		}
		signature := customerHealthAttributeProblem(customerHealthProblem{message: s.Content})
		if signature.fault != want.Fault || signature.reason != want.Reason {
			t.Fatalf("row %d signature reason drift: %+v %+v", i+1, signature, want)
		}
	}
}

func TestCustomerHealthDiagnosticPriorityAndAggregate(t *testing.T) {
	db := newFakeProdDB(t)
	cases := []struct{ content, other, party string }{
		{"status_code=502, request failed", `{"error_code":"array_above_max_length"}`, faultDownstream},
		{"status_code=502, openai_error", `{"error_code":"upstream_http2_stream_error"}`, faultUpstream},
		{"status_code=503, system cpu overloaded (current: 95%, threshold: 90%)", `{}`, faultOurs},
		{"status_code=500, prompt is too long: 120000 tokens > 100000 maximum", `{}`, faultDownstream},
	}
	var included int64
	for i, c := range cases {
		r := customerHealthDiagnosticRow(LogChainRow{Type: 5, Content: c.content}, c.other)
		counts, f := customerHealthDiagnosticFailure(r)
		if f.Fault != c.party {
			t.Fatalf("case %d: %+v", i, f)
		}
		if counts {
			included++
		}
		if _, err := db.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,`group`,content,other) VALUES(?,101,2,?,5,'m','g',?,?)", i+1, 600+i, c.content, c.other); err != nil {
			t.Fatal(err)
		}
	}
	batch := []CapacityUserMinuteSample{{BucketTs: 600, UserID: 101, ChannelID: 2, ModelName: "m", Grp: "g", Success: 6, Failed: 4, Quota: 123, TtftObserved: 3}}
	if err := classifyCustomerHealthErrors(context.Background(), db, 600, 660, batch); err != nil {
		t.Fatal(err)
	}
	if batch[0].CustomerHealthFailed != included || batch[0].Quota != 123 || batch[0].TtftObserved != 3 || batch[0].Failed != 4 {
		t.Fatal(batch)
	}
	rate := customerHealthStability(10, 0, included)
	if rate == nil || *rate != 80 {
		t.Fatal(rate, included)
	}
	// Late inserts never certify a partial classification or change the input batch.
	before := batch[0]
	if _, err := db.Exec("INSERT INTO logs(id,user_id,channel_id,created_at,type,model_name,`group`,content,other) VALUES(10,101,2,650,5,'m','g','new error','{}')"); err != nil {
		t.Fatal(err)
	}
	if err := classifyCustomerHealthErrors(context.Background(), db, 600, 660, batch); !errors.Is(err, errCustomerHealthClassificationChanged) || !reflect.DeepEqual(before, batch[0]) {
		t.Fatal(err, batch)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := classifyCustomerHealthErrors(ctx, db, 600, 660, batch); err == nil {
		t.Fatal("cancelled classification published")
	}
	batch[0].Failed = customerHealthClassificationRows + 1
	if err := classifyCustomerHealthErrors(context.Background(), db, 600, 660, batch); !errors.Is(err, errCustomerHealthClassificationBudget) {
		t.Fatal(err)
	}
	batch[0].Failed = 5
	if _, err := db.Exec("UPDATE logs SET content=? WHERE id=10", strings.Repeat("a", 16385)); err != nil {
		t.Fatal(err)
	}
	if err := classifyCustomerHealthErrors(context.Background(), db, 600, 660, batch); !errors.Is(err, errCustomerHealthClassificationBudget) {
		t.Fatal(err)
	}
}

func TestCustomerHealthDeferredSharedProjection(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	row := CapacityUserMinuteSample{BucketTs: 600, UserID: 101, ChannelID: 2, ModelName: "m", Grp: "g", Failed: 4, Success: 6, CustomerHealthFailed: 2, CustomerHealthVersion: customerHealthStabilityPolicyVersion, Quota: 100}
	if err := upsertCapacityUserMinuteSamplesDB(m.storeDB, []CapacityUserMinuteSample{row}); err != nil {
		t.Fatal(err)
	}
	row.CustomerHealthVersion = 0
	row.CustomerHealthFailed = 0
	row.Quota = 200
	for _, changed := range []bool{false, true} {
		if changed {
			row.Failed++
		}
		if err := upsertCustomerHealthDeferredSamples(m.storeDB, []CapacityUserMinuteSample{row}); err != nil {
			t.Fatal(err)
		}
		var got CapacityUserMinuteSample
		if err := m.storeDB.First(&got).Error; err != nil {
			t.Fatal(err)
		}
		if got.Quota != 200 || got.Failed != row.Failed {
			t.Fatal("shared aggregate not refreshed", got)
		}
		if !changed && (got.CustomerHealthFailed != 2 || got.CustomerHealthVersion != customerHealthStabilityPolicyVersion) {
			t.Fatal("valid classification erased", got)
		}
		if changed && got.CustomerHealthVersion != 0 {
			t.Fatal("new errors certified with stale classification", got)
		}
	}
}
