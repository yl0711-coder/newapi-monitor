package monitor

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"testing"
)

// Explicit private input only. The fixture contains numeric fields read from
// the UI, not a raw API capture, credentials, identities or pricing metadata.
// It validates amount/token replay, not historical channel attribution.
func TestAipipeUsageUSDPrivateUIHourReplay(t *testing.T) {
	path := os.Getenv("MONITOR_AIPIPE_UI_SAMPLE")
	if path == "" {
		t.Skip("private UI numeric sample not provided")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var sample struct {
		Items [][]json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(raw, &sample); err != nil || len(sample.Items) != 65 {
		t.Fatalf("invalid private sample: %v", err)
	}
	const from = int64(1791522000)
	items := make([]map[string]any, 0, len(sample.Items))
	for index, tuple := range sample.Items {
		if len(tuple) != 4 {
			t.Fatalf("invalid numeric tuple at %d", index)
		}
		created, err := rawJSONInt64Exact(tuple[0])
		if err != nil || created < from || created >= from+3600 {
			t.Fatalf("out-of-window timestamp at %d", index)
		}
		var amount string
		if err := json.Unmarshal(tuple[3], &amount); err != nil || !json.Valid([]byte(amount)) {
			t.Fatalf("invalid decimal at %d", index)
		}
		items = append(items, map[string]any{"type": 2, "created_at": tuple[0],
			"prompt_tokens": tuple[1], "completion_tokens": tuple[2], "usage_usd": json.RawMessage(amount)})
	}
	body, err := json.Marshal(map[string]any{"success": true, "data": map[string]any{"total": 65, "items": items}})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		if r.Method != http.MethodGet || r.URL.Host != "aipipe.cc" || r.URL.Path != "/api/log/self" || q.Get("type") != "2" || q.Get("p") != "1" || q.Get("start_timestamp") != "1791522000" || q.Get("end_timestamp") != "1791525599" {
			t.Fatal("unexpected offline replay request")
		}
		return recoveryHTTPResponse(http.StatusOK, string(body)), nil
	})}
	row := aipipeTestAccount()
	cred := newAPICredential{AccessToken: "fixture-only"}
	bill, err := fetchNewAPIUsageWindow(context.Background(), client, row, cred, from, from+3600)
	if err != nil || len(bill.Hours) != 1 {
		t.Fatalf("replay failed: %v", err)
	}
	hour := bill.Hours[0]
	// Independent stat endpoint previously observed: 65 rows, 29,426 tokens,
	// 1,011,122 quota. Do not derive these controls from the fixture itself.
	if hour.Requests != 65 || hour.Tokens != 29426 || hour.Quota != 1011122 || math.Abs(hour.CostUSD-2.022244) > 1e-12 {
		t.Fatal("numeric replay differs from independent stat controls")
	}
	page, err := fetchNewAPIPricingPage(context.Background(), client, row, cred, from, from+3600, 1, nil)
	if err != nil || len(page.Items) != 65 {
		t.Fatalf("pricing numeric decode failed: %v", err)
	}
	var quota int64
	for _, item := range page.Items {
		if !item.QuotaExactKnown || !item.TokensExactKnown {
			t.Fatal("observed hour contains inexact integer ledger input")
		}
		quota += item.QuotaExact
	}
	if quota != 1011122 {
		t.Fatal("billing and pricing numeric decoders disagree")
	}
	t.Log("65 UI numeric records replayed; quota=1011122 tokens=29426 amount=2.022244; not raw API or attribution verification")
}
