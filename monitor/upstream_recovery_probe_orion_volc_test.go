package monitor

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// Recovery probes must know Orion and Volcengine; otherwise every probe
// fails as an unknown shape and the account is isolated for good.
func TestRecoveryProbeSupportsOrionAndVolcengine(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	var calls []string
	orion := orionFixtureServer(t, `{"user":{"id":42,"balance":1,"role":"holder"}}`, map[string]map[string]any{"": {"items": []any{}}}, &calls)
	defer orion.Close()
	orionRow := ChannelUpstreamAccount{Domain: "orion.test", Provider: upstreamProviderOrion, BaseURL: orion.URL, UserID: 42, Enabled: true}
	if err := m.sealUpstreamAccountCredential(&orionRow, orionCredential{AccessToken: "smk_fixture"}); err != nil {
		t.Fatal(err)
	}
	for _, task := range []string{"balance", "usage", "usage_history"} {
		if err := m.probeUpstreamRecovery(context.Background(), orionRow, task); err != nil {
			t.Fatalf("orion %s probe: %v", task, err)
		}
	}
	f := &volcengineFixture{t: t, balance: "10.00", currency: "CNY", days: map[string][]volcengineBillItem{}}
	withVolcengineFixture(t, f, time.Now())
	volcRow := ChannelUpstreamAccount{Domain: "volcengine.test", Provider: upstreamProviderVolcengine, Enabled: true}
	if err := m.sealUpstreamAccountCredential(&volcRow, volcFixtureCred); err != nil {
		t.Fatal(err)
	}
	for _, task := range []string{"balance", "usage", "usage_history"} {
		if err := m.probeUpstreamRecovery(context.Background(), volcRow, task); err != nil {
			t.Fatalf("volcengine %s probe: %v", task, err)
		}
	}
	if len(f.calls) != 3 || f.calls[0] != "QueryBalanceAcct" || f.calls[1] != "ListBillDetail" {
		t.Fatalf("volcengine probe calls: %v", f.calls)
	}
}

// The probe transport stays read-only: only GET and the two Volcengine
// billing read actions pass.
func TestRecoveryProbeTransportReadOnly(t *testing.T) {
	endpoint, _ := url.Parse(volcengineBillingEndpoint)
	req := func(method, rawURL string) *http.Request {
		u, _ := url.Parse(rawURL)
		return &http.Request{Method: method, URL: u}
	}
	base := endpoint.Scheme + "://" + endpoint.Host + "/"
	cases := map[*http.Request]bool{
		req(http.MethodGet, "https://example.test/api/user/self"):           true,
		req(http.MethodPost, base+"?Action=QueryBalanceAcct&Version=x"):     true,
		req(http.MethodPost, base+"?Action=ListBillDetail&Version=x"):       true,
		req(http.MethodPost, base+"?Action=CreateOrder&Version=x"):          false,
		req(http.MethodPost, "https://example.test/?Action=ListBillDetail"): false,
		req(http.MethodPut, base+"?Action=ListBillDetail"):                  false,
	}
	for r, want := range cases {
		if got := upstreamProbeReadOnlyRequest(r); got != want {
			t.Errorf("%s %s: %v, want %v", r.Method, r.URL, got, want)
		}
	}
}
