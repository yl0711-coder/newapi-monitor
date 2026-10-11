package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func aipipeTestAccount() ChannelUpstreamAccount {
	return ChannelUpstreamAccount{Domain: "aipipe.cc", BaseURL: "https://aipipe.cc", Provider: upstreamProviderNewAPI, UserID: 189, BalanceUnit: 500000}
}

func aipipePageClient(t *testing.T, items string) *http.Client {
	t.Helper()
	return &http.Client{Transport: upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/log/self" || r.URL.Query().Get("type") != "2" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		return recoveryHTTPResponse(200, `{"success":true,"data":{"total":1,"items":[`+items+`]}}`), nil
	})}
}

func TestAipipeUsageUSDThroughExistingDecoders(t *testing.T) {
	// Redacted numeric values inspected in the provider UI; not a full export.
	const sample = `{"created_at":1791525571,"type":2,"usage_usd":0.001452,"prompt_tokens":8,"completion_tokens":16}`
	row := aipipeTestAccount()
	cred := newAPICredential{AccessToken: "fixture-only"}
	page, err := fetchNewAPIUsagePage(context.Background(), aipipePageClient(t, sample), row, cred, 1791522000, 1791525600, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Quota != 726 || page.Items[0].PromptTokens != 8 {
		t.Fatalf("unexpected bill item: %+v", page.Items)
	}
	wantFingerprint, err := canonicalUsagePageFingerprint([]json.RawMessage{json.RawMessage(sample)})
	if err != nil || page.Fingerprint != wantFingerprint {
		t.Fatal("compatibility changed original page identity", err)
	}
	pricing, err := fetchNewAPIPricingPage(context.Background(), aipipePageClient(t, sample), row, cred, 1791522000, 1791525600, 1, nil)
	if err != nil || len(pricing.Items) != 1 || !pricing.Items[0].QuotaExactKnown || pricing.Items[0].QuotaExact != 726 {
		t.Fatalf("pricing differs from billing: %+v %v", pricing.Items, err)
	}
	result, err := fetchNewAPIUsageWindow(context.Background(), aipipePageClient(t, sample), row, cred, 1791522000, 1791525600)
	if err != nil || len(result.Hours) != 1 || math.Abs(result.Hours[0].CostUSD-0.001452) > 1e-12 {
		t.Fatalf("amount was double converted: %+v %v", result, err)
	}
}

func TestAipipeUsageUSDScopePreservesOtherProviders(t *testing.T) {
	for _, scenario := range []string{"domain", "host", "provider", "http", "port"} {
		t.Run(scenario, func(t *testing.T) {
			row := aipipeTestAccount()
			switch scenario {
			case "domain":
				row.Domain = "other.example"
			case "host":
				row.BaseURL = "https://aipipe.cc.other.example"
			case "provider":
				row.Provider = upstreamProviderSub2API
			case "http":
				row.BaseURL = "http://aipipe.cc"
			case "port":
				row.BaseURL = "https://aipipe.cc:8443"
			}
			cred := newAPICredential{AccessToken: "fixture-only"}
			for _, withQuota := range []bool{false, true} {
				body := `{"created_at":1791525571,"usage_usd":0.001452}`
				if withQuota {
					body = `{"created_at":1791525571,"usage_usd":"invalid","quota":17}`
				}
				page, err := fetchNewAPIUsagePage(context.Background(), aipipePageClient(t, body), row, cred, 1791522000, 1791525600, 1, nil)
				if !withQuota && err == nil {
					t.Fatal("unverified provider accepted usage_usd")
				}
				if withQuota && (err != nil || len(page.Items) != 1 || page.Items[0].Quota != 17) {
					t.Fatalf("legacy quota changed: %+v %v", page, err)
				}
			}
		})
	}
}

func TestAipipeUsageUSDRejectsMissingInvalidAndConflictingMoney(t *testing.T) {
	for _, fields := range []string{
		``, `"usage_usd":null`, `"usage_usd":""`, `"usage_usd":-1`, `"usage_usd":true`,
		`"usage_usd":"NaN"`, `"usage_usd":"1/2"`, `"usage_usd":"1e-999999"`,
		`"usage_usd":1e30`, `"usage_usd":0.0000000000000000001`,
		`"quota":null,"usage_usd":1`, `"quota":"bad","usage_usd":1`,
		`"quota":0,"usage_usd":1`, `"quota":500000,"usage_usd":2`,
	} {
		t.Run(fmt.Sprintf("%s", fields), func(t *testing.T) {
			body := `{"created_at":1791525571`
			if fields != "" {
				body += "," + fields
			}
			body += "}"
			_, err := fetchNewAPIUsagePage(context.Background(), aipipePageClient(t, body), aipipeTestAccount(), newAPICredential{AccessToken: "fixture-only"}, 1791522000, 1791525600, 1, nil)
			if err == nil {
				t.Fatal("invalid or conflicting amount was accepted")
			}
		})
	}
}

func TestAipipeUsageUSDAmountMatrixAndTaskBoundary(t *testing.T) {
	for _, tc := range []struct {
		fields string
		quota  float64
	}{
		{`"usage_usd":0`, 0},
		{`"usage_usd":"0.001452"`, 726},
		{`"usage_usd":1.452e-3`, 726},
		{`"usage_usd":0.000001`, 0.5}, // Never round a fractional quota to zero.
		{`"usage_usd":0.001452,"quota":726`, 726},
		{`"quota":"726"`, 726},
	} {
		t.Run(tc.fields, func(t *testing.T) {
			body := json.RawMessage(`{"created_at":1791525571,` + tc.fields + `}`)
			normalized, err := normalizeAipipeUsageItem(body, aipipeQuotaPerLedgerUnit)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeLegacyNewAPIUsageItem(normalized)
			if err != nil || got.Quota != tc.quota {
				t.Fatalf("quota: %+v %v", got, err)
			}
			pricing, err := decodeNewAPIUsageItem(normalized)
			if err != nil {
				t.Fatal(err)
			}
			if tc.quota == .5 && pricing.QuotaExactKnown {
				t.Fatal("fractional quota claimed integer precision")
			}
		})
	}
	if _, err := normalizeAipipeUsageItem(json.RawMessage(`{"usage_usd":1}`), 1000000); err == nil {
		t.Fatal("unverified unit accepted")
	}
	for _, kind := range []int{0, 1, 3, 4, 5, 6} {
		if usesAipipeUsageUSD(aipipeTestAccount(), kind) {
			t.Fatalf("non-consumption type %d enabled", kind)
		}
	}
}

func TestAipipeUsageUSDHistoryFailureRetryAndPagination(t *testing.T) {
	// Synthetic pages calibrated to the observed stat total, NOT a replay of
	// the provider's 65 private records. Both one-page and multi-page paths run.
	for _, count := range []int{65, 101} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			m := newChannelUpstreamTestMonitor(t)
			m.cfg.UpstreamUsageSyncEnabled = true
			now := time.Now().Unix()
			from := cstDayStart(now) - 3600
			row := aipipeTestAccount()
			row.Enabled, row.UsageSyncEnabled = true, true
			row.UsageStatus, row.UsageNextSyncAt, row.UsageDataUntil = upstreamStatusOK, now+3600, now
			row.UsageBackfillCursor = from
			row.UsageBackfillNextSyncAt = now - 1
			cred := newAPICredential{AccessToken: "fixture-only"}
			if err := m.persistSyncedUpstreamAccount(context.Background(), &row, cred); err != nil {
				t.Fatal(err)
			}
			old := ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: from, BucketSeconds: 900, Provider: row.Provider, Requests: 1, Quota: 7 * aipipeQuotaPerLedgerUnit, CostUSD: 7, UnitPerUSD: aipipeQuotaPerLedgerUnit}
			peer := old
			peer.Domain = "unrelated.example"
			if err := m.storeDB.Create(&[]ChannelUpstreamUsageHour{old, peer}).Error; err != nil {
				t.Fatal(err)
			}
			broken := true
			setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != "GET" || r.URL.Host != "aipipe.cc" || r.URL.Path != "/api/log/self" || r.URL.Query().Get("type") != "2" {
					t.Fatalf("unexpected upstream request %s %s", r.Method, r.URL.Path)
				}
				if r.URL.Query().Get("start_timestamp") != strconv.FormatInt(from, 10) || r.URL.Query().Get("end_timestamp") != strconv.FormatInt(from+3599, 10) {
					t.Fatal("history scope changed")
				}
				page, _ := strconv.Atoi(r.URL.Query().Get("p"))
				items := []map[string]any{}
				for i := (page - 1) * upstreamUsagePageSize; i < count && i < page*upstreamUsagePageSize; i++ {
					amount := json.RawMessage(`0.01`)
					if i == 0 {
						amount = json.RawMessage(`0.001452`)
					}
					if i == count-1 {
						amount = json.RawMessage(fmt.Sprintf("%.6f", 2.022244-.001452-float64(count-2)*.01))
					}
					item := map[string]any{"id": i + 1, "created_at": from + int64(i) + 1, "type": 2, "usage_usd": amount, "prompt_tokens": 8, "completion_tokens": 16}
					if broken && i == count-1 {
						item["usage_usd"] = nil
					}
					items = append(items, item)
				}
				body, err := json.Marshal(map[string]any{"success": true, "data": map[string]any{"total": count, "items": items}})
				if err != nil {
					t.Fatal(err)
				}
				return recoveryHTTPResponse(200, string(body)), nil
			}))
			failed, err := m.syncStoredUpstreamUsageWithPriority(context.Background(), row.Domain, true, upstreamUsageLaneHistory)
			if err == nil || failed.UsageBackfillCursor != from {
				t.Fatalf("bad page advanced cursor: %+v %v", failed, err)
			}
			var preserved ChannelUpstreamUsageHour
			if err := m.storeDB.First(&preserved, "domain=? AND hour_ts=?", row.Domain, from).Error; err != nil || preserved.CostUSD != 7 {
				t.Fatalf("bad page overwrote bill: %+v %v", preserved, err)
			}
			broken = false
			// Test-only scheduling reset; production recovery uses its existing UI.
			if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", row.Domain).Update("usage_backfill_next_sync_at", 0).Error; err != nil {
				t.Fatal(err)
			}
			synced, err := m.syncStoredUpstreamUsageWithPriority(context.Background(), row.Domain, true, upstreamUsageLaneHistory)
			if err != nil || !synced.UsageBackfillDone || synced.UsageBackfillCursor != from+3600 {
				t.Fatalf("history failed: %+v %v", synced, err)
			}
			var got ChannelUpstreamUsageHour
			if err := m.storeDB.First(&got, "domain=? AND hour_ts=?", row.Domain, from).Error; err != nil {
				t.Fatal(err)
			}
			if got.Requests != int64(count) || got.Tokens != int64(count*24) || got.BucketSeconds != 3600 || math.Abs(got.CostUSD-2.022244) > 1e-12 || math.Abs(got.Quota-1011122) > 1e-8 {
				t.Fatalf("incorrect publication: %+v", got)
			}
			if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", row.Domain).Updates(map[string]any{"usage_backfill_next_sync_at": 0, "usage_backfill_done": false, "usage_backfill_cursor": from}).Error; err != nil {
				t.Fatal(err)
			}
			if _, err := m.syncStoredUpstreamUsageWithPriority(context.Background(), row.Domain, true, upstreamUsageLaneHistory); err != nil {
				t.Fatal(err)
			}
			var replay, other ChannelUpstreamUsageHour
			m.storeDB.First(&replay, "domain=? AND hour_ts=?", row.Domain, from)
			m.storeDB.First(&other, "domain=? AND hour_ts=?", peer.Domain, from)
			if replay.CostUSD != got.CostUSD || replay.Requests != got.Requests || other.CostUSD != peer.CostUSD || other.BucketSeconds != peer.BucketSeconds {
				t.Fatal("retry duplicated money or changed unrelated bill")
			}
		})
	}
}

// Billing supports fractional quota, but the existing integer pricing/cost
// ledgers do not. This regression documents the boundary; it must never be
// "fixed" by rounding the bill or silently publishing zero ledger cost.
func TestAipipeUsageUSDIntegerLedgerBoundary(t *testing.T) {
	const from = int64(1791522000)
	row := aipipeTestAccount()
	cred := newAPICredential{AccessToken: "fixture-only"}
	for _, tc := range []struct {
		amount string
		quota  int64
		exact  bool
	}{
		{"0.001452", 726, true},
		{"0.000001", 0, false},
	} {
		t.Run(tc.amount, func(t *testing.T) {
			body := `{"created_at":1791525571,"type":2,"usage_usd":` + tc.amount + `,"prompt_tokens":8,"completion_tokens":16,"token_id":1,"group":"fixture","model_name":"fixture","other":{"group_ratio":1,"billing_mode":"token"}}`
			bill, err := fetchNewAPIUsageWindow(context.Background(), aipipePageClient(t, body), row, cred, from, from+3600)
			amount, parseErr := strconv.ParseFloat(tc.amount, 64)
			if err != nil || parseErr != nil || len(bill.Hours) != 1 || math.Abs(bill.Hours[0].CostUSD-amount) > 1e-15 || bill.Hours[0].Requests != 1 {
				t.Fatalf("billing lost amount: %+v %v", bill, err)
			}
			page, err := fetchNewAPIPricingPage(context.Background(), aipipePageClient(t, body), row, cred, from, from+3600, 1, nil)
			if err != nil || len(page.Items) != 1 || page.Items[0].QuotaExactKnown != tc.exact {
				t.Fatalf("pricing decode: %+v %v", page, err)
			}
			_, hashErr := pricingSourceContentHash(page.Items, from, from+3600)
			pricing, _, pricingErr := buildNewAPIPricingHour(row, page.Items, from, from+3600)
			cost, state, costErr := buildNewAPICostHourEvidence(row, page.Items, from, from+3600, []byte("0123456789abcdef0123456789abcdef"), "fixture-only")
			if tc.exact {
				if hashErr != nil || pricingErr != nil || costErr != nil || len(pricing) != 1 || len(cost) != 1 || state.EvidenceChargeUnits != tc.quota {
					t.Fatalf("integer quota no longer publishes: %v %v %v %+v", hashErr, pricingErr, costErr, state)
				}
			} else if hashErr == nil || pricingErr == nil || costErr == nil || len(pricing) != 0 || len(cost) != 0 || !strings.Contains(costErr.Error(), "精确整数") {
				t.Fatalf("fractional amount became publishable integer evidence: %v %v %v", hashErr, pricingErr, costErr)
			}
		})
	}
}
