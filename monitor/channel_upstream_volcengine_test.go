package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Reference produced by github.com/volcengine/volc-sdk-golang v1.0.23
// base.Credentials.Sign for the same request, credentials and X-Date.
func TestVolcengineSignatureMatchesOfficialSDK(t *testing.T) {
	body := []byte(`{"BillPeriod":"2026-11","ExpenseDate":"2026-11-08","GroupPeriod":1,"Limit":100,"NeedRecordNum":1,"Offset":0}`)
	req, _ := http.NewRequest(http.MethodPost, "https://open.volcengineapi.com/?Action=ListBillDetail&Version=2022-01-01", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	at := time.Date(2026, 11, 9, 12, 46, 36, 0, time.UTC)
	signVolcengineRequest(req, body, volcengineCredential{AccessKeyID: "AKLTfixtureAccessKey", SecretAccessKey: "fixtureSecretKey=="}, "cn-beijing", "billing", at)
	if got := req.Header.Get("X-Content-Sha256"); got != "6cff94718fd7924d01bf41ec2037ff72f17ca7f72d339a805852f38280f25742" {
		t.Fatalf("payload hash = %s", got)
	}
	want := "HMAC-SHA256 Credential=AKLTfixtureAccessKey/20261109/cn-beijing/billing/request, SignedHeaders=content-type;host;x-content-sha256;x-date, Signature=4c9d2fc8cb1aa08dd30390b5196e07e2dc57336e5762335cf070c8adb66983af"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("authorization mismatch\n got %s\nwant %s", got, want)
	}
}

type volcengineFixture struct {
	t        *testing.T
	balance  string
	currency string
	days     map[string][]volcengineBillItem
	calls    []string
	errCode  string
	status   int
	offset   string // "" echo, "omit" leave out, "wrong" echo a different value
}

func (f *volcengineFixture) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action := r.URL.Query().Get("Action")
		f.calls = append(f.calls, action)
		if r.Method != http.MethodPost || r.URL.Query().Get("Version") != volcengineBillingVersion ||
			!strings.HasPrefix(r.Header.Get("Authorization"), "HMAC-SHA256 Credential=AKfixture/") || r.Header.Get("X-Date") == "" {
			f.t.Errorf("unsigned or malformed request: %s %s", r.Method, r.URL.RawQuery)
		}
		raw, _ := io.ReadAll(r.Body)
		if f.errCode != "" {
			w.WriteHeader(f.status)
			_, _ = w.Write([]byte(`{"ResponseMetadata":{"RequestId":"r","Error":{"Code":"` + f.errCode + `","Message":"echo AKfixture SKfixture"}}}`))
			return
		}
		var result any
		switch action {
		case "QueryBalanceAcct":
			result = map[string]any{"AccountID": 2100001234, "AvailableBalance": f.balance, "CashBalance": f.balance, "Currency": f.currency}
		case "ListBillDetail":
			var in struct {
				ExpenseDate string
				Offset      int
				Limit       int
				GroupPeriod int
			}
			if json.Unmarshal(raw, &in) != nil || in.GroupPeriod != 1 || in.Limit != volcengineBillPageSize {
				f.t.Errorf("bad bill request body %s", raw)
			}
			rows := f.days[in.ExpenseDate]
			end := in.Offset + in.Limit
			if end > len(rows) {
				end = len(rows)
			}
			page := []volcengineBillItem{}
			if in.Offset < len(rows) {
				page = rows[in.Offset:end]
			}
			res := map[string]any{"List": page, "Total": len(rows), "Offset": in.Offset, "Limit": in.Limit}
			switch f.offset {
			case "omit":
				delete(res, "Offset")
			case "wrong":
				res["Offset"] = in.Offset + 1
			}
			result = res
		default:
			f.t.Errorf("unexpected action %s", action)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ResponseMetadata": map[string]any{"RequestId": "r"}, "Result": result})
	}))
}

func withVolcengineFixture(t *testing.T, f *volcengineFixture, now time.Time) *httptest.Server {
	server := f.server()
	oldEndpoint, oldNow := volcengineBillingEndpoint, volcengineNow
	volcengineBillingEndpoint, volcengineNow = server.URL+"/", func() time.Time { return now }
	t.Cleanup(func() { volcengineBillingEndpoint, volcengineNow = oldEndpoint, oldNow; server.Close() })
	return server
}

var volcFixtureCred = volcengineCredential{AccessKeyID: "AKfixture", SecretAccessKey: "SKfixture"}

func TestVolcengineBalanceIsCNYFaceValue(t *testing.T) {
	f := &volcengineFixture{t: t, balance: "1234.56", currency: "CNY"}
	server := withVolcengineFixture(t, f, time.Now())
	result, accountID, err := readVolcengineBalance(context.Background(), server.Client(), volcFixtureCred)
	if err != nil || accountID != 2100001234 || result.BalanceUSD != 1234.56 || result.BalanceRaw != 1234.56 || result.BalanceUnit != 1 {
		t.Fatalf("balance = %+v id=%d err=%v", result, accountID, err)
	}
	f.currency = "USD"
	if _, _, err := readVolcengineBalance(context.Background(), server.Client(), volcFixtureCred); err == nil {
		t.Fatal("non-CNY balance must not be published")
	}
}

func TestVolcengineAuthErrorIsReconnectAndHidesMessage(t *testing.T) {
	f := &volcengineFixture{t: t, errCode: "SignatureDoesNotMatch", status: http.StatusUnauthorized}
	server := withVolcengineFixture(t, f, time.Now())
	_, _, err := readVolcengineBalance(context.Background(), server.Client(), volcFixtureCred)
	var authErr *upstreamAuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("want auth error, got %v", err)
	}
	if strings.Contains(err.Error(), "AKfixture") || strings.Contains(err.Error(), "SKfixture") || !strings.Contains(err.Error(), "SignatureDoesNotMatch") {
		t.Fatalf("error must keep the code only: %v", err)
	}
	f.errCode, f.status = "InternalError", http.StatusInternalServerError
	_, _, err = readVolcengineBalance(context.Background(), server.Client(), volcFixtureCred)
	var httpErr *upstreamHTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != 500 || errors.As(err, &authErr) {
		t.Fatalf("want retryable 500, got %v", err)
	}
}

func TestVolcengineUsageWindowSumsPagedDaysAndMarksRecentProvisional(t *testing.T) {
	day1 := time.Date(2026, 11, 7, 0, 0, 0, 0, cstLocation)
	rows := make([]volcengineBillItem, 0, 150)
	for i := 0; i < 150; i++ {
		rows = append(rows, volcengineBillItem{BillDetailID: "d" + string(rune('A'+i/26)) + string(rune('a'+i%26)), ExpenseDate: "2026-11-07", PayableAmount: "0.10", Currency: "CNY", Count: "2", Unit: "千tokens"})
	}
	rows = append(rows, volcengineBillItem{BillDetailID: "storage", ExpenseDate: "2026-11-07", PayableAmount: "5", Currency: "CNY", Count: "3", Unit: "GB"})
	f := &volcengineFixture{t: t, days: map[string][]volcengineBillItem{
		"2026-11-07": rows,
		"2026-11-08": {{BillDetailID: "x", ExpenseDate: "2026-11-08", PayableAmount: "1.5", Currency: "CNY", Count: "1000", Unit: "tokens"}},
	}}
	now := day1.Add(2*24*time.Hour + 3*time.Hour) // 2026-11-09 03:00 CST
	server := withVolcengineFixture(t, f, now.UTC())
	row := ChannelUpstreamAccount{Domain: "volces.com", Provider: upstreamProviderVolcengine}
	from := day1.Unix()
	result, err := fetchVolcengineUsageWindow(context.Background(), server.Client(), row, volcFixtureCred, from, now.Unix(), newUpstreamUsageRequestPacer(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hours) != 3 || result.Adapter != upstreamUsageAdapterVolcengine || result.DataUntil != now.Unix() {
		t.Fatalf("result = %+v", result)
	}
	d7, d8, d9 := result.Hours[0], result.Hours[1], result.Hours[2]
	if d7.HourTs != from || d7.BucketSeconds != 86400 || d7.CostUSD != 20 || d7.Quota != 20 || d7.Tokens != 300000 || d7.Requests != 0 || d7.UnitPerUSD != 1 || d7.Provisional {
		t.Fatalf("day 7 = %+v", d7)
	}
	if d8.CostUSD != 1.5 || d8.Tokens != 1000 || !d8.Provisional {
		t.Fatalf("day 8 must be provisional until 24h after it ends: %+v", d8)
	}
	if d9.HourTs != from+2*86400 || d9.BucketSeconds != 3*3600 || d9.CostUSD != 0 || !d9.Provisional {
		t.Fatalf("partial today = %+v", d9)
	}
	if got := strings.Count(strings.Join(f.calls, ","), "ListBillDetail"); got != 4 {
		t.Fatalf("want 2 pages + 1 + 1 bill calls, got %v", f.calls)
	}
}

func TestVolcengineUsageRejectsBadWindowsAndRows(t *testing.T) {
	day := time.Date(2026, 11, 7, 0, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{Domain: "volces.com", Provider: upstreamProviderVolcengine}
	for _, window := range [][2]int64{{day + 3600, day + 7200}, {day, day + 4*86400}, {day, day}} {
		if _, err := fetchVolcengineUsageWindow(context.Background(), http.DefaultClient, row, volcFixtureCred, window[0], window[1], newUpstreamUsageRequestPacer(5, 0)); err == nil {
			t.Fatalf("window %v must be rejected", window)
		}
	}
	for name, item := range map[string]volcengineBillItem{
		"currency": {BillDetailID: "a", ExpenseDate: "2026-11-07", PayableAmount: "1", Currency: "USD"},
		"date":     {BillDetailID: "a", ExpenseDate: "2026-11-06", PayableAmount: "1", Currency: "CNY"},
		"amount":   {BillDetailID: "a", ExpenseDate: "2026-11-07", PayableAmount: "", Currency: "CNY"},
	} {
		f := &volcengineFixture{t: t, days: map[string][]volcengineBillItem{"2026-11-07": {item}}}
		server := withVolcengineFixture(t, f, time.Unix(day+10*86400, 0))
		if _, err := fetchVolcengineUsageWindow(context.Background(), server.Client(), row, volcFixtureCred, day, day+86400, newUpstreamUsageRequestPacer(5, 0)); err == nil {
			t.Fatalf("%s: invalid bill row must not be published", name)
		}
	}
	dup := volcengineBillItem{BillDetailID: "same", ExpenseDate: "2026-11-07", PayableAmount: "1", Currency: "CNY"}
	f := &volcengineFixture{t: t, days: map[string][]volcengineBillItem{"2026-11-07": {dup, dup}}}
	server := withVolcengineFixture(t, f, time.Unix(day+10*86400, 0))
	if _, err := fetchVolcengineUsageWindow(context.Background(), server.Client(), row, volcFixtureCred, day, day+86400, newUpstreamUsageRequestPacer(5, 0)); err == nil {
		t.Fatal("duplicate bill rows must not be summed")
	}
}

func TestVolcengineTailRereadsRecentWholeDays(t *testing.T) {
	now := time.Date(2026, 11, 9, 10, 17, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{Provider: upstreamProviderVolcengine, UsageDataUntil: now - 600, UsageBackfillDone: true}
	plan := planUpstreamUsageSync(row, now, 30)
	if plan.tailFrom != today-2*86400 || plan.tailTo != now {
		t.Fatalf("tail = %d..%d, want %d..%d", plan.tailFrom, plan.tailTo, today-2*86400, now)
	}
	if upstreamUsageGranularity(upstreamProviderVolcengine, upstreamUsageAdapterVolcengine) != "day" {
		t.Fatal("volcengine usage must be shown as daily")
	}
}

func TestVolcengineInputAndSecrets(t *testing.T) {
	ok := &channelUpstreamSaveInput{AccessKeyID: "AK", SecretAccessKey: "SK"}
	if err := validateVolcengineInput(ok); err != nil {
		t.Fatal(err)
	}
	for _, in := range []*channelUpstreamSaveInput{
		{AccessKeyID: "AK"},
		{AccessKeyID: "AK", SecretAccessKey: "S K"},
		{AccessKeyID: "AK", SecretAccessKey: "SK", APIKey: "sk-model"},
		{AccessToken: "token"},
	} {
		if validateVolcengineInput(in) == nil {
			t.Fatalf("input %+v must be rejected", in)
		}
	}
	secrets := upstreamCredentialSecrets(volcFixtureCred)
	if len(secrets) != 2 || secrets[0] != "AKfixture" || secrets[1] != "SKfixture" {
		t.Fatalf("secrets = %v", secrets)
	}
	if !upstreamProviderUsesCNYLedger(upstreamProviderVolcengine) || upstreamProviderUsesCNYLedger(upstreamProviderOrion) {
		t.Fatal("only CNY ledger providers use face value")
	}
}

// A day still provisional after leaving the tail window (every read on its
// last re-read day failed) is re-read once more through the history lane.
func TestVolcengineStuckProvisionalDayIsReread(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Date(2026, 11, 9, 10, 17, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	stuck := today - 4*86400
	if err := m.storeDB.Create(&[]ChannelUpstreamUsageHour{
		{Domain: "volc.test", HourTs: stuck - 86400, BucketSeconds: 86400, Provider: upstreamProviderVolcengine, SourceKind: upstreamUsageAdapterVolcengine},
		{Domain: "volc.test", HourTs: stuck, BucketSeconds: 86400, Provider: upstreamProviderVolcengine, SourceKind: upstreamUsageAdapterVolcengine, Provisional: true},
		{Domain: "volc.test", HourTs: today - 86400, BucketSeconds: 86400, Provider: upstreamProviderVolcengine, SourceKind: upstreamUsageAdapterVolcengine, Provisional: true},
	}).Error; err != nil {
		t.Fatal(err)
	}
	row := ChannelUpstreamAccount{Domain: "volc.test", Provider: upstreamProviderVolcengine, UsageDataUntil: now - 600, UsageBackfillDone: true}
	plan, err := m.prioritizeVolcengineSettlement(context.Background(), row, now, planUpstreamUsageSync(row, now, 30))
	if err != nil {
		t.Fatal(err)
	}
	if plan.backfillFrom != stuck || plan.backfillTo != stuck+86400 || !plan.recentOnly {
		t.Fatalf("stuck day not re-read: %+v", plan)
	}
	row.UsageBackfillNextSyncAt = now + 60 // history backoff is respected
	if plan, _ := m.prioritizeVolcengineSettlement(context.Background(), row, now, planUpstreamUsageSync(row, now, 30)); plan.backfillTo > plan.backfillFrom {
		t.Fatalf("history backoff ignored: %+v", plan)
	}
	row.UsageBackfillNextSyncAt = 0
	m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ? AND hour_ts = ?", "volc.test", stuck).Update("provisional", false)
	if plan, _ := m.prioritizeVolcengineSettlement(context.Background(), row, now, planUpstreamUsageSync(row, now, 30)); plan.backfillTo > plan.backfillFrom {
		t.Fatalf("settled days must not be re-read: %+v", plan)
	}
}

// A bare 403 is not a credential failure (shared 403 policy applies); the
// documented permission codes still ask the user to fix the sub-user.
func TestVolcengineForbiddenWithoutAuthCodeIsNotAuthFailure(t *testing.T) {
	f := &volcengineFixture{t: t, errCode: "RequestBlocked", status: http.StatusForbidden}
	server := withVolcengineFixture(t, f, time.Now())
	_, _, err := readVolcengineBalance(context.Background(), server.Client(), volcFixtureCred)
	var authErr *upstreamAuthError
	var httpErr *upstreamHTTPError
	if errors.As(err, &authErr) || !errors.As(err, &httpErr) || httpErr.Status != http.StatusForbidden {
		t.Fatalf("bare 403 must stay an HTTP 403, got %v", err)
	}
	f.errCode = "AccessDenied"
	if _, _, err := readVolcengineBalance(context.Background(), server.Client(), volcFixtureCred); !errors.As(err, &authErr) {
		t.Fatalf("AccessDenied must ask for reconnection, got %v", err)
	}
}

// The Offset echo is optional; when present it must match the request.
func TestVolcengineBillPageOffsetEchoOptional(t *testing.T) {
	day := time.Date(2026, 11, 5, 0, 0, 0, 0, cstLocation)
	f := &volcengineFixture{t: t, days: map[string][]volcengineBillItem{}, offset: "omit"}
	server := withVolcengineFixture(t, f, time.Now())
	if _, err := fetchVolcengineBillPage(context.Background(), server.Client(), volcFixtureCred, day, 0, newUpstreamUsageRequestPacer(10, 0)); err != nil {
		t.Fatalf("missing Offset echo must be accepted: %v", err)
	}
	f.offset = "wrong"
	if _, err := fetchVolcengineBillPage(context.Background(), server.Client(), volcFixtureCred, day, 0, newUpstreamUsageRequestPacer(10, 0)); err == nil {
		t.Fatal("a mismatched Offset echo must be rejected")
	}
}
