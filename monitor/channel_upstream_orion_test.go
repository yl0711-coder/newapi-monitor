package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func orionFixtureServer(t *testing.T, me string, pages map[string]map[string]any, calls *[]string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer smk_fixture" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"status":401,"reason":"UNAUTHENTICATED","message":"echo smk_fixture"}`))
			return
		}
		switch r.URL.Path {
		case "/api/v1/console/me":
			_, _ = w.Write([]byte(me))
		case "/api/v1/console/me/requests":
			q := r.URL.Query()
			*calls = append(*calls, q.Get("cursor"))
			// Mirror the live panel: limit at most 100, no parameters it never sends.
			if q.Get("limit") != "100" || q.Has("outcome") || len(q) > 4 || q.Get("from") == "" || q.Get("to") == "" {
				t.Errorf("bad request query %s", r.URL.RawQuery)
			}
			page, ok := pages[q.Get("cursor")]
			if !ok {
				t.Errorf("unknown cursor %q", q.Get("cursor"))
				page = map[string]any{"items": []any{}}
			}
			_ = json.NewEncoder(w).Encode(page)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestOrionProfileReadsUserBalanceAndBindsIdentity(t *testing.T) {
	var calls []string
	for _, me := range []string{`{"user":{"id":42,"balance":12.5,"role":"holder"}}`, `{"id":42,"balance":"12.5"}`} {
		server := orionFixtureServer(t, me, nil, &calls)
		row := ChannelUpstreamAccount{BaseURL: server.URL}
		result, userID, err := readOrionProfile(context.Background(), server.Client(), row, orionCredential{AccessToken: "smk_fixture"})
		if err != nil || userID != 42 || result.BalanceUSD != 12.5 || result.BalanceUnit != 1 {
			t.Fatalf("%s: %+v id=%d err=%v", me, result, userID, err)
		}
		row.UserID = 7
		if _, _, err := readOrionProfile(context.Background(), server.Client(), row, orionCredential{AccessToken: "smk_fixture"}); err == nil {
			t.Fatal("a key for another account must be rejected")
		}
		server.Close()
	}
	server := orionFixtureServer(t, `{"user":{"id":42}}`, nil, &calls)
	defer server.Close()
	if _, _, err := readOrionProfile(context.Background(), server.Client(), ChannelUpstreamAccount{BaseURL: server.URL}, orionCredential{AccessToken: "smk_fixture"}); err == nil {
		t.Fatal("missing balance must not be read as zero")
	}
	_, _, err := readOrionProfile(context.Background(), server.Client(), ChannelUpstreamAccount{BaseURL: server.URL}, orionCredential{AccessToken: "smk_wrong"})
	var authErr *upstreamAuthError
	if !errors.As(err, &authErr) || strings.Contains(err.Error(), "smk_") {
		t.Fatalf("401 must be a sanitized auth error, got %v", err)
	}
}

func orionItem(id string, at time.Time, cost float64, in, out int64) map[string]any {
	return map[string]any{"request_id": id, "at": at.UTC().Format(time.RFC3339Nano), "input_tokens": in, "output_tokens": out, "cache_create_5m": 0, "cache_create_1h": 0, "cache_read": 10, "cost_billed": cost, "status": 200}
}

func TestOrionUsageFollowsCursorAndAggregatesHours(t *testing.T) {
	from := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)
	cursor := "c2"
	pages := map[string]map[string]any{
		"":   {"items": []any{orionItem("r1", from.Add(5*time.Minute), 0.25, 100, 50), orionItem("r2", from.Add(70*time.Minute), 1, 10, 5)}, "next_cursor": cursor},
		"c2": {"items": []any{orionItem("r3", from.Add(10*time.Minute), 0.5, 1, 1)}, "next_cursor": nil},
	}
	var calls []string
	server := orionFixtureServer(t, `{}`, pages, &calls)
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "modelrouter.best", BaseURL: server.URL, Provider: upstreamProviderOrion}
	result, err := fetchOrionUsageWindow(context.Background(), server.Client(), row, orionCredential{AccessToken: "smk_fixture"}, from.Unix(), from.Add(2*time.Hour).Unix(), newUpstreamUsageRequestPacer(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[1] != "c2" || len(result.Hours) != 2 || result.Adapter != upstreamUsageAdapterOrion {
		t.Fatalf("calls=%v result=%+v", calls, result)
	}
	h0, h1 := result.Hours[0], result.Hours[1]
	if h0.Requests != 2 || h0.CostUSD != 0.75 || h0.Quota != 0.75 || h0.Tokens != 100+50+10+1+1+10 || h0.UnitPerUSD != 1 {
		t.Fatalf("hour 0 = %+v", h0)
	}
	if h1.Requests != 1 || h1.CostUSD != 1 || h1.Tokens != 25 {
		t.Fatalf("hour 1 = %+v", h1)
	}
}

func TestOrionUsageRejectsIncompleteOrInconsistentPages(t *testing.T) {
	from := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)
	row := ChannelUpstreamAccount{Domain: "modelrouter.best", Provider: upstreamProviderOrion}
	cases := map[string]map[string]map[string]any{
		"out of range": {"": {"items": []any{orionItem("r1", from.Add(-time.Second), 1, 1, 1)}}},
		"after end":    {"": {"items": []any{orionItem("r1", from.Add(time.Hour+time.Second), 1, 1, 1)}}},
		"duplicate":    {"": {"items": []any{orionItem("r1", from, 1, 1, 1), orionItem("r1", from, 1, 1, 1)}}},
		"loop":         {"": {"items": []any{orionItem("r1", from, 1, 1, 1)}, "next_cursor": "a"}, "a": {"items": []any{orionItem("r2", from, 1, 1, 1)}, "next_cursor": "a"}},
		"no items":     {"": {"next_cursor": nil}},
		"no cost":      {"": {"items": []any{map[string]any{"request_id": "r1", "at": from.Format(time.RFC3339), "input_tokens": 1, "output_tokens": 1}}}},
	}
	for name, pages := range cases {
		var calls []string
		server := orionFixtureServer(t, `{}`, pages, &calls)
		row.BaseURL = server.URL
		if _, err := fetchOrionUsageWindow(context.Background(), server.Client(), row, orionCredential{AccessToken: "smk_fixture"}, from.Unix(), from.Add(time.Hour).Unix(), newUpstreamUsageRequestPacer(10, 0)); err == nil {
			t.Fatalf("%s: must not publish a partial or inconsistent window", name)
		}
		server.Close()
	}
	if _, err := fetchOrionUsageWindow(context.Background(), http.DefaultClient, row, orionCredential{AccessToken: "smk_fixture"}, from.Unix()+60, from.Add(time.Hour).Unix(), newUpstreamUsageRequestPacer(1, 0)); err == nil {
		t.Fatal("unaligned window must be rejected before any request")
	}
}

func TestOrionInputValidation(t *testing.T) {
	if err := validateOrionInput(&channelUpstreamSaveInput{AccessToken: "smk_abc"}); err != nil {
		t.Fatal(err)
	}
	for _, in := range []*channelUpstreamSaveInput{
		{AccessToken: "sk-model"}, {AccessToken: "Bearer smk_abc"}, {AccessToken: "smk_abc", Password: "p"},
		{AccessToken: "smk_abc", AccessKeyID: "AK"},
	} {
		if validateOrionInput(in) == nil {
			t.Fatalf("input %+v must be rejected", in)
		}
	}
}

// Busy Orion days must finish within the per-run page budget: history moves
// one closed hour at a time and the live tail catches up in bounded windows.
func TestOrionSyncPlanUsesBoundedWindows(t *testing.T) {
	now := time.Date(2026, 11, 9, 20, 30, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{Provider: upstreamProviderOrion, UsageBackfillCursor: today - 3*86400}
	plan := planUpstreamUsageSync(row, now, 30)
	if plan.backfillFrom != today-3*86400 || plan.backfillTo != plan.backfillFrom+3600 {
		t.Fatalf("history window [%d,%d)", plan.backfillFrom, plan.backfillTo)
	}
	if plan.tailFrom != today || plan.tailTo != today+orionTailMaxSeconds {
		t.Fatalf("first tail [%d,%d), want [%d,%d)", plan.tailFrom, plan.tailTo, today, today+orionTailMaxSeconds)
	}
	// Each catch-up run must move the watermark forward until it reaches now.
	for runs := 0; row.UsageDataUntil < now; runs++ {
		if runs > 10 {
			t.Fatalf("tail never caught up; watermark %d now %d", row.UsageDataUntil, now)
		}
		next := planUpstreamUsageSync(row, now, 30)
		if next.tailTo <= row.UsageDataUntil || next.tailFrom < today || next.tailTo-next.tailFrom > orionTailMaxSeconds {
			t.Fatalf("catch-up tail [%d,%d) after watermark %d", next.tailFrom, next.tailTo, row.UsageDataUntil)
		}
		row.UsageDataUntil = next.tailTo
	}
	row.UsageDataUntil = now - 600
	live := planUpstreamUsageSync(row, now, 30)
	if live.tailTo != now {
		t.Fatalf("caught-up tail must end now, got %d", live.tailTo)
	}
}

// If the panel treats "to" as inclusive, a record in the second that starts
// the next window is left to that window instead of failing this one.
func TestOrionUsageLeavesBoundaryRecordToNextWindow(t *testing.T) {
	from := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)
	pages := map[string]map[string]any{"": {"items": []any{
		orionItem("r1", from.Add(10*time.Minute), 1, 1, 1), orionItem("r2", from.Add(time.Hour+300*time.Millisecond), 5, 1, 1)}}}
	var calls []string
	server := orionFixtureServer(t, `{}`, pages, &calls)
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "modelrouter.best", BaseURL: server.URL, Provider: upstreamProviderOrion}
	result, err := fetchOrionUsageWindow(context.Background(), server.Client(), row, orionCredential{AccessToken: "smk_fixture"}, from.Unix(), from.Add(time.Hour).Unix(), newUpstreamUsageRequestPacer(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hours) != 1 || result.Hours[0].Requests != 1 || result.Hours[0].CostUSD != 1 {
		t.Fatalf("boundary record counted in the earlier window: %+v", result.Hours)
	}
}

// Only 401 means the key is invalid; a 403 (role, firewall) goes through the
// shared retry policy instead of isolating the account at once.
func TestOrionForbiddenIsNotAuthFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	defer server.Close()
	_, _, err := readOrionProfile(context.Background(), server.Client(), ChannelUpstreamAccount{BaseURL: server.URL}, orionCredential{AccessToken: "smk_fixture"})
	var authErr *upstreamAuthError
	var httpErr *upstreamHTTPError
	if err == nil || errors.As(err, &authErr) || !errors.As(err, &httpErr) || httpErr.Status != http.StatusForbidden {
		t.Fatalf("403 must stay an HTTP 403, got %v", err)
	}
}

// The panel answers a bad query with HTTP 400 and a JSON reason. Keep a short
// message for status, never the key, and keep 400 out of auth isolation.
func TestOrionRejectedQueryKeepsSafeMessage(t *testing.T) {
	cases := map[string]string{
		`{"status":400,"reason":"INVALID_ARGUMENT","message":"limit must be between 1 and 100"}`: "limit must be between 1 and 100",
		`{"status":400,"reason":"INVALID_ARGUMENT","message":"bad token smk_fixture"}`:           "",
	}
	for body, want := range cases {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(body))
		}))
		row := ChannelUpstreamAccount{Domain: "modelrouter.best", BaseURL: server.URL, Provider: upstreamProviderOrion}
		from := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC).Unix()
		_, err := fetchOrionUsageWindow(context.Background(), server.Client(), row, orionCredential{AccessToken: "smk_fixture"}, from, from+3600, newUpstreamUsageRequestPacer(10, 0))
		server.Close()
		var httpErr *upstreamHTTPError
		var authErr *upstreamAuthError
		if !errors.As(err, &httpErr) || httpErr.Status != http.StatusBadRequest || errors.As(err, &authErr) {
			t.Fatalf("%s: want a plain 400, got %v", body, err)
		}
		if httpErr.Message != want || strings.Contains(err.Error(), "smk_") {
			t.Fatalf("%s: message %q, want %q", body, httpErr.Message, want)
		}
	}
}

// A failed, unbilled row is not usage; a billed row counts whatever its status.
func TestOrionUsageSkipsUnbilledFailedRequests(t *testing.T) {
	from := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)
	failed := orionItem("r2", from.Add(2*time.Minute), 0, 0, 0)
	failed["status"] = 502
	billedFailed := orionItem("r3", from.Add(3*time.Minute), 0.5, 1, 1)
	billedFailed["status"] = 499
	pages := map[string]map[string]any{"": {"items": []any{orionItem("r1", from.Add(time.Minute), 1, 1, 1), failed, billedFailed}}}
	var calls []string
	server := orionFixtureServer(t, `{}`, pages, &calls)
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "modelrouter.best", BaseURL: server.URL, Provider: upstreamProviderOrion}
	result, err := fetchOrionUsageWindow(context.Background(), server.Client(), row, orionCredential{AccessToken: "smk_fixture"}, from.Unix(), from.Add(time.Hour).Unix(), newUpstreamUsageRequestPacer(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if h := result.Hours[0]; h.Requests != 2 || h.CostUSD != 1.5 {
		t.Fatalf("hour = %+v", h)
	}
}

// Orion re-reads one closed hour, not the generic three, and a caught-up tail
// at peak volume stays within one run's page budget.
func TestOrionTailOverlapIsOneHour(t *testing.T) {
	now := time.Date(2026, 11, 9, 20, 30, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{Provider: upstreamProviderOrion, UsageDataUntil: now - 600}
	plan := planUpstreamUsageSync(row, now, 30)
	wantFrom := row.UsageDataUntil - orionTailOverlapSeconds
	wantFrom -= wantFrom % 3600
	if plan.tailFrom != wantFrom || plan.tailTo != now {
		t.Fatalf("tail [%d,%d), want [%d,%d)", plan.tailFrom, plan.tailTo, wantFrom, now)
	}
	const peakRequestsPerHour = 1600
	if pages := (orionTailMaxSeconds / 3600) * peakRequestsPerHour / orionPageSize; pages > orionMaxPages {
		t.Fatalf("a full tail window needs %d pages at peak, budget %d", pages, orionMaxPages)
	}
	newAPI := row
	newAPI.Provider = upstreamProviderNewAPI
	if generic := planUpstreamUsageSync(newAPI, now, 30); generic.tailFrom >= plan.tailFrom {
		t.Fatalf("other providers keep the three-hour overlap: %d vs %d", generic.tailFrom, plan.tailFrom)
	}
}

// The panel's times are UTC; accept them with or without an explicit offset.
func TestParseOrionTimeAcceptsUTCWithOrWithoutZone(t *testing.T) {
	want := time.Date(2026, 11, 13, 9, 59, 49, 744208000, time.UTC)
	for _, raw := range []string{"2026-11-13T09:59:49.744208Z", "2026-11-13T09:59:49.744208+00:00", "2026-11-13T17:59:49.744208+08:00", "2026-11-13T09:59:49.744208"} {
		got, err := parseOrionTime(raw)
		if err != nil || !got.Equal(want) {
			t.Fatalf("%s: got %v err %v", raw, got, err)
		}
	}
	for _, bad := range []string{"", "11/13/2026 09:59", "2026-11-13 09:59:49"} {
		if _, err := parseOrionTime(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}
