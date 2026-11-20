package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

// The pre-route rejection lane feeds rejection_samples in production. Its
// CloudWatch query once shared a switch branch with the retired Shadow
// reconciliation; this pins the exact query text so that removing Shadow (or
// any later refactor) cannot change what production asks CloudWatch for.
func TestPreRouteRejectQueryTextIsPinned(t *testing.T) {
	from := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	source, query, limit, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{
		Kind: cwQueryWorkerPreRouteReject, From: from, To: from.Add(time.Hour), Limit: cloudWatchLogsPreRouteLimit,
	})
	if err != nil || source.ID != cwSourceWorkerNewAPI || limit != cloudWatchLogsPreRouteLimit {
		t.Fatalf("source=%+v limit=%d err=%v", source, limit, err)
	}
	sum := sha256.Sum256([]byte(query))
	const want = "c09a8d3681fd907688da7c72b1c26db40e31d34f71eedb762befff1af554ac6a"
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("pre-route rejection query changed: sha256=%s\n%s", got, query)
	}
}

func TestCanonicalRejectionReasonClosedSet(t *testing.T) {
	cases := map[string]string{
		"no_channel": "route_no_channel", "No Available Channel": "route_no_channel", "route-no-channel": "route_no_channel", "noavailablechannel": "route_no_channel",
		"invalid_token": "invalid_token", "token_invalid": "invalid_token",
		"token_disabled": "token_disabled", "disabled_token": "token_disabled",
		"model_forbidden": "model_forbidden", "token_model_forbidden": "model_forbidden",
		"model_not_found": "model_not_found", "unknown_model": "model_not_found",
		"quota_account": "quota_account", "quota_insufficient": "quota_account", "insufficient_quota": "quota_account",
		"user_quota_insufficient": "quota_account", "token_quota_insufficient": "quota_account", "pre_consume_failed": "quota_account",
		"rate_limited": "rate_limited", "rate.limit": "rate_limited", "too many requests": "rate_limited",
		"  Upstream_5xx ": "upstream_5xx",
	}
	for in, want := range cases {
		if got := canonicalRejectionReason(in); got != want {
			t.Fatalf("canonicalRejectionReason(%q)=%q want %q", in, got, want)
		}
	}
	for _, in := range []string{"route_no_channel", "invalid_token", "token_disabled", "model_forbidden", "model_not_found", "pre_consume_failed", "too_many_requests"} {
		if !isPreRouteRejectionCategory(in) {
			t.Fatalf("%q must be a pre-route rejection category", in)
		}
	}
	for _, in := range []string{"upstream_5xx", "timeout", ""} {
		if isPreRouteRejectionCategory(in) {
			t.Fatalf("%q must not be a pre-route rejection category", in)
		}
	}
}

func TestLastUpstreamStatus(t *testing.T) {
	if got := lastUpstreamStatus(nil); got != 0 {
		t.Fatalf("empty=%d", got)
	}
	if got := lastUpstreamStatus([]int{502, 504, 200}); got != 200 {
		t.Fatalf("last=%d", got)
	}
}
