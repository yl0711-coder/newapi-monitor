package monthlysource

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func tokenSegmentFixture(t *testing.T, month string, n int) ([]byte, FundingMonthCapturePlan) {
	t.Helper()
	ids := make([]int64, n)
	var members strings.Builder
	for i := range ids {
		ids[i] = int64(201 + i)
		fmt.Fprintf(&members, "%d,", ids[i])
	}
	scope := OrganizationMonthScope{Version: 1, Mode: OrganizationMonthScopeMode, GroupID: 1, AuthVersion: 1, MemberFingerprint: digest([]byte(members.String()))[:32], MemberManifest: strings.Repeat("b", 64), Funding: FundingMonthRequest{Month: month, SourceIdentity: strings.Repeat("a", 64), Rules: fundingTestRules(), UserIDs: ids}}
	plan := FundingMonthCapturePlan{Version: 1, Key: digest([]byte(month + members.String())), Scope: scope}
	start, err := time.ParseInLocation("2006-01", month, fundingMonthZone)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		for at := start; at.Before(start.AddDate(0, 1, 0)); at = at.AddDate(0, 0, 1) {
			s, err := NewFundingDayScan(scope.Funding.SourceIdentity, scope.Funding.Rules, ids, at.Unix(), at.AddDate(0, 0, 1).Unix())
			if err != nil {
				t.Fatal(err)
			}
			plan.Scans = append(plan.Scans, s)
		}
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return raw, plan
}

func TestTokenSegmentsExactCoverageForCalendarAndLargeRoster(t *testing.T) {
	for _, month := range []string{"2026-02", "2024-02", "2026-09", "2026-05"} {
		for _, members := range []int{1, 10, 50, 66, 67, 200} {
			t.Run(fmt.Sprintf("%s_%d", month, members), func(t *testing.T) {
				raw, plan := tokenSegmentFixture(t, month, members)
				before := append([]byte(nil), raw...)
				got, err := PlanTokenCaptureSegments(t.Context(), raw, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if got.TotalSlots != len(plan.Scans)*members || got.PlanSHA256 != digest(raw) {
					t.Fatal("incorrect exact work size")
				}
				cursor := 0
				seen := map[string]bool{}
				for i, s := range got.Segments {
					if s.Ordinal != i || s.FromOrdinal != cursor || s.ToOrdinal <= s.FromOrdinal || s.ToOrdinal-s.FromOrdinal > 4096 || s.Phase != s.FromOrdinal/(got.TotalSlots/2) || (s.ToOrdinal-1)/(got.TotalSlots/2) != s.Phase || !validDigest(s.SHA256) || seen[s.SHA256] {
						t.Fatal("gap/overlap/phase/size drift", s)
					}
					seen[s.SHA256] = true
					// Every original request remains exactly its original account/day,
					// not a divided organization or invented aggregate observation.
					for ordinal := s.FromOrdinal; ordinal < s.ToOrdinal; ordinal++ {
						r, err := tokenCaptureRequest(plan, ordinal)
						if err != nil {
							t.Fatal(err)
						}
						if r.UserID != plan.Scope.Funding.UserIDs[ordinal%members] || r.SourceIdentity != plan.Scope.Funding.SourceIdentity || r.To-r.From != 86400 {
							t.Fatal("request membership changed")
						}
					}
					cursor = s.ToOrdinal
				}
				if cursor != got.TotalSlots || !reflect.DeepEqual(before, raw) {
					t.Fatal("missing slots or modified original")
				}
				if members == 200 && month == "2026-05" && (got.TotalSlots != 12400 || len(got.Segments) != 4) {
					t.Fatal("large work not partitioned", got.TotalSlots, len(got.Segments))
				}
				if err := ValidateTokenCaptureSegments(t.Context(), raw, got, time.Now()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestTokenSegmentsRejectTamperingAndKeepAuthorityBinding(t *testing.T) {
	raw, _ := tokenSegmentFixture(t, "2026-05", 200)
	for _, kind := range []string{"missing", "duplicate", "overlap", "phase", "hash", "scope", "job", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			p, err := PlanTokenCaptureSegments(t.Context(), raw, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing":
				p.Segments = p.Segments[:len(p.Segments)-1]
			case "duplicate":
				p.Segments[1] = p.Segments[0]
			case "overlap":
				p.Segments[1].FromOrdinal--
			case "phase":
				p.Segments[1].Phase = 1
			case "hash":
				p.Segments[0].SHA256 = digest([]byte("forged"))
			case "scope":
				p.ScopeSHA256 = digest([]byte("other complete roster"))
			case "job":
				p.JobKey = digest([]byte("other task"))
			case "oversize":
				p.Segments[0].ToOrdinal = 4097
			}
			if err := ValidateTokenCaptureSegments(t.Context(), raw, p, time.Now()); err == nil {
				t.Fatal("forged schedule accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := PlanTokenCaptureSegments(ctx, raw, time.Now()); err == nil {
		t.Fatal("canceled planning accepted")
	}
	if _, err := PlanTokenCaptureSegments(nil, raw, time.Now()); err == nil { //nolint:staticcheck // SA1012: explicitly test nil-context rejection.
		t.Fatal("nil context accepted")
	}
	if _, err := PlanTokenCaptureSegments(t.Context(), append(raw, ' '), time.Now()); err == nil {
		t.Fatal("noncanonical original accepted")
	}
	if _, err := PlanTokenCaptureSegments(t.Context(), raw, time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("open month accepted")
	}
}
