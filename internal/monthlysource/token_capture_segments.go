package monthlysource

// Scheduling metadata only. Segments do NOT certify captured work, change the
// full organization scope, reset retries or relax independent observations.
import (
	"bytes"
	"context"
	"encoding/json"
	"time"
)

const tokenCaptureSegmentSlots = 4096

type TokenCaptureSegment struct {
	Ordinal     int    `json:"ordinal"`
	Phase       int    `json:"phase"`
	FromOrdinal int    `json:"from_ordinal"`
	ToOrdinal   int    `json:"to_ordinal_exclusive"`
	SHA256      string `json:"sha256"`
}

type TokenCaptureSegments struct {
	Version     int                   `json:"version"`
	Mode        string                `json:"mode"`
	JobKey      string                `json:"job_key"`
	PlanSHA256  string                `json:"plan_sha256"`
	ScopeSHA256 string                `json:"scope_sha256"`
	TotalSlots  int                   `json:"total_slots"`
	Segments    []TokenCaptureSegment `json:"segments"`
}

// PlanTokenCaptureSegments takes the ORIGINAL immutable month-plan bytes, not
// client dates/rosters or a replacement child scope. It opens no connection and
// creates no table/task. Funding, wallets, splitting and final indexing remain
// distinct budgets; TotalSlots counts baseline TOKEN observations only.
func PlanTokenCaptureSegments(parent context.Context, raw []byte, now time.Time) (TokenCaptureSegments, error) {
	var empty TokenCaptureSegments
	if parent == nil {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	if err := parent.Err(); err != nil {
		return empty, err
	}
	if len(raw) == 0 || len(raw) > fundingCapturePlanLimit {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	var original FundingMonthCapturePlan
	if json.Unmarshal(raw, &original) != nil {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	plan, err := decodeFundingMonthCapturePlan(raw, original.Key, original.Scope, now)
	if err != nil {
		return empty, err
	}
	count := len(plan.Scans) * len(plan.Scope.Funding.UserIDs)
	if count < 2 || count > 12400 || count%2 != 0 {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	scope, err := canonicalFundingCaptureScope(plan.Scope, now)
	if err != nil {
		return empty, err
	}
	out := TokenCaptureSegments{Version: 1, Mode: "bounded_original_token_schedule_not_captured_work_or_bill", JobKey: plan.Key, PlanSHA256: digest(raw), ScopeSHA256: digest(scope), TotalSlots: count, Segments: []TokenCaptureSegment{}}
	// Never make a segment straddle the two independent sweeps. The real
	// scheduler must STILL enforce the existing whole-first-sweep 5-minute gap.
	for phase := 0; phase < 2; phase++ {
		for from := phase * (count / 2); from < (phase+1)*(count/2); {
			if err := parent.Err(); err != nil {
				return empty, err
			}
			to := min(from+tokenCaptureSegmentSlots, (phase+1)*(count/2))
			segment := TokenCaptureSegment{Ordinal: len(out.Segments), Phase: phase, FromOrdinal: from, ToOrdinal: to}
			identity, _ := json.Marshal(struct {
				Plan    string              `json:"plan_sha256"`
				Segment TokenCaptureSegment `json:"segment"`
			}{out.PlanSHA256, segment})
			segment.SHA256 = digest(identity)
			out.Segments = append(out.Segments, segment)
			from = to
		}
	}
	return out, nil
}

// Recompute boundaries/identity from original bytes. A syntactically valid
// forged hash or reordered, overlapping, missing or duplicated segment cannot
// be used as a schedule. This is NOT permission to claim from a new task budget.
func ValidateTokenCaptureSegments(parent context.Context, raw []byte, got TokenCaptureSegments, now time.Time) error {
	want, err := PlanTokenCaptureSegments(parent, raw, now)
	if err != nil {
		return err
	}
	a, err := json.Marshal(want)
	if err != nil {
		return err
	}
	b, err := json.Marshal(got)
	if err != nil {
		return err
	}
	if !bytes.Equal(a, b) {
		return ErrTokenMonthCaptureIntegrity
	}
	return nil
}
