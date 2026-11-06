package monthlysource

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCapturedTokenValidatedPlanReusePreservesEveryOriginalAndMoney(t *testing.T) {
	db, _, key, scope, plan, expected := retainedTokenMonthCapture(t)
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	originals, report, err := LoadCapturedTokenMonth(t.Context(), db, key, scope, 201)
	if err != nil || !reflect.DeepEqual(report, expected) {
		t.Fatal("independent public original reader failed", err)
	}
	if err := confirmFundingCapturePlan(t.Context(), db, key, raw); err != nil {
		t.Fatal(err)
	}
	pairs, reused, err := loadCapturedTokenMonthPlan(t.Context(), db, plan, 201)
	if err != nil || !reflect.DeepEqual(originals, pairs) || !reflect.DeepEqual(report, reused) {
		t.Fatal("plan reuse changed original bytes or exact money", err)
	}
	after, err := json.Marshal(plan)
	if err != nil || string(after) != string(raw) {
		t.Fatal("account normalization mutated the pinned plan", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := confirmFundingCapturePlan(ctx, db, key, raw); err == nil {
		t.Fatal("cancelled plan reuse was accepted")
	}
}

func TestCapturedTokenPlanReuseRejectsDigestAndByteChanges(t *testing.T) {
	for _, scenario := range []string{"indexed_digest", "noncanonical_bytes", "valid_changed_plan"} {
		t.Run(scenario, func(t *testing.T) {
			db, _, key, scope, plan, _ := retainedTokenMonthCapture(t)
			raw, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			if err := confirmFundingCapturePlan(t.Context(), db, key, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("DROP TRIGGER monthly_funding_capture_plan_no_update"); err != nil {
				t.Fatal(err)
			}
			changed := append([]byte(nil), raw...)
			sha := digest(changed)
			switch scenario {
			case "indexed_digest":
				sha = strings.Repeat("0", 64)
			case "noncanonical_bytes":
				changed = append(changed, ' ')
				sha = digest(changed)
			case "valid_changed_plan":
				other := plan
				other.Scans = append([]FundingScan(nil), plan.Scans...)
				half := len(other.Scans) / 2
				other.Scans[0], other.Scans[half] = other.Scans[half], other.Scans[0]
				changed, err = json.Marshal(other)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := decodeFundingMonthCapturePlan(changed, key, scope, time.Now()); err != nil {
					t.Fatal("fixture must be valid but DIFFERENT from the pinned plan", err)
				}
				sha = digest(changed)
			}
			if _, err := db.Exec("UPDATE portal_monthly_funding_capture_plans SET payload=?,sha256=? WHERE key=?", changed, sha, key); err != nil {
				t.Fatal(err)
			}
			if err := confirmFundingCapturePlan(t.Context(), db, key, raw); !errors.Is(err, ErrFundingMonthCaptureIntegrity) {
				t.Fatal("changed plan reused a cached decoding", scenario, err)
			}
		})
	}
}
