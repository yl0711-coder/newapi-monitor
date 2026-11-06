package monitor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Explicit opt-in: TEST approvals only, existing retained files read-only,
// synthetic credentials and a new temporary database. Never a production bill.
func TestMonthlyReviewRetainedLocalJourney(t *testing.T) {
	keys := []string{"MONTHLY_REVIEW_INPUT", "MONTHLY_REVIEW_CONTEXT", "MONTHLY_CANDIDATE_BUNDLE", "MONTHLY_BATCH_INPUT"}
	paths := make([]string, len(keys))
	for i, key := range keys {
		paths[i] = os.Getenv(key)
		if paths[i] == "" {
			t.Skip("explicit retained input/context/candidate/comparison paths required")
		}
		if !filepath.IsAbs(paths[i]) {
			t.Fatal("absolute local paths required")
		}
	}
	raw := make([][]byte, len(paths))
	for i, path := range paths {
		var err error
		raw[i], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	var bundle MonthlyPreparationBundle
	var previous []monthlyBatchEntry
	if err := json.Unmarshal(raw[2], &bundle); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw[3], &previous); err != nil {
		t.Fatal(err)
	}
	entries := monthlyBundleAcceptanceEntries(t, bundle, previous)
	now := time.Now()
	ready, blocked, members := 0, 0, 0
	for i, entry := range bundle.Entries {
		review := monthlyReviewApproval(raw[2], entry, now)
		if entry.Status == "blocked" {
			// Use a well-formed nonmatching digest to prove explicit approval
			// cannot waive missing evidence or generate a partial customer bill.
			review.CandidateSHA256 = monthlyDigest([]byte("synthetic blocked target"))
		}
		evidence, err := ReviewMonthlyCandidate(raw[0], raw[1], raw[2], monthlyReviewJSON(t, review), now)
		if entry.Status == "blocked" {
			if err == nil || evidence != nil {
				t.Fatal("missing historical evidence bypassed")
			}
			blocked++
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		entries[i].Evidence = evidence
		ready++
		members += len(entry.Members)
	}
	out := os.Getenv("MONTHLY_REVIEW_OUTPUT")
	if out == "" {
		out = t.TempDir()
	} else if !filepath.IsAbs(out) {
		t.Fatal("absolute private local acceptance output required")
	}
	monthlyBatchLocalJourney(t, entries, out)
	for i, path := range paths {
		after, err := os.ReadFile(path)
		if err != nil || monthlyDigest(after) != monthlyDigest(raw[i]) {
			t.Fatal("retained input modified", err)
		}
	}
	t.Logf("review/import/read/download: %d bills, %d member rows; %d unresolved months unchanged", ready, members, blocked)
}
