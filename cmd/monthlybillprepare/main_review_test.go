package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type reviewCLIPaths struct {
	source, input, context, bundle, review, output string
	now                                            time.Time
}

func reviewCLIBytes(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func reviewCLIDigest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

func reviewCLIInput(t *testing.T) reviewCLIPaths {
	t.Helper()
	source, input, contextPath, bundle := preparationBundleCLIInput(t)
	f := reviewCLIPaths{source: source, input: input, context: contextPath, bundle: bundle, review: filepath.Join(filepath.Dir(input), "review.json"), output: filepath.Join(filepath.Dir(input), "reviewed.json"), now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	if err := prepareBundle(f.input, f.context, f.bundle, f.now); err != nil {
		t.Fatal(err)
	}
	raw := reviewCLIBytes(t, f.bundle)
	var b monitor.MonthlyPreparationBundle
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	r := monitor.MonthlyCandidateReview{Version: 1, GroupID: 1, Month: "2026-06", BundleSHA256: reviewCLIDigest(raw), CandidateSHA256: b.Entries[0].CandidateSHA256, Decision: "approve", ReviewedBy: "synthetic-cli-review-only", ReviewedAt: f.now.Unix(), SourcesReviewed: true, MemberScopeReviewed: true, WalletBasisReviewed: true}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.review, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f reviewCLIPaths) args() []string {
	return []string{"-input", f.input, "-bundle-context", f.context, "-review-bundle", f.bundle, "-review-request", f.review, "-out", f.output}
}

func TestMonthlyReviewCLIProducesOneEvidenceAndPreservesInputs(t *testing.T) {
	f := reviewCLIInput(t)
	before := map[string][]byte{}
	for _, path := range []string{f.source, f.input, f.context, f.bundle, f.review} {
		before[path] = reviewCLIBytes(t, path)
	}
	var out bytes.Buffer
	if err := run(f.args(), &out, f.now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "未写数据库、未发布账单") {
		t.Fatal("misleading publication message", out.String())
	}
	raw := reviewCLIBytes(t, f.output)
	var e monitor.MonthlyBillEvidence
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	var b monitor.MonthlyPreparationBundle
	if err := json.Unmarshal(before[f.bundle], &b); err != nil {
		t.Fatal(err)
	}
	if e.Version != 2 || e.GroupID != 1 || e.Month != "2026-06" || e.ReviewedBy != "synthetic-cli-review-only" || !reflect.DeepEqual(e.Users, b.Entries[0].Candidate.Users) {
		t.Fatal("wrong customer, month or amount")
	}
	if !strings.Contains(e.SourceReference, reviewCLIDigest(before[f.review])) {
		t.Fatal("review fingerprint omitted")
	}
	st, err := os.Stat(f.output)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("output permissions", err)
	}
	if err := run(f.args(), &out, f.now); err == nil {
		t.Fatal("overwrote review output")
	}
	if !bytes.Equal(raw, reviewCLIBytes(t, f.output)) {
		t.Fatal("existing output changed")
	}
	f.output = filepath.Join(filepath.Dir(f.input), "same-review-new-path.json")
	if err := run(f.args(), &out, f.now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, reviewCLIBytes(t, f.output)) {
		t.Fatal("repeated explicit review not deterministic")
	}
	for path, old := range before {
		if !bytes.Equal(old, reviewCLIBytes(t, path)) {
			t.Fatal("input modified", path)
		}
	}
	temps, err := filepath.Glob(filepath.Join(filepath.Dir(f.input), ".monthly-candidate-*"))
	if err != nil || len(temps) != 0 {
		t.Fatal("temporary file leaked", temps, err)
	}
}

func TestMonthlyReviewCLIInvalidSourceOrApprovalCreatesNothing(t *testing.T) {
	for _, scenario := range []string{"source drift", "bundle drift", "input drift", "context drift", "missing approval", "approval symlink", "oversized approval", "relative output", "output symlink"} {
		t.Run(scenario, func(t *testing.T) {
			f := reviewCLIInput(t)
			switch scenario {
			case "source drift", "bundle drift", "input drift", "context drift":
				path := map[string]string{"source drift": f.source, "bundle drift": f.bundle, "input drift": f.input, "context drift": f.context}[scenario]
				if err := os.WriteFile(path, append(reviewCLIBytes(t, path), '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing approval":
				if err := os.WriteFile(f.review, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized approval":
				if err := os.WriteFile(f.review, bytes.Repeat([]byte(" "), monitor.MonthlyReviewRequestLimit+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "approval symlink":
				link := filepath.Join(filepath.Dir(f.input), "linked-review.json")
				if err := os.Symlink(f.review, link); err != nil {
					t.Fatal(err)
				}
				f.review = link
			case "relative output":
				f.output = "review-relative.json"
			case "output symlink":
				if err := os.Symlink(f.source, f.output); err != nil {
					t.Fatal(err)
				}
			}
			originalSource := reviewCLIBytes(t, f.source)
			var out bytes.Buffer
			if err := run(f.args(), &out, f.now); err == nil {
				t.Fatal("invalid source/approval accepted")
			}
			if !bytes.Equal(originalSource, reviewCLIBytes(t, f.source)) {
				t.Fatal("source overwritten")
			}
			if scenario != "output symlink" {
				if _, err := os.Lstat(f.output); !os.IsNotExist(err) {
					t.Fatal("output created on failure", err)
				}
			}
			temps, err := filepath.Glob(filepath.Join(filepath.Dir(f.input), ".monthly-candidate-*"))
			if err != nil || len(temps) != 0 {
				t.Fatal("temporary output leaked", err)
			}
		})
	}
}

func TestMonthlyReviewCLIModeSelectionCannotPublish(t *testing.T) {
	f := reviewCLIInput(t)
	for _, args := range [][]string{
		{"-review-bundle", f.bundle},
		{"-review-request", f.review},
		{"-review-bundle", f.bundle, "-review-request", f.review},
		append(f.args(), "-apply"), append(f.args(), "-db", f.source), append(f.args(), "unexpected"),
	} {
		var out bytes.Buffer
		if err := run(args, &out, f.now); err == nil {
			t.Fatal("incomplete or mutating command accepted", args)
		}
		if _, err := os.Lstat(f.output); !os.IsNotExist(err) {
			t.Fatal("wrong-mode output created", err)
		}
	}
	// Existing no-context report mode stays available and unchanged.
	legacy := filepath.Join(filepath.Dir(f.input), "legacy.json")
	var out bytes.Buffer
	if err := run([]string{"-input", f.input, "-out", legacy}, &out, f.now); err != nil {
		t.Fatal(err)
	}
	var r monitor.MonthlyPreparationReport
	if err := json.Unmarshal(reviewCLIBytes(t, legacy), &r); err != nil {
		t.Fatal(err)
	}
	if r.Mode != "local_reconciliation_draft_not_publication" {
		t.Fatal("legacy mode changed")
	}
}
