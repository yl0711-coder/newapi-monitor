package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestMonthlyResolutionCLIReadOnlyExclusiveAndModeIsolation(t *testing.T) {
	source, input, output := preparationCLIInput(t)
	before, sourceBefore := reviewCLIBytes(t, input), reviewCLIBytes(t, source)
	args := []string{"-input", input, "-out", output, "-resolution-plan"}
	now := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	if err := run(args, &bytes.Buffer{}, now); err != nil {
		t.Fatal(err)
	}
	var plan monitor.MonthlyResolutionPlan
	if err := json.Unmarshal(reviewCLIBytes(t, output), &plan); err != nil || len(plan.Cases) != 1 || plan.Cases[0].Code != "verify_legacy_cutoff" || plan.Mode != "internal_resolution_plan_not_publication" {
		t.Fatal(plan, err)
	}
	if !bytes.Equal(before, reviewCLIBytes(t, input)) || !bytes.Equal(sourceBefore, reviewCLIBytes(t, source)) {
		t.Fatal("modified source evidence")
	}
	st, err := os.Stat(output)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("private output permissions", err)
	}
	if err := run(args, &bytes.Buffer{}, now); err == nil {
		t.Fatal("overwrote a report")
	}
	for _, extra := range [][]string{{"-historical-preview"}, {"-bundle-context", "/unused"}, {"-review-request", "/unused"}} {
		_, nextInput, nextOutput := preparationCLIInput(t)
		bad := append([]string{"-input", nextInput, "-out", nextOutput, "-resolution-plan"}, extra...)
		if err := run(bad, &bytes.Buffer{}, now); err == nil {
			t.Fatal("mixed recovery and publication modes")
		}
		if _, err := os.Stat(nextOutput); !os.IsNotExist(err) {
			t.Fatal("mixed mode left output", err)
		}
	}
	_, _, freshOutput := preparationCLIInput(t)
	if err := os.WriteFile(source, []byte("changed synthetic source"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-input", input, "-out", freshOutput, "-resolution-plan"}, &bytes.Buffer{}, now); err == nil {
		t.Fatal("accepted source drift")
	}
	if _, err := os.Stat(freshOutput); !os.IsNotExist(err) {
		t.Fatal("source drift left an actionable report", err)
	}
}
