//go:build unix

package main

import (
	"context"
	"testing"
)

func TestHandoffCheckRejectsMissingAndUnrelatedFlags(t *testing.T) {
	for _, tc := range []struct{ job, digest, receiver, flag string }{
		{"", "hash", "/local/receiver", "action"},
		{"/local/job", "", "/local/receiver", "action"},
		{"/local/job", "hash", "", "action"},
		{"/local/job", "hash", "/local/receiver", "backup"},
		{"/local/job", "hash", "/local/receiver", "evidence"},
		{"/local/job", "hash", "/local/receiver", "next-job-dir"},
	} {
		if report, err := runHandoffCheck(context.Background(), tc.job, tc.digest, tc.receiver, map[string]bool{tc.flag: true}); err == nil || report != nil {
			t.Fatal("invalid readonly invocation accepted")
		}
	}
}

func TestHandoffWriteActionsCannotSelectArbitraryReceiver(t *testing.T) {
	for _, tc := range []struct{ action, receiver, next, flag string }{
		{"handoff-run", "/live.db", "", "receiver-backup"},
		{"handoff-run", "", "/live", "next-job-dir"},
		{"handoff-run", "", "", "evidence"},
		{"handoff-run", "", "", "backup"},
		{"handoff-plan", "", "/new", "action"},
		{"handoff-plan", "/closed.db", "", "action"},
		{"handoff-plan", "/closed.db", "/new", "series-plan"},
		{"handoff-plan", "/closed.db", "/new", "handoff-max-hours"},
	} {
		if out, err := runHandoffAction(context.Background(), tc.action, "/job", "hash", tc.receiver, tc.next, 2, map[string]bool{tc.flag: true}); err == nil || out != nil {
			t.Fatal("unsafe handoff invocation accepted", tc)
		}
	}
}

func TestHandoffRunRejectsInvalidLimit(t *testing.T) {
	for _, limit := range []int{-1, 0, 11} {
		out, err := runHandoffAction(context.Background(), "handoff-run", "/must-not-access", "hash", "", "", limit, map[string]bool{"handoff-max-hours": true})
		if err == nil || out != nil || err.Error() != "handoff max-hours must be between 1 and 10" {
			t.Fatal(limit, out, err)
		}
	}
}
