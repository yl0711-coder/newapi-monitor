//go:build unix

package main

import (
	"context"
	"testing"
)

func TestRelevantInspectionRejectsMissingAndUnrelatedInputs(t *testing.T) {
	for _, tc := range []struct{ main, facts, epoch, from, to, unrelated string }{
		{"", "/facts.db", "epoch", "2026-05-01", "2026-05-02", "action"},
		{"/main.db", "", "epoch", "2026-05-01", "2026-05-02", "action"},
		{"/main.db", "/facts.db", "", "2026-05-01", "2026-05-02", "action"},
		{"/main.db", "/facts.db", "epoch", "", "2026-05-02", "action"},
		{"/main.db", "/facts.db", "epoch", "2026-05-01", "", "action"},
		{"/main.db", "/facts.db", "epoch", "2026-05-01", "2026-05-02", "job-dir"},
		{"/main.db", "/facts.db", "epoch", "2026-05-01", "2026-05-02", "evidence"},
		{"/main.db", "/facts.db", "epoch", "2026-05-01", "2026-05-02", "confirm-plan"},
		{"/main.db", "/facts.db", "epoch", "2026-05-01", "2026-05-02", "handoff-max-hours"},
		{"/main.db", "/facts.db", "epoch", "2026-05-01", "2026-05-02", "receiver-backup"},
	} {
		out, err := runRelevantInspection(context.Background(), tc.main, tc.facts, tc.epoch, tc.from, tc.to, map[string]bool{tc.unrelated: true})
		if err == nil || out != nil {
			t.Fatal("unsafe or incomplete read-only invocation accepted", tc)
		}
	}
}
