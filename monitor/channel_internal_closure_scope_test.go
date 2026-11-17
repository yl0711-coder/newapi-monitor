package monitor

import "testing"

func TestInternalCostOutsideClosureIsScopeNotPendingVerification(t *testing.T) {
	m := &Monitor{cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"}}}
	pending := ChannelUpstreamUsageMetrics{Available: true, CostUSD: 10, InternalFilterStatus: "backfilling",
		InternalFilterReasons: []string{channelInternalPairingUnverified, channelInternalOwnershipUnknown}}

	out := m.markChannelInternalCostOutsideClosure(pending, "other.example", 3)
	if out.InternalFilterStatus != channelInternalCostOutsideClosure || out.InternalFilterReasons != nil {
		t.Fatalf("outside the whitelist: status=%q reasons=%v", out.InternalFilterStatus, out.InternalFilterReasons)
	}
	// Amounts and net-cost availability are untouched.
	if out.CostUSD != 10 || out.BusinessCostAvailable || out.BusinessAdjustedCostAvailable {
		t.Fatalf("amounts changed: %+v", out)
	}
	if got := m.markChannelInternalCostOutsideClosure(pending, "4sapi.com", 3); got.InternalFilterStatus != "backfilling" || len(got.InternalFilterReasons) != 2 {
		t.Fatalf("pilot domain must keep its real diagnostics: %+v", got)
	}
	if got := m.markChannelInternalCostOutsideClosure(pending, "other.example", 0); got.InternalFilterStatus != "backfilling" {
		t.Fatalf("no internal accounts: status must not change, got %q", got.InternalFilterStatus)
	}
	for _, kept := range []string{"complete", "inconsistent"} {
		in := pending
		in.InternalFilterStatus = kept
		if got := m.markChannelInternalCostOutsideClosure(in, "other.example", 3); got.InternalFilterStatus != kept {
			t.Fatalf("%s must be preserved, got %q", kept, got.InternalFilterStatus)
		}
	}
	off := &Monitor{cfg: Settings{ChannelCostClosureEnabled: false, ChannelCostClosureDomains: []string{"4sapi.com"}}}
	if got := off.markChannelInternalCostOutsideClosure(pending, "4sapi.com", 3); got.InternalFilterStatus != channelInternalCostOutsideClosure {
		t.Fatalf("closure disabled: every upstream is outside it, got %q", got.InternalFilterStatus)
	}
}
