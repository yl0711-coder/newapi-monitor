package monthlydraft

import (
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestHistoricalBalanceScopeRetainsIndependentDecision(t *testing.T) {
	h := &monitor.MonthlyHistoricalBalanceRecognition{DecisionSource: "decision#accepted-policy", DecisionSHA256: strings.Repeat("d", 64)}
	scope := AutomaticScope{Group: monitor.MonthlyBundleGroup{Source: "directory#members"}, Accounts: []AutomaticWalletScope{{UserID: 77, WalletOnly: true, WalletSource: "source#wallet", Reconstructions: []monitor.MonthlySnapshotReconstruction{{HistoricalBalance: h}}}}}
	source := monitor.MonthlyPreparationSource{ID: "automatic_scope", SHA256: strings.Repeat("s", 64)}
	if !independentRetainedScope(scope, source) {
		t.Fatal("independent historical decision rejected")
	}
	refs, digests := organizationHistoryRefs(nil, scope)
	if !refs["decision"] || !digests[h.DecisionSHA256] {
		t.Fatal("history slicing dropped the accepted policy source or exact hash")
	}
	h.DecisionSource = source.ID + "#self-approved"
	if independentRetainedScope(scope, source) {
		t.Fatal("scope file can approve its own historical adjustment")
	}
	h.DecisionSource, h.DecisionSHA256 = "decision#accepted-policy", source.SHA256
	if independentRetainedScope(scope, source) {
		t.Fatal("scope bytes can masquerade as the independent decision")
	}
}
