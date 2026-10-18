package monitor

import (
	"context"
	"reflect"
	"testing"
)

func TestFinanceClosureIdentityInvalidatesReportAndPeriodFingerprint(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "identity.example")
	ctx := context.Background()
	const from, to = int64(1788195600), int64(1788199200)
	for _, fullReport := range []bool{true, false} {
		before, err := m.financeReportSourceFingerprintForScope(ctx, from, to, fullReport)
		if err != nil {
			t.Fatal(err)
		}
		name := "first"
		if !fullReport {
			name = "second"
		}
		// An account-only change is material even without a credential update:
		// account is part of the retained cost evidence namespace.
		if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", "identity.example").Update("account", name).Error; err != nil {
			t.Fatal(err)
		}
		after, err := m.financeReportSourceFingerprintForScope(ctx, from, to, fullReport)
		if err != nil {
			t.Fatal(err)
		}
		if before == after {
			t.Fatalf("account identity change reused old fingerprint: fullReport=%v", fullReport)
		}
		repeated, err := m.financeReportSourceFingerprintForScope(ctx, from, to, fullReport)
		if err != nil || repeated != after {
			t.Fatalf("unchanged identity was not stable: fullReport=%v err=%v", fullReport, err)
		}
	}
}

func TestFinanceClosureEvidenceIsolatesAccountIdentity(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "identity.example")
	ctx := context.Background()
	var account ChannelUpstreamAccount
	if err := m.storeDB.First(&account, "domain=?", "identity.example").Error; err != nil {
		t.Fatal(err)
	}
	hour := int64(1788195600)
	old := account
	old.UserID = 42
	currentEpoch, oldEpoch := newAPIUpstreamAccountEpoch(account), newAPIUpstreamAccountEpoch(old)
	states := []ChannelUpstreamCostHourState{
		{Domain: account.Domain, Provider: account.Provider, AccountEpoch: currentEpoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "observed", ReconcileStatus: "pending", Requests: 3, EvidenceRows: 1},
		{Domain: account.Domain, Provider: account.Provider, AccountEpoch: oldEpoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "verified", ReconcileStatus: "matched", Requests: 100, EvidenceRows: 1},
		{Domain: account.Domain, Provider: upstreamProviderSub2API, AccountEpoch: currentEpoch, HourTs: hour + 3600, SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "verified", ReconcileStatus: "matched", Requests: 200, EvidenceRows: 1},
		{Domain: "removed.example", Provider: account.Provider, AccountEpoch: oldEpoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "verified", ReconcileStatus: "matched", Requests: 500, EvidenceRows: 1},
	}
	if err := m.storeDB.Create(&states).Error; err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: hour, ToTs: hour + 7200}
	details := []financeCostDetailView{{Domain: account.Domain}, {Domain: "removed.example"}}
	got, _, err := m.loadFinanceClosureEvidence(ctx, scope, details)
	if err != nil {
		t.Fatal(err)
	}
	row := got[account.Domain]
	if row.EvidenceHours != 1 || row.VerifiedHours != 0 || row.EvidenceRequests != 3 || row.VerifiedRequests != 0 || row.FirstHour != hour {
		t.Fatalf("old account/provider evidence contaminated current coverage: %+v", row)
	}
	if _, ok := got["removed.example"]; ok {
		t.Fatal("orphan evidence became current account coverage")
	}
	// Changing identity selects the retained old account evidence, not a sum
	// of both namespaces. This test changes metadata only in its synthetic DB.
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", account.Domain).Update("user_id", old.UserID).Error; err != nil {
		t.Fatal(err)
	}
	got, _, err = m.loadFinanceClosureEvidence(ctx, scope, details)
	if err != nil {
		t.Fatal(err)
	}
	row = got[account.Domain]
	if row.EvidenceHours != 1 || row.VerifiedHours != 1 || row.EvidenceRequests != 100 || row.VerifiedRequests != 100 {
		t.Fatalf("identity switch not reflected: %+v", row)
	}
	var after []ChannelUpstreamCostHourState
	if err := m.storeDB.Order("domain,account_epoch,hour_ts").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	before := append([]ChannelUpstreamCostHourState(nil), states...)
	// Compare by primary key so storage ordering cannot hide any deletion/edit.
	for _, state := range before {
		found := false
		for _, saved := range after {
			if reflect.DeepEqual(state, saved) {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("readiness read changed retained evidence")
		}
	}
	if len(before) != len(after) {
		t.Fatal("readiness read changed evidence count")
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := m.loadFinanceClosureEvidence(ctx, scope, details); err == nil {
		t.Fatal("canceled read succeeded")
	}
}

func TestFinanceClosureEvidenceKeepsSub2CurrentNamespace(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "sub2.example")
	account := ChannelUpstreamAccount{Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: "https://sub2.example", UserID: 7, Account: "current"}
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", account.Domain).Updates(map[string]any{"provider": account.Provider, "user_id": account.UserID, "account": account.Account}).Error; err != nil {
		t.Fatal(err)
	}
	const hour = int64(1788195600)
	current := ChannelUpstreamCostHourState{Domain: account.Domain, Provider: account.Provider, AccountEpoch: newAPIUpstreamAccountEpoch(account), HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "verified", ReconcileStatus: "matched", Requests: 7, EvidenceRows: 1}
	states := []ChannelUpstreamCostHourState{current}
	for _, change := range []func(*ChannelUpstreamAccount){
		func(a *ChannelUpstreamAccount) { a.Account = "previous" },
		func(a *ChannelUpstreamAccount) { a.BaseURL = "https://old.example" },
		func(a *ChannelUpstreamAccount) { a.Provider = upstreamProviderNewAPI },
	} {
		old := account
		change(&old)
		state := current
		state.AccountEpoch = newAPIUpstreamAccountEpoch(old)
		state.Provider = old.Provider
		state.Requests = 99
		states = append(states, state)
	}
	outOfRange := current
	outOfRange.HourTs = hour + 3600
	states = append(states, outOfRange)
	oldSemantics := current
	oldSemantics.SemanticsVersion--
	states = append(states, oldSemantics)
	if err := m.storeDB.Create(&states).Error; err != nil {
		t.Fatal(err)
	}
	rows, _, err := m.loadFinanceClosureEvidence(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, []financeCostDetailView{{Domain: account.Domain}})
	if err != nil {
		t.Fatal(err)
	}
	got := rows[account.Domain]
	if got.EvidenceHours != 1 || got.VerifiedHours != 1 || got.EvidenceRequests != 7 || got.VerifiedRequests != 7 {
		t.Fatalf("current Sub2 evidence dropped or old identity retained: %+v", got)
	}
}
