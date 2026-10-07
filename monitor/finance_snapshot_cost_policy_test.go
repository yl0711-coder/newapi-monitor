package monitor

import (
	"encoding/json"
	"testing"
)

func TestFinancePriorSnapshotCostPolicyRequiresUnchangedProviders(t *testing.T) {
	for _, tc := range []struct {
		payload string
		want    bool
	}{
		{`{"statement":{"known_user_consumption":{"micro_usd":"123"}}}`, true},
		{`{"statement":{"known_upstream_billed_cost":{"micro_usd":"100"}}}`, false},
		{`{"periods":[{"statement":{"known_upstream_billed_cost":{"micro_usd":"100"}}}]}`, false},
		{`{"days":[{"statement":{"known_raw_corrected_upstream_cost":{"micro_usd":"100"}}}]}`, false},
		{`{"cost_details":[{"provider":"tokenforce"}]}`, false},
		{`{"cost_details":[{"provider":""}]}`, false},
		{`{"cost_details":[{"provider":"future-adapter"}]}`, false},
		{`{"cost_details":[{"provider":"newapi"},{"provider":"tokenforce"}]}`, false},
		{`{"cost_details":[{"provider":"newapi"},{"provider":"sub2api"},{"provider":"aicodewith"}]}`, true},
	} {
		ok, err := financePriorCostPolicyCompatible([]byte(tc.payload))
		if err != nil || ok != tc.want {
			t.Fatalf("ok=%v want=%v err=%v: %s", ok, tc.want, err, tc.payload)
		}
	}
	if _, err := financePriorCostPolicyCompatible([]byte("{")); err == nil {
		t.Fatal("malformed prior snapshot accepted")
	}
}

func TestFinanceUpgradeSnapshotV5TokenForceAmountsCannotLeak(t *testing.T) {
	m, request, now, _ := newFinanceUpgradeSnapshotFixture(t)
	for _, provider := range []string{"tokenforce", "newapi"} {
		t.Run(provider, func(t *testing.T) {
			payload, err := json.Marshal(map[string]any{
				"enabled": true, "from": request.from.Unix(), "to": request.to.Unix(), "generated_at": now.Unix(),
				"statement":    map[string]any{"known_upstream_billed_cost": economicsMoney(10_000_000)},
				"cost_details": []map[string]string{{"provider": provider}},
			})
			if err != nil {
				t.Fatal(err)
			}
			key := financeUpgradeProjectionKey(request, "accounting-gift-horizon-v5")
			if err := m.persistFinanceReportSnapshotShadow(key, "old-source", payload, now); err != nil {
				t.Fatal(err)
			}
			_, ok, err := m.loadFinanceUpgradeSnapshot(request, now)
			if err != nil {
				t.Fatal(err)
			}
			// A fixture also has an older customer-only snapshot. It remains a
			// safe dated fallback, but the converted TokenForce payload must not.
			if !ok {
				t.Fatal("safe dated fallback was lost")
			}
			got, _, err := m.loadFinanceUpgradeSnapshot(request, now)
			if err != nil || (string(got) == string(payload)) != (provider == "newapi") {
				t.Fatal("old TokenForce money leaked or unchanged provider was blocked", string(got), err)
			}
			if _, _, _, current, err := m.loadFinanceReportSnapshot(request, now); err != nil || current {
				t.Fatal("prior projection promoted to current proof", err)
			}
		})
	}
}

func TestFinanceUpgradeSnapshotNativeRechargeRemainsDatedOnly(t *testing.T) {
	m, request, now, _ := newFinanceUpgradeSnapshotFixture(t)
	payload, err := json.Marshal(map[string]any{
		"enabled": true, "from": request.from.Unix(), "to": request.to.Unix(), "generated_at": now.Unix(),
		"statement":    map[string]any{"known_raw_corrected_upstream_cost": economicsMoney(140056)},
		"cost_details": []map[string]string{{"provider": "tokenforce"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	oldKey := financeUpgradeProjectionKey(request, "accounting-platform-recharge-v6")
	if oldKey == request.logicalKey() {
		t.Fatal("zero policy reused a pre-zero full-report key")
	}
	if err := m.persistFinanceReportSnapshotShadow(oldKey, "old-source", payload, now); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.loadFinanceUpgradeSnapshot(request, now)
	if err != nil || !ok || string(got) != string(payload) {
		t.Fatal("native-unit dated fallback was lost", err)
	}
	if _, _, _, current, err := m.loadFinanceReportSnapshot(request, now); err != nil || current {
		t.Fatal("dated projection became current zero-policy proof", err)
	}
}
