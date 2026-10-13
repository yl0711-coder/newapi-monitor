package monitor

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

type tokenReconcileFixture struct {
	m        *Monitor
	account  ChannelUpstreamAccount
	day      int64
	key      []byte
	cursor   ChannelUpstreamTokenScanCheckpoint
	rows     []ChannelUpstreamPricingHourEvidence
	states   []ChannelUpstreamPricingHourState
	controls []ChannelUpstreamUsageHour
}

func newTokenReconcileFixture(t *testing.T) tokenReconcileFixture {
	t.Helper()
	m, account, items, day, key := sub2TokenStoreFixture(t)
	if err := m.storeDB.AutoMigrate(&ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamPricingHourState{}, &ChannelUpstreamUsageHour{}); err != nil {
		t.Fatal(err)
	}
	f := tokenReconcileFixture{m: m, account: account, day: day, key: key}
	m.cfg.ChannelCostHMACKey, m.cfg.ChannelCostHMACKeyID = string(key), "v1"
	legacy := sub2ObservationLegacy(t, account, items, day)
	ctx := context.Background()
	var err error
	for scan := 0; scan < 2; scan++ {
		if scan != 0 {
			f.cursor, err = m.resetSub2TokenScanCheckpoint(ctx, account, day, f.cursor.Revision, day+86400, key, "v1")
			if err != nil {
				t.Fatal(err)
			}
		}
		f.cursor, err = m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), f.cursor.Revision, day+86400, key, "v1")
		if err != nil {
			t.Fatal(err)
		}
		f.cursor, err = m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[2:], 2), f.cursor.Revision, day+86400, key, "v1")
		if err != nil {
			t.Fatal(err)
		}
		f.cursor, err = m.recordSub2TokenObservation(ctx, account, day, f.cursor, legacy, day+86400, key, "v1")
		if err != nil {
			t.Fatal(err)
		}
	}
	for hour := day; hour < day+86400; hour += 3600 {
		state, err := pricingHourStateFromEvidence(account, hour, legacy[hour], day+86400)
		if err != nil {
			t.Fatal(err)
		}
		state.Status, state.VerifiedScans, state.ReconcileStatus = "verified", 2, "matched"
		state.LegacyQuota, state.LegacyRequests, state.LegacyTokens = state.FinalQuota, state.EligibleRequests, state.Tokens
		f.states = append(f.states, state)
		f.controls = append(f.controls, ChannelUpstreamUsageHour{Domain: account.Domain, Provider: account.Provider, HourTs: hour, BucketSeconds: 3600, Requests: state.EligibleRequests, Tokens: state.Tokens, CostUSD: float64(state.FinalQuota) / 1e6, FetchedAt: day + 86400})
		f.rows = append(f.rows, legacy[hour]...)
	}
	return f
}

func (f tokenReconcileFixture) check(t *testing.T, token string) upstreamTokenReconcileView {
	t.Helper()
	v, err := checkSub2TokenReconciliation(f.account, token, f.day, f.day+86400, f.cursor, f.rows, f.states, f.controls, f.key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestUpstreamTokenReconcileCompleteWithoutActivation(t *testing.T) {
	f := newTokenReconcileFixture(t)
	v := f.check(t, "575")
	if v.Status != "evidence_ready" || v.ReadyHours != 24 || v.BlockedHours != 0 || v.RoundingHours != 0 || !v.TokenSeen || v.AttributionVerified {
		t.Fatalf("unexpected readiness: %+v", v)
	}
	if v = f.check(t, "999"); v.Status != "token_not_observed" || v.TokenSeen || v.AttributionVerified {
		t.Fatal("unseen token accepted")
	}
	// A mutable token name is never needed; both same-group IDs are proven.
	if v = f.check(t, "574"); v.Status != "evidence_ready" || !v.TokenSeen {
		t.Fatal("same-group second token was lost")
	}
}

func TestUpstreamTokenReconcileFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(*tokenReconcileFixture)
		status     string
		hourStatus string
	}{
		{"missing-control", func(f *tokenReconcileFixture) { f.controls = f.controls[1:] }, "reconciliation_blocked", "control_missing"},
		{"provisional", func(f *tokenReconcileFixture) { f.controls[0].Provisional = true }, "reconciliation_blocked", "control_invalid"},
		{"day-not-hour", func(f *tokenReconcileFixture) { f.controls[0].BucketSeconds = 86400 }, "reconciliation_blocked", "control_invalid"},
		{"wrong-provider", func(f *tokenReconcileFixture) { f.controls[0].Provider = upstreamProviderNewAPI }, "reconciliation_blocked", "control_invalid"},
		{"changed-control", func(f *tokenReconcileFixture) { f.controls[0].Requests++ }, "reconciliation_blocked", "control_changed"},
		{"changed-state", func(f *tokenReconcileFixture) { f.states[0].FinalQuota++ }, "reconciliation_blocked", "pricing_state_changed"},
		{"unverified", func(f *tokenReconcileFixture) { f.states[0].VerifiedScans = 1 }, "reconciliation_blocked", "pricing_unverified"},
		{"missing-state", func(f *tokenReconcileFixture) { f.states = f.states[1:] }, "reconciliation_blocked", "pricing_unverified"},
		{"nan", func(f *tokenReconcileFixture) { f.controls[0].CostUSD = math.NaN() }, "reconciliation_blocked", "control_invalid"},
		{"large-float", func(f *tokenReconcileFixture) { f.controls[0].CostUSD = float64(int64(1)<<53) / 1e6 }, "reconciliation_blocked", "control_invalid"},
		{"negative", func(f *tokenReconcileFixture) { f.controls[0].CostUSD = -1 }, "reconciliation_blocked", "control_invalid"},
		{"one-scan", func(f *tokenReconcileFixture) { f.cursor.StableScans = 1 }, "waiting_second_scan", ""},
		{"uncompleted", func(f *tokenReconcileFixture) { f.cursor.SuccessRevision-- }, "scan_not_completed", ""},
		{"corrupt-envelope", func(f *tokenReconcileFixture) { f.cursor.Envelope = "bad" }, "evidence_invalid", ""},
		{"changed-pricing", func(f *tokenReconcileFixture) { f.rows[0].FinalQuota++ }, "pricing_evidence_changed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTokenReconcileFixture(t)
			tc.mutate(&f)
			v := f.check(t, "575")
			if v.Status != tc.status || v.AttributionVerified {
				t.Fatalf("unexpected result %+v", v)
			}
			if tc.hourStatus != "" && (len(v.Hours) != 24 || v.Hours[0].Status != tc.hourStatus || v.BlockedHours != 1) {
				t.Fatalf("wrong hourly block %+v", v)
			}
		})
	}
}

func TestUpstreamTokenReconcileRoundingIsNotEquality(t *testing.T) {
	for _, delta := range []int64{-3, -2, 0, 2, 3} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			f := newTokenReconcileFixture(t)
			index := -1
			for i, s := range f.states {
				if s.EligibleRequests == 2 {
					index = i
					break
				}
			}
			if index < 0 {
				t.Fatal("fixture missing two-request hour")
			}
			units := f.states[index].FinalQuota - delta
			f.controls[index].CostUSD = float64(units) / 1e6
			f.states[index].LegacyQuota = units
			v := f.check(t, "575")
			h := v.Hours[index]
			want := "rounding_only"
			if delta == 0 {
				want = "matched"
			}
			if delta > 2 || delta < -2 {
				want = "control_mismatch"
			}
			if h.Status != want || h.DeltaUnits != fmt.Sprint(delta) || h.ToleranceUnits != "2" || v.AttributionVerified {
				t.Fatalf("lost rounding evidence %+v", h)
			}
		})
	}
}

func TestUpstreamTokenReconcileHTTPReadOnlyAndAuthorized(t *testing.T) {
	f := newTokenReconcileFixture(t)
	ctx := context.Background()
	if _, err := f.m.saveUpstreamTokenConfig(ctx, tokenConfigInput(f.account, 185, "575", 0), "fixture", f.day+86400); err != nil {
		t.Fatal(err)
	}
	for _, rows := range []any{&f.rows, &f.states, &f.controls} {
		if err := f.m.storeDB.Create(rows).Error; err != nil {
			t.Fatal(err)
		}
	}
	f.m.cfg.LocalSnapshotOnly = true
	f.m.cfg.SessionSecret = "local-token-check-fixture"
	sqlDB, err := f.m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := f.m.storeDB.Exec("PRAGMA query_only=ON").Error; err != nil {
		t.Fatal(err)
	}
	defer f.m.storeDB.Exec("PRAGMA query_only=OFF")
	r := tokenConfigRouter(f.m)
	path := fmt.Sprintf("/channels/upstream/token-check?domain=%s&channel_id=185&day_ts=%d", f.account.Domain, f.day)
	for _, role := range []int{0, roleAdmin} {
		w := tokenConfigRequest(f.m, r, http.MethodGet, path, "", role)
		if w.Code != http.StatusForbidden && w.Code != http.StatusUnauthorized {
			t.Fatal("non-root read accepted", w.Code)
		}
	}
	w := tokenConfigRequest(f.m, r, http.MethodGet, path, "", roleRoot)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"evidence_ready"`) || w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "secret-not-for-response") {
		t.Fatalf("readiness HTTP %d %s", w.Code, w.Body.String())
	}
	if tokenConfigRequest(f.m, r, http.MethodGet, path+"1", "", roleRoot).Code != 400 {
		t.Fatal("invalid day accepted")
	}
	if tokenConfigRequest(f.m, r, http.MethodPost, path, "{}", roleRoot).Code != 404 {
		t.Fatal("check has a write route")
	}
	stored, err := f.m.loadSub2TokenScanCheckpoint(ctx, f.account, f.day, f.key, "v1")
	if err != nil || !reflect.DeepEqual(stored, f.cursor) {
		t.Fatal("read changed scan state", err)
	}
	for _, model := range []any{&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{}, &ChannelCostSourceBinding{}, &ChannelEconomicsDirtyHour{}} {
		var count int64
		if err := f.m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("check wrote %T: %v", model, err)
		}
	}
}

func TestSub2TokenObservationChangedResumeRevokesPreviousSuccess(t *testing.T) {
	f := newSub2ShadowHTTPFixture(t)
	f.configure(t, "575", 1)
	f.m.cfg.ChannelCostClosureEnabled = true
	for scan := 0; scan < 2; scan++ {
		if scan > 0 {
			if err := f.m.deletePricingPageCheckpoint(context.Background(), f.account.Domain, newAPIUpstreamAccountEpoch(f.account), f.day); err != nil {
				t.Fatal(err)
			}
		}
		if _, done, err := f.fetch(10); err != nil || !done {
			t.Fatal(err)
		}
	}
	before, err := f.m.loadSub2TokenScanCheckpoint(context.Background(), f.account, f.day, []byte(f.m.cfg.ChannelCostHMACKey), f.m.cfg.ChannelCostHMACKeyID)
	if err != nil || before.StableScans != 2 {
		t.Fatal("fixture not previously stable", err)
	}
	f.changed.Store(true)
	rows, done, err := f.fetch(10) // legacy completed cursor resumes, no new full scan
	if err != nil || !done {
		t.Fatal("legacy stopped", err)
	}
	assertSub2ShadowLegacyTotal(t, rows)
	after, err := f.m.loadSub2TokenScanCheckpoint(context.Background(), f.account, f.day, []byte(f.m.cfg.ChannelCostHMACKey), f.m.cfg.ChannelCostHMACKeyID)
	// Invalidation now advances the revision before the probe, fencing a
	// delayed successful finish for the previous revision as well.
	if err != nil || after.SuccessRevision != 0 || after.StableScans != 0 || after.Envelope != before.Envelope || after.Revision != before.Revision+1 || after.UpdatedAt != f.day+86400 {
		t.Fatal("contradictory probe left previous success active", err)
	}
}
