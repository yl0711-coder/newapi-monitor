package monitor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func sub2RecoveryFixture(t *testing.T) *sub2ShadowHTTPFixture {
	t.Helper()
	f := newSub2ShadowHTTPFixture(t)
	guard := newUpstreamHostGuard(f.m.storeDB, upstreamHostGuardOptions{Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0})
	f.m.upstreamClient = installUpstreamHostGuardForTest(newUpstreamHTTPClient(upstreamSyncTimeout(f.m.cfg)), f.m.storeDB, guard)
	t.Cleanup(f.m.upstreamClient.CloseIdleConnections)
	rows, done, err := f.fetch(10) // old collector completed before optional ID existed
	if err != nil || !done {
		t.Fatal(err)
	}
	f.account.Enabled, f.account.UsageSyncEnabled = true, true
	if err := f.m.storeDB.Save(&f.account).Error; err != nil {
		t.Fatal(err)
	}
	f.m.cfg.ChannelCostClosureEnabled = true
	f.m.cfg.UpstreamPricingLedgerEnabled = true
	f.m.cfg.UpstreamPricingLedgerDomains = []string{f.account.Domain}
	f.configure(t, "575", 1)
	keyCheck, err := channelCostKeyCheck([]byte(f.m.cfg.ChannelCostHMACKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Create(&ChannelCostKeyRegistry{KeyID: f.m.cfg.ChannelCostHMACKeyID, KeyCheck: keyCheck}).Error; err != nil {
		t.Fatal(err)
	}
	for hour, evidence := range rows {
		state, err := pricingHourStateFromEvidence(f.account, hour, evidence, f.day+86400)
		if err != nil {
			t.Fatal(err)
		}
		state.Status, state.VerifiedScans = "verified", 2
		if err := f.m.storeDB.Create(&state).Error; err != nil {
			t.Fatal(err)
		}
		if len(evidence) > 0 {
			if err := f.m.storeDB.Create(&evidence).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	state := ChannelUpstreamPricingSyncState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), SemanticsVersion: upstreamPricingSemanticsVersion,
		BackfillStartHour: f.day, BackfillNextHour: f.day + 86400, BackfillDone: true, Status: "ok", TailNextSyncAt: f.day + 2*86400 + 300}
	if err := f.m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	f.requests.Store(0)
	return f
}

func sub2RecoveryProtected(t *testing.T, f *sub2ShadowHTTPFixture) string {
	t.Helper()
	var all [][]map[string]any
	for _, model := range []any{&ChannelUpstreamAccount{}, &ChannelUpstreamTokenConfig{}, &ChannelUpstreamPricingPageCheckpoint{}, &ChannelUpstreamPricingSyncState{},
		&ChannelUpstreamPricingHourState{}, &ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamUsageHour{}, &ChannelCostSourceBinding{}, &ChannelCostKeyRegistry{},
		&ChannelEconomicsDirtyHour{}, &ChannelCostDirtyHour{}, &ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{},
		&ChannelEconomicsHourPublication{}, &ChannelEconomicsHourCurrent{}, &ChannelEconomicsHourManifestPublication{}, &ChannelEconomicsHourManifestCurrent{}, &ChannelEconomicsGlobalHourFact{}} {
		var rows []map[string]any
		if err := f.m.storeDB.Model(model).Order("rowid").Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		all = append(all, rows)
	}
	data, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func sub2RecoveryRun(f *sub2ShadowHTTPFixture, at int64, budget int) (sub2TokenRecoveryResult, error) {
	return f.m.recoverSub2TokenEvidenceDay(context.Background(), f.account, sub2APICredential{AccessToken: "synthetic", ExpiresAt: at + 3600}, f.day, at, newUpstreamUsageRequestPacer(budget, 0))
}

func TestSub2TokenRecoveryTwoIndependentScansWithoutLegacyWrites(t *testing.T) {
	f := sub2RecoveryFixture(t)
	before := sub2RecoveryProtected(t, f)
	now := f.day + 86405
	got, err := sub2RecoveryRun(f, now, 4)
	if err != nil || got.Status != "waiting_second_scan" || got.StableScans != 1 || f.requests.Load() != 3 {
		t.Fatal(got, err, f.requests.Load())
	}
	if _, err := sub2RecoveryRun(f, now+1, 4); err != nil || f.requests.Load() != 3 {
		t.Fatal("immediate retry was treated as a new scan", err)
	}
	f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
	got, err = sub2RecoveryRun(f, now+60, 4)
	if err != nil || got.Status != "observed" || got.StableScans != 2 || f.requests.Load() != 6 {
		t.Fatal(got, err, f.requests.Load())
	}
	for i := 0; i < 3; i++ {
		if _, err := sub2RecoveryRun(f, now+120, 4); err != nil {
			t.Fatal(err)
		}
	}
	if f.requests.Load() != 6 || sub2RecoveryProtected(t, f) != before {
		t.Fatal("recovery changed existing accounting or repeated complete fetch")
	}
}

func TestSub2TokenRecoveryBudgetResumeAndFinalProbe(t *testing.T) {
	f := sub2RecoveryFixture(t)
	before := sub2RecoveryProtected(t, f)
	now := f.day + 86405
	got, err := sub2RecoveryRun(f, now, 2)
	if err != nil || got.Status != "paging" || f.requests.Load() != 2 {
		t.Fatal(got, err, f.requests.Load())
	}
	cursor, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
	if err != nil || cursor.SuccessRevision != 0 {
		t.Fatal("partial scan published observation", err)
	}
	f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB}
	got, err = sub2RecoveryRun(f, now+60, 2)
	if err != nil || got.Status != "waiting_second_scan" || got.StableScans != 1 || f.requests.Load() != 4 {
		t.Fatal(got, err, f.requests.Load())
	}
	if sub2RecoveryProtected(t, f) != before {
		t.Fatal("legacy changed")
	}
}

func TestSub2TokenRecoveryRejectsChangedAndMissingIdentity(t *testing.T) {
	for _, mode := range []string{"missing", "probe-change", "resume-change", "corrupt", "ledger-change", "store-failure"} {
		t.Run(mode, func(t *testing.T) {
			f := sub2RecoveryFixture(t)
			now := f.day + 86405
			switch mode {
			case "missing":
				f.missing.Store(true)
			case "probe-change":
				f.changeOnFinalProbe.Store(true)
			case "resume-change", "corrupt":
				if _, err := sub2RecoveryRun(f, now, 2); err != nil {
					t.Fatal(err)
				}
				if mode == "corrupt" {
					if err := f.m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Where("domain=?", f.account.Domain).UpdateColumn("envelope", "corrupt").Error; err != nil {
						t.Fatal(err)
					}
				} else {
					f.changed.Store(true)
				}
			case "ledger-change":
				if err := f.m.storeDB.Model(&ChannelUpstreamPricingHourState{}).Where("domain=?", f.account.Domain).UpdateColumn("content_hash", "changed").Error; err != nil {
					t.Fatal(err)
				}
			case "store-failure":
				if err := f.m.storeDB.Exec(`CREATE TRIGGER reject_token_observation BEFORE UPDATE OF success_revision ON channel_upstream_token_scan_checkpoints BEGIN SELECT RAISE(ABORT,'synthetic'); END`).Error; err != nil {
					t.Fatal(err)
				}
			}
			before := sub2RecoveryProtected(t, f)
			if _, err := sub2RecoveryRun(f, now+60, 4); err == nil {
				t.Fatal("invalid recovery accepted")
			}
			cursor, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
			if err != nil || cursor.SuccessRevision != 0 || sub2RecoveryProtected(t, f) != before {
				t.Fatal("failure escaped staging", err)
			}
			if mode == "ledger-change" && f.requests.Load() != 0 {
				t.Fatal("invalid legacy fetched upstream")
			}
			if mode == "probe-change" || mode == "resume-change" || mode == "missing" {
				f.missing.Store(false)
				f.changed.Store(false)
				f.changeOnFinalProbe.Store(false)
				got, err := sub2RecoveryRun(f, now+120, 4)
				if err != nil || got.StableScans != 1 {
					t.Fatal("recovery stuck after source repaired", got, err)
				}
			}
		})
	}
}

func TestSub2TokenRecoveryGuardsMakeNoRequests(t *testing.T) {
	for _, mode := range []string{"readonly", "disabled", "not-allowed", "no-intent", "cleared", "legacy-pending", "legacy-cooldown", "bad-key", "account-disabled", "changed-account", "open-day", "busy", "no-budget", "expired", "canceled", "before-config"} {
		t.Run(mode, func(t *testing.T) {
			f := sub2RecoveryFixture(t)
			now := f.day + 86405
			ctx := context.Background()
			pacer := newUpstreamUsageRequestPacer(4, 0)
			cred := sub2APICredential{AccessToken: "synthetic", ExpiresAt: now + 3600}
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "readonly":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "not-allowed":
				f.m.cfg.UpstreamPricingLedgerDomains = nil
			case "no-intent":
				must(f.m.storeDB.Where("domain=?", f.account.Domain).Delete(&ChannelUpstreamTokenConfig{}).Error)
			case "cleared":
				f.configure(t, "", 2)
			case "legacy-pending":
				must(f.m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain=?", f.account.Domain).UpdateColumn("backfill_next_hour", f.day).Error)
			case "legacy-cooldown":
				must(f.m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain=?", f.account.Domain).UpdateColumn("status", upstreamStatusError).Error)
			case "bad-key":
				f.m.cfg.ChannelCostHMACKey = "wrong-key-0123456789abcdef01234567"
			case "account-disabled":
				must(f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).UpdateColumn("enabled", false).Error)
			case "changed-account":
				f.account.Account = "other"
			case "open-day":
				now = f.day + 1
			case "busy":
				f.m.upstreamPricingMu.Lock()
				defer f.m.upstreamPricingMu.Unlock()
			case "no-budget":
				pacer = nil
			case "expired":
				cred.ExpiresAt = now
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "before-config":
				now += 86400
				cred.ExpiresAt = now + 3600
			}
			before := sub2RecoveryProtected(t, f)
			_, _ = f.m.recoverSub2TokenEvidenceDay(ctx, f.account, cred, f.day, now, pacer)
			var count int64
			if err := f.m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			if f.requests.Load() != 0 || count != 0 || !reflect.DeepEqual(before, sub2RecoveryProtected(t, f)) {
				t.Fatal("guard allowed requests or writes", f.requests.Load(), count)
			}
		})
	}
}

func TestSub2TokenRecoveryHistoricalIntentAfterClear(t *testing.T) {
	f := sub2RecoveryFixture(t)
	if err := f.m.storeDB.Model(&ChannelUpstreamTokenConfig{}).Where("domain=? AND revision=1", f.account.Domain).UpdateColumn("created_at", f.day-1).Error; err != nil {
		t.Fatal(err)
	}
	f.configure(t, "", 2)
	before := sub2RecoveryProtected(t, f)
	got, err := sub2RecoveryRun(f, f.day+2*86400+60, 4)
	if err != nil || got.Status != "waiting_second_scan" || got.StableScans != 1 {
		t.Fatal(got, err)
	}
	if sub2RecoveryProtected(t, f) != before {
		t.Fatal("historical evidence changed old money")
	}
}

func TestSub2TokenRecoveryHTTPFailurePreservesResume(t *testing.T) {
	for _, status := range []int{401, 429} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			f := sub2RecoveryFixture(t)
			now := f.day + 86405
			if _, err := sub2RecoveryRun(f, now, 2); err != nil {
				t.Fatal(err)
			}
			before := sub2RecoveryProtected(t, f)
			cursor, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			transport := upstreamRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"120"}}, Body: io.NopCloser(strings.NewReader(`{"message":"synthetic failure"}`)), Request: req}, nil
			})
			guard := newUpstreamHostGuard(f.m.storeDB, upstreamHostGuardOptions{Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0})
			f.m.upstreamClient = installUpstreamHostGuardForTest(&http.Client{Transport: transport}, f.m.storeDB, guard)
			_, err = sub2RecoveryRun(f, now+60, 4)
			if err == nil || calls != 1 {
				t.Fatal("HTTP error retried inside recovery", err, calls)
			}
			if status == 429 && upstreamRetryAt(err) <= time.Now().Unix() {
				t.Fatal("Retry-After lost", err)
			}
			after, readErr := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
			if readErr != nil || !reflect.DeepEqual(cursor, after) || sub2RecoveryProtected(t, f) != before {
				t.Fatal("HTTP failure changed resume/money", readErr)
			}
		})
	}
}
