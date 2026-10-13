package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func sub2RecoveryScheduleFixture(t *testing.T) (*sub2ShadowHTTPFixture, int64) {
	t.Helper()
	f := sub2RecoveryFixture(t)
	now := f.day + 86405
	f.m.cfg.UpstreamCredentialSecret = "local-recovery-scheduler-test"
	credential := sub2APICredential{AccessToken: "synthetic", ExpiresAt: time.Now().Unix() + 86400}
	sealed, err := f.m.sealUpstreamCredential(f.account.Domain, f.account.Provider, credential)
	if err != nil {
		t.Fatal(err)
	}
	f.account.Credential, f.account.CredentialVersion = sealed, upstreamCredentialVersion
	if err := f.m.storeDB.Save(&f.account).Error; err != nil {
		t.Fatal(err)
	}
	return f, now
}

func TestSub2TokenRecoveryScheduleAuthPauseAndReauthorization(t *testing.T) {
	f, now := sub2RecoveryScheduleFixture(t)
	client := f.m.upstreamClient
	calls := 0
	guard := newUpstreamHostGuard(f.m.storeDB, upstreamHostGuardOptions{Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0})
	f.m.upstreamClient = installUpstreamHostGuardForTest(&http.Client{Transport: upstreamRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 401, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"message":"expired"}`)), Request: req}, nil
	})}, f.m.storeDB, guard)
	before := sub2RecoveryProtected(t, f)
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now); err == nil {
		t.Fatal("401 accepted")
	}
	w := sub2RecoveryJob(t, f)
	if w.Status != "reconnect" || w.NextAttemptAt != upstreamAccountIsolatedUntil {
		t.Fatal(w)
	}
	for i := 0; i < 3; i++ {
		if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now+60); err != nil {
			t.Fatal(err)
		}
		if c := sub2RecoveryCandidate(t, f, now+60); c.DayTs != 0 {
			t.Fatal("auth-paused account remained due", c)
		}
	}
	if calls != 1 || sub2RecoveryProtected(t, f) != before {
		t.Fatal("401 repeated or mutated legacy")
	}
	// Ordinary successful balance sync can reseal identical credentials with
	// a random nonce. That is not a new authorization and must stay paused.
	unchanged, err := f.m.credentialForAccount(f.account)
	if err != nil {
		t.Fatal(err)
	}
	resealed, err := f.m.sealUpstreamCredential(f.account.Domain, f.account.Provider, unchanged)
	if err != nil {
		t.Fatal(err)
	}
	f.account.Credential = resealed
	if err := f.m.storeDB.Save(&f.account).Error; err != nil {
		t.Fatal(err)
	}
	if c := sub2RecoveryCandidate(t, f, now+60); c.DayTs != 0 {
		t.Fatal("encryption nonce bypassed authentication pause", c)
	}
	// Simulates an explicit successful account reauthorization, not an edit
	// performed by recovery. Same domain/epoch, new sealed credential.
	sealed, err := f.m.sealUpstreamCredential(f.account.Domain, f.account.Provider, sub2APICredential{AccessToken: "new-synthetic", ExpiresAt: now + 3600})
	if err != nil {
		t.Fatal(err)
	}
	f.account.Credential = sealed
	if err := f.m.storeDB.Save(&f.account).Error; err != nil {
		t.Fatal(err)
	}
	f.m.upstreamClient = client
	if c := sub2RecoveryCandidate(t, f, now+60); c.DayTs != f.day {
		t.Fatal("new authorization did not reactivate", c)
	}
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now+60); err != nil {
		t.Fatal(err)
	}
	if w := sub2RecoveryJob(t, f); w.Status != "waiting_second_scan" || w.Attempts != 0 {
		t.Fatal(w)
	}
}

func TestSub2TokenRecoveryScheduleToPublishedMoney(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprint(shared), func(t *testing.T) {
			f, now := sub2TokenDayFixture(t, shared)
			tokenPipelineStore(t, f)
			if err := f.m.storeDB.AutoMigrate(&ChannelUpstreamPricingSyncState{}, &UpstreamHostCircuit{}); err != nil {
				t.Fatal(err)
			}
			f.m.cfg.UpstreamPricingLedgerEnabled = true
			f.m.cfg.UpstreamPricingLedgerDomains = []string{f.account.Domain}
			f.m.cfg.UpstreamCredentialSecret = "synthetic-e2e"
			f.account.Enabled, f.account.UsageSyncEnabled = true, true
			sealed, err := f.m.sealUpstreamCredential(f.account.Domain, f.account.Provider, sub2APICredential{AccessToken: "synthetic", ExpiresAt: now + 86400})
			if err != nil {
				t.Fatal(err)
			}
			f.account.Credential, f.account.CredentialVersion = sealed, upstreamCredentialVersion
			if err := f.m.storeDB.Save(&f.account).Error; err != nil {
				t.Fatal(err)
			}
			state := ChannelUpstreamPricingSyncState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), SemanticsVersion: upstreamPricingSemanticsVersion, BackfillNextHour: f.day + 86400, BackfillDone: true, TailNextSyncAt: now + 3600, Status: "ok"}
			if err := f.m.storeDB.Create(&state).Error; err != nil {
				t.Fatal(err)
			}
			if err := f.m.storeDB.Where("domain=? AND day_ts=?", f.account.Domain, f.day).Delete(&ChannelUpstreamTokenScanCheckpoint{}).Error; err != nil {
				t.Fatal(err)
			}
			_, items, _, _ := sub2TokenFixture(t)
			var wire []map[string]any
			for _, item := range items {
				wire = append(wire, map[string]any{"id": item.ID, "api_key_id": item.UpstreamTokenID, "created_at": time.Unix(item.CreatedAt+86400, 0).Format(time.RFC3339), "model": item.ModelName, "input_tokens": item.PromptTokens, "output_tokens": item.CompletionTokens, "actual_cost": fmt.Sprintf("%d.%06d", item.ActualCostMicros/1000000, item.ActualCostMicros%1000000), "rate_multiplier": item.EffectiveRatio.Text, "group": map[string]any{"name": item.SourceGroup}})
			}
			body, err := json.Marshal(map[string]any{"data": map[string]any{"items": wire, "total": len(wire), "page": 1, "page_size": 100, "pages": 1}})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			transport := upstreamRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body)), Request: req}, nil
			})
			guard := newUpstreamHostGuard(nil, upstreamHostGuardOptions{Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0})
			f.m.upstreamClient = installUpstreamHostGuardForTest(&http.Client{Transport: transport}, nil, guard)
			legacy, money := tokenDayLegacySnapshot(t, f), tokenPublishedSnapshot(t, f)
			// Discovery may first validate the older pre-activation proof day;
			// follow the real fair queue, not a hard-coded repeated target.
			for i := 0; i < 4; i++ {
				at := now + int64(i)*60
				candidate, err := f.m.nextSub2TokenRecovery(context.Background(), f.account, state, at)
				if err != nil {
					t.Fatal(err)
				}
				if candidate.DayTs == 0 {
					continue
				}
				if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, candidate.DayTs, at); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 4 || tokenPublishedSnapshot(t, f) != money {
				t.Fatal("recovery bypassed accounting pipeline", calls)
			}
			for i := 0; i < 10; i++ {
				at := now + 300 + int64(i)*60
				f.m.syncDueUpstreamTokenWorkAt(context.Background(), at)
				f.m.syncDueChannelEconomicsAt(context.Background(), at)
			}
			tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 23)
			var rows []ChannelEconomicsHourPublication
			if err := f.m.storeDB.Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			var cost, corrected, revenue int64
			channels := map[int]int64{}
			for _, r := range rows {
				cost += r.UpstreamChargeUnits
				corrected += r.CorrectedCostMicroUSD
				revenue += r.RevenueMicroUSD
				channels[r.LocalChannelID] += r.UpstreamChargeUnits
				if shared && r.ProfitKnown {
					t.Fatal("shared profit falsely precise")
				}
			}
			wantCorrected := int64(48149)
			wantChannels := map[int]int64{185: 246912, 186: 234567}
			if shared {
				wantCorrected = 48148
				wantChannels = map[int]int64{0: 481479, 185: 0, 186: 0}
			}
			if cost != 481479 || corrected != wantCorrected || revenue != 3000000 || !reflect.DeepEqual(channels, wantChannels) || tokenDayLegacySnapshot(t, f) != legacy {
				t.Fatal("recovered money not conserved", cost, corrected, revenue, channels)
			}
			stable := tokenPublishedSnapshot(t, f)
			f.m.syncDueUpstreamTokenWorkAt(context.Background(), now+1200)
			f.m.syncDueChannelEconomicsAt(context.Background(), now+1200)
			if tokenPublishedSnapshot(t, f) != stable {
				t.Fatal("repeat publication duplicated recovered cost")
			}
		})
	}
}

func sub2RecoveryJob(t *testing.T, f *sub2ShadowHTTPFixture) ChannelUpstreamTokenWorkState {
	t.Helper()
	var w ChannelUpstreamTokenWorkState
	if err := upstreamTokenWorkScope(f.m.storeDB, ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "scan_day", DayTs: f.day}).Take(&w).Error; err != nil {
		t.Fatal(err)
	}
	return w
}

func sub2RecoveryCandidate(t *testing.T, f *sub2ShadowHTTPFixture, now int64) sub2TokenRecoveryCandidate {
	t.Helper()
	var state ChannelUpstreamPricingSyncState
	if err := f.m.storeDB.Where("domain=?", f.account.Domain).Take(&state).Error; err != nil {
		t.Fatal(err)
	}
	c, err := f.m.nextSub2TokenRecovery(context.Background(), f.account, state, now)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSub2TokenRecoverySchedulePersistsProgressAndCooldown(t *testing.T) {
	f, now := sub2RecoveryScheduleFixture(t)
	before := sub2RecoveryProtected(t, f)
	if got := sub2RecoveryCandidate(t, f, now); got.DayTs != f.day {
		t.Fatal(got)
	}
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now); err != nil {
		t.Fatal(err)
	}
	w := sub2RecoveryJob(t, f)
	if w.Status != "waiting_second_scan" || w.NextAttemptAt != now+60 || w.LeaseUntil != 0 || w.Attempts != 0 {
		t.Fatal(w)
	}
	f.m = &Monitor{cfg: f.m.cfg, storeDB: f.m.storeDB, upstreamClient: f.m.upstreamClient}
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now+1); err != nil {
		t.Fatal(err)
	}
	if f.requests.Load() != 3 || sub2RecoveryJob(t, f) != w || sub2RecoveryCandidate(t, f, now+1).DayTs != 0 {
		t.Fatal("restart bypassed cooldown")
	}
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now+60); err != nil {
		t.Fatal(err)
	}
	w = sub2RecoveryJob(t, f)
	if w.Status != "observed" || w.NextAttemptAt != now+60+upstreamTokenCostRecheck || f.requests.Load() != 6 {
		t.Fatal(w, f.requests.Load())
	}
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, w.NextAttemptAt); err != nil {
		t.Fatal(err)
	}
	if f.requests.Load() != 6 || sub2RecoveryProtected(t, f) != before {
		t.Fatal("completed recovery refetched or rewrote legacy")
	}
}

func TestSub2TokenRecoveryScheduleFailureBackoffAndRepair(t *testing.T) {
	f, now := sub2RecoveryScheduleFixture(t)
	f.missing.Store(true)
	before := sub2RecoveryProtected(t, f)
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now); err == nil {
		t.Fatal("missing identity accepted")
	}
	w := sub2RecoveryJob(t, f)
	if w.Status != "retry" || w.Attempts != 1 || w.NextAttemptAt <= now || w.LeaseUntil != 0 {
		t.Fatal(w)
	}
	calls := f.requests.Load()
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now+1); err != nil {
		t.Fatal(err)
	}
	if f.requests.Load() != calls {
		t.Fatal("failed scan hot loop")
	}
	f.missing.Store(false)
	if err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, w.NextAttemptAt); err != nil {
		t.Fatal(err)
	}
	if got := sub2RecoveryJob(t, f); got.Status != "waiting_second_scan" || got.Attempts != 0 {
		t.Fatal(got)
	}
	if sub2RecoveryProtected(t, f) != before {
		t.Fatal("recovery changed pricing failure state or money")
	}
}

func TestSub2TokenRecoveryScheduleClaimFailureAndPriority(t *testing.T) {
	for _, mode := range []string{"claim-failure", "normal-due", "normal-cooldown", "busy", "readonly", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			f, now := sub2RecoveryScheduleFixture(t)
			switch mode {
			case "claim-failure":
				if err := f.m.storeDB.Exec(`CREATE TRIGGER reject_recovery_claim BEFORE INSERT ON channel_upstream_token_work_states BEGIN SELECT RAISE(ABORT,'synthetic'); END`).Error; err != nil {
					t.Fatal(err)
				}
			case "normal-due":
				if err := f.m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain=?", f.account.Domain).UpdateColumn("tail_next_sync_at", 0).Error; err != nil {
					t.Fatal(err)
				}
			case "normal-cooldown":
				if err := f.m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain=?", f.account.Domain).UpdateColumn("status", upstreamStatusError).Error; err != nil {
					t.Fatal(err)
				}
			case "busy":
				release, err := f.m.tryAcquireUpstreamAccountBackground(f.account.Domain)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
			case "readonly":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			}
			before := sub2RecoveryProtected(t, f)
			err := f.m.syncStoredSub2TokenRecoveryAt(context.Background(), f.account.Domain, f.day, now)
			if mode == "busy" && !errors.Is(err, errUpstreamAccountBusy) {
				t.Fatal(err)
			}
			var count int64
			if err := f.m.storeDB.Model(&ChannelUpstreamTokenWorkState{}).Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			if f.requests.Load() != 0 || count != 0 || sub2RecoveryProtected(t, f) != before {
				t.Fatal("guard allowed work", mode)
			}
		})
	}
}

func TestSub2TokenRecoveryScheduleUsesExistingDispatcher(t *testing.T) {
	f, _ := sub2RecoveryScheduleFixture(t)
	now := time.Now().Unix()
	if err := f.m.storeDB.Model(&ChannelUpstreamTokenConfig{}).Where("domain=?", f.account.Domain).UpdateColumn("created_at", f.day-1).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain=?", f.account.Domain).UpdateColumn("tail_next_sync_at", now+3600).Error; err != nil {
		t.Fatal(err)
	}
	before := sub2RecoveryProtected(t, f)
	f.m.syncDueUpstreamPricing(context.Background())
	if f.requests.Load() != 3 || sub2RecoveryJob(t, f).Status != "waiting_second_scan" || sub2RecoveryProtected(t, f) != before {
		t.Fatal("dispatcher did not isolate recovery")
	}
	f.m.syncDueUpstreamPricing(context.Background())
	if f.requests.Load() != 3 {
		t.Fatal("dispatcher bypassed cooldown")
	}
}

func TestSub2TokenRecoveryScheduleLeaseAndRetryAfterFenced(t *testing.T) {
	f, now := sub2RecoveryScheduleFixture(t)
	key := ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "scan_day", DayTs: f.day}
	claim, ok, err := f.m.claimUpstreamTokenWork(context.Background(), key, 0, now)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if claim.LeaseUntil-now <= int64(upstreamPricingOperationTimeout(f.m.cfg)/time.Second) {
		t.Fatal("lease shorter than operation")
	}
	if sub2RecoveryCandidate(t, f, now+1).DayTs != 0 {
		t.Fatal("live lease remained due")
	}
	newClaim, ok, err := f.m.claimUpstreamTokenWork(context.Background(), key, 0, claim.LeaseUntil)
	if err != nil || !ok {
		t.Fatal("expired lease not reclaimed", err)
	}
	if err := f.m.finishUpstreamTokenWorkWithRetryAt(context.Background(), claim, 0, now, "observed", false, 0); err == nil {
		t.Fatal("stale finish accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.m.finishUpstreamTokenWorkWithRetryAt(ctx, newClaim, 0, now+60, "retry", true, now+3600); err != nil {
		t.Fatal(err)
	}
	w := sub2RecoveryJob(t, f)
	if w.NextAttemptAt != now+3600 || w.LeaseUntil != 0 || w.Attempts != 1 {
		t.Fatal(w)
	}
}
