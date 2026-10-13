package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestSub2CloseoutReauthorizationPreservesPinnedDate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/login":
			_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"test-access","refresh_token":"test-refresh","expires_in":3600}}`))
		case "/api/v1/user/profile":
			_, _ = w.Write([]byte(`{"code":0,"data":{"balance":88.5}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	domain := normalizeChannelBaseDomain(server.URL)
	if err := m.storeDB.Create(&ChannelSnap{ID: 2, Name: "sub2", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	usageEnabled := true
	payload := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderSub2API, BaseURL: server.URL, Email: "test@example.com", Password: "synthetic-password", UsageSyncEnabled: &usageEnabled}
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload); w.Code != http.StatusOK {
		t.Fatalf("initial save: %d %s", w.Code, w.Body.String())
	}
	day := cstDayStart(time.Now().Unix()) - 2*86400
	marker := sub2CloseoutProgressPrefix + time.Unix(day, 0).In(cstLocation).Format("2006-01-02")
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", domain).Updates(map[string]any{
		"usage_status": upstreamStatusReconnect, "usage_next_sync_at": upstreamAccountIsolatedUntil,
		"usage_backfill_next_sync_at": upstreamAccountIsolatedUntil, "usage_backfill_progress": marker,
		"usage_backfill_cursor": day, "usage_backfill_consecutive_fails": 3,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload); w.Code != http.StatusOK {
		t.Fatalf("reauthorization: %d %s", w.Code, w.Body.String())
	}
	var saved ChannelUpstreamAccount
	if err := m.storeDB.Where("domain=?", domain).First(&saved).Error; err != nil {
		t.Fatal(err)
	}
	if saved.UsageBackfillProgress != marker || saved.UsageBackfillCursor != day || saved.UsageBackfillNextSyncAt != 0 || saved.UsageBackfillConsecutiveFails != 0 {
		t.Fatalf("reauthorization lost task or retained isolation: %+v", saved)
	}
}

func TestSub2CloseoutDurableBeforeCanceledRequest(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var saved ChannelUpstreamAccount
		err := m.storeDB.First(&saved).Error
		observed <- err == nil && hasSub2PendingCloseout(saved)
		cancel()
		<-r.Context().Done()
	}))
	defer server.Close()
	now := time.Now().Unix()
	day := cstDayStart(now) - 86400
	a := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), BaseURL: server.URL, Provider: upstreamProviderSub2API, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600, UsageBackfillDone: true, UsageBackfillCursor: day + 86400}
	if err := m.persistSyncedUpstreamAccount(ctx, &a, sub2APICredential{AccessToken: "bill-test-access", ExpiresAt: now + 3600}); err != nil {
		t.Fatal(err)
	}
	h := ChannelUpstreamUsageHour{Domain: a.Domain, Provider: a.Provider, HourTs: day, BucketSeconds: 3600, Provisional: true, CostUSD: 7}
	if err := m.storeDB.Create(&h).Error; err != nil {
		t.Fatal(err)
	}
	_, err := m.syncStoredUpstreamUsageLaneBackground(ctx, a.Domain, upstreamUsageLaneHistory)
	if err == nil {
		t.Fatal("canceled request succeeded")
	}
	select {
	case pinned := <-observed:
		if !pinned {
			t.Fatal("network started before durable pin")
		}
	default:
		t.Fatal("request never reached test upstream")
	}
	var saved ChannelUpstreamAccount
	if err := m.storeDB.First(&saved).Error; err != nil {
		t.Fatal(err)
	}
	got, err := sub2PendingCloseoutDay(saved, now+86400)
	if err != nil || got != day {
		t.Fatalf("cancel/restart lost original date: %d %v", got, err)
	}
	var savedBill ChannelUpstreamUsageHour
	if err := m.storeDB.First(&savedBill).Error; err != nil || !reflect.DeepEqual(savedBill, h) {
		t.Fatalf("cancellation changed bill: %+v %v", savedBill, err)
	}
}

func TestSub2CloseoutPinnedRetryAcrossMidnights(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	ctx := context.Background()
	now := cstDayStart(time.Now().Unix()) + 12*3600
	day := cstDayStart(now) - 86400
	a := ChannelUpstreamAccount{Domain: "pinned.example", Provider: upstreamProviderSub2API, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageBackfillDone: true, UsageBackfillCursor: day + 86400}
	if err := m.storeDB.Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	h := ChannelUpstreamUsageHour{Domain: a.Domain, Provider: a.Provider, HourTs: day, BucketSeconds: 3600, CostUSD: 7, Provisional: true}
	if err := m.storeDB.Create(&h).Error; err != nil {
		t.Fatal(err)
	}
	plan := upstreamUsageSyncPlan{backfillFrom: day, backfillTo: day + 86400, recentOnly: true}
	if err := m.pinSub2Closeout(ctx, &a, plan); err != nil {
		t.Fatal(err)
	}
	marker := a.UsageBackfillProgress
	for _, delta := range []int64{0, 86400, 2 * 86400} {
		// Reload durable state as a restarted worker would, even if the tail
		// reopened history since the pin was created.
		if err := m.storeDB.Where("domain=?", a.Domain).First(&a).Error; err != nil {
			t.Fatal(err)
		}
		a.UsageBackfillDone = delta == 0
		plan = planUpstreamUsageSyncForLane(a, now+delta, 7, upstreamUsageLaneHistory)
		got, err := m.prioritizeSub2RecentUsage(ctx, a, now+delta, plan)
		if err != nil || got.backfillFrom != day || got.backfillTo != day+86400 || !got.recentOnly {
			t.Fatalf("date moved: %+v %v", got, err)
		}
		applyUpstreamUsageBackfillResult(&a, errors.New("temporary test failure"), now+delta, m.cfg)
		if a.UsageBackfillProgress != marker {
			t.Fatal("failed retry discarded date")
		}
		if err := m.persistUpstreamAccount(ctx, &a); err != nil {
			t.Fatal(err)
		}
		for _, at := range []int64{now + delta + 1, a.UsageBackfillNextSyncAt + 1} {
			due, err := m.loadDueUpstreamUsageAccountsForLane(ctx, at, 1, upstreamUsageLaneHistory)
			if err != nil || (len(due) == 1) != (at > a.UsageBackfillNextSyncAt) {
				t.Fatalf("backoff ignored: %d %+v %v", at, due, err)
			}
		}
	}
	var saved ChannelUpstreamUsageHour
	if err := m.storeDB.First(&saved).Error; err != nil || !reflect.DeepEqual(saved, h) {
		t.Fatalf("retry changed bill: %+v %v", saved, err)
	}
}

func TestSub2CloseoutRecoveryWithoutRemoteReplay(t *testing.T) {
	for _, kind := range []string{"already_committed", "invalid_date", "future_date", "missing_bill"} {
		t.Run(kind, func(t *testing.T) {
			server, paths, mu := sub2BillFixture(t, map[string]any{}, 0)
			m := newChannelUpstreamTestMonitor(t)
			ctx := context.Background()
			now := time.Now().Unix()
			day := cstDayStart(now) - 2*86400
			a := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), BaseURL: server.URL, Provider: upstreamProviderSub2API, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600, UsageBackfillDone: true, UsageBackfillCursor: day + 86400, UsageBackfillProgress: sub2CloseoutProgressPrefix + time.Unix(day, 0).In(cstLocation).Format("2006-01-02")}
			if kind == "invalid_date" {
				a.UsageBackfillProgress = sub2CloseoutProgressPrefix + "invalid"
			}
			if kind == "future_date" {
				a.UsageBackfillProgress = sub2CloseoutProgressPrefix + time.Unix(cstDayStart(now)+86400, 0).In(cstLocation).Format("2006-01-02")
			}
			if err := m.persistSyncedUpstreamAccount(ctx, &a, sub2APICredential{AccessToken: "test-access", ExpiresAt: now + 3600}); err != nil {
				t.Fatal(err)
			}
			h := ChannelUpstreamUsageHour{Domain: a.Domain, Provider: a.Provider, HourTs: day, BucketSeconds: 86400, SourceKind: sub2VerifiedDaySource, FetchedAt: day + 86401, CostUSD: 7, Quota: 7}
			if kind != "missing_bill" {
				if err := m.storeDB.Create(&h).Error; err != nil {
					t.Fatal(err)
				}
			}
			got, err := m.syncStoredUpstreamUsageLaneBackground(ctx, a.Domain, upstreamUsageLaneHistory)
			if (err != nil) != (kind != "already_committed") {
				t.Fatalf("unexpected result: %+v %v", got, err)
			}
			var saved ChannelUpstreamAccount
			if err := m.storeDB.Where("domain=?", a.Domain).First(&saved).Error; err != nil {
				t.Fatal(err)
			}
			if saved.UsageBackfillCursor != a.UsageBackfillCursor {
				t.Fatal("recovery moved historical cursor")
			}
			if kind == "already_committed" {
				if saved.UsageBackfillProgress != "" {
					t.Fatal("verified date not acknowledged")
				}
			} else if saved.UsageBackfillProgress != a.UsageBackfillProgress || saved.UsageBackfillNextSyncAt <= now || saved.UsageBackfillConsecutiveFails != 1 {
				t.Fatalf("lost pin/backoff: %+v", saved)
			}
			mu.Lock()
			calls := len(*paths)
			mu.Unlock()
			if calls != 0 {
				t.Fatalf("recovery made %d unnecessary network requests", calls)
			}
			if kind != "missing_bill" {
				var savedBill ChannelUpstreamUsageHour
				if err := m.storeDB.First(&savedBill).Error; err != nil || !reflect.DeepEqual(savedBill, h) {
					t.Fatalf("recovery changed bill: %+v %v", savedBill, err)
				}
			}
		})
	}
}

func TestSub2CloseoutPinCancellationAndConflict(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	ctx := context.Background()
	day := cstDayStart(time.Now().Unix()) - 86400
	a := ChannelUpstreamAccount{Domain: "pin.example", Provider: upstreamProviderSub2API}
	if err := m.storeDB.Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	stale := a
	plan := upstreamUsageSyncPlan{backfillFrom: day, backfillTo: day + 86400, recentOnly: true}
	if err := m.pinSub2Closeout(ctx, &a, plan); err != nil {
		t.Fatal(err)
	}
	applyUpstreamUsageBackfillResult(&a, context.Canceled, time.Now().Unix(), m.cfg)
	var saved ChannelUpstreamAccount
	if err := m.storeDB.First(&saved).Error; err != nil || saved.UsageBackfillProgress != a.UsageBackfillProgress || !hasSub2PendingCloseout(saved) {
		t.Fatalf("cancellation lost pin: %+v %v", saved, err)
	}
	if err := m.pinSub2Closeout(ctx, &stale, plan); err == nil {
		t.Fatal("stale worker overwrote persisted progress")
	}
	plan.backfillFrom -= 86400
	plan.backfillTo -= 86400
	if err := m.pinSub2Closeout(ctx, &a, plan); err == nil {
		t.Fatal("allowed pending date replacement")
	}
}
