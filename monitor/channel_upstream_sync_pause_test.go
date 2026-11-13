package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"
)

// Automatic sync pauses only for a domain marked 停止使用 that has no enabled
// channel; each condition alone keeps syncing, and the pause lifts by itself.
func TestUpstreamSyncPausesOnlyForRetiredDomainWithoutEnabledChannel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":500000}}`))
		case "/api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	m := newChannelUpstreamTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	const (
		paused      = "paused.example"       // 停止使用 + no enabled channel
		retiredLive = "retired-live.example" // 停止使用 + one enabled channel
		offOnly     = "off-only.example"     // no enabled channel, not marked
		normal      = "normal.example"
		recent      = "recent.example" // 停止使用 + no enabled channel, but traffic within the grace period
	)
	channels := []ChannelSnap{
		{ID: 1, BaseDomain: paused, Status: 2, UpdatedAt: now},
		{ID: 2, BaseDomain: paused, Status: 3, UpdatedAt: now},
		// A deleted channel is no longer in NewAPI; its last status does not count.
		{ID: 3, BaseDomain: paused, Status: 1, UpdatedAt: now, DeletedAt: now},
		{ID: 4, BaseDomain: retiredLive, Status: 2, UpdatedAt: now},
		{ID: 5, BaseDomain: retiredLive, Status: 1, UpdatedAt: now},
		{ID: 6, BaseDomain: offOnly, Status: 2, UpdatedAt: now},
		{ID: 7, BaseDomain: normal, Status: 1, UpdatedAt: now},
		{ID: 8, BaseDomain: recent, Status: 2, UpdatedAt: now},
	}
	if err := m.storeDB.Create(&channels).Error; err != nil {
		t.Fatal(err)
	}
	// A stale sample of the paused domain is outside the grace period; the
	// recently used one must keep syncing until its final usage is imported.
	samples := []StabilityHourSample{
		{HourTs: now - now%3600 - int64(upstreamSyncPauseGraceSeconds) - 7200, ChannelID: 1, ModelName: "m", Grp: "g", Success: 5},
		{HourTs: now - now%3600 - 3600, ChannelID: 8, ModelName: "m", Grp: "g", Success: 3},
	}
	if err := m.storeDB.Create(&samples).Error; err != nil {
		t.Fatal(err)
	}
	for _, domain := range []string{paused, retiredLive, recent} {
		if err := m.storeDB.Create(&ChannelUpstreamRetirement{Domain: domain, Retiring: true, UpdatedAt: now}).Error; err != nil {
			t.Fatal(err)
		}
	}
	domains := []string{normal, offOnly, paused, recent, retiredLive}
	for i, domain := range domains {
		row := ChannelUpstreamAccount{Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: int64(i + 1),
			Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK}
		sealed, err := m.sealUpstreamCredential(domain, row.Provider, newAPICredential{AccessToken: "token"})
		if err != nil {
			t.Fatal(err)
		}
		row.Credential, row.CredentialVersion = sealed, upstreamCredentialVersion
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	unpaused := []string{normal, offOnly, recent, retiredLive}

	got, err := m.upstreamSyncPausedDomains(ctx)
	if err != nil || len(got) != 1 || !got[paused] {
		t.Fatalf("paused domains=%v err=%v, want only %s", got, err, paused)
	}
	views, err := m.loadChannelUpstreamViews(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, domain := range domains {
		if views[domain].SyncPaused != (domain == paused) {
			t.Fatalf("%s view sync_paused=%v", domain, views[domain].SyncPaused)
		}
	}

	loaded := func(lane upstreamUsageLane) []string {
		t.Helper()
		rows, err := m.loadDueUpstreamUsageAccountsForLane(ctx, now, 50, lane)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.Domain)
		}
		sort.Strings(out)
		return out
	}
	same := func(got, want []string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}
	if tail := loaded(upstreamUsageLaneTail); !same(tail, unpaused) {
		t.Fatalf("usage tail candidates=%v want %v", tail, unpaused)
	}
	if history := loaded(upstreamUsageLaneHistory); !same(history, unpaused) {
		t.Fatalf("usage history candidates=%v want %v", history, unpaused)
	}
	// Error logs, funds and pricing select accounts through the same fragment.
	var selected []string
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("enabled = ? AND usage_sync_enabled = ?", true, true).
		Where(upstreamSyncNotPausedSQL).Order("domain ASC").Pluck("domain", &selected).Error; err != nil || !same(selected, unpaused) {
		t.Fatalf("log sync candidates=%v err=%v want %v", selected, err, unpaused)
	}

	m.syncDueUpstreamAccounts(ctx)
	for _, domain := range domains {
		var row ChannelUpstreamAccount
		if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
			t.Fatal(err)
		}
		if attempted := row.LastAttemptAt != 0; attempted != (domain != paused) {
			t.Fatalf("%s balance attempted=%v status=%s", domain, attempted, row.Status)
		}
	}

	// Enabling any channel resumes automatically.
	if err := m.storeDB.Model(&ChannelSnap{}).Where("id = ?", 1).Update("status", 1).Error; err != nil {
		t.Fatal(err)
	}
	if tail := loaded(upstreamUsageLaneTail); !same(tail, domains) {
		t.Fatalf("enabled channel must resume sync, candidates=%v", tail)
	}
	// So does clearing the mark while every channel stays disabled.
	if err := m.storeDB.Model(&ChannelSnap{}).Where("id = ?", 1).Update("status", 2).Error; err != nil {
		t.Fatal(err)
	}
	if tail := loaded(upstreamUsageLaneTail); !same(tail, unpaused) {
		t.Fatalf("disabled again must pause, candidates=%v", tail)
	}
	if err := m.storeDB.Model(&ChannelUpstreamRetirement{}).Where("domain = ?", paused).Update("retiring", false).Error; err != nil {
		t.Fatal(err)
	}
	if tail := loaded(upstreamUsageLaneTail); !same(tail, domains) {
		t.Fatalf("cleared mark must resume sync, candidates=%v", tail)
	}
	if got, err := m.upstreamSyncPausedDomains(ctx); err != nil || len(got) != 0 {
		t.Fatalf("no domain should stay paused: %v err=%v", got, err)
	}
}
