package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func orionCheckpointFixture(t *testing.T, from int64, count int) (*httptest.Server, *[]int) {
	t.Helper()
	calls := []int{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := 0
		if cursor := r.URL.Query().Get("cursor"); cursor != "" {
			var err error
			page, err = strconv.Atoi(cursor)
			if err != nil {
				t.Error(err)
			}
		}
		calls = append(calls, page)
		items := []any{}
		for i := page * 100; i < min((page+1)*100, count); i++ {
			items = append(items, orionItem(fmt.Sprintf("raw-request-%d", i), time.Unix(from+10, 0), 0.125, 1, 1))
		}
		out := map[string]any{"items": items, "next_cursor": nil}
		if (page+1)*100 < count {
			out["next_cursor"] = strconv.Itoa(page + 1)
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(s.Close)
	return s, &calls
}

func TestOrionCheckpointDenseHourResumesWithoutPartialPublication(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Now().Unix()/3600*3600 - 2*3600
	server, calls := orionCheckpointFixture(t, from, 5100)
	row := ChannelUpstreamAccount{Domain: "orion.test", BaseURL: server.URL, Provider: upstreamProviderOrion, UserID: 42,
		Enabled: true, UsageSyncEnabled: true, CredentialVersion: upstreamCredentialVersion, UsageStatus: upstreamStatusError, UsageDataUntil: from, UsageBackfillDone: true}
	cred := orionCredential{AccessToken: "smk_fixture"}
	var err error
	row.Credential, err = m.sealUpstreamCredential(row.Domain, row.Provider, cred)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	old := ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: from, BucketSeconds: 600, CostUSD: 7, Provider: row.Provider}
	if err = m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	first, err := m.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneTail)
	if err != nil || first.UsageDataUntil != from || first.UsageStatus != upstreamStatusPending || first.UsageLastSuccessAt != 0 {
		t.Fatalf("partial run changed public state: status=%s until=%d last_success=%d err=%v", first.UsageStatus, first.UsageDataUntil, first.UsageLastSuccessAt, err)
	}
	var cp OrionUsageCheckpoint
	if err = m.storeDB.First(&cp).Error; err != nil || cp.NextPage != 51 || cp.Complete {
		t.Fatalf("checkpoint=%+v err=%v", cp, err)
	}
	var bill ChannelUpstreamUsageHour
	if err = m.storeDB.First(&bill, "domain = ?", row.Domain).Error; err != nil || bill.CostUSD != 7 {
		t.Fatal("partial read replaced old bill", err)
	}
	var staged OrionUsagePage
	if err = m.storeDB.First(&staged).Error; err != nil || strings.Contains(staged.ItemsJSON, "raw-request-") {
		t.Fatal("staging leaked raw request identity", err)
	}
	// Actually close/reopen SQLite, not merely reuse an in-memory object.
	if err = m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).Update("usage_next_sync_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	var databases []struct{ Name, File string }
	if err = m.storeDB.Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		t.Fatal(err)
	}
	filename := ""
	for _, database := range databases {
		if database.Name == "main" {
			filename = database.File
		}
	}
	if filename == "" {
		t.Fatal("main database path unavailable")
	}
	connection, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := gorm.Open(sqlite.Open(filename), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if connection, closeErr := reopened.DB(); closeErr == nil {
			_ = connection.Close()
		}
	})
	restarted := &Monitor{cfg: m.cfg, storeDB: reopened, upstreamCredentialPersistent: true}
	restarted.upstreamClient = newGuardedTestClient(server.Client().Transport, reopened, &fakeUpstreamGuardClock{now: time.Now()}, 0, 0)
	second, err := restarted.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneTail)
	if err != nil || second.UsageDataUntil != from+3600 || second.UsageStatus != upstreamStatusOK {
		t.Fatalf("resume status=%s until=%d err=%v", second.UsageStatus, second.UsageDataUntil, err)
	}
	if err = reopened.First(&bill, "domain = ?", row.Domain).Error; err != nil || bill.Requests != 5100 || bill.CostUSD != 637.5 || bill.BucketSeconds != 3600 {
		t.Fatalf("final bill=%+v err=%v", bill, err)
	}
	if len(*calls) != 102 || (*calls)[50] != 50 || (*calls)[51] != 0 || (*calls)[101] != 50 {
		t.Fatalf("resume did not continue at saved cursor: calls=%v", *calls)
	}
	// A replay replaces the same window, never adds its money twice.
	result, yielded, err := restarted.syncOrionUsageCheckpoint(context.Background(), row, cred, "tail", from, from+3600, newUpstreamUsageRequestPacer(2, 0))
	if err != nil || yielded || result.Hours[0].CostUSD != 637.5 {
		t.Fatalf("replay yielded=%v err=%v result=%+v", yielded, err, result)
	}
}

func TestOrionCheckpointPinsOpenEndAndSeparatesHistory(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Now().Unix()/3600*3600 - 3600
	server, _ := orionCheckpointFixture(t, from, 210)
	row := ChannelUpstreamAccount{Domain: "pin.test", Provider: upstreamProviderOrion, BaseURL: server.URL}
	cred := orionCredential{AccessToken: "smk_fixture"}
	_, yielded, err := m.syncOrionUsageCheckpoint(context.Background(), row, cred, "tail", from, from+600, newUpstreamUsageRequestPacer(1, 0))
	if err != nil || !yielded {
		t.Fatal(yielded, err)
	}
	result, yielded, err := m.syncOrionUsageCheckpoint(context.Background(), row, cred, "tail", from, from+900, newUpstreamUsageRequestPacer(6, 0))
	if err != nil || yielded || result.DataUntil != from+600 || result.Hours[0].Requests != 210 {
		t.Fatalf("moving end invalidated pending read: result=%+v yield=%v err=%v", result, yielded, err)
	}
	row.UsageDataUntil = result.DataUntil
	result, yielded, err = m.syncOrionUsageCheckpoint(context.Background(), row, cred, "tail", from, from+900, newUpstreamUsageRequestPacer(6, 0))
	if err != nil || yielded || result.DataUntil != from+900 {
		t.Fatal("published open hour never advanced", result.DataUntil, yielded, err)
	}
	_, yielded, err = m.syncOrionUsageCheckpoint(context.Background(), row, cred, "history", from, from+3600, newUpstreamUsageRequestPacer(1, 0))
	if err != nil || !yielded {
		t.Fatal(yielded, err)
	}
	var count int64
	if err = m.storeDB.Model(&OrionUsageCheckpoint{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatal("tail/history collided", count, err)
	}
}

func TestOrionCheckpointAtomicPageCommitRollsBackOnFailure(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Now().Unix()/3600*3600 - 3600
	server, _ := orionCheckpointFixture(t, from, 110)
	row := ChannelUpstreamAccount{Domain: "atomic.test", Provider: upstreamProviderOrion, BaseURL: server.URL}
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("orion_test_fail", func(tx *gorm.DB) {
		if tx.Statement.Schema != nil && tx.Statement.Schema.Name == "OrionUsageCheckpoint" {
			_ = tx.AddError(errors.New("synthetic disk write failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Create().Remove("orion_test_fail") })
	_, _, err := m.syncOrionUsageCheckpoint(context.Background(), row, orionCredential{AccessToken: "smk_fixture"}, "tail", from, from+3600, newUpstreamUsageRequestPacer(2, 0))
	if err == nil {
		t.Fatal("write failure hidden")
	}
	var count int64
	if err = m.storeDB.Model(&OrionUsagePage{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("page escaped failed checkpoint transaction", count, err)
	}
}

func TestOrionCheckpointRejectsDuplicateAndChangedSource(t *testing.T) {
	for _, scenario := range []string{"duplicate", "changed", "expired"} {
		t.Run(scenario, func(t *testing.T) {
			m := newChannelUpstreamTestMonitor(t)
			from := time.Now().Unix()/3600*3600 - 3600
			changed := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				cursor := r.URL.Query().Get("cursor")
				if cursor != "" && scenario == "expired" {
					w.WriteHeader(http.StatusGone)
					return
				}
				id := "r1"
				if cursor != "" && scenario != "duplicate" {
					id = "r2"
				} else if cursor == "" && changed && scenario == "changed" {
					id = "new-first"
				}
				page := map[string]any{"items": []any{orionItem(id, time.Unix(from+1, 0), 1, 1, 1)}, "next_cursor": nil}
				if cursor == "" {
					page["next_cursor"] = "next"
				}
				_ = json.NewEncoder(w).Encode(page)
			}))
			defer server.Close()
			row := ChannelUpstreamAccount{Domain: "bad.test", Provider: upstreamProviderOrion, BaseURL: server.URL}
			cred := orionCredential{AccessToken: "smk_fixture"}
			_, yielded, err := m.syncOrionUsageCheckpoint(context.Background(), row, cred, "tail", from, from+3600, newUpstreamUsageRequestPacer(1, 0))
			if err != nil || !yielded {
				t.Fatal(yielded, err)
			}
			changed = true
			result, yielded, err := m.syncOrionUsageCheckpoint(context.Background(), row, cred, "tail", from, from+3600, newUpstreamUsageRequestPacer(4, 0))
			if scenario == "changed" {
				if err != nil || !yielded || len(result.Hours) != 0 {
					t.Fatal("changed source was published", yielded, err)
				}
			} else if err == nil || len(result.Hours) != 0 {
				t.Fatal("bad source was published", err)
			}
		})
	}
}

func TestOrionCheckpointDetectsMiddlePageChangeWithUnchangedFirstPage(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Now().Unix()/3600*3600 - 3600
	secondReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		cost, id := 1.0, "first"
		page := map[string]any{"next_cursor": "second"}
		if cursor != "" {
			secondReads++
			id = "second"
			page["next_cursor"] = nil
			if secondReads > 1 {
				cost = 2
			}
		}
		page["items"] = []any{orionItem(id, time.Unix(from+1, 0), cost, 1, 1)}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "middle.test", Provider: upstreamProviderOrion, BaseURL: server.URL}
	result, yielded, err := m.syncOrionUsageCheckpoint(context.Background(), row, orionCredential{AccessToken: "smk_fixture"}, "tail", from, from+3600, newUpstreamUsageRequestPacer(6, 0))
	if err != nil || !yielded || len(result.Hours) != 0 || secondReads != 2 {
		t.Fatal("unchanged first page hid middle-page correction", yielded, secondReads, err)
	}
	var count int64
	if err = m.storeDB.Model(&OrionUsageCheckpoint{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("changed source checkpoint not reset", count, err)
	}
}

func TestOrionCheckpointPreservesProgressOn429(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Now().Unix()/3600*3600 - 3600
	limited := true
	base := upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("cursor") != "" && limited {
			return recoveryHTTPResponse(429, `{"message":"limited"}`), nil
		}
		id, next := "first", `"next"`
		if r.URL.Query().Get("cursor") != "" {
			id, next = "second", "null"
		}
		item, _ := json.Marshal(orionItem(id, time.Unix(from+1, 0), 1, 1, 1))
		return recoveryHTTPResponse(200, `{"items":[`+string(item)+`],"next_cursor":`+next+`}`), nil
	})
	m.upstreamClient = &http.Client{Transport: base}
	row := ChannelUpstreamAccount{Domain: "limited-orion.test", Provider: upstreamProviderOrion, BaseURL: "https://limited-orion.test"}
	cred := orionCredential{AccessToken: "smk_fixture"}
	_, _, err := m.syncOrionUsageCheckpoint(context.Background(), row, cred, "tail", from, from+3600, newUpstreamUsageRequestPacer(6, 0))
	var status *upstreamHTTPError
	if !errors.As(err, &status) || status.Status != 429 {
		t.Fatal("429 hidden", err)
	}
	var cp OrionUsageCheckpoint
	if err = m.storeDB.First(&cp).Error; err != nil || cp.NextPage != 2 {
		t.Fatal("429 lost saved page", cp.NextPage, err)
	}
	// Replace only this fixture's transport/guard to represent cooldown expiry.
	limited = false
	m.upstreamClient = newGuardedTestClient(base, m.storeDB, &fakeUpstreamGuardClock{now: time.Now().Add(time.Hour)}, 0, 0)
	result, yielded, err := m.syncOrionUsageCheckpoint(context.Background(), row, cred, "tail", from, from+3600, newUpstreamUsageRequestPacer(6, 0))
	if err != nil || yielded || result.Hours[0].Requests != 2 {
		t.Fatal("429 resume failed", yielded, err)
	}
}
