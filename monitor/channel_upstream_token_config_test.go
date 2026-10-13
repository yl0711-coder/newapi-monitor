package monitor

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func tokenConfigFixture(t *testing.T) (*Monitor, ChannelUpstreamAccount) {
	t.Helper()
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelUpstreamAccount{}, &ChannelSnap{}, &ChannelUpstreamTokenConfig{}, &ChannelFinanceVersion{}, &ChannelDomainCost{}, &ChannelFinanceChannelCost{}); err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: "sub2.example", Provider: upstreamProviderSub2API,
		BaseURL: "https://sub2.example", Account: "test-account", UserID: 31, Credential: "secret-not-for-response"}
	if err := db.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&[]ChannelSnap{
		{ID: 185, BaseDomain: account.Domain, Name: "same-name", Groups: "group-a,group-b", Status: 1},
		{ID: 186, BaseDomain: account.Domain, Name: "same-name", Groups: "group-c", Status: 2},
		{ID: 187, BaseDomain: "other.example", Groups: "group-a", Status: 1},
	}).Error; err != nil {
		t.Fatal(err)
	}
	return &Monitor{storeDB: db}, account
}

func tokenConfigInput(account ChannelUpstreamAccount, channel int, id string, rev int64) upstreamTokenConfigInput {
	return upstreamTokenConfigInput{Domain: account.Domain, ChannelID: channel,
		AccountEpoch: newAPIUpstreamAccountEpoch(account), TokenID: &id, ExpectedRevision: rev}
}

func TestUpstreamTokenConfigSharedIntentUsesOnlyCurrentAccountRevisions(t *testing.T) {
	m, account := tokenConfigFixture(t)
	ctx := context.Background()
	for _, channel := range []int{185, 186} {
		if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, channel, "575", 0), "fixture", 100); err != nil {
			t.Fatal(err)
		}
	}
	view, err := m.loadUpstreamTokenConfig(ctx, account.Domain, 185)
	if err != nil || !reflect.DeepEqual(view.SharedChannelIDs, []int{185, 186}) || view.AttributionVerified {
		t.Fatalf("shared intent %+v %v", view, err)
	}
	if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 186, "", 1), "fixture", 101); err != nil {
		t.Fatal(err)
	}
	view, err = m.loadUpstreamTokenConfig(ctx, account.Domain, 185)
	if err != nil || len(view.SharedChannelIDs) != 0 {
		t.Fatal("cleared historical revision still shared", err)
	}
	if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 186, "575", 2), "fixture", 102); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", account.Domain).Update("account", "new-account").Error; err != nil {
		t.Fatal(err)
	}
	view, err = m.loadUpstreamTokenConfig(ctx, account.Domain, 185)
	if err != nil || view.TokenID != "" || len(view.SharedChannelIDs) != 0 {
		t.Fatal("shared IDs crossed upstream account epoch", err)
	}
	var count int64
	if err := m.storeDB.Model(&ChannelCostSourceBinding{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("shared intent activated binding", err)
	}
}

func TestUpstreamTokenConfigAppendAndClearWithoutFinancialSideEffects(t *testing.T) {
	m, account := tokenConfigFixture(t)
	ctx := context.Background()
	view, err := m.loadUpstreamTokenConfig(ctx, account.Domain, 185)
	if err != nil || view.Revision != 0 || view.State != "unconfigured" {
		t.Fatalf("empty config: %+v %v", view, err)
	}
	in := tokenConfigInput(account, 185, "", 0)
	view, err = m.saveUpstreamTokenConfig(ctx, in, "tester", 100)
	if err != nil || !view.Unchanged || view.Revision != 0 {
		t.Fatalf("empty optional ID must not create history: %+v %v", view, err)
	}
	in = tokenConfigInput(account, 185, " 00575 ", 0)
	view, err = m.saveUpstreamTokenConfig(ctx, in, "tester", 101)
	if err != nil || view.Revision != 1 || view.TokenID != "575" || view.State != "pending_verification" || view.AttributionVerified {
		t.Fatalf("saved intent must not be a verified binding: %+v %v", view, err)
	}
	in.ExpectedRevision = 1
	view, err = m.saveUpstreamTokenConfig(ctx, in, "tester", 102)
	if err != nil || !view.Unchanged || view.UpdatedAt != 101 {
		t.Fatalf("same value created a redundant version: %+v %v", view, err)
	}
	in.TokenID = nil
	view, err = m.saveUpstreamTokenConfig(ctx, in, "tester", 103)
	if err != nil || !view.Unchanged || view.TokenID != "575" {
		t.Fatalf("omitted field cleared stored ID: %+v %v", view, err)
	}
	in = tokenConfigInput(account, 185, "9007199254740993", 1)
	view, err = m.saveUpstreamTokenConfig(ctx, in, "tester", 104)
	if err != nil || view.TokenID != "9007199254740993" || view.Revision != 2 {
		t.Fatalf("large ID was rounded: %+v %v", view, err)
	}
	in = tokenConfigInput(account, 185, "", 2)
	view, err = m.saveUpstreamTokenConfig(ctx, in, "tester", 105)
	if err != nil || view.State != "unconfigured" || view.Revision != 3 || view.TokenID != "" {
		t.Fatalf("explicit clear failed: %+v %v", view, err)
	}
	var history []ChannelUpstreamTokenConfig
	if err := m.storeDB.Order("revision").Find(&history).Error; err != nil || len(history) != 3 || history[0].TokenID != "575" || history[1].TokenID != "9007199254740993" {
		t.Fatalf("previous values must remain available: %+v %v", history, err)
	}
	for _, model := range []any{&ChannelFinanceVersion{}, &ChannelDomainCost{}, &ChannelFinanceChannelCost{}, &ChannelCostSourceBinding{}, &ChannelCostDirtyHour{}, &ChannelEconomicsDirtyHour{}, &ChannelUpstreamCostHourEvidence{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("config save wrote to %T: count=%d err=%v", model, count, err)
		}
	}
}

func TestUpstreamTokenConfigScopeAndAccountChanges(t *testing.T) {
	m, account := tokenConfigFixture(t)
	ctx := context.Background()
	for _, id := range []int{185, 186} {
		view, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, id, "575", 0), "tester", 101)
		if err != nil || view.AttributionVerified {
			t.Fatalf("shared token must not be automatically attributed: %+v %v", view, err)
		}
	}
	for _, id := range []int{187, 999, 0, -1} {
		if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, id, "575", 0), "tester", 102); !errors.Is(err, errUpstreamTokenScope) {
			t.Fatalf("channel %d must fail scope check: %v", id, err)
		}
	}
	// Names, local group membership and status aren't identity. Credential
	// rotation likewise must not make the stored account identity disappear.
	if err := m.storeDB.Model(&ChannelSnap{}).Where("id = ?", 185).Updates(map[string]any{"name": "renamed", "groups": "new-group", "status": 2}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", account.Domain).Update("credential", "rotated-secret").Error; err != nil {
		t.Fatal(err)
	}
	view, err := m.loadUpstreamTokenConfig(ctx, account.Domain, 185)
	if err != nil || view.TokenID != "575" || view.Revision != 1 {
		t.Fatalf("rename/disable/refresh lost ID: %+v %v", view, err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", account.Domain).Update("user_id", 32).Error; err != nil {
		t.Fatal(err)
	}
	view, err = m.loadUpstreamTokenConfig(ctx, account.Domain, 185)
	if err != nil || view.TokenID != "" || view.Revision != 0 || view.AccountEpoch == newAPIUpstreamAccountEpoch(account) {
		t.Fatalf("different account inherited old binding: %+v %v", view, err)
	}
	if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 185, "574", 1), "tester", 103); !errors.Is(err, errUpstreamTokenConflict) {
		t.Fatalf("stale account save must fail: %v", err)
	}
	account.UserID = 32
	if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 185, "574", 0), "tester", 104); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.storeDB.Model(&ChannelUpstreamTokenConfig{}).Count(&count).Error; err != nil || count != 3 {
		t.Fatalf("old account configuration must be retained: %d %v", count, err)
	}
}

func TestUpstreamTokenConfigConcurrentSaveAndRollback(t *testing.T) {
	m, account := tokenConfigFixture(t)
	ctx := context.Background()
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, id := range []string{"574", "575"} {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			_, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 185, token, 0), "tester", 100)
			results <- err
		}(id)
	}
	wg.Wait()
	close(results)
	success, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, errUpstreamTokenConflict):
			conflicts++
		default:
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("concurrent writers: success=%d conflicts=%d", success, conflicts)
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER fail_token_config_insert BEFORE INSERT ON channel_upstream_token_configs BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 185, "999", 1), "tester", 101); err == nil {
		t.Fatal("expected synthetic transaction failure")
	}
	view, err := m.loadUpstreamTokenConfig(ctx, account.Domain, 185)
	if err != nil || view.Revision != 1 || view.TokenID == "999" {
		t.Fatalf("failed transaction modified current value: %+v %v", view, err)
	}
}

func TestUpstreamTokenConfigRejectsInvalidAndReadOnlyWrites(t *testing.T) {
	m, account := tokenConfigFixture(t)
	ctx := context.Background()
	for _, id := range []string{"-1", "0", "1e3", "575.0", "9223372036854775808", "sk-not-an-id"} {
		if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 185, id, 0), "tester", 100); err == nil {
			t.Fatal("invalid token ID accepted")
		}
	}
	for _, actor := range []string{"", strings.Repeat("x", 129), "x\ny"} {
		if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 185, "575", 0), actor, 100); err == nil {
			t.Fatal("invalid actor accepted")
		}
	}
	m.cfg.LocalSnapshotOnly = true
	if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(account, 185, "575", 0), "tester", 100); !errors.Is(err, errUpstreamTokenReadOnly) {
		t.Fatal(err)
	}
	if _, err := m.loadUpstreamTokenConfig(ctx, account.Domain, 185); err != nil {
		t.Fatalf("readonly loading must remain available: %v", err)
	}
	m.cfg.LocalSnapshotOnly = false
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := m.saveUpstreamTokenConfig(cancelled, tokenConfigInput(account, 185, "575", 0), "tester", 100); err == nil {
		t.Fatal("cancelled operation must not save")
	}
	var count int64
	if err := m.storeDB.Model(&ChannelUpstreamTokenConfig{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("rejected operations wrote rows: %d %v", count, err)
	}
}

func TestUpstreamTokenConfigUnsupportedProviderNeverClaimsAttribution(t *testing.T) {
	m, account := tokenConfigFixture(t)
	account.Provider = "aicodewith"
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", account.Domain).Update("provider", account.Provider).Error; err != nil {
		t.Fatal(err)
	}
	view, err := m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(account, 185, "575", 0), "tester", 100)
	if err != nil || view.State != "unsupported" || view.AttributionVerified {
		t.Fatalf("unsupported provider must retain aggregate behavior: %+v %v", view, err)
	}
}

func TestUpstreamTokenConfigMigrationIsAdditiveAndRepeatable(t *testing.T) {
	m, account := tokenConfigFixture(t)
	before := ChannelFinanceVersion{Domain: account.Domain, Version: 3, SnapshotJSON: `{"retained":true}`, EffectiveAt: 3600}
	if err := m.storeDB.Create(&before).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.storeDB.AutoMigrate(&ChannelUpstreamTokenConfig{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(account, 185, "575", 0), "tester", 100); err != nil {
		t.Fatal(err)
	}
	var after ChannelFinanceVersion
	if err := m.storeDB.First(&after, before.ID).Error; err != nil || before != after {
		t.Fatalf("finance version changed: %v", err)
	}
	// Old code can still read the old models. This is a local schema check,
	// not a substitute for the later previous-image rollback acceptance.
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Where("domain = ?", account.Domain).First(&after).Error; err != nil {
		t.Fatal(err)
	}
}
