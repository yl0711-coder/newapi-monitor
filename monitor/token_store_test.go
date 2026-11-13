package monitor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestTokenStoreIsolationRestartAndCurrentDirectory(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	if err := m.storeDB.Where("1=1").Delete(&TokenMeteringState{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.AutoMigrate(&UsageUserSnapshot{}, &ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []any{&UsageUserSnapshot{UserID: 2, Username: "local-customer"}, &ChannelSnap{ID: 3, Name: "current-channel", BaseDomain: "https://current.example.test/path"}} {
		if err := m.storeDB.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	m.cfg.TokenStorePath = filepath.Join(t.TempDir(), "token", "facts.db")
	if err := m.openTokenStore(); err != nil {
		t.Fatal(err)
	}
	if m.tokenStore() == m.storeDB {
		t.Fatal("explicit store fell back to main")
	}
	raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0,"cache_creation_tokens":0}`)
	raw.ID, raw.CreatedAt, raw.UserID, raw.ChannelID = 1, now-600, 2, 3
	fact, _ := parseTokenMetering(m.cfg, raw, now)
	fact.LinkState, fact.LinkRevision = "unique", 1
	if err := m.tokenStore().Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Create(&TokenDetectionState{SourceEpoch: fact.SourceEpoch, LeaseToken: "store-fixture", LeaseUntil: now + 30}).Error; err != nil {
		t.Fatal(err)
	}
	// This storage test uses a bounded RAM disk smaller than the production
	// two-GiB reserve. Exercise the fenced processor without a source read.
	if err := m.processTokenDetection(context.Background(), fact.SourceEpoch, fact.LogID, detectionPolicy(), "store-fixture", now); err != nil {
		t.Fatal(err)
	}
	var heads []TokenDetectionCurrent
	if err := m.tokenStore().Find(&heads).Error; err != nil || len(heads) != 1 {
		t.Fatalf("isolated processing: %d %v", len(heads), err)
	}
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error {
		displays, err := m.tokenDetectionDisplays(tx, fact.SourceEpoch, heads)
		if err != nil {
			return err
		}
		d := displays[1]
		if d.CustomerName != "local-customer" || d.ChannelName != "current-channel" || d.Upstream != "example.test" || !d.EvidenceAvailable {
			t.Fatalf("directory/frozen evidence lost across stores: %+v", d)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var mainCount int64
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&mainCount).Error; err != nil || mainCount != 0 {
		t.Fatalf("Token wrote shared main store: count=%d %v", mainCount, err)
	}
	sqlDB, _ := m.tokenDB.DB()
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
	m.tokenDB = nil
	if m.tokenStore() != nil {
		t.Fatal("failed explicit store fell back to main")
	}
	if err := m.openTokenStore(); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("history lost after restart: %d %v", count, err)
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}

func TestTokenStoreRejectsPopulatedMainAndSharedPaths(t *testing.T) {
	m, _, original := detectionFixture(t)
	defer m.Close()
	if m.tokenStore() != m.storeDB {
		t.Fatal("legacy default changed")
	}
	m.cfg.StorePath = original
	m.cfg.TokenStorePath = original
	if err := m.openTokenStore(); err == nil {
		t.Fatal("shared path accepted")
	}
	m.cfg.TokenStorePath = filepath.Join(t.TempDir(), "token.db")
	if err := m.openTokenStore(); err == nil {
		t.Fatal("existing source checkpoint was silently abandoned")
	}
	if m.tokenStore() != nil {
		t.Fatal("failed explicit store redirected writes")
	}
}

func TestTokenMeteringInitialLookbackOnlySeedsNewEpoch(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	m.cfg.TokenFactsSourceEpoch = "new-epoch"
	m.cfg.TokenFactsInitialLookbackSeconds = 900
	first, claimed, err := m.claimTokenMetering(context.Background(), now)
	if err != nil || !claimed || first.StartAt != (now-900)/300*300 || first.ScanThrough != first.StartAt {
		t.Fatalf("new bounded start: %+v claimed=%v err=%v", first, claimed, err)
	}
	w := TokenMeteringWindow{SourceEpoch: first.SourceEpoch, StartAt: first.StartAt, EndAt: first.StartAt + 300, CursorAt: first.StartAt + 100, CursorID: 123}
	if err := m.storeDB.Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenFactsInitialLookbackSeconds = 7 * 86400
	resumed, claimed, err := m.claimTokenMetering(context.Background(), now+60)
	if err != nil || !claimed || resumed.StartAt != first.StartAt || resumed.ScanThrough != first.ScanThrough {
		t.Fatalf("restart redefined the persisted range: %+v %v", resumed, err)
	}
	got, found, err := m.nextTokenMeteringWindow(context.Background(), resumed.SourceEpoch, resumed.LeaseToken, now+60, false)
	if err != nil || !found || got.CursorAt != w.CursorAt || got.CursorID != w.CursorID {
		t.Fatalf("unfinished cursor lost: %+v %v", got, err)
	}
}

func TestTokenStoreNotificationFixtureUsesSharedConfigAndIsolatedOutbox(t *testing.T) {
	m, now, tokenPath := tokenDeliveryFixture(t)
	defer m.Close()
	tokenDeliveryCandidate(t, m, now)
	mainDB, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "main.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	if err := mainDB.AutoMigrate(&AlertConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := mainDB.Create(&AlertConfig{ID: 1, Enabled: true, SMTPHost: "shared-fixture.invalid"}).Error; err != nil {
		t.Fatal(err)
	}
	m.tokenDB, m.storeDB = m.storeDB, mainDB
	m.cfg.TokenStorePath = tokenPath
	if err := m.tokenDB.Migrator().DropTable(&AlertConfig{}); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenNotificationsEnabled = true
	calls := 0
	if err := m.runTokenNotificationRound(context.Background(), now, func(_ context.Context, cfg AlertConfig, _, _, _ string) error {
		if cfg.SMTPHost != "shared-fixture.invalid" {
			t.Fatal("wrong configuration store")
		}
		calls++
		return nil // Local fixture only; never opens SMTP or a network connection.
	}); err != nil {
		t.Fatal(err)
	}
	var outbox TokenNotifyOutbox
	if err := m.tokenDB.First(&outbox).Error; err != nil {
		t.Fatal(err)
	}
	if calls != 1 || outbox.State != "sent" {
		t.Fatalf("isolated notification routing: calls=%d state=%s", calls, outbox.State)
	}
}
