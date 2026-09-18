package monitor

import (
	"path/filepath"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestOpenExistingStabilityBackfillStoreDoesNotRunUnrelatedMigrations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monitor.db")
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(
		&MetricSample{},
		&StabilityHourSample{},
		&ChannelTestHourSample{},
		&StabilityHourIngestState{},
		&StabilityBackfillJob{},
		&ChannelUpstreamAccount{},
	); err != nil {
		t.Fatal(err)
	}
	// A full Monitor startup would attempt to decrypt and migrate this legacy
	// row. The bounded stability command must not read or rewrite it.
	account := ChannelUpstreamAccount{Domain: "spring.test", Provider: upstreamProviderAICodeWith, Credential: "not-a-valid-sealed-credential"}
	if err := db.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()

	opened, err := openExistingStabilityBackfillStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		sqlDB, _ := opened.DB()
		_ = sqlDB.Close()
	}()
	var got ChannelUpstreamAccount
	if err := opened.First(&got, "domain = ?", account.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if got.Credential != account.Credential {
		t.Fatalf("unrelated credential was changed: %q", got.Credential)
	}
}

func TestOpenExistingStabilityBackfillStoreRefusesMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	if _, err := openExistingStabilityBackfillStore(path); err == nil {
		t.Fatal("missing main store must not be created")
	}
}
