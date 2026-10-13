//go:build unix

package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/gorm"
)

// This is an opt-in test of a known sealed backup, never a live DB. The source
// is opened only by the guarded copy helper. All SQL, DDL and saves use a newly
// created private t.TempDir copy, which is removed by the test harness.
func TestUpstreamTokenConfigRetainedCopy(t *testing.T) {
	source := os.Getenv("MONITOR_TOKEN_CONFIG_ACCEPTANCE_DB")
	if source == "" {
		t.Skip("requires the sealed local baseline; no live database is used")
	}
	const wantSHA = "fd84006b4d21b330a4bb1dde00c0840eb2e2a864e8bb8236b6ff9eb2a1cd5ea9"
	copyPath := filepath.Join(t.TempDir(), "token-config-rehearsal.db")
	digest, err := giftLocalCopyBackup(source, copyPath)
	if err != nil || digest != wantSHA {
		t.Fatal("source must be the known closed baseline", err)
	}
	db, closeDB, err := giftLocalDatabase(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db = db.WithContext(ctx)
	models := []any{
		&ChannelUpstreamAccount{}, &ChannelSnap{}, &ChannelFinanceVersion{},
		&ChannelFinanceChannelCost{}, &ChannelDomainCost{}, &ChannelDomainGroupCost{},
		&ChannelUpstreamUsageHour{}, &ChannelUpstreamPricingHourEvidence{},
		&ChannelUpstreamPricingPageCheckpoint{}, &ChannelCostPageCheckpoint{},
		&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{},
		&ChannelCostSourceBinding{}, &ChannelCostDirtyHour{}, &ChannelEconomicsDirtyHour{},
		&ChannelEconomicsHourPublication{}, &ChannelEconomicsHourCurrent{},
		&ChannelEconomicsHourManifestPublication{}, &ChannelEconomicsHourManifestCurrent{},
		&ChannelEconomicsGlobalHourFact{}, &ChannelCostKeyRegistry{},
	}
	before := tokenConfigProtectedHashes(t, db, models)
	for i := 0; i < 2; i++ {
		if err := db.AutoMigrate(&ChannelUpstreamTokenConfig{}, &ChannelUpstreamTokenScanCheckpoint{}, &ChannelUpstreamTokenWorkState{}); err != nil {
			t.Fatal(err)
		}
	}
	var candidate struct {
		Domain    string
		ChannelID int
	}
	if err := db.Table("channel_snaps s").Select("s.base_domain AS domain, s.id AS channel_id").
		Joins("JOIN channel_upstream_accounts a ON a.domain = s.base_domain").
		Where("a.provider IN ?", []string{upstreamProviderNewAPI, upstreamProviderSub2API}).
		Order("s.id").Limit(1).Scan(&candidate).Error; err != nil || candidate.ChannelID <= 0 {
		t.Fatal("no candidate channel in retained fixture", err)
	}
	m := &Monitor{storeDB: db}
	view, err := m.loadUpstreamTokenConfig(ctx, candidate.Domain, candidate.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	token := "575" // synthetic intent only; no upstream claim or verification
	in := upstreamTokenConfigInput{Domain: candidate.Domain, ChannelID: candidate.ChannelID, AccountEpoch: view.AccountEpoch, TokenID: &token}
	view, err = m.saveUpstreamTokenConfig(ctx, in, "local-acceptance", time.Now().Unix())
	if err != nil || view.AttributionVerified || view.State != "pending_verification" {
		t.Fatalf("save: %+v %v", view, err)
	}
	in.ExpectedRevision, token = view.Revision, ""
	if _, err := m.saveUpstreamTokenConfig(ctx, in, "local-acceptance", time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	// Test the new independent cursor against a real schema/account identity,
	// but feed only synthetic pages and write only this disposable DB copy.
	var sub2Account ChannelUpstreamAccount
	result := db.Select("domain", "provider", "base_url", "user_id", "account").
		Where("provider = ? AND (user_id > 0 OR TRIM(account) <> '')", upstreamProviderSub2API).
		Order("domain").Limit(1).Find(&sub2Account)
	if result.Error != nil || result.RowsAffected != 1 {
		t.Fatal("no Sub2 account identity in retained fixture", result.Error)
	}
	_, items, day, key := sub2TokenFixture(t)
	cursor, err := m.saveSub2TokenScanPage(ctx, sub2Account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "local-test-v1")
	if err != nil {
		t.Fatal("save synthetic shadow cursor", err)
	}
	cursor, err = m.saveSub2TokenScanPage(ctx, sub2Account, day, sub2TokenPage(items[2:], 2), cursor.Revision, day+86401, key, "local-test-v1")
	if err != nil {
		t.Fatal("resume synthetic shadow cursor", err)
	}
	legacy := sub2ObservationLegacy(t, sub2Account, items, day)
	observed, err := m.recordSub2TokenObservation(ctx, sub2Account, day, cursor, legacy, day+86401, key, "local-test-v1")
	if err != nil || observed.StableScans != 1 {
		t.Fatal("record synthetic complete observation", err)
	}
	retry, err := m.recordSub2TokenObservation(ctx, sub2Account, day, cursor, legacy, day+86402, key, "local-test-v1")
	if err != nil || retry != observed {
		t.Fatal("observation replay changed state", err)
	}
	var sub2Channel ChannelSnap
	if err := db.Select("id").Where("base_domain = ?", sub2Account.Domain).Order("id").First(&sub2Channel).Error; err != nil {
		t.Fatal("no retained Sub2 physical channel", err)
	}
	config, err := m.loadUpstreamTokenConfig(ctx, sub2Account.Domain, sub2Channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.saveUpstreamTokenConfig(ctx, tokenConfigInput(sub2Account, sub2Channel.ID, "575", config.Revision), "local-acceptance", day+86402); err != nil {
		t.Fatal(err)
	}
	m.cfg.ChannelCostHMACKey, m.cfg.ChannelCostHMACKeyID = string(key), "local-test-v1"
	m.cfg.LocalSnapshotOnly = true
	readiness, err := m.loadUpstreamTokenReconciliation(ctx, sub2Account.Domain, sub2Channel.ID, day, day+86402)
	m.cfg.LocalSnapshotOnly = false
	if err != nil || readiness.Status != "waiting_second_scan" || readiness.AttributionVerified {
		t.Fatal("retained one-scan evidence was treated as ready", readiness.Status, err)
	}
	if _, err := m.resetSub2TokenScanCheckpoint(ctx, sub2Account, day, cursor.Revision, day+86402, key, "local-test-v1"); err != nil {
		t.Fatal("reset synthetic shadow cursor", err)
	}
	after := tokenConfigProtectedHashes(t, db, models)
	for table, hash := range before {
		if after[table] != hash {
			t.Fatalf("protected table changed: %s", table)
		}
	}
	var check string
	if err := db.Raw("PRAGMA quick_check").Scan(&check).Error; err != nil || check != "ok" {
		t.Fatal("copy integrity check failed", err)
	}
	if giftLocalFileHash(t, source) != wantSHA {
		t.Fatal("sealed source was modified")
	}
	t.Logf("%d original tables unchanged; configuration and independent cursor writes only in a disposable copy", len(models))
}

// Stream complete logical rows into a digest, instead of only comparing sums
// that could hide compensating changes. No record content is written to logs.
func tokenConfigProtectedHashes(t *testing.T, db *gorm.DB, models []any) map[string]string {
	t.Helper()
	out := make(map[string]string, len(models))
	for _, model := range models {
		stmt := &gorm.Statement{DB: db}
		if err := stmt.Parse(model); err != nil {
			t.Fatal(err)
		}
		table := stmt.Schema.Table
		rows, err := db.Model(model).Order("rowid").Rows()
		if err != nil {
			t.Fatal("read protected table", table, err)
		}
		out[table] = tokenConfigRowsDigest(t, rows)
	}
	return out
}

func tokenConfigRowsDigest(t *testing.T, rows *sql.Rows) string {
	t.Helper()
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	encoder := json.NewEncoder(hash)
	if err := encoder.Encode(columns); err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Encode(values); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
