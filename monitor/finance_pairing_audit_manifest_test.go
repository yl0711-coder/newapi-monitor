package monitor

import (
	"context"
	"reflect"
	"testing"
)

func TestFinancePairingAuditUsesAuthoritativePublication(t *testing.T) {
	db, publications := publishEconomicsCoverageFixture(t, economicsCoverageFixture{true, true, true, true, true, false})
	m := &Monitor{storeDB: db}
	ctx := context.Background()
	scope := stabilityScope{FromTs: 3600, ToTs: 7200}
	before, err := m.loadFinancePairingAudit(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if before.PairedRows != 1 || before.BlockedRows != 0 {
		t.Fatalf("unexpected base audit: %+v", before)
	}
	// Old identities retain their own child pointers. Only the domain-hour
	// manifest chooses the current identity; a child pointer alone is not it.
	stale := publications[0]
	stale.AccountEpoch, stale.PublicationID, stale.LogicalKey = "old-account", "old-pub", "old-logical"
	stale.CoverageStatus, stale.ProfitKnown = "unallocated_cost", false
	if err := db.Create(&stale).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelEconomicsHourCurrent{LogicalKey: stale.LogicalKey, PublicationID: stale.PublicationID}).Error; err != nil {
		t.Fatal(err)
	}
	// A child with no published manifest must not be called settled either.
	unpublished := stale
	unpublished.Domain, unpublished.PublicationID, unpublished.LogicalKey = "unpublished.example", "unpublished", "unpublished-key"
	if err := db.Create(&unpublished).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelEconomicsHourCurrent{LogicalKey: unpublished.LogicalKey, PublicationID: unpublished.PublicationID}).Error; err != nil {
		t.Fatal(err)
	}
	var saved []ChannelEconomicsHourPublication
	if err := db.Order("publication_id").Find(&saved).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		after, err := m.loadFinancePairingAudit(ctx, scope)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("old/unpublished children changed current audit: before=%+v after=%+v err=%v", before, after, err)
		}
	}
	var retained []ChannelEconomicsHourPublication
	if err := db.Order("publication_id").Find(&retained).Error; err != nil || !reflect.DeepEqual(saved, retained) {
		t.Fatal("read-only audit changed retained publications", err)
	}
	for _, outside := range []stabilityScope{{FromTs: 0, ToTs: 3600}, {FromTs: 7200, ToTs: 10800}} {
		audit, err := m.loadFinancePairingAudit(ctx, outside)
		if err != nil || audit.PublicationRows != 0 || len(audit.LedgerDomains) != 0 {
			t.Fatal("audit crossed half-open range", audit, err)
		}
	}
	// A genuinely selected incomplete publication still remains a blocker.
	// This is a synthetic manifest switch, not a production repair operation.
	if err := db.Model(&ChannelEconomicsHourManifestPublication{}).
		Where("domain=? AND hour_ts=?", "4sapi.com", 3600).
		Updates(map[string]any{"authoritative_epoch": stale.AccountEpoch, "coverage_status": "unallocated_cost", "profit_known": false}).Error; err != nil {
		t.Fatal(err)
	}
	selected, err := m.loadFinancePairingAudit(ctx, scope)
	if err != nil || selected.PublicationRows != 1 || selected.BlockedRows != 1 || selected.PairedRows != 0 || selected.StatusCounts["unallocated_cost"] != 1 {
		t.Fatal("authoritative incomplete result was hidden", selected, err)
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := m.loadFinancePairingAudit(ctx, scope); err == nil {
		t.Fatal("canceled audit succeeded")
	}
}
