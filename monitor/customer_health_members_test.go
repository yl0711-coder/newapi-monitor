package monitor

import (
	"context"
	"testing"
)

func TestCustomerHealthInputUsesLocalDirectoryWithoutProductionUsers(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	if err := m.storeDB.Create(&UserDirectoryEntry{UserID: 811, Username: "local-health-user", SyncedAt: 1}).Error; err != nil {
		t.Fatal(err)
	}

	byName, err := m.resolveCustomerHealthInput(nil, "local-health-user", 0)
	if err != nil {
		t.Fatalf("local username should resolve without production DB: %v", err)
	}
	if byName.UserID != 811 || byName.Username != "local-health-user" {
		t.Fatalf("unexpected local resolution: %+v", byName)
	}

	// A numeric ID is accepted even when the local directory has never seen it.
	// This is the important logchain-only path: no users-table lookup is needed.
	byID, err := m.resolveCustomerHealthInput(nil, "", 812)
	if err != nil || byID.UserID != 812 {
		t.Fatalf("numeric ID should be accepted without directory entry: member=%+v err=%v", byID, err)
	}

	if _, err := m.resolveCustomerHealthInput(nil, "not-in-local-directory", 0); err == nil {
		t.Fatal("unknown username must not fall back to a production users query")
	}
}

func TestCustomerHealthMembershipIsIndependentFromUsageList(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()

	healthGroup := CustomerHealthGroup{Name: "Health company", CreatedAt: 1}
	if err := m.storeDB.Create(&healthGroup).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&CustomerHealthMember{UserID: 701, GroupID: healthGroup.ID, Username: "health-user"}).Error; err != nil {
		t.Fatal(err)
	}
	legacyGroup := CustomerGroup{Name: "Usage company", CreatedAt: 1}
	if err := m.storeDB.Create(&legacyGroup).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 702, GroupID: legacyGroup.ID, Username: "usage-user"}).Error; err != nil {
		t.Fatal(err)
	}

	companies, err := m.customerHealthCompanies(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(companies) != 1 || companies[0].groupID != healthGroup.ID || companies[0].members[0].UserID != 701 {
		t.Fatalf("customer health must read independent tables: %+v", companies)
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", legacyGroup.ID).Update("name", "changed usage company").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TrackedUser{}).Where("user_id = ?", 702).Update("group_id", healthGroup.ID).Error; err != nil {
		t.Fatal(err)
	}
	companies, err = m.customerHealthCompanies(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(companies) != 1 || companies[0].name != "Health company" || len(companies[0].members) != 1 || companies[0].members[0].UserID != 701 {
		t.Fatalf("legacy usage edits must not change customer health: %+v", companies)
	}
	// Deleting the customer-health records must leave the usage records intact;
	// the two pages now have separate lifecycle tables.
	if err := m.storeDB.Where("group_id = ?", healthGroup.ID).Delete(&CustomerHealthMember{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Delete(&CustomerHealthGroup{}, healthGroup.ID).Error; err != nil {
		t.Fatal(err)
	}
	var usageMember TrackedUser
	if err := m.storeDB.First(&usageMember, 702).Error; err != nil {
		t.Fatalf("usage member was affected by customer-health deletion: %v", err)
	}
	var usageCompany CustomerGroup
	if err := m.storeDB.First(&usageCompany, legacyGroup.ID).Error; err != nil {
		t.Fatalf("usage company was affected by customer-health deletion: %v", err)
	}
}

func TestCustomerHealthLegacyMigrationCopiesOnceAndPreservesIDs(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()

	// openStore creates the marker for a new empty store.  Remove it to model a
	// pre-upgrade database containing only the legacy usage tables.
	if err := m.storeDB.Delete(&CustomerHealthMigrationState{}, 1).Error; err != nil {
		t.Fatal(err)
	}
	legacyGroup := CustomerGroup{Name: "Migrated company", Note: "legacy", CreatedAt: 22}
	if err := m.storeDB.Create(&legacyGroup).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 703, GroupID: legacyGroup.ID, Username: "legacy-user", Email: "legacy@example.test", AddedAt: 23}).Error; err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyCustomerHealthTables(m.storeDB); err != nil {
		t.Fatal(err)
	}
	var copiedGroup CustomerHealthGroup
	if err := m.storeDB.First(&copiedGroup, legacyGroup.ID).Error; err != nil {
		t.Fatal(err)
	}
	var copiedMember CustomerHealthMember
	if err := m.storeDB.First(&copiedMember, 703).Error; err != nil {
		t.Fatal(err)
	}
	if copiedGroup.Name != legacyGroup.Name || copiedMember.GroupID != legacyGroup.ID || copiedMember.Email != "legacy@example.test" {
		t.Fatalf("legacy rows were not copied faithfully: group=%+v member=%+v", copiedGroup, copiedMember)
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", legacyGroup.ID).Update("name", "legacy renamed").Error; err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyCustomerHealthTables(m.storeDB); err != nil {
		t.Fatal(err)
	}
	var unchanged CustomerHealthGroup
	if err := m.storeDB.First(&unchanged, legacyGroup.ID).Error; err != nil {
		t.Fatal(err)
	}
	if unchanged.Name != legacyGroup.Name {
		t.Fatalf("one-time migration reran after legacy edits: %+v", unchanged)
	}
}
