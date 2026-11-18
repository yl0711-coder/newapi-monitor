package monitor

import (
	"context"
	"os"
	"strings"
	"testing"
)

// The pre-migration plan ID must change whenever the schema created by
// openStore changes; a restart under an unchanged plan reuses the pinned
// rollback snapshot (see preMigrationPlanID). v1.18.67 added two tables
// without bumping it, which nothing caught. This test records the plan ID
// together with the schema hash of a freshly migrated store and fails when
// the schema changes but the plan ID does not.
//
// After an intentional schema change: bump preMigrationPlanID (and
// preMigrationCombinedPlanID), then run
//
//	UPDATE_SCHEMA_PLAN_GOLDEN=1 go test ./monitor -run TestSchemaChangeRequiresPlanBump
//
// Scope: models migrated by openStore (main and usage-facts sets). Feature
// stores migrated elsewhere (token store, nginx evidence, ECS ownership,
// simple monthly generation) are not covered yet.
const schemaPlanGoldenPath = "testdata/schema_plan.golden"

func TestSchemaChangeRequiresPlanBump(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	sqlDB, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	inv, err := inspectSQLiteLogicalInventory(context.Background(), sqlDB)
	if err != nil {
		t.Fatal(err)
	}
	current := preMigrationPlanID + "\n" + inv.SchemaSHA256 + "\n"
	if os.Getenv("UPDATE_SCHEMA_PLAN_GOLDEN") == "1" {
		if err := os.WriteFile(schemaPlanGoldenPath, []byte(current), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(schemaPlanGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("malformed %s", schemaPlanGoldenPath)
	}
	goldenPlan, goldenHash := lines[0], lines[1]
	switch {
	case goldenPlan == preMigrationPlanID && goldenHash != inv.SchemaSHA256:
		t.Fatalf("the migrated schema changed but preMigrationPlanID did not: bump preMigrationPlanID and preMigrationCombinedPlanID, then update %s (see the comment above this test)", schemaPlanGoldenPath)
	case goldenPlan != preMigrationPlanID:
		t.Fatalf("preMigrationPlanID changed: update %s (see the comment above this test)", schemaPlanGoldenPath)
	}
}
