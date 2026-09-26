package financegiftexport

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

func TestGiftScopeExportRefusesUnboundedPlans(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		kind, key, estimate string
		ok                  bool
	}{
		{"ref", "idx_user", "19", true}, {"range", "idx_user_created", "10000", true},
		{"ALL", "", "19", false}, {"index", "idx_user", "19", false},
		{"ref", "idx_user", "10001", false}, {"ref", "idx_user", "", false}, {"ref", "", "10", false},
	} {
		rows, err := db.Query("SELECT 'logs' AS `table`,? AS `type`,? AS `key`,? AS `rows`", tc.kind, tc.key, tc.estimate)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateGiftScopePlan(rows); (err == nil) != tc.ok {
			t.Fatalf("plan=%+v err=%v", tc, err)
		}
	}
	query := giftScopeExportSQL()
	projection := strings.Split(query, " FROM logs")[0]
	for _, sensitive := range []string{"content", "other", "token_name", "request_id", "password"} {
		if strings.Contains(projection, sensitive) {
			t.Fatal("sensitive column exported", sensitive)
		}
	}
	for _, required := range []string{"MAX_EXECUTION_TIME(2000)", "user_id=?", "created_at>=?", "created_at<?", "LIMIT 101"} {
		if !strings.Contains(query, required) {
			t.Fatal("missing query bound", required)
		}
	}
}
