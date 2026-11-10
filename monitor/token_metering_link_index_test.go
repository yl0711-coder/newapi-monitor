package monitor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"gorm.io/gorm"
)

// Capture the executed ORM queries rather than a separately rewritten SQL.
// A constant-false OR seed used to force an evidence-table scan and prevent
// even the first association from finishing against the production-sized DB.
func TestTokenMeteringLinkUsesIndexedIdentityLookups(t *testing.T) {
	for _, profiles := range []int{1, 3} {
		t.Run(fmt.Sprintf("profiles_%d", profiles), func(t *testing.T) {
			m, now, _ := tokenMeteringFixture(t)
			start := tokenMeteringStateRead(t, m).StartAt
			const count = 24
			for id := int64(1); id <= count; id++ {
				tokenMeteringSourceRow(t, m, id, start+id, fmt.Sprintf("request-%d", id))
			}
			tokenMeteringRun(t, m, now)
			for id := int64(1); id <= count; id++ {
				for profile := 1; profile <= profiles; profile++ {
					key := fmt.Sprintf("key-%d", profile)
					hash := nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", fmt.Sprintf("request-%d", id))
					if profile > 1 {
						ref := TokenMeteringRef{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, KeyID: key, Hash: hash}
						if err := m.storeDB.Create(&ref).Error; err != nil {
							t.Fatal(err)
						}
					}
					e := NginxRequestEvidence{EventID: fmt.Sprintf("event-%d-%d", id, profile), EventMS: (start + id) * 1000,
						HMACKeyID: key, OneAPIIDHMAC: hash, Route: "/v1/responses", Method: "POST", Status: 200,
						RequestLength: ptrTokenMetering(100), RequestLengthState: "valid", RequestLengthVersion: "request-length-v1"}
					if err := m.nginxEvidenceDB.Create(&e).Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			// Exact identifiers under another key must not become candidates.
			if err := m.nginxEvidenceDB.Create(&NginxRequestEvidence{EventID: "foreign-key", HMACKeyID: "unrelated-key",
				OneAPIIDHMAC: nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", "request-1")}).Error; err != nil {
				t.Fatal(err)
			}
			type recorded struct {
				db   *gorm.DB
				sql  string
				args []any
			}
			var mu sync.Mutex
			queries := []recorded{}
			for _, db := range []*gorm.DB{m.storeDB, m.nginxEvidenceDB} {
				db := db
				if err := db.Callback().Query().After("gorm:query").Register("token_index_regression", func(tx *gorm.DB) {
					if tx.Statement.Table == "token_metering_refs" || tx.Statement.Table == "nginx_request_evidences" {
						mu.Lock()
						queries = append(queries, recorded{db, tx.Statement.SQL.String(), append([]any(nil), tx.Statement.Vars...)})
						mu.Unlock()
					}
				}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Callback().Query().Remove("token_index_regression") })
			}
			n, err := m.linkTokenMeteringBatch(context.Background(), now)
			if err != nil || n != count {
				t.Fatalf("association did not finish: n=%d err=%v", n, err)
			}
			mu.Lock()
			snapshot := append([]recorded(nil), queries...)
			mu.Unlock()
			if len(snapshot) != 3 {
				t.Fatalf("expected refs, ownership and evidence lookups, got %d", len(snapshot))
			}
			for _, q := range snapshot {
				var plan []struct{ Detail string }
				if err := q.db.Raw("EXPLAIN QUERY PLAN "+q.sql, q.args...).Scan(&plan).Error; err != nil {
					t.Fatal(err)
				}
				indexed := false
				for _, row := range plan {
					if strings.Contains(row.Detail, "SCAN ") {
						t.Fatalf("identity lookup scans the table: %s; SQL=%s", row.Detail, q.sql)
					}
					indexed = indexed || strings.Contains(row.Detail, "SEARCH ")
				}
				if !indexed {
					t.Fatalf("identity lookup lacks an indexed search: %+v", plan)
				}
			}
			for id := int64(1); id <= count; id++ {
				f := tokenMeteringFactRead(t, m, id)
				want := "unique"
				if profiles > 1 {
					want = "ambiguous"
				}
				if f.LinkState != want || f.LinkCheckedAt != now {
					t.Fatalf("key isolation or multiple-event semantics changed: %+v", f)
				}
			}
		})
	}
}

func TestTokenDetectionWorkUsesCoveringFactIndex(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 5; id++ {
		detectionFact(t, m, id, now-600+id, 3, 20000, 0, 0)
	}
	tokenCheckActualQueuePlans(t, m, now, false)
}

func TestTokenDetectionHealthUsesCoveredIdentityScan(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 5; id++ {
		detectionFact(t, m, id, now-600+id, 3, 20000, 0, 0)
	}
	tokenCheckActualQueuePlans(t, m, now, true)
}

func tokenCheckActualQueuePlans(t *testing.T, m *Monitor, now int64, health bool) {
	t.Helper()
	type captured struct {
		sql  string
		args []any
	}
	var queries []captured
	if err := m.storeDB.Callback().Row().After("gorm:row").Register("token_queue_plan", func(tx *gorm.DB) {
		q := tx.Statement.SQL.String()
		if (strings.HasPrefix(q, "SELECT ") || strings.HasPrefix(q, "WITH ")) && (strings.Contains(q, " FROM "+tokenReportPendingMembers) || strings.Contains(q, "WITH due AS MATERIALIZED")) {
			queries = append(queries, captured{q, append([]any(nil), tx.Statement.Vars...)})
		}
	}); err != nil {
		t.Fatal(err)
	}
	var jobs []TokenMeteringFact
	var err error
	if health {
		jobs, err = tokenDetectionPendingSample(m.storeDB, m.cfg.TokenFactsSourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now, 50)
	} else {
		jobs, err = tokenDetectionJobsBounded(m.storeDB, m.cfg.TokenFactsSourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now, 50, 15)
	}
	_ = m.storeDB.Callback().Row().Remove("token_queue_plan")
	if err != nil || len(jobs) != 5 || len(queries) < 5 {
		t.Fatal("bounded work selection", len(jobs), len(queries), err)
	}
	found := map[string]bool{}
	for _, q := range queries {
		if strings.Contains(q.sql, "token_metering_facts") || !strings.Contains(q.sql, "LIMIT") {
			t.Fatal("unbounded source/queue query", q.sql)
		}
		var plan []struct{ Detail string }
		if err := m.storeDB.Raw("EXPLAIN QUERY PLAN "+q.sql, q.args...).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		for _, p := range plan {
			for _, idx := range []string{"idx_token_detection_pending_fresh_v1", "idx_token_detection_discovery_v1", "idx_token_detection_retry_schedule_v1", "idx_token_detection_pending_due_v1"} {
				found[idx] = found[idx] || strings.Contains(p.Detail, idx)
			}
			if strings.Contains(p.Detail, "SCAN c") || strings.Contains(p.Detail, "SCAN token_detection_retries") || strings.Contains(p.Detail, "SCAN token_report_pending_members") {
				t.Fatal("retained table scan", plan)
			}
		}
		if strings.HasPrefix(q.sql, "WITH prefix") && !strings.Contains(q.sql, "LIMIT 512") {
			t.Fatal("repair discovery lost its independent row cap")
		}
	}
	for _, idx := range []string{"idx_token_detection_pending_fresh_v1", "idx_token_detection_discovery_v1", "idx_token_detection_retry_schedule_v1", "idx_token_detection_pending_due_v1"} {
		if !found[idx] {
			t.Fatal("actual queue did not use index", idx)
		}
	}
	t.Logf("health=%t bounded scheduling queries=%d indexes=%v", health, len(queries), found)
}
