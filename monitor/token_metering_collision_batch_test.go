package monitor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm/logger"
)

type tokenCollisionQueryCounter struct {
	logger.Interface
	queries *int
}

func (l tokenCollisionQueryCounter) Trace(_ context.Context, _ time.Time, sql func() (string, int64), _ error) {
	query, _ := sql()
	if strings.HasPrefix(strings.ToUpper(query), "SELECT") && strings.Contains(query, "token_metering_refs") {
		*l.queries++
	}
}

func TestTokenMeteringCollisionBatchPreservesOldAndSamePageConflicts(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "previous")
	tokenMeteringRun(t, m, now)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=1").Updates(map[string]any{
		"link_state": "unique", "evidence_level": "exact", "request_length": 123, "length_state": "valid",
	}).Error; err != nil {
		t.Fatal(err)
	}
	for id := int64(2); id <= 129; id++ {
		request := fmt.Sprintf("unique-%d", id)
		if id == 126 || id == 127 {
			request = "in-page"
		}
		if id == 128 {
			request = "previous"
		}
		tokenMeteringSourceRow(t, m, id, start+id, request)
	}
	queries := 0
	original := m.tokenStore().Config.Logger
	m.tokenStore().Config.Logger = tokenCollisionQueryCounter{Interface: logger.Default.LogMode(logger.Silent), queries: &queries}
	tokenMeteringReplay(t, m, now+1)
	m.tokenStore().Config.Logger = original
	if queries > 12 {
		t.Fatalf("unique inputs caused per-reference driver queries: %d", queries)
	}
	if state := tokenMeteringStateRead(t, m); state.FactCount != 129 {
		t.Fatal("batch lost facts", state.FactCount)
	}
	for _, id := range []int64{1, 126, 127} {
		fact := tokenMeteringFactRead(t, m, id)
		if fact.LinkReason != "request_id_reused_by_consumptions" || fact.EvidenceLevel != "unavailable" || fact.RequestLength != nil {
			t.Fatal("reused identity retained unique evidence", id, fact.LinkState, fact.LinkReason)
		}
	}
	for _, id := range []int64{2, 125, 129} {
		fact := tokenMeteringFactRead(t, m, id)
		if fact.LinkState == "ambiguous" || fact.LinkState == "conflict" {
			t.Fatal("unrelated reference inherited collision", id)
		}
	}
}
