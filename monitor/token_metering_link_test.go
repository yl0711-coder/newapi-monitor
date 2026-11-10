package monitor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"gorm.io/gorm"
)

func TestTokenMeteringAssociationChunksRollbackAndResume(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	for id := int64(1); id <= 17; id++ {
		request := fmt.Sprintf("chunk-request-%d", id)
		tokenMeteringSourceRow(t, m, id, start+id, request)
		tokenMeteringEvidence(t, m, fmt.Sprintf("chunk-event-%d", id), start+id, 1000+id, request)
	}
	tokenMeteringRun(t, m, now)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("link_next_at>0").Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	before := tokenMeteringStateRead(t, m)
	selected, selectErr := tokenMeteringLinkJobs(m.tokenStore(), m.cfg.TokenFactsSourceEpoch, now, min(m.cfg.tokenProcessingBatchRows(), 200), m.cfg.tokenProcessingPollSeconds())
	if selectErr != nil || len(selected) != 17 {
		t.Fatal(selected, selectErr)
	}
	committedIDs := map[int64]bool{}
	for _, fact := range selected[:4] {
		committedIDs[fact.LogID] = true
	}
	injected := errors.New("association chunk fixture failure")
	writes := 0
	if err := m.tokenStore().Callback().Update().Before("gorm:update").Register("association_chunk_failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" {
			writes++
			if writes == 11 {
				tx.AddError(injected)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	committed, err := m.linkTokenMeteringBatch(context.Background(), now)
	_ = m.tokenStore().Callback().Update().Remove("association_chunk_failure")
	// Four cheap writes commit first; the learned eight-row second chunk
	// contains write eleven and must roll back as one unit.
	if !errors.Is(err, injected) || committed != 4 {
		t.Fatalf("failed association batch lost committed prefix: count=%d err=%v", committed, err)
	}
	var retainedBytes int64
	for id := int64(1); id <= 17; id++ {
		fact := tokenMeteringFactRead(t, m, id)
		retainedBytes += int64(len(fact.EvidenceJSON))
		if committedIDs[id] {
			if fact.LinkState != "unique" || fact.LinkAttempts != 1 || fact.RequestLength == nil || *fact.RequestLength != 1000+id {
				t.Fatal("committed evidence changed", id, fact.LinkState, fact.LinkAttempts)
			}
		} else if fact.LinkAttempts != 0 || fact.EvidenceJSON != "" || fact.LinkNextAt != now {
			t.Fatal("rolled-back association leaked evidence or retry state", id, fact.LinkAttempts)
		}
	}
	if state := tokenMeteringStateRead(t, m); state.FactBytes != before.FactBytes+retainedBytes {
		t.Fatal("rolled-back evidence leaked into byte ledger", state.FactBytes, before.FactBytes, retainedBytes)
	}
	committed, err = m.linkTokenMeteringBatch(context.Background(), now)
	if err != nil || committed != 13 {
		t.Fatal("association did not resume only its uncommitted suffix", committed, err)
	}
	for id := int64(1); id <= 17; id++ {
		if fact := tokenMeteringFactRead(t, m, id); fact.LinkState != "unique" || fact.LinkAttempts != 1 {
			t.Fatal("resumed association duplicated or skipped a request", id, fact.LinkState, fact.LinkAttempts)
		}
	}
}

func tokenMeteringEvidence(t *testing.T, m *Monitor, id string, created, length int64, request string) {
	t.Helper()
	row := NginxRequestEvidence{EventID: id, EventMS: created * 1000, Node: "synthetic", Route: "/v1/responses", Method: "POST", Status: 200,
		HMACKeyID: "key-1", OneAPIIDHMAC: nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", request), RequestLength: ptrTokenMetering(length), RequestLengthState: "valid", RequestLengthVersion: "request-length-v1"}
	if err := m.nginxEvidenceDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
}
func tokenMeteringLinkDue(t *testing.T, m *Monitor, now int64) {
	t.Helper()
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("link_next_at > 0").Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.linkTokenMeteringBatch(context.Background(), now); err != nil {
		t.Fatal(err)
	}
}
func TestTokenMeteringLocalAssociationLateConflictRetained(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	tokenMeteringLinkDue(t, m, now)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "waiting" || f.RequestLength != nil {
		t.Fatalf("%+v", f)
	}
	tokenMeteringEvidence(t, m, "event-a", start+1, 0, "one")
	tokenMeteringLinkDue(t, m, now+60)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "unique" || f.RequestLength == nil || *f.RequestLength != 0 || f.EvidenceLevel != "exact" {
		t.Fatalf("%+v", f)
	}
	tokenMeteringEvidence(t, m, "event-b", start+2, 10000, "one")
	tokenMeteringLinkDue(t, m, now+120)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "conflict" || f.RequestLength != nil || f.LinkNextAt == 0 {
		t.Fatalf("selected newest instead of conflict %+v", f)
	}
	if err := m.nginxEvidenceDB.Delete(&NginxRequestEvidence{}, "event_id = ?", "event-b").Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringLinkDue(t, m, now+180)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "conflict" || f.RequestLength != nil || f.EvidenceLevel != "unavailable" {
		t.Fatalf("%+v", f)
	}
}
func TestTokenMeteringAmbiguityAndConsumptionIdentityCollision(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	tokenMeteringEvidence(t, m, "event-a", start+1, 100, "one")
	tokenMeteringEvidence(t, m, "event-b", start+1, 100, "one")
	tokenMeteringLinkDue(t, m, now)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "ambiguous" || f.LinkReason != "multiple_entry_events" {
		t.Fatalf("%+v", f)
	}
	if err := m.nginxEvidenceDB.Delete(&NginxRequestEvidence{}, "event_id = ?", "event-b").Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringSourceRow(t, m, 2, start+2, "one")
	tokenMeteringReplay(t, m, now+1)
	tokenMeteringLinkDue(t, m, now+2)
	for _, id := range []int64{1, 2} {
		if f := tokenMeteringFactRead(t, m, id); f.LinkReason != "request_id_reused_by_consumptions" || f.EvidenceLevel == "exact" {
			t.Fatalf("%+v", f)
		}
	}
}
func TestTokenMeteringLinkScopePilotExpiryAndTruncation(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	tokenMeteringEvidence(t, m, "event-a", start+1, 100, "one")
	m.cfg.NginxEvidenceMode = "pilot"
	tokenMeteringLinkDue(t, m, now)
	if f := tokenMeteringFactRead(t, m, 1); f.EvidenceLevel != "correlated" {
		t.Fatalf("%+v", f)
	}
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("event_id = ?", "event-a").Update("event_ms", (start-7201)*1000).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringLinkDue(t, m, now+1)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "conflict" {
		t.Fatalf("%+v", f)
	}
	if err := m.nginxEvidenceDB.Delete(&NginxRequestEvidence{}, "event_id = ?", "event-a").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id = 1").Update("link_attempts", 23).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringLinkDue(t, m, now+2)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "conflict" || f.LinkNextAt != 0 {
		t.Fatalf("%+v", f)
	}
	// The batch candidate cap must never turn a truncated result into unique.
	f := tokenMeteringFactRead(t, m, 1)
	f.LinkState, f.LinkReason = "waiting", "evidence_pending"
	got := resolveTokenMeteringLink(f, nil, nil, nil, true, "verified", now)
	if got.LinkState != "ambiguous" || got.LinkReason != "candidate_budget_exceeded" {
		t.Fatalf("%+v", got)
	}
}
func TestTokenMeteringBatchCandidateCapAndNoSourceFallback(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	rows := make([]NginxRequestEvidence, 1001)
	for i := range rows {
		rows[i] = NginxRequestEvidence{EventID: fmt.Sprintf("event-%04d", i), EventMS: (start + 1) * 1000, HMACKeyID: "key-1", OneAPIIDHMAC: nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", "one")}
	}
	if err := m.nginxEvidenceDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	// Removing the production source cannot affect local association.
	if err := m.prodDB.Close(); err != nil {
		t.Fatal(err)
	}
	tokenMeteringLinkDue(t, m, now)
	f := tokenMeteringFactRead(t, m, 1)
	if f.LinkState != "ambiguous" || f.LinkReason != "candidate_budget_exceeded" {
		t.Fatalf("%+v", f)
	}
}

func TestTokenMeteringLinkSnapshotBudgetAndKnownEvidence(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	tokenMeteringEvidence(t, m, "event-a", start+1, 100, "one")
	baseline := tokenMeteringStateRead(t, m).FactBytes
	m.cfg.TokenFactsMaxBytes = baseline
	tokenMeteringLinkDue(t, m, now)
	f := tokenMeteringFactRead(t, m, 1)
	if f.LinkReason != "snapshot_storage_limit" || f.RequestLength != nil || tokenMeteringStateRead(t, m).FactBytes != baseline {
		t.Fatalf("%+v", f)
	}
	m.cfg.TokenFactsMaxBytes = baseline + 10000
	tokenMeteringLinkDue(t, m, now+60)
	f = tokenMeteringFactRead(t, m, 1)
	if f.LinkState != "unique" || f.EvidenceSnapshotAt != now+60 || f.StoredBytes != baseline+int64(len(f.EvidenceJSON)) {
		t.Fatalf("%+v", f)
	}
	snapshot := f.EvidenceJSON
	if err := m.nginxEvidenceDB.Delete(&NginxRequestEvidence{}, "event_id = ?", "event-a").Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringLinkDue(t, m, now+120)
	f = tokenMeteringFactRead(t, m, 1)
	if f.LinkState != "waiting" || f.RequestLength != nil || f.EvidenceJSON != snapshot || f.EvidenceSnapshotAt != now+60 {
		t.Fatalf("known evidence erased or presented as current: %+v", f)
	}
}
func TestTokenMeteringLateDuplicateInvalidatesClosedUniqueLink(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	tokenMeteringEvidence(t, m, "event-a", start+1, 100, "one")
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id = 1").Update("link_attempts", 23).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringLinkDue(t, m, now+60)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "unique" || f.LinkNextAt != 0 {
		t.Fatalf("%+v", f)
	}
	tokenMeteringSourceRow(t, m, 2, start+2, "one")
	tokenMeteringReplay(t, m, now+61)
	if f := tokenMeteringFactRead(t, m, 1); f.LinkState != "ambiguous" || f.EvidenceLevel != "unavailable" {
		t.Fatalf("closed link stayed exact: %+v", f)
	}
}

func TestTokenMeteringEventIDOwnersParticipateInCollisionDetection(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	eventID := "shared-event-id"
	refs := []TokenMeteringRef{
		{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 1, KeyID: "event-id", Hash: eventID, Version: "event-id/v1"},
		{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 2, KeyID: "event-id", Hash: eventID, Version: "event-id/v1"},
	}
	if err := m.storeDB.Create(&refs).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.nginxEvidenceDB.Create(&NginxRequestEvidence{EventID: eventID, EventMS: now * 1000, Status: 200, Route: "/v1/responses", Method: "POST"}).Error; err != nil {
		t.Fatal(err)
	}
	_, owners, truncated, err := m.tokenMeteringLinkInputs(context.Background(), refs)
	if err != nil || truncated || len(owners[tokenMeteringRefKey("event-id", eventID)]) != 2 {
		t.Fatalf("event-id owners omitted from collision probe: owners=%v truncated=%v err=%v", owners, truncated, err)
	}
	for _, id := range []int64{1, 2} {
		fact := TokenMeteringFact{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, CreatedAt: now, LinkDeadline: now + 3600}
		got := resolveTokenMeteringLink(fact, []TokenMeteringRef{{SourceEpoch: fact.SourceEpoch, LogID: id, KeyID: "event-id", Hash: eventID, Version: "event-id/v1"}}, map[string][]NginxRequestEvidence{tokenMeteringRefKey("event-id", eventID): {{EventID: eventID, EventMS: now * 1000, Status: 200, Route: "/v1/responses", Method: "POST"}}}, owners, false, "verified", now)
		if got.LinkState != "ambiguous" || got.LinkReason != "request_id_reused_by_consumptions" {
			t.Fatalf("event-id collision was not retained: %+v", got)
		}
	}
}

func TestTokenMeteringEvidenceQueryFailureIsDurableAndRetryable(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 903, start+1, "query-failure")
	tokenMeteringRun(t, m, now)
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id=?", 903).Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	conn, err := m.nginxEvidenceDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	committed, remaining, linkErr := m.linkTokenMeteringBatchProgress(context.Background(), now)
	if linkErr == nil || committed != 1 || !remaining {
		t.Fatalf("evidence query failure was hidden: committed=%d remaining=%v err=%v", committed, remaining, linkErr)
	}
	fact := tokenMeteringFactRead(t, m, 903)
	if fact.LinkState != "evidence_query_failed" || fact.LinkReason != "evidence_query_failed" || fact.LinkNextAt <= now {
		t.Fatalf("query failure did not enter durable retry state: %+v", fact)
	}
}
