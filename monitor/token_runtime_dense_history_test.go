package monitor

import (
	"database/sql"
	"fmt"
	"testing"
)

// The real slow-query samples contained 10,675 facts for one user/model and no
// eligible control. Seed that density together with both unprocessed and stale
// historical work, then use the actual source/association/detection loops.
func tokenRuntimeSeedDenseHistory(t *testing.T, m *Monitor, payload string, start, now int64) {
	t.Helper()
	const count = 24000
	epoch, hash := m.cfg.TokenFactsSourceEpoch, tokenDetectionPolicyHash(detectionPolicy())
	var stored int64
	for batch := int64(0); batch < count; batch += 128 {
		var facts []TokenMeteringFact
		var heads []TokenDetectionCurrent
		for n := batch; n < min(batch+128, count); n++ {
			raw := tokenMeteringRawFixture(payload)
			raw.ID, raw.CreatedAt = 10_000_000+n, start-1800+n%1200
			raw.UserID, raw.ChannelID, raw.Model, raw.Group = 2, 3, "synthetic-model", "scheduler-fixture"
			if n%2 == 0 {
				raw.ChannelID = 4
			}
			raw.Input, raw.Output, raw.Quota = sql.NullString{String: "100", Valid: true}, sql.NullString{String: "10", Valid: true}, sql.NullString{String: "200", Valid: true}
			f, _ := parseTokenMetering(m.cfg, raw, now)
			length := int64(1000)
			if n%2 == 0 {
				length = 5000
			}
			f.RequestLength, f.LengthState, f.LinkState, f.EvidenceLevel = &length, "valid", "unique", "exact"
			f.LinkRevision, f.LinkCheckedAt, f.LinkNextAt = 1, now, now+3600
			f.EvidenceJSON = tokenDetectionJSON([]tokenMeteringCandidate{{EventID: fmt.Sprintf("retained-%d", n), Node: "dense-fixture", EventMS: f.CreatedAt * 1000, Status: 200, Route: "/v1/messages", Length: &length, LengthState: "valid", LengthVersion: "request-length-v1"}})
			f.StoredBytes += int64(len(f.EvidenceJSON))
			facts = append(facts, f)
			stored += f.StoredBytes
			if n < count/2 {
				heads = append(heads, TokenDetectionCurrent{SourceEpoch: epoch, LogID: f.LogID, CreatedAt: f.CreatedAt, UserID: f.UserID, ChannelID: f.ChannelID, Model: f.Model, UserGroup: f.UserGroup, Revision: 1, FactRevision: 0, LinkRevision: 1, PolicyHash: hash, NextAt: 0, ResultJSON: "{}", FirstEvaluatedAt: now, EvaluatedAt: now, ExpiresAt: now + 86400})
			}
		}
		if err := m.tokenStore().CreateInBatches(facts, 128).Error; err != nil {
			t.Fatal(err)
		}
		if len(heads) > 0 {
			if err := m.tokenStore().CreateInBatches(heads, 128).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", epoch).Updates(map[string]any{"fact_count": count, "fact_bytes": stored}).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("dense retained fixture facts=%d headless=%d stale_heads=%d plus expired_heads=8192", count, count/2, count/2)
}
