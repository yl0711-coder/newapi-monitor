package monitor

import (
	"context"
	"fmt"
	"testing"
)

func TestTokenMeteringRecentAssociationSurvivesHistoricalRetryBacklog(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	m.cfg.TokenProcessingBatchRows = 50
	for id := int64(1); id <= 96; id++ {
		raw := tokenMeteringRawFixture(`{"usage_semantic":"anthropic","claude":true,"cache_tokens":0,"cache_creation_tokens":0}`)
		raw.ID, raw.RequestID, raw.CreatedAt = id, fmt.Sprintf("recent-association-%d", id), now-86400+id
		seenAt := now - 600
		if id > 80 {
			raw.CreatedAt, seenAt = now-300+id-80, now
		}
		f, refs := parseTokenMetering(m.cfg, raw, seenAt)
		if id == 96 {
			f.LinkNextAt = now + 300 // Future backoff must not be bypassed.
		}
		if err := m.tokenStore().Create(&f).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.tokenStore().Create(refs).Error; err != nil {
			t.Fatal(err)
		}
		e := NginxRequestEvidence{EventID: fmt.Sprintf("case-%d", id), EventMS: raw.CreatedAt * 1000, HMACKeyID: refs[0].KeyID, OneAPIIDHMAC: refs[0].Hash,
			Status: 200, Route: "/v1/messages", RequestLength: ptrTokenMetering(1000), RequestLengthState: "valid", RequestLengthVersion: "request-length-v1"}
		if err := m.nginxEvidenceDB.Create(&e).Error; err != nil {
			t.Fatal(err)
		}
	}
	n, err := m.linkTokenMeteringBatch(context.Background(), now)
	if err != nil || n != 50 {
		t.Fatal("bounded association did not commit", n, err)
	}
	old, recent := 0, 0
	for id := int64(1); id <= 96; id++ {
		f := tokenMeteringFactRead(t, m, id)
		if f.LinkAttempts > 1 || id == 96 && f.LinkAttempts != 0 {
			t.Fatal("duplicate processing or premature retry")
		}
		if f.LinkAttempts == 1 {
			if f.LinkState != "unique" || f.RequestLength == nil {
				t.Fatal("association did not preserve entry evidence")
			}
			if id > 80 {
				recent++
			} else {
				old++
			}
		}
	}
	if recent != 15 || old != 35 {
		t.Fatal("old or recent associations starved", old, recent)
	}
	// A second worker turn advances remaining historical rows instead of
	// immediately repeating the recent rows whose next-at is in the future.
	n, err = m.linkTokenMeteringBatch(context.Background(), now)
	if err != nil || n != 45 {
		t.Fatal("historical association did not continue", n, err)
	}
}
