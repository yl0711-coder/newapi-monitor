package monitor

import (
	"context"
	"testing"
)

func TestTokenReviewParserReplayHashAndRawTransport(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	raw := tokenMeteringRawFixture(`{"request_path":"/v1/custom/responses","request_method":"POST","request_length":137,"request_length_version":"source-v1","request_length_meaning":"http_request_bytes","request_length_observed_at":123,"request_conversion":true,"event_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}`)
	first, refs := parseTokenMetering(m.cfg, raw, now)
	for turn := 0; turn < 100; turn++ {
		got, _ := parseTokenMetering(m.cfg, raw, now+int64(turn))
		if got.FactHash != first.FactHash || got.SourceFacts != first.SourceFacts || got.MissingFields != first.MissingFields {
			t.Fatal("identical source replay changed fact identity/revision operands", turn)
		}
	}
	if first.RequestID != raw.RequestID || first.SourceEventID == "" || first.RequestPath != "/v1/custom/responses" || len(refs) == 0 {
		t.Fatal("raw source identity/transport was dropped")
	}
	d := tokenDetectionDisplay{}
	tokenDetectionDisplayTransport(&d, first)
	if d.RequestLength == nil || *d.RequestLength != "137" || d.RequestLengthObservedAt != 123 || d.RequestLengthMeaning != "http_request_bytes" || d.RequestLengthSource != "newapi.logs.other.request_length" {
		t.Fatalf("source transport unavailable in pending display: %+v", d)
	}
}

func TestTokenReviewHeadlessFactsRemainPagedAndVisible(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 3; id++ {
		detectionFact(t, m, id, now-100+id, 3, 10000, 0, 0)
	}
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 200, To: now, Limit: 2}
	r, err := m.tokenDetectionReport(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	items := r["items"].([]map[string]any)
	if len(items) != 2 || r["has_more"] != true {
		t.Fatal("headless facts disappeared from main list", len(items), r["has_more"])
	}
	for _, item := range items {
		d := item["display"].(tokenDetectionDisplay)
		if !d.Stale || d.RequestID == "" || d.RecordedInput == nil || d.DetectionState != "detection_pending" || len(d.MissingFields) == 0 {
			t.Fatalf("pending facts presented as finished/empty: %+v", d)
		}
	}
	last := items[1]["request"].(TokenDetectionCurrent)
	q.AfterAt, q.AfterID = last.CreatedAt, last.LogID
	r, err = m.tokenDetectionReport(context.Background(), q)
	if err != nil || len(r["items"].([]map[string]any)) != 1 {
		t.Fatal("pending keyset lost suffix", err)
	}
}

func TestTokenReviewDetectionCleanupProtectsUnknownAndReferenced(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	cutoff := now - 4*86400
	for id, state := range []string{"", "evidence_insufficient", "association_pending", "confirmed", "candidate", "not_hit"} {
		head := TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: int64(id + 1), CreatedAt: cutoff - 1, ExpiresAt: now - 1, Stage: 3, SourceComplete: true, DetectionState: state, ResultJSON: `{}`}
		if err := db.Create(&head).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil || count != 6 {
		t.Fatal("unverified cleanup deleted evidence", count, err)
	}
	tokenCleanupAuthorizeFixture(t, m, now, cutoff)
	ref := TokenDetectionReference{ID: "protected", SnapshotJSON: `{"samples":[{"epoch":"` + m.cfg.TokenFactsSourceEpoch + `","log_id":6}]}`}
	if err := db.Create(&ref).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil || count != 6 {
		t.Fatal("pending or referenced evidence deleted", count, err)
	}
}

func TestTokenReviewInvalidReferenceFailsClosed(t *testing.T) {
	m, now, _ := tokenCleanupFixture(t, 1)
	defer m.Close()
	if err := m.tokenStore().Create(&TokenDetectionReference{ID: "broken", SnapshotJSON: `{"samples":[{"log_id":1}]}`}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	assertTokenCleanupLedgers(t, m, 2)
}
