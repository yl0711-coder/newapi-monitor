package monitor

import (
	"net/http"
	"strings"
	"testing"
)

func TestNginxEvidenceReplayEnrichesAndRejectsConflictingIdentity(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	in := validEvidenceBatch()
	in.Events[0].RequestLength = nil
	in.Events[0].RequestLengthState = "missing"
	in.Events[0].RequestLengthVersion = "request-length-v1"
	post := func(want int) {
		t.Helper()
		in.PayloadHash = ""
		if w := postNginxEvidence(t, m, in); w.Code != want {
			t.Fatalf("status=%d want=%d body=%s", w.Code, want, w.Body.String())
		}
		if want == http.StatusOK {
			in.Source.StartOffset = in.Source.EndOffset
			in.Source.EndOffset += 200
		}
	}
	post(http.StatusOK)
	var generation int64
	if err := m.nginxEvidenceDB.Raw(`SELECT generation FROM nginx_evidence_change_clock_v1 WHERE id=1`).Scan(&generation).Error; err != nil {
		t.Fatal(err)
	}
	in.BatchID = "evidence_enriched_abcdefgh"
	length := int64(4096)
	in.Events[0].RequestLength = &length
	in.Events[0].RequestLengthState = "valid"
	in.Events[0].RequestLengthVersion = "request-length-v1"
	in.Events[0].OneAPIRequestID = "synthetic-request-id"
	post(http.StatusOK)
	var got NginxRequestEvidence
	if err := m.nginxEvidenceDB.Where("event_id=?", in.Events[0].EventID).Take(&got).Error; err != nil || got.RequestLength == nil || *got.RequestLength != 4096 || got.OneAPIRequestID != "synthetic-request-id" {
		t.Fatalf("enrichment lost: %+v %v", got, err)
	}
	var enriched int64
	if err := m.nginxEvidenceDB.Raw(`SELECT generation FROM nginx_evidence_change_clock_v1 WHERE id=1`).Scan(&enriched).Error; err != nil || enriched != generation+1 {
		t.Fatalf("no durable wake: before=%d after=%d %v", generation, enriched, err)
	}
	in.BatchID = "evidence_same_event_abcdefgh"
	post(http.StatusOK)
	m.cfg.NginxEvidencePreviousHMACKeyID, m.cfg.NginxEvidencePreviousHMACKey = "key-2", strings.Repeat("z", 32)
	in.BatchID = "evidence_rotated_key_abcdefgh"
	in.HMACKeyID = "key-2"
	in.Events[0].OneAPIIDHMAC = strings.Repeat("d", 64)
	post(http.StatusOK)
	if err := m.nginxEvidenceDB.Where("event_id=?", in.Events[0].EventID).Take(&got).Error; err != nil || got.HMACKeyID != "key-1" {
		t.Fatalf("rotation replaced original hash identity: %+v %v", got, err)
	}
	in.BatchID = "evidence_conflicting_abcdefgh"
	in.Events[0].OneAPIRequestID = "different-request-id"
	post(http.StatusConflict)
	var after int64
	if err := m.nginxEvidenceDB.Raw(`SELECT generation FROM nginx_evidence_change_clock_v1 WHERE id=1`).Scan(&after).Error; err != nil || after != enriched {
		t.Fatalf("noop/conflict changed generation: %d %d %v", enriched, after, err)
	}
	var batches int64
	if err := m.nginxEvidenceDB.Model(&NginxEvidenceIngestBatch{}).Where("batch_id=?", in.BatchID).Count(&batches).Error; err != nil || batches != 0 {
		t.Fatalf("conflict advanced batch: %d %v", batches, err)
	}
}
