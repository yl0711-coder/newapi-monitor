package monitor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

// This opt-in check uses a disposable copy of a real stopped-writer snapshot,
// including its large fact payloads, due reviews and undetected history.
func TestTokenDetectionSavedBacklogThroughput(t *testing.T) {
	snapshot := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to a stopped-writer snapshot")
	}
	m, _ := tokenIsolatedRuntimeFixture(t, snapshot)
	var state TokenDetectionState
	if err := m.tokenStore().First(&state).Error; err != nil {
		t.Fatal(err)
	}
	var revision TokenDetectionRevision
	if err := m.tokenStore().First(&revision).Error; err != nil {
		t.Fatal(err)
	}
	var frozen tokenDetectionFrozen
	if err := json.Unmarshal([]byte(revision.FrozenJSON), &frozen); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenFactsSourceEpoch = state.SourceEpoch
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(frozen.Policy)
	m.cfg.TokenProcessingBatchRows = 250
	m.cfg.TokenProcessingPollSeconds = 15
	m.cfg.TokenDetectionMaxRows = 1200000
	m.cfg.TokenDetectionMaxBytes = 24 << 30
	now := time.Now().Unix()
	measure := func(label string) []TokenMeteringFact {
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		jobs, err := tokenDetectionJobsBounded(m.tokenReadStore().WithContext(ctx), state.SourceEpoch, tokenDetectionPolicyHash(frozen.Policy), now, 250, 15)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		t.Logf("%s queue=%d elapsed=%s", label, len(jobs), time.Since(started))
		return jobs
	}
	if err := m.tokenStore().Migrator().DropIndex(&TokenMeteringFact{}, "idx_token_detection_fact_identity"); err != nil {
		t.Fatal(err)
	}
	before := measure("without covering identity index")
	if err := m.tokenStore().Migrator().CreateIndex(&TokenMeteringFact{}, "idx_token_detection_fact_identity"); err != nil {
		t.Fatal(err)
	}
	after := measure("with covering identity index")
	if len(before) != len(after) {
		t.Fatal("index changed queue size")
	}
	for i := range before {
		if before[i].LogID != after[i].LogID {
			t.Fatal("index changed scheduling order")
		}
	}
	var initial, final int64
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&initial).Error; err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 4; round++ {
		started := time.Now()
		n, err := m.runTokenDetectionRound(context.Background(), now+int64(round*15))
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&final).Error; err != nil {
			t.Fatal(err)
		}
		t.Logf("snapshot round=%d processed=%d new_heads=%d elapsed=%s error=%v", round, n, final-initial, time.Since(started), err)
		if n == 0 {
			t.Fatal("real saved backlog made no committed progress")
		}
	}
	assertTokenCapacityLedger(t, m.tokenStore())
	if final <= initial {
		t.Fatal("saved backlog did not process any new identities")
	}
}

func TestTokenRuntimeSavedPayloadPageBudget(t *testing.T) {
	snapshot := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to a stopped-writer snapshot")
	}
	m, _ := tokenIsolatedRuntimeFixture(t, snapshot)
	m.cfg.TokenFactsMaxRows, m.cfg.TokenFactsMaxBytes = 1200000, 6<<30
	var state TokenMeteringState
	if err := m.tokenStore().First(&state).Error; err != nil {
		t.Fatal(err)
	}
	var facts []TokenMeteringFact
	if err := m.tokenReadStore().Where("source_epoch=? AND created_at>=? AND created_at<?", state.SourceEpoch, 1793086500, 1793086800).Order("created_at,log_id").Limit(256).Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 256 {
		t.Fatal("real payload fixture requires one full page")
	}
	oldIDs := make([]int64, len(facts))
	for i, f := range facts {
		oldIDs[i] = f.LogID
	}
	var refs []TokenMeteringRef
	if err := m.tokenReadStore().Where("source_epoch=? AND log_id IN ?", state.SourceEpoch, oldIDs).Find(&refs).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	from := now/300*300 - 600
	const offset = int64(1_000_000_000)
	raw := make([]tokenMeteringRaw, len(facts))
	var payload int64
	for i := range facts {
		f := &facts[i]
		f.LogID += offset
		f.CreatedAt, f.FirstSeenAt, f.LastSeenAt = from+int64(i/2), now, now
		f.Revision, f.LinkRevision = 1, 0
		f.FactHash = tokenDetectionHash([]any{f.LogID, f.CreatedAt, f.SourceFacts})
		f.LinkState, f.LinkReason, f.EvidenceLevel = "waiting", "waiting_evidence", "unavailable"
		f.LinkNextAt, f.LinkDeadline, f.LinkAttempts = now, now+3600, 0
		f.EvidenceJSON, f.EvidenceSnapshotAt, f.RequestLength, f.LengthState = "", 0, nil, "missing"
		payload += int64(len(f.SourceFacts))
		raw[i] = tokenMeteringRaw{ID: f.LogID, CreatedAt: f.CreatedAt}
	}
	for i := range refs {
		refs[i].LogID += offset
		refs[i].Hash = tokenDetectionHash([]any{refs[i].Hash, refs[i].LogID})
	}
	state.LeaseToken, state.LeaseUntil = "local-page-budget-test", now+30
	if err := m.tokenStore().Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	w := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: from, EndAt: from + 300, CursorAt: from}
	if err := m.tokenStore().Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	var saved TokenMeteringWindow
	for written, turns := 0, 0; written < len(raw); turns++ {
		if turns >= 8 {
			t.Fatal("page prefix failed to resume")
		}
		started := time.Now()
		n, err := m.commitTokenMeteringReadPage(context.Background(), state.LeaseToken, w, raw[written:], facts[written:], refs, false, now)
		t.Logf("saved real payload page turn=%d committed=%d payload_bytes=%d elapsed=%s error=%v", turns, n, payload, time.Since(started), err)
		if err != nil || n <= 0 {
			t.Fatalf("durable page progress: %d %v", n, err)
		}
		written += n
		if err := m.tokenReadStore().First(&saved, "source_epoch=? AND start_at=?", state.SourceEpoch, from).Error; err != nil {
			t.Fatal(err)
		}
		if saved.CursorID != raw[written-1].ID || saved.Done {
			t.Fatal("page prefix was not durably checkpointed")
		}
		w = saved
	}
	var after TokenMeteringState
	if err := m.tokenReadStore().First(&after, "source_epoch=?", state.SourceEpoch).Error; err != nil {
		t.Fatal(err)
	}
	if saved.CursorID != raw[255].ID || saved.CursorAt != raw[255].CreatedAt || saved.Done || after.ScanThrough != state.ScanThrough || after.FactCount != state.FactCount+256 {
		t.Fatal("page did not publish exactly its durable cursor and fact count")
	}
}

func TestTokenRuntimeFrozenArtifactRecomputation(t *testing.T) {
	file := os.Getenv("TOKEN_RUNTIME_REVISIONS")
	if file == "" {
		t.Skip("set TOKEN_RUNTIME_REVISIONS to pinned runtime revision evidence")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var artifact struct {
		Image string `json:"image"`
		Rows  []struct {
			LogID        int64  `json:"log_id"`
			Revision     int64  `json:"revision"`
			DecisionHash string `json:"decision_hash"`
			FrozenHex    string `json:"frozen_hex"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatal(err)
	}
	if len(artifact.Rows) != 1093 {
		t.Fatal("fixed runtime window requires all 1093 revisions")
	}
	for _, row := range artifact.Rows {
		encoded, err := hex.DecodeString(row.FrozenHex)
		if err != nil {
			t.Fatal(err)
		}
		body, _, err := tokenStorageDecode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		var frozen tokenDetectionFrozen
		if err := json.Unmarshal([]byte(body), &frozen); err != nil {
			t.Fatal(err)
		}
		if frozen.Input.Fact.LogID != row.LogID || tokenDetectionHash(frozen) != row.DecisionHash {
			t.Fatal("runtime frozen identity/hash mismatch")
		}
		actual := tokenDetectionEvaluate(frozen.Input, frozen.Policy, frozen.Result.Reference, frozen.CachePriorSignals, frozen.Result.SourceComplete)
		if tokenDetectionJSON(actual) != tokenDetectionJSON(frozen.Result) {
			t.Fatalf("runtime decision cannot be reproduced: request=%d revision=%d", row.LogID, row.Revision)
		}
	}
	t.Logf("runtime_image=%s pinned_frozen_revisions_recomputed=%d", artifact.Image, len(artifact.Rows))
}
