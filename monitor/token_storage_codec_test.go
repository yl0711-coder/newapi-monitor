package monitor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestTokenStorageCodecLegacyExactRoundTripAndCorruption(t *testing.T) {
	raw := `{"note":"中文","tokens":"9007199254740993","values":[` + strings.Repeat(`"same",`, 1000) + `null]}`
	encoded, err := tokenStorageEncode(raw)
	if err != nil || len(encoded) >= len(raw) {
		t.Fatalf("encoding: %d %v", len(encoded), err)
	}
	for _, data := range [][]byte{[]byte(raw), encoded} {
		decoded, _, err := tokenStorageDecode(data)
		if err != nil || decoded != raw {
			t.Fatalf("JSON bytes changed: %v", err)
		}
	}
	corrupt := append([]byte{}, encoded...)
	corrupt[len(corrupt)-1] ^= 1
	var oversized bytes.Buffer
	oversized.WriteString(tokenStorageHeader)
	w := gzip.NewWriter(&oversized)
	_, _ = io.Copy(w, strings.NewReader(strings.Repeat("x", tokenStorageDecodedLimit+1)))
	_ = w.Close()
	for _, data := range [][]byte{encoded[:len(encoded)-3], append(append([]byte{}, encoded...), 1), corrupt, oversized.Bytes()} {
		if _, _, err := tokenStorageDecode(data); err == nil {
			t.Fatal("corrupt or unbounded compressed value accepted")
		}
	}
}

func TestTokenStorageCompressionPreservesDecisionDisplayExportAndPayloadBudget(t *testing.T) {
	m, now, _ := tokenDeliveryFixture(t)
	defer m.Close()
	h := tokenDeliveryCandidate(t, m, now)
	var fact TokenMeteringFact
	var revision TokenDetectionRevision
	if err := m.storeDB.First(&fact).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&revision).Error; err != nil {
		t.Fatal(err)
	}
	originalFact, originalRevision, originalHash := fact.SourceFacts, revision.FrozenJSON, revision.DecisionHash
	revision.CompactStorage = true
	for _, model := range []any{&fact, &revision} {
		if err := m.storeDB.Save(model).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, model := range []any{&fact, &revision} {
		if err := m.storeDB.First(model).Error; err != nil {
			t.Fatal(err)
		}
	}
	if fact.SourceFacts != originalFact || revision.FrozenJSON != originalRevision || revision.DecisionHash != originalHash || !revision.CompactStorage {
		t.Fatal("compression changed frozen evidence or did not survive reload")
	}
	assertTokenCapacityLedger(t, m.storeDB)
	displays, err := m.tokenDetectionDisplays(m.storeDB, h.SourceEpoch, []TokenDetectionCurrent{h})
	if err != nil || !displays[h.LogID].EvidenceAvailable || displays[h.LogID].Input == nil {
		t.Fatalf("compressed display unavailable: %+v %v", displays, err)
	}
	router := gin.New()
	router.POST("/detail", m.serveTokenDetectionDetail)
	body, _ := json.Marshal(map[string]any{"source_epoch": h.SourceEpoch, "log_id": h.LogID})
	r := httptest.NewRequest(http.MethodPost, "/detail", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, r)
	if response.Code != 200 || !strings.Contains(response.Body.String(), originalHash) {
		t.Fatalf("compressed detail: %d %s", response.Code, response.Body.String())
	}
	exported, err := m.createTokenExport(context.Background(), "fixture-actor", tokenDeliveryExportRequest(m, h, now), now)
	if err != nil || len(exported.Payload) == 0 {
		t.Fatalf("compressed export failed: %v", err)
	}
	files := tokenDeliveryUnzip(t, exported.Payload)
	if len(files) == 0 {
		t.Fatal("empty evidence export")
	}
	var sizes struct{ FactBytes, FrozenBytes, HeadBytes int64 }
	if err := m.storeDB.Raw(`SELECT (SELECT SUM(length(CAST(source_facts AS BLOB))) FROM token_metering_facts) AS fact_bytes,
		(SELECT SUM(length(CAST(frozen_json AS BLOB))) FROM token_detection_revisions) AS frozen_bytes,
		(SELECT SUM(length(CAST(result_json AS BLOB))) FROM token_detection_currents) AS head_bytes`).Scan(&sizes).Error; err != nil {
		t.Fatal(err)
	}
	if sizes.FactBytes != int64(len(originalFact)) || sizes.FrozenBytes >= int64(len(originalRevision)) {
		t.Fatalf("compressed storage was not used: %+v", sizes)
	}
	t.Logf("representative_compression_only fact_json_raw=%d fact_stored=%d frozen_json_raw=%d frozen_stored=%d head_bytes=%d", len(originalFact), sizes.FactBytes, len(originalRevision), sizes.FrozenBytes, sizes.HeadBytes)
}

func TestTokenStorageRetentionBasisDoesNotExtendFirstEvaluation(t *testing.T) {
	s := Settings{TokenDetectionRetentionDays: 4, TokenDetectionRetentionBasis: "first_evaluated"}
	if err := validateTokenProcessingResources(s); err != nil {
		t.Fatal(err)
	}
	if s.tokenDetectionExpiresAt(100, 200) != 100+4*86400 || s.tokenDetectionExpiresAt(0, 200) != 200+4*86400 {
		t.Fatal("first evaluation expiry drift")
	}
	s.TokenDetectionRetentionBasis = "last_evaluated"
	if s.tokenDetectionExpiresAt(100, 200) != 200+4*86400 {
		t.Fatal("legacy retention changed")
	}
	s.TokenStoreMinFreeBytes = 1 << 30
	if validateTokenProcessingResources(s) == nil {
		t.Fatal("reserve below two GiB accepted")
	}
}

func TestTokenStorageIsolatedDiskBudgetPreservesAbsoluteReserve(t *testing.T) {
	const total, free = int64(329 << 30), int64(25 << 30)
	if tokenStoreDiskWithinBudget(total, free, 80, 2<<30) {
		t.Fatal("legacy percentage guard bypassed")
	}
	if !tokenStoreDiskWithinBudget(total, free, 98, 8<<30) {
		t.Fatal("explicit isolated budget ignored")
	}
	for _, available := range []int64{(8 << 30) - 1, -1, total + 1} {
		if tokenStoreDiskWithinBudget(total, available, 98, 8<<30) {
			t.Fatal("absolute reserve or invalid disk accepted")
		}
	}
	if tokenStoreDiskWithinBudget(total, 8<<30, 95, 8<<30) {
		t.Fatal("percentage cap ignored")
	}
}

func TestTokenStorageFullReviewSizeAndProcessing(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	// Isolated-store writes use the codec; source reads are not involved.
	m.tokenDB = m.storeDB
	m.cfg.TokenProcessingBatchRows = 250
	m.cfg.TokenProcessingPollSeconds = 15
	m.cfg.TokenDetectionRetentionDays = 4
	m.cfg.TokenDetectionRetentionBasis = "first_evaluated"
	base := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	// Scheduled reference expansion requires comparable request evidence.
	// Missing evidence now waits for changed facts instead of writing four
	// identical insufficient decisions. Isolate cohorts here so this test
	// continues to exercise all four actual review stages and their storage.
	input, err := tokenDetectionInputOf(base)
	if err != nil || !tokenComparable(input) {
		t.Fatal("four-stage compression fixture requires comparable evidence", err)
	}
	rows := make([]TokenMeteringFact, 199)
	for i := range rows {
		rows[i] = base
		rows[i].LogID = int64(i + 2)
		rows[i].UserID = int64(i + 1000)
	}
	if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	for stage := int64(0); stage < 4; stage++ {
		processed, err := m.runTokenDetectionRound(context.Background(), now+stage*7200)
		if err != nil || processed != 200 {
			t.Fatalf("bounded compressed processing stage=%d count=%d err=%v", stage, processed, err)
		}
	}
	var sizes struct{ FactBytes, FrozenBytes, HeadBytes, Revisions, PageSize, PageCount int64 }
	if err := m.storeDB.Raw(`SELECT
		(SELECT SUM(length(CAST(source_facts AS BLOB))) FROM token_metering_facts) AS fact_bytes,
		(SELECT SUM(length(CAST(frozen_json AS BLOB))) FROM token_detection_revisions) AS frozen_bytes,
		(SELECT SUM(length(CAST(result_json AS BLOB))) FROM token_detection_currents) AS head_bytes,
		(SELECT COUNT(*) FROM token_detection_revisions) AS revisions`).Scan(&sizes).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Raw("PRAGMA page_size").Scan(&sizes.PageSize).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Raw("PRAGMA page_count").Scan(&sizes.PageCount).Error; err != nil {
		t.Fatal(err)
	}
	var head TokenDetectionCurrent
	if err := m.storeDB.First(&head).Error; err != nil {
		t.Fatal(err)
	}
	if sizes.Revisions != 800 || head.Stage != 3 || head.ExpiresAt != now+4*86400 {
		t.Fatalf("retention or revision drift: %+v %+v", sizes, head)
	}
	assertTokenCapacityLedger(t, m.storeDB)
	t.Logf("representative_compact_all_reviews requests=200 reviews=%d elapsed_ms=%d fact_stored=%d frozen_stored=%d head_bytes=%d database_bytes=%d", sizes.Revisions, time.Since(started).Milliseconds(), sizes.FactBytes, sizes.FrozenBytes, sizes.HeadBytes, sizes.PageSize*sizes.PageCount)
}

func TestTokenStorageFourAndSevenDayFactRetentionBoundary(t *testing.T) {
	for _, days := range []int{4, 7} {
		t.Run(strconv.Itoa(days), func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			m.cfg.TokenFactsEnabled = true
			m.cfg.TokenFactsRetentionDays = days
			if err := validateTokenMeteringSettings(m.cfg); err != nil {
				t.Fatal(err)
			}
			cutoff := (now - int64(days)*86400) / 300 * 300
			var totalBytes int64
			for index, at := range []int64{cutoff - 1, cutoff, cutoff + 1} {
				raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0,"cache_creation_tokens":0}`)
				raw.ID, raw.CreatedAt = int64(index+1), at
				f, _ := parseTokenMetering(m.cfg, raw, now)
				f.LinkState, f.LinkNextAt = "unique", 0
				if f.LinkDeadline > at+int64(days)*86400 {
					t.Fatal("link review exceeds retained facts")
				}
				if err := m.storeDB.Create(&f).Error; err != nil {
					t.Fatal(err)
				}
				tokenCleanupSettledFact(t, m, f, now)
				totalBytes += f.StoredBytes
			}
			if err := m.storeDB.Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Updates(map[string]any{"fact_count": 3, "fact_bytes": totalBytes}).Error; err != nil {
				t.Fatal(err)
			}
			tokenCleanupAuthorizeFixture(t, m, now, cutoff)
			if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
				t.Fatal(err)
			}
			var rows []TokenMeteringFact
			if err := m.storeDB.Order("created_at").Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 || rows[0].CreatedAt != cutoff {
				t.Fatalf("retention boundary changed: %+v", rows)
			}
			m.cfg.TokenFactsInitialLookbackSeconds = (days + 1) * 86400
			if validateTokenMeteringSettings(m.cfg) == nil {
				t.Fatal("initial scan outside retention accepted")
			}
			m.cfg.TokenFactsRetentionDays = 3
			m.cfg.TokenFactsInitialLookbackSeconds = 900
			if validateTokenMeteringSettings(m.cfg) == nil {
				t.Fatal("three-day retention must be rejected")
			}
		})
	}
}

func TestTokenStorageFourDayReferenceExpiryPreservesUnavailableAnchor(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenDetectionRetentionDays = 4
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-200, 3, 10000, 0, 0)
	detectionRun(t, m, now)
	var refs []TokenDetectionReference
	if err := m.storeDB.Find(&refs).Error; err != nil || len(refs) == 0 {
		t.Fatalf("reference fixture: %d %v", len(refs), err)
	}
	for _, ref := range refs {
		if ref.ExpiresAt != now+4*86400 {
			t.Fatal("reference retained beyond four days")
		}
	}
	if err := m.cleanupTokenDetection(context.Background(), now+4*86400); err != nil {
		t.Fatal(err)
	}
	var retained []TokenDetectionReference
	if err := m.storeDB.Find(&retained).Error; err != nil || len(retained) == 0 {
		t.Fatalf("anchor identity lost: %v", err)
	}
	for _, ref := range retained {
		if ref.SnapshotJSON == "" || ref.StoredBytes == 0 {
			t.Fatal("referenced evidence deleted merely because its live eligibility expired")
		}
	}
}
