package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestValidateCloudWatchNginxSettings(t *testing.T) {
	valid := Settings{CloudWatchLogsEnabled: true, CloudWatchNginxEnabled: true, NginxEnabled: true,
		CloudWatchNginxPollSeconds: 300, CloudWatchNginxLookbackHours: 168}
	if err := validateCloudWatchNginxSettings(valid); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Settings{
		{CloudWatchNginxEnabled: true, NginxEnabled: true, CloudWatchNginxPollSeconds: 300, CloudWatchNginxLookbackHours: 168},
		{CloudWatchLogsEnabled: true, CloudWatchNginxEnabled: true, CloudWatchNginxPollSeconds: 300, CloudWatchNginxLookbackHours: 168},
		{CloudWatchLogsEnabled: true, CloudWatchNginxEnabled: true, NginxEnabled: true, LocalSnapshotOnly: true, CloudWatchNginxPollSeconds: 300, CloudWatchNginxLookbackHours: 168},
		{CloudWatchLogsEnabled: true, CloudWatchNginxEnabled: true, NginxEnabled: true, CloudWatchNginxPollSeconds: 59, CloudWatchNginxLookbackHours: 168},
		{CloudWatchLogsEnabled: true, CloudWatchNginxEnabled: true, NginxEnabled: true, CloudWatchNginxPollSeconds: 300, CloudWatchNginxLookbackHours: 169},
	} {
		if err := validateCloudWatchNginxSettings(bad); err == nil {
			t.Fatalf("unsafe config accepted: %+v", bad)
		}
	}
}

func TestCloudWatchNginxQueryIsClosedAndUsesContinuousLimit(t *testing.T) {
	from := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	source, query, limit, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{
		Kind: cwQueryWorkerNginxContinuous, From: from, To: from.Add(time.Hour), Limit: cloudWatchLogsNginxLimit,
	})
	if err != nil || source.ID != cwSourceWorkerNginx || limit != cloudWatchLogsNginxLimit {
		t.Fatalf("source=%+v limit=%d err=%v", source, limit, err)
	}
	for _, required := range []string{"ispresent(request_method)", "request_time", "@message like", "limit 10000"} {
		if !strings.Contains(query, required) {
			t.Fatalf("continuous query missing %q: %s", required, query)
		}
	}
	if _, _, _, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{
		Kind: cwQueryWorkerNginxContinuous, From: from, To: from.Add(time.Hour), Value: "user input",
	}); cloudWatchLogsErrorKindOf(err) != cwLogsErrInvalid {
		t.Fatalf("continuous query accepted caller value: %v", err)
	}
}

func TestCloudWatchNginxSamplesDeduplicateAndAggregate(t *testing.T) {
	from := int64(1_800_000_000) / 60 * 60
	status, request100, request500, upstream, bytes := 200, int64(100), int64(500), int64(80), int64(123)
	evidence := []cloudWatchStructuredEvidence{
		{Kind: cwEvidenceNginxAccess, EventRef: "a", EventMS: (from + 1) * 1000, Method: "POST", Route: "/v1/responses?ignored=1", Status: &status, RequestMS: &request100, UpstreamMS: &upstream, BytesSent: &bytes, UpstreamStatuses: []int{200}, OneAPIIDHMAC: "id"},
		{Kind: cwEvidenceNginxAccess, EventRef: "a", EventMS: (from + 1) * 1000, Method: "POST", Route: "/v1/responses", Status: &status, RequestMS: &request100},
		{Kind: cwEvidenceNginxAccess, EventRef: "b", EventMS: (from + 2) * 1000, Method: "POST", Route: "/v1/responses", Status: &status, RequestMS: &request500, UpstreamMS: &upstream, UpstreamStatuses: []int{200}},
		{Kind: cwEvidenceNginxError, EventRef: "e", EventMS: (from + 3) * 1000, Category: "upstream_timeout", Severity: "error"},
	}
	access, errors, lastAccess, lastError, err := cloudWatchNginxSamples(evidence, from, from+60)
	if err != nil {
		t.Fatal(err)
	}
	if len(access) != 1 || access[0].Count != 2 || access[0].RequestTimeSumMS != 600 || access[0].RequestTimeMaxMS != 500 ||
		access[0].UpstreamTimeCount != 2 || access[0].UpstreamTimeSumMS != 160 || access[0].RequestIDPresent != 1 ||
		access[0].LatencyCount != 2 || access[0].Latency0To1s != 2 || access[0].BytesSent != 123 {
		t.Fatalf("access=%+v", access)
	}
	if len(errors) != 1 || errors[0].Count != 1 || errors[0].Category != "upstream_timeout" ||
		lastAccess != from+2 || lastError != from+3 {
		t.Fatalf("errors=%+v last=%d/%d", errors, lastAccess, lastError)
	}
}

func TestPersistCloudWatchNginxRequestEvidenceUsesHMACOnlyRows(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&NginxRequestEvidence{}, &NginxEvidenceIngestBatch{}, &NginxEvidenceSourceState{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	from := now - 60
	status, requestMS, upstreamMS := 502, int64(1200), int64(1100)
	item := cloudWatchStructuredEvidence{
		Kind: cwEvidenceNginxAccess, EventRef: strings.Repeat("a", 64), EventMS: now * 1000,
		HMACKeyID: "cw-1", NginxIDHMAC: strings.Repeat("b", 64), OneAPIIDHMAC: strings.Repeat("c", 64),
		Method: "POST", Route: "/v1/responses", Status: &status, RequestMS: &requestMS,
		UpstreamMS: &upstreamMS, UpstreamStatuses: []int{502}, Completion: "incomplete_at_edge",
	}
	m := &Monitor{nginxEvidenceDB: db, cfg: Settings{NginxEvidenceMode: "verified", NginxEvidenceRetentionHours: 168}}
	// The same CloudWatch event may be returned twice when a query is split or
	// replayed.  It must be persisted once; otherwise the EventID primary key
	// makes the whole window fail and the cursor never advances.
	duplicate := item
	duplicate.Status = func() *int { v := 503; return &v }()
	if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), []cloudWatchStructuredEvidence{item, duplicate}, from, now+60, "cw-test"); err != nil {
		t.Fatal(err)
	}
	var got NginxRequestEvidence
	if err := db.First(&got, "event_id = ?", item.EventRef).Error; err != nil {
		t.Fatal(err)
	}
	if got.Node != cloudWatchNginxNode || got.OneAPIIDHMAC != item.OneAPIIDHMAC || got.UpstreamStatus != 502 || got.UpstreamAttempts != 1 || got.BatchID != "cw-test" {
		t.Fatalf("persisted evidence mismatch: %+v", got)
	}
	var count int64
	if err := db.Model(&NginxRequestEvidence{}).Where("event_id = ?", item.EventRef).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("duplicate event persisted %d times", count)
	}
}

func TestPersistCloudWatchNginxRequestEvidenceLargeBatchChunksAndReplaces(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:large-evidence-chunks?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&NginxRequestEvidence{}, &NginxEvidenceIngestBatch{}, &NginxEvidenceSourceState{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	from, to := now-600, now+60
	stale := NginxRequestEvidence{EventID: strings.Repeat("f", 64), EventMS: (now - 500) * 1000,
		Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 599, RequestMS: 1,
		OneAPIIDHMAC: strings.Repeat("e", 64), HMACKeyID: "cw-1"}
	if err := db.Create(&stale).Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{nginxEvidenceDB: db, cfg: Settings{NginxEvidenceMode: "verified", NginxEvidenceRetentionHours: 168}}
	makeEvidence := func(count, statusCode int) []cloudWatchStructuredEvidence {
		out := make([]cloudWatchStructuredEvidence, 0, count)
		for i := 0; i < count; i++ {
			status, requestMS, upstreamMS := statusCode, int64(10+i%17), int64(8+i%11)
			out = append(out, cloudWatchStructuredEvidence{
				Kind: cwEvidenceNginxAccess, EventRef: fmt.Sprintf("%064x", i+1), EventMS: (now - 120 + int64(i%60)) * 1000,
				HMACKeyID: "cw-1", NginxIDHMAC: fmt.Sprintf("%064x", i+10_000), OneAPIIDHMAC: fmt.Sprintf("%064x", i+20_000),
				Method: "POST", Route: "/v1/responses", Status: &status, RequestMS: &requestMS,
				UpstreamMS: &upstreamMS, UpstreamStatuses: []int{statusCode}, Completion: "complete_at_edge",
			})
		}
		return out
	}
	first := makeEvidence(cloudWatchNginxPersistMaxEvents+501, 502)
	if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), first, from, to, "cw-large-1"); err != nil {
		t.Fatalf("large evidence batch failed: %v", err)
	}
	var count int64
	if err := db.Model(&NginxRequestEvidence{}).Where("node = ? AND event_ms >= ? AND event_ms < ?", cloudWatchNginxNode, from*1000, to*1000).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != int64(len(first)) {
		t.Fatalf("large evidence batch count=%d want=%d", count, len(first))
	}
	if err := db.First(&NginxRequestEvidence{}, "event_id = ?", stale.EventID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("stale row survived replacement: err=%v", err)
	}

	// A replay must replace the prior rows, not accumulate them.  Reusing the
	// same EventRef with a changed status also proves the delete happened before
	// the independent insert chunks.
	second := makeEvidence(cloudWatchNginxPersistMaxEvents+17, 200)
	if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), second, from, to, "cw-large-2"); err != nil {
		t.Fatalf("large evidence replay failed: %v", err)
	}
	if err := db.Model(&NginxRequestEvidence{}).Where("node = ? AND event_ms >= ? AND event_ms < ?", cloudWatchNginxNode, from*1000, to*1000).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != int64(len(second)) {
		t.Fatalf("replayed evidence accumulated rows: count=%d want=%d", count, len(second))
	}
	var replaced NginxRequestEvidence
	if err := db.First(&replaced, "event_id = ?", second[0].EventRef).Error; err != nil {
		t.Fatal(err)
	}
	if replaced.Status != 200 || replaced.BatchID != "cw-large-2" {
		t.Fatalf("replayed evidence did not replace old row: %+v", replaced)
	}
}

func TestPersistCloudWatchNginxRequestEvidenceRejectsCrossWindowEventConflict(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:cross-window-evidence-conflict?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&NginxRequestEvidence{}, &NginxEvidenceIngestBatch{}, &NginxEvidenceSourceState{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	from, to := now-60, now+60
	eventID := strings.Repeat("d", 64)
	// Keep the existing identity outside the replacement window.  The
	// CloudWatch event identity is globally unique, so silently ignoring this
	// collision would hide a parser/source-boundary bug.
	existing := NginxRequestEvidence{EventID: eventID, EventMS: (from - 120) * 1000,
		Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 200,
		RequestMS: 10, OneAPIIDHMAC: strings.Repeat("e", 64), HMACKeyID: "cw-1"}
	if err := db.Create(&existing).Error; err != nil {
		t.Fatal(err)
	}
	status, requestMS := 502, int64(20)
	item := cloudWatchStructuredEvidence{Kind: cwEvidenceNginxAccess, EventRef: eventID,
		EventMS: (from + 1) * 1000, HMACKeyID: "cw-1", OneAPIIDHMAC: strings.Repeat("f", 64),
		Method: "POST", Route: "/v1/responses", Status: &status, RequestMS: &requestMS,
		Completion: "complete_at_edge"}
	m := &Monitor{nginxEvidenceDB: db, cfg: Settings{NginxEvidenceMode: "verified", NginxEvidenceRetentionHours: 168}}
	err = m.persistCloudWatchNginxRequestEvidence(context.Background(), []cloudWatchStructuredEvidence{item}, from, to, "cw-conflict")
	if err == nil || !strings.Contains(err.Error(), "insert cloudwatch nginx evidence batch") {
		t.Fatalf("cross-window identity conflict was not surfaced: %v", err)
	}
	var got NginxRequestEvidence
	if err := db.First(&got, "event_id = ?", eventID).Error; err != nil {
		t.Fatal(err)
	}
	if got.EventMS != existing.EventMS || got.Status != existing.Status {
		t.Fatalf("conflicting row was overwritten: got=%+v existing=%+v", got, existing)
	}
}

func TestCloudWatchNginxErrorDetailIsBoundedAndRedacted(t *testing.T) {
	local := cloudWatchNginxErrorDetail(errors.New("sqlite: UNIQUE constraint failed: nginx_request_evidence.event_id"))
	if !strings.Contains(local, "UNIQUE constraint failed") {
		t.Fatalf("local persistence detail was discarded: %q", local)
	}
	secret := cloudWatchNginxErrorDetail(errors.New("request failed Authorization: Bearer very-secret-token"))
	if strings.Contains(secret, "very-secret-token") || strings.Contains(secret, "Authorization") {
		t.Fatalf("sensitive detail leaked: %q", secret)
	}
	wrapped := cloudWatchNginxErrorDetail(newCloudWatchLogsError(cwLogsErrThrottled, "GetQueryResults", errors.New("AWS secret token")))
	if wrapped != "throttled operation=GetQueryResults cause=failed" {
		t.Fatalf("unexpected safe CloudWatch detail: %q", wrapped)
	}
}

func TestPublishCloudWatchNginxWindowReplacesOwnRowsAndPreservesLegacy(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	from := time.Now().Unix() / 60 * 60
	old := []NginxMinuteSample{
		{BucketTs: from, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 200, Count: 9},
		{BucketTs: from, Node: "legacy", Route: "/v1/responses", Method: "POST", Status: 200, Count: 4},
	}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	state := CloudWatchNginxCursor{ID: cloudWatchNginxCursorID, CoverageFromTs: from,
		NextTs: from, ThroughTs: from, TargetThroughTs: from + 60, SemanticsVersion: cloudWatchNginxVersion}
	rows := []NginxMinuteSample{{BucketTs: from, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 200, Count: 2}}
	if err := m.publishCloudWatchNginxWindow(context.Background(), &state, from, from+60, rows, nil, from+10, 0, from+60); err != nil {
		t.Fatal(err)
	}
	var stored []NginxMinuteSample
	if err := m.storeDB.Order("node ASC").Find(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if len(stored) != 2 || stored[0].Node != cloudWatchNginxNode || stored[0].Count != 2 || stored[1].Node != "legacy" || stored[1].Count != 4 {
		t.Fatalf("stored=%+v", stored)
	}
	if state.Status != "caught_up" || state.ThroughTs != from+60 || state.NextTs != from+60 {
		t.Fatalf("state=%+v", state)
	}
	var cursor CloudWatchNginxCursor
	if err := m.storeDB.First(&cursor, "id = ?", cloudWatchNginxCursorID).Error; err != nil || cursor.ThroughTs != from+60 {
		t.Fatalf("cursor=%+v err=%v", cursor, err)
	}
}

func TestCloudWatchNginxRepairWindowCyclesOverClosedHours(t *testing.T) {
	target := time.Date(2026, 9, 20, 15, 37, 0, 0, time.UTC).Unix()
	state := CloudWatchNginxCursor{CoverageFromTs: target - int64(7*24*time.Hour/time.Second)}
	start, end := cloudWatchNginxRepairBounds(state, target)
	if end != time.Date(2026, 9, 20, 15, 0, 0, 0, time.UTC).Unix() ||
		end-start != int64(cloudWatchNginxRepairHorizon/time.Second) {
		t.Fatalf("unexpected repair bounds: %d..%d", start, end)
	}
	from, to, ok := cloudWatchNginxRepairWindow(state, target)
	if !ok || from != start || to != start+3600 {
		t.Fatalf("unexpected first repair window: %d..%d ok=%v", from, to, ok)
	}
	state.RepairNextTs = end - 3600
	from, to, ok = cloudWatchNginxRepairWindow(state, target)
	if !ok || from != end-3600 || to != end {
		t.Fatalf("unexpected last repair window: %d..%d ok=%v", from, to, ok)
	}
	state.RepairNextTs = end
	from, to, ok = cloudWatchNginxRepairWindow(state, target)
	if !ok || from != start || to != start+3600 {
		t.Fatalf("repair cursor did not wrap: %d..%d ok=%v", from, to, ok)
	}
}

func TestCloudWatchNginxEvidenceWindowUsesValidatorBoundedChunks(t *testing.T) {
	state := CloudWatchNginxCursor{EvidenceCoverageFromTs: 1000, EvidenceNextTs: 1000}
	from, to, ok := cloudWatchNginxEvidenceWindow(state, 20_000)
	if !ok || from != 1000 || to != 1000+int64(cloudWatchNginxEvidenceChunkWindow/time.Second) {
		t.Fatalf("unexpected evidence chunk: %d..%d ok=%v", from, to, ok)
	}
	if to-from > int64(cloudWatchLogsMaxWindow/time.Second) {
		t.Fatalf("evidence chunk exceeds CloudWatch validator range: %d seconds", to-from)
	}
	state.EvidenceNextTs = 20_000
	if _, _, ok := cloudWatchNginxEvidenceWindow(state, 20_000); ok {
		t.Fatal("evidence cursor at target should not produce another chunk")
	}
	state.EvidenceStatus = "disabled"
	if _, _, ok := cloudWatchNginxEvidenceWindow(state, 30_000); ok {
		t.Fatal("disabled evidence lane must not issue CloudWatch queries")
	}
}

func TestCloudWatchNginxEvidenceChunkIsAcceptedByInsightsValidator(t *testing.T) {
	from := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	to := from.Add(cloudWatchNginxEvidenceChunkWindow)
	if _, _, _, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{
		Kind: cwQueryWorkerNginxContinuous, From: from, To: to, Limit: cloudWatchLogsNginxLimit,
	}); err != nil {
		t.Fatalf("configured evidence chunk must be queryable: %v", err)
	}
	if _, _, _, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{
		Kind: cwQueryWorkerNginxContinuous, From: from, To: to.Add(time.Second), Limit: cloudWatchLogsNginxLimit,
	}); cloudWatchLogsErrorKindOf(err) != cwLogsErrInvalid {
		t.Fatalf("range over validator limit should be rejected, err=%v", err)
	}
}

func TestPublishCloudWatchNginxRepairReplacesLateRowsWithoutMovingMainCursor(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	target := time.Now().Unix() / 3600 * 3600
	from := target - int64(24*time.Hour/time.Second)
	state := CloudWatchNginxCursor{
		ID: cloudWatchNginxCursorID, CoverageFromTs: target - int64(7*24*time.Hour/time.Second),
		NextTs: target, ThroughTs: target, TargetThroughTs: target,
		RepairNextTs: from, SemanticsVersion: cloudWatchNginxVersion, Status: "caught_up",
	}
	first := []NginxMinuteSample{{BucketTs: from, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 200, Count: 1}}
	if err := m.publishCloudWatchNginxRepairWindow(context.Background(), &state, from, from+3600, first, nil, from+1, 0, target); err != nil {
		t.Fatal(err)
	}
	state.RepairNextTs = from
	late := []NginxMinuteSample{{BucketTs: from, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 200, Count: 2}}
	if err := m.publishCloudWatchNginxRepairWindow(context.Background(), &state, from, from+3600, late, nil, from+2, 0, target); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.storeDB.Model(&NginxMinuteSample{}).
		Where("node = ? AND bucket_ts >= ? AND bucket_ts < ?", cloudWatchNginxNode, from, from+3600).
		Select("COALESCE(SUM(count), 0)").Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("late repair accumulated duplicate rows: count=%d", count)
	}
	if state.NextTs != target || state.ThroughTs != target || state.Status != "caught_up" || state.LastRepairAt == 0 {
		t.Fatalf("repair changed main cursor: %+v", state)
	}
}

func TestCloudWatchNginxRepairFailureKeepsCursor(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	target := time.Now().Unix() / 3600 * 3600
	from := target - int64(24*time.Hour/time.Second)
	state := CloudWatchNginxCursor{
		ID: cloudWatchNginxCursorID, CoverageFromTs: target - int64(7*24*time.Hour/time.Second),
		NextTs: target, ThroughTs: target, RepairNextTs: from,
		SemanticsVersion: cloudWatchNginxVersion, Status: "caught_up",
	}
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := m.publishCloudWatchNginxRepairWindow(ctx, &state, from, from+3600, nil, nil, 0, 0, target)
	if err == nil {
		t.Fatal("canceled repair unexpectedly succeeded")
	}
	if state.RepairNextTs != from || state.NextTs != target || state.ThroughTs != target {
		t.Fatalf("failed repair moved in-memory cursor: %+v", state)
	}
	var stored CloudWatchNginxCursor
	if dbErr := m.storeDB.First(&stored, "id = ?", cloudWatchNginxCursorID).Error; dbErr != nil {
		t.Fatal(dbErr)
	}
	if stored.RepairNextTs != from || stored.NextTs != target || stored.ThroughTs != target {
		t.Fatalf("failed repair moved stored cursor: %+v", stored)
	}
}

func TestPersistCloudWatchNginxRequestEvidenceEmptyWindowRemovesStaleRows(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&NginxRequestEvidence{}, &NginxEvidenceIngestBatch{}, &NginxEvidenceSourceState{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	stale := NginxRequestEvidence{EventID: strings.Repeat("d", 64), EventMS: now * 1000, Node: cloudWatchNginxNode,
		Route: "/v1/responses", Method: "POST", Status: 200, RequestMS: 10,
		OneAPIIDHMAC: strings.Repeat("e", 64), HMACKeyID: "cw-1"}
	if err := db.Create(&stale).Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{nginxEvidenceDB: db, cfg: Settings{NginxEvidenceMode: "verified", NginxEvidenceRetentionHours: 168}}
	if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), nil, now-60, now+60, "cw-empty"); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&NginxRequestEvidence{}).Where("event_id = ?", stale.EventID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("complete empty window retained stale evidence: %d", count)
	}
}

func TestAttachNginxEvidenceReturnsNewestRowPerRequestWithoutGlobalTruncation(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&NginxRequestEvidence{}, &NginxEvidenceIngestBatch{}, &NginxEvidenceSourceState{}); err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("k", 32)
	m := &Monitor{nginxEvidenceDB: db, cfg: Settings{NginxEvidenceMode: "verified", NginxEvidenceHMACKey: key, NginxEvidenceHMACKeyID: "key-1"}}
	firstID, secondID := "req-first", "req-second"
	firstHash := nginxEvidenceIDHMAC(key, "oneapi-request-id", firstID)
	secondHash := nginxEvidenceIDHMAC(key, "oneapi-request-id", secondID)
	rows := []NginxRequestEvidence{
		{EventID: strings.Repeat("1", 64), EventMS: 100, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 499, OneAPIIDHMAC: firstHash, HMACKeyID: "key-1"},
		{EventID: strings.Repeat("2", 64), EventMS: 200, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 502, OneAPIIDHMAC: firstHash, HMACKeyID: "key-1"},
		{EventID: strings.Repeat("3", 64), EventMS: 300, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 200, OneAPIIDHMAC: firstHash, HMACKeyID: "key-1"},
		{EventID: strings.Repeat("4", 64), EventMS: 150, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 503, OneAPIIDHMAC: secondHash, HMACKeyID: "key-1"},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	logs := []LogChainRow{{RequestID: firstID}, {RequestID: secondID}}
	if err := m.attachNginxEvidence(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	if logs[0].EdgeEvidence == nil || logs[0].EdgeEvidence.Status != 200 {
		t.Fatalf("newest evidence was not selected for first request: %+v", logs[0].EdgeEvidence)
	}
	if logs[1].EdgeEvidence == nil || logs[1].EdgeEvidence.Status != 503 {
		t.Fatalf("later request evidence was truncated: %+v", logs[1].EdgeEvidence)
	}
}

func TestLoadCloudWatchNginxCursorRestoresRepairCursor(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.CloudWatchNginxLookbackHours = 168
	now := time.Date(2026, 9, 20, 15, 37, 0, 0, time.UTC)
	_, target := cloudWatchNginxRange(now, m.cfg.CloudWatchNginxLookbackHours)
	repairNext := target/3600*3600 - 12*3600
	state := CloudWatchNginxCursor{
		ID: cloudWatchNginxCursorID, CoverageFromTs: target - 168*3600,
		NextTs: target, ThroughTs: target, TargetThroughTs: target,
		RepairNextTs: repairNext, LastRepairAt: now.Add(-time.Minute).Unix(),
		SemanticsVersion: cloudWatchNginxVersion, Status: "caught_up",
	}
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	restored, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	if restored.RepairNextTs != repairNext || restored.LastRepairAt != state.LastRepairAt ||
		restored.NextTs != target || restored.ThroughTs != target {
		t.Fatalf("repair cursor was not restored: %+v", restored)
	}
}

func TestLoadCloudWatchNginxCursorInitializesIndependentEvidenceBackfill(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.NginxEvidenceMode = "verified"
	m.cfg.NginxEvidenceRetentionHours = 168
	evidenceDB, err := gorm.Open(sqlite.Open("file:evidence-cursor-test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	m.nginxEvidenceDB = evidenceDB
	if err := evidenceDB.AutoMigrate(&NginxRequestEvidence{}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 20, 15, 37, 0, 0, time.UTC)
	_, target := cloudWatchNginxRange(now, m.cfg.CloudWatchNginxLookbackHours)
	state := CloudWatchNginxCursor{
		ID: cloudWatchNginxCursorID, CoverageFromTs: target - 72*3600,
		NextTs: target, ThroughTs: target, TargetThroughTs: target,
		SemanticsVersion: cloudWatchNginxVersion, Status: "caught_up",
	}
	if err := m.storeDB.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	got, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	wantFrom, wantTarget := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	if got.EvidenceCoverageFromTs != wantFrom || got.EvidenceNextTs != wantFrom ||
		got.EvidenceThroughTs != wantFrom || got.EvidenceStatus != "running" || wantTarget <= wantFrom {
		t.Fatalf("旧主水位 caught_up 时 evidence 补扫未从保留期起点初始化: got=%+v want=%d..%d", got, wantFrom, wantTarget)
	}
}

// TestCloudWatchNginxRealClosedWindowReconciliation is opt-in because it needs
// read-only AWS credentials and a read-only mount of a populated Monitor DB.
// It compares only closed aggregates and never prints raw logs or identifiers.
func TestCloudWatchNginxRealClosedWindowReconciliation(t *testing.T) {
	dbPath := strings.TrimSpace(os.Getenv("MONITOR_CLOUDWATCH_NGINX_RECONCILE_DB"))
	fromRaw := strings.TrimSpace(os.Getenv("MONITOR_CLOUDWATCH_NGINX_RECONCILE_FROM"))
	toRaw := strings.TrimSpace(os.Getenv("MONITOR_CLOUDWATCH_NGINX_RECONCILE_TO"))
	if dbPath == "" || fromRaw == "" || toRaw == "" {
		t.Skip("real CloudWatch/SQLite reconciliation is not configured")
	}
	from, err := strconv.ParseInt(fromRaw, 10, 64)
	if err != nil {
		t.Fatalf("invalid reconcile from: %v", err)
	}
	to, err := strconv.ParseInt(toRaw, 10, 64)
	if err != nil || to <= from || to-from > int64(cloudWatchNginxWindow/time.Second) {
		t.Fatalf("invalid reconcile range: %d..%d", from, to)
	}

	m := &Monitor{cloudWatchLogs: newCloudWatchLogsRuntime(true, nil)}
	parser, err := newCloudWatchEvidenceParser(strings.Repeat("r", 32), "reconcile", false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	queries := 0
	evidence, err := m.queryCloudWatchNginxRange(ctx, parser, time.Unix(from, 0).UTC(), time.Unix(to, 0).UTC(), &queries, 0)
	if err != nil {
		t.Fatalf("CloudWatch closed-window query failed after %d queries: %v", queries, err)
	}
	wantAccess, wantErrors, _, _, err := cloudWatchNginxSamples(evidence, from, to)
	if err != nil {
		t.Fatal(err)
	}

	db, err := gorm.Open(sqlite.Open("file:"+dbPath+"?mode=ro"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open read-only reconciliation DB: %v", err)
	}
	var gotAccess []NginxMinuteSample
	if err := db.Where("node = ? AND bucket_ts >= ? AND bucket_ts < ?", cloudWatchNginxNode, from, to).
		Order("bucket_ts, route, method, status, upstream_status").Find(&gotAccess).Error; err != nil {
		t.Fatal(err)
	}
	var gotErrors []NginxErrorMinuteSample
	if err := db.Where("node = ? AND bucket_ts >= ? AND bucket_ts < ?", cloudWatchNginxNode, from, to).
		Order("bucket_ts, category, severity").Find(&gotErrors).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotAccess, wantAccess) || !reflect.DeepEqual(gotErrors, wantErrors) {
		var gotRequests, wantRequests, gotErrorCount, wantErrorCount int64
		for _, row := range gotAccess {
			gotRequests += row.Count
		}
		for _, row := range wantAccess {
			wantRequests += row.Count
		}
		for _, row := range gotErrors {
			gotErrorCount += row.Count
		}
		for _, row := range wantErrors {
			wantErrorCount += row.Count
		}
		firstAccessMismatch := "none"
		for i := 0; i < len(gotAccess) && i < len(wantAccess); i++ {
			if !reflect.DeepEqual(gotAccess[i], wantAccess[i]) {
				firstAccessMismatch = fmt.Sprintf("stored=%+v queried=%+v", gotAccess[i], wantAccess[i])
				break
			}
		}
		firstErrorMismatch := "none"
		for i := 0; i < len(gotErrors) && i < len(wantErrors); i++ {
			if !reflect.DeepEqual(gotErrors[i], wantErrors[i]) {
				firstErrorMismatch = fmt.Sprintf("stored=%+v queried=%+v", gotErrors[i], wantErrors[i])
				break
			}
		}
		t.Fatalf("closed-window mismatch: access rows=%d/%d requests=%d/%d error rows=%d/%d errors=%d/%d queries=%d first_access=%s first_error=%s",
			len(gotAccess), len(wantAccess), gotRequests, wantRequests,
			len(gotErrors), len(wantErrors), gotErrorCount, wantErrorCount, queries,
			firstAccessMismatch, firstErrorMismatch)
	}
	t.Logf("closed-window match: access_rows=%d error_rows=%d queries=%d", len(gotAccess), len(gotErrors), queries)
}

func TestNginxReportPrefersCloudWatchWithinSameMinute(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.NginxEnabled, m.cfg.NginxRetentionDays, m.cfg.CloudWatchNginxEnabled = true, 7, true
	m.cfg.NginxExpectedNodes = []string{}
	bucket := time.Now().Unix() / 60 * 60
	rows := []NginxMinuteSample{
		{BucketTs: bucket, Node: "legacy", Route: "/v1/responses", Method: "POST", Status: 200, Count: 9, RequestTimeSumMS: 900},
		{BucketTs: bucket, Node: cloudWatchNginxNode, Route: "/v1/responses", Method: "POST", Status: 200, Count: 2, RequestTimeSumMS: 200},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&NginxSourceState{Node: cloudWatchNginxNode, LastEventTs: bucket, LastIngestTs: time.Now().Unix(), BacklogKnown: true}).Error; err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/stability/edge?days=1", nil)
	m.serveNginxEdge(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("report: %d %s", recorder.Code, recorder.Body.String())
	}
	var report NginxEdgeReport
	if err := json.Unmarshal(recorder.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Summary.Requests != 2 || len(report.Nodes) != 1 || report.Nodes[0].Name != cloudWatchNginxNode || len(report.Sources) != 1 {
		t.Fatalf("report double counted overlapping sources: %+v", report)
	}
}

func TestCloudWatchNginxCursorBumpsMigrationPlan(t *testing.T) {
	if !strings.Contains(preMigrationPlanID, "v52") || !strings.Contains(preMigrationPlanID, "nginx-cursor-v1-nginx-repair-cursor-v1") || !strings.Contains(preMigrationPlanID, "nginx-evidence-backfill-v1") {
		t.Fatalf("CloudWatch Nginx 水位表加入 AutoMigrate 后必须产生独立迁移快照: %s", preMigrationPlanID)
	}
	if !strings.Contains(preMigrationCombinedPlanID, "v52") || !strings.Contains(preMigrationCombinedPlanID, "nginx-cursor-v1-nginx-repair-cursor-v1") || !strings.Contains(preMigrationCombinedPlanID, "nginx-evidence-backfill-v1") {
		t.Fatalf("source-v2 组合方案也必须为 CloudWatch Nginx 水位生成独立迁移快照: %s", preMigrationCombinedPlanID)
	}
}
