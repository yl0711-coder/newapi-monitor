package monitor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestFinanceBackgroundRefreshFailureLoggedAndCachePreserved(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "refresh.example")
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	request := financeReportRequest{from: from, to: from.Add(time.Hour), configurationHash: "changed"}
	want := []byte(`{"enabled":true,"generated_at":123}`)
	m.getFinanceReportCache().PutWithStale(request.cacheKey(), want, time.Minute, time.Minute, time.Now())
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(previous)
	m.refreshFinanceReport(context.Background(), request)
	got, ok := m.getFinanceReportCache().Get(request.cacheKey(), time.Now())
	if !ok || !bytes.Equal(got, want) {
		t.Fatal("failed refresh replaced existing report")
	}
	if !strings.Contains(output.String(), "经营核算后台刷新失败") || !strings.Contains(output.String(), "报表生成期间已变更") || !strings.Contains(output.String(), "elapsed_ms") {
		t.Fatalf("missing diagnostic: %s", output.String())
	}
}

func TestFinanceDisabledHandlerDoesNotReadStoresOrSnapshots(t *testing.T) {
	m := &Monitor{cfg: Settings{FinanceEnabled: false, FinanceStartDate: "2026-05-01", FinanceReportSnapshotReadEnabled: true}}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/finance/report?from=2026-05-01&to=2026-05-02", nil)
	m.serveFinanceOperatingReport(c)
	if w.Code != http.StatusOK {
		t.Fatalf("disabled handler touched unavailable store: %d %s", w.Code, w.Body.String())
	}
	var report financeOperatingReport
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Enabled || report.Stage != "disabled" {
		t.Fatalf("disabled feature reused enabled payload: %+v", report)
	}
}

func TestFinanceReportCacheKeySeparatesRangesAndSnapshots(t *testing.T) {
	base := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0)}
	cases := []financeReportRequest{
		base,
		{from: base.from, to: time.Unix(10800, 0)},
		{from: base.from, to: base.to, snapshotAsOf: 7200},
		{from: base.from, to: base.to, snapshotAsOf: 7200, snapshotClamped: true},
		{from: base.from, to: base.to, sourceFingerprint: "source-v2"},
	}
	seen := make(map[string]struct{}, len(cases))
	for _, request := range cases {
		key := request.cacheKey()
		if _, exists := seen[key]; exists {
			t.Fatalf("finance report cache key collision: %q", key)
		}
		seen[key] = struct{}{}
	}
}

func TestFinanceReportPayloadUsesBoundedCachedJSON(t *testing.T) {
	m := &Monitor{}
	request := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0)}
	want := []byte(`{"enabled":true,"generated_at":123}`)
	m.getFinanceReportCache().PutWithStale(request.cacheKey(), want, time.Minute, time.Minute, time.Now())

	got, status, err := m.financeReportPayload(context.Background(), request, false)
	if err != nil {
		t.Fatal(err)
	}
	if status != "hit" || string(got) != string(want) {
		t.Fatalf("cached finance response mismatch: status=%q payload=%s", status, got)
	}
	entries, bytes := m.getFinanceReportCache().size()
	if entries != 1 || bytes != len(want) {
		t.Fatalf("unexpected bounded cache size: entries=%d bytes=%d", entries, bytes)
	}
}

func TestFinanceReportPayloadMissHitAndForcedRefresh(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "cache.example")
	loc, _ := time.LoadLocation("Asia/Shanghai")
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, loc)
	request := financeReportRequest{from: from, to: from.Add(time.Hour)}

	first, status, err := m.financeReportPayload(context.Background(), request, false)
	if err != nil || status != "miss" || len(first) == 0 {
		t.Fatalf("first finance report build failed: status=%q bytes=%d err=%v", status, len(first), err)
	}
	second, status, err := m.financeReportPayload(context.Background(), request, false)
	if err != nil || status != "hit" || string(second) != string(first) {
		t.Fatalf("finance report cache hit mismatch: status=%q bytes=%d err=%v", status, len(second), err)
	}
	refreshed, status, err := m.financeReportPayload(context.Background(), request, true)
	if err != nil || status != "refresh" || len(refreshed) == 0 {
		t.Fatalf("forced finance report refresh failed: status=%q bytes=%d err=%v", status, len(refreshed), err)
	}
}

func TestFinanceReportSnapshotShadowRoundTripIsExactAndBounded(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(dir, "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true,
	}}
	payload := []byte(`{"enabled":true,"generated_at":123,"statement":{"known_user_consumption":{"micro_usd":"900000"}}}`)
	base := time.Unix(1_789_752_000, 0)
	for index := 0; index < financeReportCacheMaxEntries+2; index++ {
		if err := m.persistFinanceReportSnapshotShadow(
			fmt.Sprintf("key-%d", index), "source-v1", payload, base.Add(time.Duration(index)*time.Second),
		); err != nil {
			t.Fatal(err)
		}
	}
	cacheDir, err := financeReportSnapshotDir(m.cfg.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != financeReportCacheMaxEntries {
		t.Fatalf("persistent shadow cache is not bounded: entries=%d", len(entries))
	}
	latestPath, err := financeReportSnapshotPath(m.cfg.StorePath, fmt.Sprintf("key-%d", financeReportCacheMaxEntries+1))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(latestPath)
	if err != nil {
		t.Fatal(err)
	}
	var envelope financeReportSnapshotEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if envelope.FormatVersion != financeReportSnapshotFormatVersion ||
		envelope.CacheKey != fmt.Sprintf("key-%d", financeReportCacheMaxEntries+1) ||
		envelope.SourceFingerprint != "source-v1" ||
		envelope.PayloadSHA256 != hex.EncodeToString(digest[:]) || string(envelope.Payload) != string(payload) {
		t.Fatalf("persistent shadow snapshot changed finance payload: %+v", envelope)
	}
	info, err := os.Stat(latestPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("persistent shadow snapshot permissions=%o want=600", info.Mode().Perm())
	}
}

func TestFinanceReportSnapshotShadowIsDisabledAndRejectsInvalidPayload(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{StorePath: filepath.Join(dir, "monitor.db")}}
	if err := m.persistFinanceReportSnapshotShadow("disabled", "source-v1", []byte(`{"ok":true}`), time.Now()); err != nil {
		t.Fatal(err)
	}
	cacheDir, err := financeReportSnapshotDir(m.cfg.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		t.Fatalf("disabled shadow cache wrote to disk: err=%v", err)
	}
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	if err := m.persistFinanceReportSnapshotShadow("invalid", "source-v1", []byte(`{"broken":`), time.Now()); err == nil {
		t.Fatal("invalid finance snapshot payload must be rejected")
	}
}

func TestFinanceReportPayloadReadsValidatedPersistentSnapshot(t *testing.T) {
	dir := t.TempDir()
	request := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0), configurationHash: "config-v1", sourceFingerprint: "source-v1"}
	payload := []byte(`{"enabled":true,"generated_at":456}`)
	now := time.Now().Truncate(time.Second)
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(dir, "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true,
		FinanceReportSnapshotReadEnabled:   true,
	}}
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), request.sourceFingerprint, payload, now); err != nil {
		t.Fatal(err)
	}

	// A fresh process with no database handles proves the read returns before
	// the expensive report builder is reached.
	reader := &Monitor{cfg: Settings{
		StorePath:                        m.cfg.StorePath,
		FinanceReportSnapshotReadEnabled: true,
	}}
	got, status, err := reader.financeReportPayload(context.Background(), request, false)
	if err != nil || status != "persistent-hit" || string(got) != string(payload) {
		t.Fatalf("persistent finance cache hit mismatch: status=%q payload=%s err=%v", status, got, err)
	}
	entries, bytes := reader.getFinanceReportCache().size()
	if entries != 2 || bytes != 2*len(payload) {
		t.Fatalf("persistent hit did not warm bounded memory cache: entries=%d bytes=%d", entries, bytes)
	}
}

func TestFinanceReportSnapshotReadClassifiesStaleExpiredAndCorrupt(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(dir, "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true,
		FinanceReportSnapshotReadEnabled:   true,
	}}
	payload := []byte(`{"enabled":true,"generated_at":789}`)
	now := time.Now().Truncate(time.Second)

	staleRequest := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0), configurationHash: "stale", sourceFingerprint: "source-v2"}
	if err := m.persistFinanceReportSnapshotShadow(staleRequest.logicalKey(), "source-v1", payload, now.Add(-financeReportCacheTTL-time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, _, state, ok, err := m.loadFinanceReportSnapshot(staleRequest, now)
	if err != nil || !ok || state != "stale" || string(got) != string(payload) {
		t.Fatalf("stale persistent snapshot mismatch: state=%q ok=%v payload=%s err=%v", state, ok, got, err)
	}

	expiredRequest := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0), configurationHash: "expired", sourceFingerprint: "source-v1"}
	if err := m.persistFinanceReportSnapshotShadow(expiredRequest.logicalKey(), expiredRequest.sourceFingerprint, payload, now.Add(-financeReportPersistentStale)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, err := m.loadFinanceReportSnapshot(expiredRequest, now); err != nil || ok {
		t.Fatalf("expired persistent snapshot remained readable: ok=%v err=%v", ok, err)
	}

	corruptRequest := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(7200, 0), configurationHash: "corrupt", sourceFingerprint: "source-v1"}
	if err := m.persistFinanceReportSnapshotShadow(corruptRequest.logicalKey(), corruptRequest.sourceFingerprint, payload, now); err != nil {
		t.Fatal(err)
	}
	path, err := financeReportSnapshotPath(m.cfg.StorePath, corruptRequest.logicalKey())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var envelope financeReportSnapshotEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.PayloadSHA256 = strings.Repeat("0", 64)
	raw, err = json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, err := m.loadFinanceReportSnapshot(corruptRequest, now); err == nil || ok {
		t.Fatalf("corrupt persistent snapshot was accepted: ok=%v err=%v", ok, err)
	}
}

func TestFinanceReportSourceFingerprintTracksOnlyRelevantPublishedChanges(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "fingerprint.example")
	ctx := context.Background()
	from := int64(1_777_516_800)
	to := from + 2*3600

	base, err := m.financeReportSourceFingerprint(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := m.financeReportSourceFingerprint(ctx, from, to)
	if err != nil || repeat != base {
		t.Fatalf("finance source fingerprint is not deterministic: base=%q repeat=%q err=%v", base, repeat, err)
	}

	inside := StabilityHourIngestState{
		HourTs: from, Status: "complete", Rows: 1, Requests: 2, Tokens: 3,
		Quota: 4, TrafficClassVersion: stabilityTrafficClassificationVersion,
		UpdatedAt: from + 10, CompletedAt: from + 10,
	}
	if err := m.storeDB.Create(&inside).Error; err != nil {
		t.Fatal(err)
	}
	changed, err := m.financeReportSourceFingerprint(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if changed == base {
		t.Fatal("published in-range finance input did not invalidate source fingerprint")
	}

	outside := StabilityHourIngestState{
		HourTs: to + 3600, Status: "complete", Rows: 9, Requests: 9, Tokens: 9,
		Quota: 9, TrafficClassVersion: stabilityTrafficClassificationVersion,
		UpdatedAt: to + 3610, CompletedAt: to + 3610,
	}
	if err := m.storeDB.Create(&outside).Error; err != nil {
		t.Fatal(err)
	}
	unchanged, err := m.financeReportSourceFingerprint(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged != changed {
		t.Fatal("out-of-range hourly publication unnecessarily invalidated finance fingerprint")
	}
}

func TestFinanceReportSnapshotMatchingFingerprintSurvivesShortTTL(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(dir, "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true,
		FinanceReportSnapshotReadEnabled:   true,
	}}
	request := financeReportRequest{
		from: time.Unix(3600, 0), to: time.Unix(7200, 0),
		configurationHash: "config-v1", sourceFingerprint: "source-v1",
	}
	payload := []byte(`{"enabled":true,"generated_at":999}`)
	now := time.Now().Truncate(time.Second)
	if err := m.persistFinanceReportSnapshotShadow(
		request.logicalKey(), request.sourceFingerprint, payload, now.Add(-2*financeReportCacheTTL),
	); err != nil {
		t.Fatal(err)
	}
	got, _, state, ok, err := m.loadFinanceReportSnapshot(request, now)
	if err != nil || !ok || state != "fresh" || string(got) != string(payload) {
		t.Fatalf("matching source fingerprint should keep snapshot fresh: state=%q ok=%v payload=%s err=%v", state, ok, got, err)
	}
}

func TestFinancePriorRangeSnapshotIsExplicitAndBounded(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(dir, "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true,
		FinanceReportSnapshotReadEnabled:   true,
	}}
	now := time.Now().Truncate(time.Second)
	request := financeReportRequest{
		from: time.Unix(3600, 0), to: time.Unix(3600, 0).Add(50 * time.Hour),
		configurationHash: "same-config", sourceFingerprint: "current-source",
	}
	prior := request
	prior.to = request.to.Add(-28 * time.Hour)
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":%d,"to":%d,"generated_at":%d}`,
		prior.from.Unix(), prior.to.Unix(), now.Add(-28*time.Hour).Unix()))
	if err := m.persistFinanceReportSnapshotShadow(prior.logicalKey(), "old-source", payload, now.Add(-28*time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.loadPriorFinanceReportSnapshot(request, now)
	if err != nil || !ok || !bytes.Equal(got, payload) {
		t.Fatalf("prior interval not recovered: ok=%t payload=%s err=%v", ok, got, err)
	}
	changedConfig := request
	changedConfig.configurationHash = "changed-config"
	if _, ok, err := m.loadPriorFinanceReportSnapshot(changedConfig, now); err != nil || ok {
		t.Fatalf("snapshot crossed configuration boundary: ok=%t err=%v", ok, err)
	}
	localSnapshot := request
	localSnapshot.snapshotAsOf = prior.to.Unix()
	if _, ok, err := m.loadPriorFinanceReportSnapshot(localSnapshot, now); err != nil || ok {
		t.Fatalf("static local snapshot used prior-range fallback: ok=%t err=%v", ok, err)
	}
	if _, _, _, ok, err := m.loadFinanceReportSnapshot(prior, now.Add(financeReportPersistentStale)); err != nil || ok {
		t.Fatalf("expired snapshot remained readable: ok=%t err=%v", ok, err)
	}
}

func TestFinancePriorRangeSnapshotRejectsMismatchedPayloadBounds(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(dir, "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true,
		FinanceReportSnapshotReadEnabled:   true,
	}}
	now := time.Now().Truncate(time.Second)
	request := financeReportRequest{from: time.Unix(3600, 0), to: time.Unix(10800, 0), configurationHash: "config"}
	prior := request
	prior.to = prior.to.Add(-time.Hour)
	bad := []byte(fmt.Sprintf(`{"enabled":true,"from":%d,"to":%d,"generated_at":%d}`,
		prior.from.Unix(), request.to.Unix(), now.Unix()))
	if err := m.persistFinanceReportSnapshotShadow(prior.logicalKey(), "old-source", bad, now); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.loadPriorFinanceReportSnapshot(request, now); err == nil || ok {
		t.Fatalf("mismatched snapshot interval accepted: ok=%t err=%v", ok, err)
	}
}

func TestFinanceReportPayloadServesPriorRangeWithoutLiveDatabase(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(dir, "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true,
		FinanceReportSnapshotReadEnabled:   true,
	}}
	stopped, cancel := context.WithCancel(context.Background())
	cancel() // A stale display must not need a running background worker.
	m.backgroundCtx = stopped
	now := time.Now().Truncate(time.Second)
	request := financeReportRequest{
		from: time.Unix(3600, 0), to: time.Unix(3600, 0).Add(30 * time.Hour),
		configurationHash: "config", sourceFingerprint: "latest-source",
	}
	prior := request
	prior.to = prior.to.Add(-28 * time.Hour)
	payload := []byte(fmt.Sprintf(`{"enabled":true,"from":%d,"to":%d,"generated_at":%d}`,
		prior.from.Unix(), prior.to.Unix(), now.Add(-28*time.Hour).Unix()))
	if err := m.persistFinanceReportSnapshotShadow(prior.logicalKey(), "old-source", payload, now.Add(-28*time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, status, err := m.financeReportPayload(context.Background(), request, false)
	if err != nil || status != "persistent-prior-stale-refreshing" || !bytes.Equal(got, payload) {
		t.Fatalf("prior-range display failed closed or rewrote the interval: status=%s payload=%s err=%v", status, got, err)
	}
	if _, ok := m.getFinanceReportCache().Get(request.cacheKey(), time.Now()); ok {
		t.Fatal("prior-range payload was cached under the current range")
	}
}
