package monitor

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func seedInvestigationResultCache(m *Monitor, in logChainInvestigationInput, result logChainInvestigationResult) string {
	owner := m.investigationDigest("cloudwatch-investigation-operator", "owner")
	key := m.investigationDigest("cloudwatch-investigation-cache", owner, m.investigationScopeDigest(in))
	m.investigationMu.Lock()
	defer m.investigationMu.Unlock()
	m.initInvestigationStateLocked()
	m.investigationCache[key] = logChainInvestigationCacheEntry{Result: result, ExpiresAt: time.Now().Add(time.Minute)}
	return key
}

func TestInvestigationCacheHitPreservesPartialAndHistoricalAuditGap(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        string
		auditRecorded bool
	}{
		{name: "partial", status: "partial", auditRecorded: true},
		{name: "partial_with_audit_gap", status: "partial", auditRecorded: false},
		{name: "complete_with_audit_gap", status: "complete", auditRecorded: false},
		{name: "complete", status: "complete", auditRecorded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeCloudWatchLogsClient{}
			m := newLogChainCloudWatchTestMonitor(t, client)
			enableInvestigationAudit(t, m)
			in := logChainInvestigationInput{From: time.Now().Add(-2 * time.Hour), To: time.Now().Add(-time.Hour), NewAPIRequestID: "request-cache-gap"}
			original := logChainInvestigationResult{
				OK: true, InvestigationID: "original-investigation", Status: tc.status,
				Scope: m.investigationScopeView(in), AuditRecorded: tc.auditRecorded, CandidatesComplete: tc.status == "complete",
				Summary: logChainInvestigationSummary{Classification: "insufficient_evidence", EvidenceLevel: "ambiguous"},
				Cost:    logChainInvestigationCost{Queries: 2, BytesScanned: 1024},
				SourceStatus: []logChainCloudWatchSourceStatus{{
					Source: cwSourceWorkerNginx, Status: "found", Partial: tc.status == "partial", Truncated: tc.status == "partial",
				}},
				BlindSpots:                    []string{"来源查询尚未完整"},
				associationCandidates:         []LogChainRow{{RequestID: "private-reverse-candidate"}},
				associationCandidatesRequired: true,
				associationCandidatesComplete: tc.status == "complete", associationEdgesComplete: tc.status == "complete",
			}
			if !tc.auditRecorded {
				original.BlindSpots = append(original.BlindSpots, "原任务完成事件的审计写入失败。")
			}
			key := seedInvestigationResultCache(m, in, original)
			for attempt := 0; attempt < 2; attempt++ {
				got, err := m.createLogChainInvestigation("owner", in)
				if err != nil {
					t.Fatal(err)
				}
				if !got.Cost.CacheHit || got.Status != tc.status || got.AuditRecorded != tc.auditRecorded {
					t.Fatalf("cache hit upgraded original completeness: %+v", got)
				}
				if got.associationCandidatesRequired != original.associationCandidatesRequired || got.associationCandidatesComplete != original.associationCandidatesComplete || got.associationEdgesComplete != original.associationEdgesComplete || !reflect.DeepEqual(got.associationCandidates, original.associationCandidates) {
					t.Fatal("cache changed private reverse-lookup coverage proof")
				}
				if got.InvestigationID == original.InvestigationID || got.CandidatesComplete != original.CandidatesComplete || !reflect.DeepEqual(got.SourceStatus, original.SourceStatus) || !reflect.DeepEqual(got.BlindSpots, original.BlindSpots) || got.Summary != original.Summary {
					t.Fatalf("cache hit lost original evidence/audit gaps: %+v", got)
				}
				polled, ok := m.getLogChainInvestigation("owner", got.InvestigationID)
				if !ok || polled.Status != tc.status || polled.AuditRecorded != tc.auditRecorded {
					t.Fatalf("polling disagrees with cached result: %+v", polled)
				}
				var audit CloudWatchInvestigationAudit
				if err := m.storeDB.Where("investigation_id = ?", got.InvestigationID).First(&audit).Error; err != nil {
					t.Fatal(err)
				}
				if audit.Event != "cache_hit" || audit.Status != tc.status || !audit.CacheHit {
					t.Fatalf("cache audit misstated source completeness: %+v", audit)
				}
			}
			if !reflect.DeepEqual(m.investigationCache[key].Result, original) {
				t.Fatal("reading the cache mutated the original result")
			}
			client.mu.Lock()
			queryCount := len(client.filterInputs) + len(client.startInputs)
			client.mu.Unlock()
			if queryCount != 0 {
				t.Fatalf("cache hit ran %d CloudWatch queries", queryCount)
			}
		})
	}
}

func TestInvestigationCacheHitAuditFailureKeepsOriginalResult(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	enableInvestigationAudit(t, m)
	in := logChainInvestigationInput{From: time.Now().Add(-2 * time.Hour), To: time.Now().Add(-time.Hour), NewAPIRequestID: "request-cache-audit-failure"}
	original := logChainInvestigationResult{OK: true, Status: "partial", AuditRecorded: true, BlindSpots: []string{"原始来源缺口"}}
	key := seedInvestigationResultCache(m, in, original)
	const callback = "test:cache_hit_audit_failure"
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
		if audit, ok := tx.Statement.Dest.(*CloudWatchInvestigationAudit); ok && audit.Event == "cache_hit" {
			_ = tx.AddError(errors.New("cache hit audit unavailable"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.storeDB.Callback().Create().Remove(callback) }()
	got, err := m.createLogChainInvestigation("owner", in)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "partial" || got.AuditRecorded || !strings.Contains(strings.Join(got.BlindSpots, "\n"), "本次缓存命中结果可用，但审计写入失败") {
		t.Fatalf("cache hit audit failure was hidden: %+v", got)
	}
	if !reflect.DeepEqual(m.investigationCache[key].Result, original) {
		t.Fatal("a separate cache-hit audit failure mutated the original cache entry")
	}
}

func TestInvestigationPublishesCacheOnlyAfterFinalAudit(t *testing.T) {
	for _, failedEvent := range []string{"started", "finished"} {
		t.Run(failedEvent, func(t *testing.T) {
			m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
			enableInvestigationAudit(t, m)
			in := logChainInvestigationInput{From: time.Now().Add(-3 * time.Hour), To: time.Now().Add(-2 * time.Hour), NewAPIRequestID: "request-final-audit"}
			cachePublishedBeforeAudit, finishedSeen := false, false
			const callback = "test:final_audit_gap"
			if err := m.storeDB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
				audit, ok := tx.Statement.Dest.(*CloudWatchInvestigationAudit)
				if !ok {
					return
				}
				if audit.Event == "finished" {
					finishedSeen = true
					m.investigationMu.Lock()
					cachePublishedBeforeAudit = len(m.investigationCache) > 0
					m.investigationMu.Unlock()
				}
				if audit.Event == failedEvent {
					_ = tx.AddError(errors.New("injected lifecycle audit failure"))
				}
			}); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = m.storeDB.Callback().Create().Remove(callback) }()
			created, err := m.createLogChainInvestigation("owner", in)
			if err != nil {
				t.Fatal(err)
			}
			m.investigationWG.Wait()
			if !finishedSeen || cachePublishedBeforeAudit {
				t.Fatalf("cache was published before final audit: finished=%v premature_cache=%v", finishedSeen, cachePublishedBeforeAudit)
			}
			completed, ok := m.getLogChainInvestigation("owner", created.InvestigationID)
			if !ok || completed.AuditRecorded || (completed.Status != "complete" && completed.Status != "partial") {
				t.Fatalf("completed result did not retain lifecycle audit failure: %+v", completed)
			}
			cached, err := m.createLogChainInvestigation("owner", in)
			if err != nil {
				t.Fatal(err)
			}
			if !cached.Cost.CacheHit || cached.Status != completed.Status || cached.AuditRecorded || !reflect.DeepEqual(cached.BlindSpots, completed.BlindSpots) {
				t.Fatalf("successful cache-hit audit erased the earlier %s failure: %+v", failedEvent, cached)
			}
		})
	}
}
