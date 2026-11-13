package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"gorm.io/gorm"
)

func TestTokenLatestPipelineAcrossCollectionAndLateEvidenceCycles(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	start, _ := tokenRecentBacklog(t, m, now)
	if err := m.tokenStore().Create(&TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: start, EndAt: start + 300, CursorAt: start, NextAt: now + 3600, Attempts: 4, LastReason: "source_read_failed"}).Error; err != nil {
		t.Fatal(err)
	}
	// An unavailable historical change feed must not block exact recent links.
	// Deliberately omit its clock table while leaving request evidence readable.
	if err := m.nginxEvidenceDB.Exec("CREATE TABLE nginx_evidence_changes_v1(generation INTEGER,event_id TEXT)").Error; err != nil {
		t.Fatal(err)
	}
	for cycle := int64(0); cycle < 4; cycle++ {
		at, id := now+25+cycle*30, 1000+cycle
		requestID := fmt.Sprintf("latest-pipeline-%d", id)
		tokenMeteringSourceRow(t, m, id, at-1, requestID)
		tokenMeteringRun(t, m, at)
		fact := tokenMeteringFactRead(t, m, id)
		if fact.RequestID != requestID || fact.CreatedAt != at-1 || fact.FirstSeenAt != at {
			t.Fatal("new source fact did not enter the current cycle", fact.LogID, fact.RequestID, fact.FirstSeenAt)
		}
		detectionRun(t, m, at)
		before, result := detectionHead(t, m, id)
		if before.DetectionState != "partially_evaluated" || !result.Rules[0].Eligible || result.Rules[0].Hit {
			t.Fatal("recorded usage waited for entry evidence", before.DetectionState, result.Rules)
		}
		tokenMeteringEvidence(t, m, fmt.Sprintf("latest-event-%d", id), at-1, 1200+cycle, requestID)
		linked, _, linkErr := m.linkTokenMeteringBatchProgress(context.Background(), at+1)
		if linked == 0 || linkErr == nil {
			t.Fatal("historical failure was hidden or blocked the recent commit", linked, linkErr)
		}
		fact = tokenMeteringFactRead(t, m, id)
		if fact.LinkState != "unique" || fact.RequestLength == nil || *fact.RequestLength != 1200+cycle {
			t.Fatal("late exact evidence was not attached", fact.LinkState, fact.LinkReason)
		}
		detectionRun(t, m, at+2)
		after, _ := detectionHead(t, m, id)
		if after.LinkRevision != fact.LinkRevision || after.Revision != before.Revision+1 || after.EvaluatedAt != at+2 {
			t.Fatal("late evidence did not revise the same decision", before, after)
		}
		q := tokenDetectionQuery{SourceEpoch: fact.SourceEpoch, From: now, To: at + 3, Limit: 10, NewestFirst: true}
		report, err := m.tokenDetectionReport(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		items := report["items"].([]map[string]any)
		if len(items) != int(cycle+1) {
			t.Fatal("page omitted or duplicated a committed fact", len(items), cycle)
		}
		display := items[0]["display"].(tokenDetectionDisplay)
		if display.RequestID != requestID || display.Stale || display.AssociationState != "unique" || display.RecordedInput == nil || *display.RecordedInput != "100" || display.Output == nil || *display.Output != "10" || display.RecordedQuota == nil || *display.RecordedQuota != "200" || len(display.MissingFields) == 0 {
			t.Fatal("page lost exact identity, recorded operands or missing fields", display)
		}
		state := tokenMeteringStateRead(t, m)
		if state.FactCount != cycle+1 || state.ScanThrough != start || state.CompleteThrough != start {
			t.Fatal("latest cycle changed the historical gap or duplicated facts", state)
		}
	}
}

func TestTokenLatestOpenEOFAndRestartSeal(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	now += 25
	state := tokenMeteringStateRead(t, m)
	start := now / 300 * 300
	tokenMeteringSourceRow(t, m, 1, start+5, "latest-1")
	claimed, ok, err := m.claimTokenMetering(context.Background(), now)
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	w, ok, err := m.nextTokenMeteringWork(context.Background(), state.SourceEpoch, claimed.LeaseToken, now, tokenMeteringLaneRecent)
	if err != nil || !ok || w.StartAt != start {
		t.Fatal(w, ok, err)
	}
	commit := func(w TokenMeteringWindow, at int64) {
		raw, done, err := m.readTokenMeteringPage(context.Background(), w)
		if err != nil {
			t.Fatal(err)
		}
		facts := []TokenMeteringFact{}
		refs := []TokenMeteringRef{}
		for _, row := range raw {
			f, r := parseTokenMetering(m.cfg, row, at)
			facts = append(facts, f)
			refs = append(refs, r...)
		}
		if err = m.commitTokenMeteringPage(context.Background(), claimed.LeaseToken, w, raw, facts, refs, done, at); err != nil {
			t.Fatal(err)
		}
	}
	commit(w, now)
	if err = m.tokenStore().First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, start).Error; err != nil {
		t.Fatal(err)
	}
	if w.Done || w.SourceComplete || w.CursorID != 1 || w.OpenObservedAt != now {
		t.Fatal("open EOF fabricated coverage", w)
	}
	if got := tokenMeteringStateRead(t, m); got.ScanThrough != state.ScanThrough || got.FactCount != 1 {
		t.Fatal(got)
	}
	// A delayed row before the original keyset position is recovered by overlap.
	tokenMeteringSourceRow(t, m, 2, start+3, "late-2")
	resumed := &Monitor{cfg: m.cfg, storeDB: m.storeDB, prodDB: m.prodDB}
	if err = m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", state.SourceEpoch).Update("lease_until", now+1000).Error; err != nil {
		t.Fatal(err)
	}
	w.NextAt = now + 10
	tokenMeteringResumeWindow(&w, now+10)
	if err = m.tokenStore().Save(&w).Error; err != nil {
		t.Fatal(err)
	}
	m = resumed
	commit(w, now+10)
	if got := tokenMeteringStateRead(t, m); got.FactCount != 2 {
		t.Fatal("overlap lost or duplicated row", got)
	}
	if err = m.tokenStore().First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, start).Error; err != nil {
		t.Fatal(err)
	}
	sealAt := start + 300 + tokenMeteringLagSeconds
	tokenMeteringResumeWindow(&w, sealAt)
	if !w.SealStarted || w.CursorAt != start || w.CursorID != 0 {
		t.Fatal("seal must reread bounded bucket", w)
	}
	if err = m.tokenStore().Save(&w).Error; err != nil {
		t.Fatal(err)
	}
	commit(w, sealAt)
	if err = m.tokenStore().First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, start).Error; err != nil {
		t.Fatal(err)
	}
	if !w.Done || !w.SourceComplete {
		t.Fatal(w)
	}
}

func TestTokenLatestFailedBucketDoesNotBlockTail(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	state := tokenMeteringStateRead(t, m)
	tail := now / 300 * 300
	failed := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: tail - 300, EndAt: tail, CursorAt: tail - 299, CursorID: 123, NextAt: now + 1000, Attempts: 5, LastReason: "source_read_failed"}
	if err := m.tokenStore().Create(&failed).Error; err != nil {
		t.Fatal(err)
	}
	w, ok, err := tokenMeteringRecentWindow(m.tokenStore(), state, now)
	if err != nil || !ok || w.StartAt != tail {
		t.Fatal(w, ok, err)
	}
	var after TokenMeteringWindow
	m.tokenStore().First(&after, "source_epoch=? AND start_at=?", state.SourceEpoch, failed.StartAt)
	if after.CursorID != 123 || after.Attempts != 5 || after.NextAt != failed.NextAt {
		t.Fatal(after)
	}
}

func TestTokenLatestWaitingRunsIndependentRules(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 501, now-5, 3, 100, 20, 7)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=?", f.SourceEpoch, f.LogID).Updates(map[string]any{"link_state": "waiting", "evidence_level": "unavailable", "evidence_json": "[]", "request_length": nil, "link_revision": 2}).Error; err != nil {
		t.Fatal(err)
	}
	err := m.tokenStore().Transaction(func(tx *gorm.DB) error {
		a, e := prepareTokenDetection(tx, f.SourceEpoch, f.LogID, detectionPolicy(), now)
		if e != nil {
			return e
		}
		if !a.Brief.Candidate || a.Brief.FormulaDifference == nil || *a.Brief.FormulaDifference != "7" || a.Assessment.State != "local_metering_difference" || a.Assessment.UpstreamConfirmed {
			t.Fatalf("%+v %+v", a.Brief, a.Assessment)
		}
		if a.Brief.Rules[0].Eligible != true || a.Brief.Rules[1].Eligible {
			t.Fatal(a.Brief.Rules)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTokenLatestQueueWorksDuringMigration(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 8; id++ {
		detectionFact(t, m, id, now-86400+id, 3, 100, 20, 0)
	}
	detectionFact(t, m, 99, now-2, 3, 100, 20, 0)
	if err := m.tokenStore().Exec("UPDATE " + tokenPendingMigrationTable + " SET ready=0").Error; err != nil {
		t.Fatal(err)
	}
	jobs, err := tokenDetectionJobsBounded(m.tokenStore(), m.cfg.TokenFactsSourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now, 4, 60)
	if err != nil || len(jobs) == 0 || jobs[0].LogID != 99 {
		t.Fatal(jobs, err)
	}
	ready, err := tokenPendingProjectionReady(m.tokenStore())
	if err != nil || ready {
		t.Fatal(ready, err)
	}
	// Selection alone is insufficient: the materialized identity must pass
	// the normal lease, preparation, revision validation and commit path.
	processed, err := m.runTokenDetectionRound(context.Background(), now)
	if err != nil || processed != 1 {
		t.Fatal("recent decision blocked by historical projection", processed, err)
	}
	head, _ := detectionHead(t, m, 99)
	fact := tokenMeteringFactRead(t, m, 99)
	if head.FactRevision != fact.Revision || head.LinkRevision != fact.LinkRevision || head.DetectionState == "" {
		t.Fatal("recent decision did not commit the current evidence", head)
	}
	if ready, err = tokenPendingProjectionReady(m.tokenStore()); err != nil || ready {
		t.Fatal("recent commit manufactured global readiness", ready, err)
	}
	if processed, err = m.runTokenDetectionRound(context.Background(), now+1); err != nil || processed != 0 {
		t.Fatal("unchanged recent decision was rewritten", processed, err)
	}
	after, _ := detectionHead(t, m, 99)
	if after.Revision != head.Revision {
		t.Fatal("idempotent commit changed revision", head.Revision, after.Revision)
	}
}

func TestTokenLatestNginxFailureKeepsIndependentProof(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	now += 15
	if err := m.nginxEvidenceDB.AutoMigrate(&CloudWatchNginxRecentWindow{}); err != nil {
		t.Fatal(err)
	}
	tail := now/60*60 - 120
	failed := CloudWatchNginxRecentWindow{StartAt: tail - 60, EndAt: tail, NextAt: now + 300, Attempts: 3, LastError: "timeout"}
	if err := m.nginxEvidenceDB.Create(&failed).Error; err != nil {
		t.Fatal(err)
	}
	w, ok, err := selectCloudWatchNginxRecentWindow(m.nginxEvidenceDB, now)
	if err != nil || !ok || w.StartAt != tail {
		t.Fatal(w, ok, err)
	}
	s := cloudWatchNginxSchedule{recentFrom: w.StartAt, recentTo: w.EndAt}
	lane, _, _, _, ok := s.choose(CloudWatchNginxCursor{NextTs: now - 86400}, now-120, time.Unix(now, 0), time.Minute)
	if !ok || lane != nginxRecent {
		t.Fatal(lane, ok)
	}
}

func TestTokenLatestProducerFieldsPreserveObservedShapes(t *testing.T) {
	// Shapes taken from the bounded read-only source audit, with synthetic values.
	raw := tokenMeteringRawFixture(`{"usage_adapter":"newapi-rc26","usage_semantic":"openai","request_conversion":["OpenAI Responses"],"billing_unit":"tokens","cache_tokens":20}`)
	f, _ := parseTokenMetering(Settings{}, raw, raw.CreatedAt+1)
	u := tokenMeteringDecode(t, f)
	if u.Fields["request_conversion"].State != "valid" || u.Fields["request_conversion"].Value != `["OpenAI Responses"]` || u.Adapter != "newapi-rc26" || u.Fields["billing_unit"].Value != "tokens" {
		t.Fatal(u)
	}
	if u.Fields["unsupported_cost_context"].State != "" || u.Fields["currency"].State != "missing" {
		t.Fatal("metadata invented cost context/currency", u.Fields)
	}
	raw.Other = sql.NullString{Valid: true, String: `{"request_conversion":["Claude Messages"],"claude":true}`}
	f, _ = parseTokenMetering(Settings{}, raw, raw.CreatedAt+1)
	u = tokenMeteringDecode(t, f)
	if u.TotalInput != nil || u.Fields["request_conversion"].State != "valid" {
		t.Fatal("transport label guessed usage semantics", u)
	}
}

func TestTokenLatestStalePolicyAndReferenceUseExistingQueue(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 700, now-4, 1, 100, 20, 0)
	detectionRun(t, m, now)
	head, _ := detectionHead(t, m, f.LogID)
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Where("log_id=?", f.LogID).Update("policy_hash", "previous-policy").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Exec("UPDATE " + tokenPendingMigrationTable + " SET ready=0").Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+2)
	updated, _ := detectionHead(t, m, f.LogID)
	if updated.Revision < head.Revision || updated.PolicyHash != tokenDetectionPolicyHash(detectionPolicy()) || updated.EvaluatedAt != now+2 {
		t.Fatal(updated)
	}
	ready, err := tokenPendingProjectionReady(m.tokenStore())
	if err != nil || ready {
		t.Fatal("recent work changed global migration proof", ready, err)
	}
	q := tokenDetectionQuery{SourceEpoch: f.SourceEpoch, From: now - 900, To: now + 10}
	progress, err := tokenDetectionLatestProgress(m.tokenStore(), q, updated.PolicyHash)
	if err != nil || progress.DetectedThrough != f.CreatedAt {
		t.Fatal(progress, err)
	}
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Where("log_id=?", f.LogID).Update("reference_generation", updated.ReferenceGeneration+1).Error; err != nil {
		t.Fatal(err)
	}
	progress, err = tokenDetectionLatestProgress(m.tokenStore(), q, updated.PolicyHash)
	if err != nil || progress.DetectedThrough != 0 {
		t.Fatal("stale reference counted as completed", progress, err)
	}
}

func TestTokenLatestDescendingPageCombinesPendingAndDecisions(t *testing.T) {
	m, now, _ := detectionFixture(t)
	for id := int64(1); id <= 6; id++ {
		detectionFact(t, m, id, now-10+id/2, 1, 100, 20, 0)
	}
	detectionRun(t, m, now)
	if err := m.tokenStore().Where("log_id IN ?", []int64{1, 3, 5}).Delete(&TokenDetectionCurrent{}).Error; err != nil {
		t.Fatal(err)
	}
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 900, To: now + 1, Limit: 2, NewestFirst: true}
	var ids []int64
	for page := 0; page < 3; page++ {
		report, err := m.tokenDetectionReport(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		items := report["items"].([]map[string]any)
		if len(items) != 2 {
			t.Fatal(items)
		}
		for _, item := range items {
			h := item["request"].(TokenDetectionCurrent)
			ids = append(ids, h.LogID)
			q.AfterAt, q.AfterID = h.CreatedAt, h.LogID
			d := item["display"].(tokenDetectionDisplay)
			if d.RequestID == "" || d.RecordedQuota == nil || d.FirstSeenAt == 0 {
				t.Fatal(d)
			}
		}
	}
	if tokenDetectionJSON(ids) != "[6,5,4,3,2,1]" {
		t.Fatal(ids)
	}
}

func TestTokenLatestNginxAppendEnrichesWithoutHistoricalProof(t *testing.T) {
	m, _, _ := tokenMeteringFixture(t)
	m.cfg.NginxEvidenceRetentionHours = 168
	if err := m.nginxEvidenceDB.AutoMigrate(&CloudWatchNginxEvidenceCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	proof := CloudWatchNginxEvidenceCheckpoint{ID: cloudWatchNginxCursorID, CoverageFromTs: now - 86400, ThroughTs: now - 3600}
	if err := m.nginxEvidenceDB.Create(&proof).Error; err != nil {
		t.Fatal(err)
	}
	status, millis := 200, int64(25)
	item := cloudWatchStructuredEvidence{Kind: cwEvidenceNginxAccess, EventRef: strings.Repeat("a", 64), EventMS: now * 1000, OneAPIIDHMAC: strings.Repeat("b", 64), HMACKeyID: "key-1", OneAPIRequestID: "latest-entry", Route: "/v1/responses", Method: "POST", Status: &status, RequestMS: &millis, RequestLengthState: "missing", RequestLengthVersion: "request-length-v1", Completion: "complete_at_edge"}
	save := func(batch string) {
		t.Helper()
		if err := m.persistCloudWatchNginxEvidenceMode(context.Background(), []cloudWatchStructuredEvidence{item}, now-60, now+1, batch, true); err != nil {
			t.Fatal(err)
		}
	}
	save("first")
	length := int64(1234)
	item.RequestLength, item.RequestLengthState, item.RequestLengthVersion = &length, "valid", "request-length-v1"
	save("enriched")
	save("identical")
	var got NginxRequestEvidence
	if err := m.nginxEvidenceDB.First(&got, "event_id=?", item.EventRef).Error; err != nil {
		t.Fatal(err)
	}
	if got.RequestLength == nil || *got.RequestLength != length || got.BatchID != "enriched" {
		t.Fatal("enriched evidence missing or unchanged replay rewritten", got)
	}
	if err := m.persistCloudWatchNginxEvidenceMode(context.Background(), nil, now-60, now+1, "empty-observation", true); err != nil {
		t.Fatal(err)
	}
	var count int64
	m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count)
	var after CloudWatchNginxEvidenceCheckpoint
	m.nginxEvidenceDB.First(&after)
	if count != 1 || tokenDetectionJSON(after) != tokenDetectionJSON(proof) {
		t.Fatal("recent observation changed historical proof or deleted evidence", after, count)
	}
	// JSON encoding remains bounded/provenanced for API exposure.
	if _, err := json.Marshal(got); err != nil {
		t.Fatal(err)
	}
}

func TestTokenLatestLateLinkRevisesOnce(t *testing.T) {
	m, now, _ := detectionFixture(t)
	f := detectionFact(t, m, 601, now-5, 3, 100, 20, 0)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=?", f.SourceEpoch, f.LogID).Updates(map[string]any{"link_state": "waiting", "evidence_level": "unavailable", "evidence_json": "[]", "request_length": nil, "link_revision": 2}).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	before, result := detectionHead(t, m, f.LogID)
	if before.DetectionState != "partially_evaluated" || result.Rules[0].Eligible != true || result.Rules[3].Eligible != true {
		t.Fatal(before.DetectionState, result.Rules)
	}
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=?", f.SourceEpoch, f.LogID).Updates(map[string]any{"link_state": f.LinkState, "evidence_level": f.EvidenceLevel, "evidence_json": f.EvidenceJSON, "request_length": f.RequestLength, "link_revision": 3}).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+2)
	after, _ := detectionHead(t, m, f.LogID)
	if after.Revision != before.Revision+1 || after.LinkRevision != 3 {
		t.Fatal("late evidence not committed as new revision", before, after)
	}
	detectionRun(t, m, now+4)
	stable, _ := detectionHead(t, m, f.LogID)
	if stable.Revision != after.Revision {
		t.Fatal("unchanged evidence rewritten", after, stable)
	}
}

func TestTokenLatestNginxInterruptedObservationRestartsWithoutFalseProof(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			m, at := newCloudWatchNginxRecoveryTestMonitor(t)
			m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID = strings.Repeat("t", 32), "test-only"
			if err := m.nginxEvidenceDB.AutoMigrate(&CloudWatchNginxRecentWindow{}); err != nil {
				t.Fatal(err)
			}
			from, to := at.Unix()-120, at.Unix()+10
			before := CloudWatchNginxRecentWindow{StartAt: from, EndAt: to - 5, SuccessThrough: to - 5, LastSuccessAt: to - 5, Attempts: 2}
			if err := m.nginxEvidenceDB.Create(&before).Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			client := &fakeCloudWatchLogsClient{}
			client.getFn = func(ctx context.Context, _ *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
				if mode == "cancel" {
					cancel()
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
			m.cloudWatchLogs.poll = func(context.Context) error { return nil }
			if _, err := m.runCloudWatchNginxRecentWindow(ctx, from, to); err == nil {
				t.Fatal("interrupted observation reported completion")
			}
			var saved CloudWatchNginxRecentWindow
			if err := m.nginxEvidenceDB.First(&saved, "start_at=?", from).Error; err != nil || saved.LastState != "deferred" || saved.Attempts != 2 || saved.SuccessThrough != before.SuccessThrough || saved.LastSuccessAt != before.LastSuccessAt {
				t.Fatal("yield changed proof or counted a source failure", saved, err)
			}
			client.getFn = func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
				return &cloudwatchlogs.GetQueryResultsOutput{Status: cwtypes.QueryStatusComplete}, nil
			}
			restarted := &Monitor{cfg: m.cfg, nginxEvidenceDB: m.nginxEvidenceDB, cloudWatchLogs: m.cloudWatchLogs}
			if _, err := restarted.runCloudWatchNginxRecentWindow(context.Background(), from, to); err != nil {
				t.Fatal(err)
			}
			if err := m.nginxEvidenceDB.First(&saved, "start_at=?", from).Error; err != nil || saved.LastState != "observed" || saved.Attempts != 0 || saved.SuccessThrough != to {
				t.Fatal("restart did not resume same observation", saved, err)
			}
		})
	}
}
