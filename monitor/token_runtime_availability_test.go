package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func tokenIsolatedRuntimeFixture(t *testing.T, snapshot string) (*Monitor, int64) {
	t.Helper()
	m, now, _ := detectionFixture(t)
	t.Cleanup(m.Close)
	if err := m.storeDB.Where("1=1").Delete(&TokenMeteringState{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.AutoMigrate(&UsageUserSnapshot{}, &ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenStorePath = filepath.Join(t.TempDir(), "token.db")
	if profile := os.Getenv("TOKEN_TEST_RUNTIME_DISK_PROFILE"); profile != "" {
		if profile != "v132-approved" {
			t.Fatal("unknown explicit runtime disk profile")
		}
		// Match the inspected, already authorized live D-drive settings. The
		// default fixture retains the normal 2-GiB/80-percent guard.
		m.cfg.TokenStoreMinFreeBytes = 8 << 30
		m.cfg.TokenStoreMaxUsedPercent = 98
	}
	if snapshot != "" {
		started := time.Now()
		if os.Getenv("TOKEN_TEST_CONSUME_DISPOSABLE_SNAPSHOT") == "1" {
			// Online backup already made this private copy. Avoid a second multi-GB
			// copy on the developer disk; never consume a recovery/frozen source.
			marker, err := os.ReadFile(snapshot + ".disposable")
			rel, relErr := filepath.Rel(os.TempDir(), snapshot)
			if err != nil || string(marker) != "token-review-20261030\n" || relErr != nil || strings.HasPrefix(rel, "..") || filepath.Base(snapshot) != "token-review-disposable.db" {
				t.Fatal("requires a marked private online backup under TMPDIR")
			}
			// Keep the original name for the schema-size evidence below.
			m.cfg.TokenStorePath = snapshot
			t.Cleanup(func() {
				m.Close()
				for _, suffix := range []string{"", "-wal", "-shm", ".disposable"} {
					if err := os.Remove(snapshot + suffix); err != nil && !os.IsNotExist(err) {
						t.Error(err)
					}
				}
			})
		} else if err := tokenTestCopyFile(snapshot, m.cfg.TokenStorePath); err != nil {
			t.Fatal(err)
		}
		t.Logf("stopped snapshot copied to disposable disk fixture in %s", time.Since(started))
	}
	initializing := time.Now()
	if err := m.openTokenStore(); err != nil {
		t.Fatal(err)
	}
	t.Logf("isolated Token store initialization elapsed=%s", time.Since(initializing))
	if snapshot != "" {
		info, err := os.Stat(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		var pages, pageBytes int64
		if err := m.tokenStore().Raw("PRAGMA page_count").Scan(&pages).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.tokenStore().Raw("PRAGMA page_size").Scan(&pageBytes).Error; err != nil {
			t.Fatal(err)
		}
		t.Logf("snapshot schema baseline_bytes=%d initialized_logical_bytes=%d delta_bytes=%d", info.Size(), pages*pageBytes, pages*pageBytes-info.Size())
	}
	var disk syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(m.tokenStorePath()), &disk); err != nil {
		t.Fatal(err)
	}
	t.Logf("isolated fixture path=%q free=%d total=%d reserve=%d maxUsedPercent=%d", m.tokenStorePath(), disk.Bavail*uint64(disk.Bsize), disk.Blocks*uint64(disk.Bsize), m.cfg.TokenStoreMinFreeBytes, m.cfg.TokenStoreMaxUsedPercent)
	if !m.tokenMeteringDiskOK() {
		t.Fatal("test filesystem does not meet unchanged Token disk reserve")
	}
	return m, now
}

// A local writer can hold a bounded page/worker transaction for longer than an
// HTTP deadline. WAL readers must still see a coherent committed snapshot.
func TestTokenRuntimeReadsWhileWriterBusy(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	for id := int64(1); id <= 30; id++ {
		raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0,"cache_creation_tokens":0}`)
		raw.ID, raw.CreatedAt = id, now-600+id
		fact, _ := parseTokenMetering(m.cfg, raw, now)
		fact.LinkState, fact.LinkRevision = "unique", 1
		if err := m.tokenStore().Create(&fact).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.runTokenDetectionRound(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	locked, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- m.tokenStore().Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&TokenDetectionState{}).Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Update("last_reason", "uncommitted").Error; err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	defer func() {
		close(release)
		if err := <-finished; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-locked:
	case err := <-finished:
		t.Fatalf("writer: %v", err)
	}
	// Control: reproduce the old single-pool starvation on this exact database.
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	var count int64
	blockedErr := m.tokenStore().WithContext(blockedCtx).Model(&TokenDetectionCurrent{}).Count(&count).Error
	blockedCancel()
	if !errors.Is(blockedErr, context.DeadlineExceeded) {
		t.Fatalf("writer contention control: %v", blockedErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 600, To: now - 540, Limit: 25}
	page, err := m.tokenDetectionReport(ctx, q)
	if err != nil {
		t.Fatalf("report blocked by writer: %v", err)
	}
	if len(page["items"].([]map[string]any)) != 25 || page["has_more"] != true {
		t.Fatalf("first page: %v", page["has_more"])
	}
	last := page["items"].([]map[string]any)[24]["request"].(TokenDetectionCurrent)
	q.AfterAt, q.AfterID = last.CreatedAt, last.LogID
	page, err = m.tokenDetectionReport(ctx, q)
	if err != nil || len(page["items"].([]map[string]any)) != 5 || page["has_more"] != false {
		t.Fatalf("pagination while writing: %v", err)
	}
	for _, endpoint := range []string{"health", "detection-health", "detail"} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest("POST", "/", strings.NewReader(`{"log_id":1}`))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Request = c.Request.WithContext(ctx)
		switch endpoint {
		case "health":
			m.serveTokenMeteringHealth(c)
		case "detection-health":
			m.serveTokenDetectionHealth(c)
		case "detail":
			m.serveTokenDetectionDetail(c)
		}
		if rec.Code != 200 {
			t.Fatalf("%s while writing: %d %s", endpoint, rec.Code, rec.Body.String())
		}
	}
	if err := m.tokenReadStore().Exec("UPDATE token_detection_states SET last_reason='reader-must-not-write'").Error; err == nil {
		t.Fatal("isolated reader can write")
	}
}

func TestTokenRuntimeHistoricalSnapshot(t *testing.T) {
	snapshot := os.Getenv("TOKEN_FIX_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_FIX_SNAPSHOT to a disposable saved real snapshot")
	}
	m, _ := tokenIsolatedRuntimeFixture(t, snapshot)
	var state TokenDetectionState
	if err := m.tokenStore().First(&state).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenFactsSourceEpoch = state.SourceEpoch
	var revision TokenDetectionRevision
	if err := m.tokenStore().First(&revision).Error; err != nil {
		t.Fatal(err)
	}
	var frozen tokenDetectionFrozen
	if err := json.Unmarshal([]byte(revision.FrozenJSON), &frozen); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(frozen.Policy)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	jobs, err := tokenDetectionJobs(m.tokenStore().WithContext(ctx), state.SourceEpoch, tokenDetectionPolicyHash(frozen.Policy), 1793154000)
	if err != nil {
		t.Fatalf("snapshot queue: %v", err)
	}
	t.Logf("snapshot queue jobs=%d elapsed=%s", len(jobs), time.Since(started))
	started = time.Now()
	page, err := m.tokenDetectionReport(ctx, tokenDetectionQuery{SourceEpoch: state.SourceEpoch, From: 1793086500, To: 1793086560, Limit: 25})
	if err != nil {
		t.Fatalf("snapshot minute report: %v", err)
	}
	t.Logf("snapshot report rows=%d elapsed=%s", len(page["items"].([]map[string]any)), time.Since(started))
	// Reconcile every result in the same five-minute window used by the saved
	// independent source comparison. Exercise real handler pagination and frozen
	// detail decoding, then recompute each decision from its frozen operands.
	const from, to = int64(1793086500), int64(1793086800)
	var facts []TokenMeteringFact
	if err := m.tokenReadStore().Where("source_epoch=? AND created_at>=? AND created_at<?", state.SourceEpoch, from, to).Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	byID := map[int64]TokenMeteringFact{}
	for _, f := range facts {
		byID[f.LogID] = f
	}
	seen := map[int64]bool{}
	q := tokenDetectionQuery{SourceEpoch: state.SourceEpoch, From: from, To: to, Limit: 25}
	for {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest("POST", "/anomalies/token/detection/query", strings.NewReader(tokenDetectionJSON(q)))
		c.Request.Header.Set("Content-Type", "application/json")
		m.serveTokenDetectionQuery(c)
		if rec.Code != 200 {
			t.Fatalf("snapshot HTTP page: %d", rec.Code)
		}
		var response struct {
			Items []struct {
				Request TokenDetectionCurrent
				Result  tokenDetectionResult
			}
			HasMore bool `json:"has_more"`
			Summary tokenDetectionSummary
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Summary.Observed != int64(len(facts)) {
			t.Fatalf("summary/facts count: %d/%d", response.Summary.Observed, len(facts))
		}
		for _, item := range response.Items {
			h := item.Request
			f, ok := byID[h.LogID]
			if !ok || seen[h.LogID] || h.CreatedAt != f.CreatedAt || h.UserID != f.UserID || h.ChannelID != f.ChannelID || h.Model != f.Model || h.UserGroup != f.UserGroup || h.FactRevision != f.Revision {
				t.Fatal("facts/head identity or revision mismatch")
			}
			seen[h.LogID] = true
			detail := httptest.NewRecorder()
			dc, _ := gin.CreateTestContext(detail)
			dc.Request = httptest.NewRequest("POST", "/anomalies/token/detection/detail", strings.NewReader(tokenDetectionJSON(map[string]any{"source_epoch": h.SourceEpoch, "log_id": h.LogID, "revision": h.Revision})))
			dc.Request.Header.Set("Content-Type", "application/json")
			m.serveTokenDetectionDetail(dc)
			if detail.Code != 200 {
				t.Fatalf("snapshot detail: %d", detail.Code)
			}
			var body struct {
				Revision     int64
				DecisionHash string `json:"decision_hash"`
				Evidence     tokenDetectionFrozen
			}
			if err := json.Unmarshal(detail.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			fr := body.Evidence
			if body.Revision != h.Revision || body.DecisionHash != h.DecisionHash || tokenDetectionHash(fr) != h.DecisionHash {
				t.Fatal("frozen detail hash/revision mismatch")
			}
			parsed, err := tokenDetectionInputOf(f)
			if err != nil {
				t.Fatal(err)
			}
			if tokenDetectionJSON(parsed.Usage) != tokenDetectionJSON(fr.Input.Usage) {
				t.Fatal("local source operands differ from frozen operands")
			}
			again := tokenDetectionEvaluate(fr.Input, fr.Policy, fr.Result.Reference, fr.CachePriorSignals, fr.Result.SourceComplete)
			if tokenDetectionJSON(again) != tokenDetectionJSON(fr.Result) {
				t.Fatal("frozen decision cannot be reproduced")
			}
			brief := fr.Result
			brief.Reference.Samples = nil
			if tokenDetectionJSON(brief) != tokenDetectionJSON(item.Result) {
				t.Fatal("API display result differs from frozen decision")
			}
			q.AfterAt, q.AfterID = h.CreatedAt, h.LogID
		}
		if !response.HasMore {
			break
		}
	}
	if len(seen) != len(facts) || len(seen) != 1093 {
		t.Fatalf("fixed-window pagination: %d/%d", len(seen), len(facts))
	}
	t.Logf("fixed window facts -> frozen recomputation -> HTTP pages/details: %d matched", len(seen))
}

func TestTokenRuntimeReadsDuringCollectionAndDetection(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	rows := int64(1600)
	if value := os.Getenv("TOKEN_TEST_CONCURRENCY_ROWS"); value != "" {
		var err error
		rows, err = strconv.ParseInt(value, 10, 64)
		if err != nil || rows < 30 || rows > 1600 {
			t.Fatal("invalid explicit concurrency fixture size")
		}
	}
	start := (now-tokenMeteringLagSeconds)/300*300 - 300
	if err := m.tokenStore().Create(&TokenMeteringState{SourceEpoch: m.cfg.TokenFactsSourceEpoch, SourceMode: "complete", StartAt: start, ScanThrough: start}).Error; err != nil {
		t.Fatal(err)
	}
	sourceTx, err := m.prodDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= rows; id++ {
		if _, err := sourceTx.Exec("INSERT INTO logs(id,created_at,type,user_id,channel_id,model_name,`group`,prompt_tokens,completion_tokens,quota,is_stream,use_time,other,request_id) VALUES(?,?,2,2,3,'synthetic-model','actual-group',100,10,200,1,1,'{}','')", id, start+1); err != nil {
			_ = sourceTx.Rollback()
			t.Fatal(err)
		}
	}
	if err := sourceTx.Commit(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	workers := make(chan error, 2)
	collectionDone := make(chan struct{})
	budgetYields := 0
	detectRound := func(round int64) error {
		// Production gives association and detection independent budgets. A
		// slow link turn must not pass its nearly expired context to detection.
		linkCtx, linkStop := context.WithTimeout(ctx, 5*time.Second)
		linked, linkErr := m.linkTokenMeteringBatch(linkCtx, now+round)
		linkExpired := errors.Is(linkCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
		linkStop()
		if linkErr != nil && !(linkExpired && linked > 0 && errors.Is(linkErr, context.DeadlineExceeded)) {
			return linkErr
		}
		// Deliberately exercise a caller budget shorter than the worker's own
		// deadline. A partial round requires durable progress, released lease,
		// and an explicit incomplete health state before the next round resumes.
		roundCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		processed, err := m.runTokenDetectionRound(roundCtx, now+round)
		expired := errors.Is(roundCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
		stop()
		if err != nil && expired && processed > 0 {
			var state TokenDetectionState
			if readErr := m.tokenReadStore().First(&state).Error; readErr != nil {
				return readErr
			}
			if state.LeaseUntil != 0 || state.LeaseToken != "" || state.LastReason != "local_detection_incomplete" {
				return errors.New("partial detection round lost its incomplete state or retained its lease")
			}
			budgetYields++
			return nil
		}
		return err
	}
	collectorRounds := int64(0)
	go func() {
		defer close(collectionDone)
		// A fixed five turns assumes a particular disk/CPU speed and can stop
		// after a valid durable prefix. Require the whole source window to drain
		// within a wall-clock bound while the same API reads remain active.
		deadline := time.Now().Add(time.Minute)
		for round := int64(0); ; round++ {
			if _, err := m.runTokenMeteringRound(ctx, time.Unix(now+round, 0)); err != nil {
				workers <- fmt.Errorf("collector round %d: %w", round, err)
				return
			}
			collectorRounds++
			var state TokenMeteringState
			if err := m.tokenReadStore().First(&state).Error; err != nil {
				workers <- err
				return
			}
			if state.FactCount == rows && state.ScanThrough == start+300 {
				break
			}
			if time.Now().After(deadline) {
				workers <- errors.New("collector did not drain the fixed source window within one minute")
				return
			}
		}
		workers <- nil
	}()
	go func() {
		for round := int64(0); ; round++ {
			if err := detectRound(round); err != nil {
				workers <- fmt.Errorf("detector round %d: %w", round, err)
				return
			}
			select {
			case <-collectionDone:
				// One final round observes the last committed collector page.
				err := detectRound(round + 1)
				workers <- err
				return
			case <-ctx.Done():
				workers <- ctx.Err()
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	go func() { done <- errors.Join(<-workers, <-workers) }()
	// Both real local workers write while handlers read their committed state.
	reads := 0
	for {
		q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: start, To: start + 60, Limit: 25}
		queryCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		page, err := m.tokenDetectionReport(queryCtx, q)
		stop()
		if err != nil {
			cancel()
			<-done
			t.Fatalf("concurrent report: %v", err)
		}
		for _, health := range []func(*gin.Context){m.serveTokenMeteringHealth, m.serveTokenDetectionHealth} {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("GET", "/", nil)
			health(c)
			if rec.Code != 200 {
				cancel()
				<-done
				t.Fatalf("concurrent health: %d %s", rec.Code, rec.Body.String())
			}
		}
		items := page["items"].([]map[string]any)
		if len(items) > 0 {
			h := items[0]["request"].(TokenDetectionCurrent)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/", strings.NewReader(tokenDetectionJSON(map[string]any{"log_id": h.LogID, "revision": h.Revision})))
			c.Request.Header.Set("Content-Type", "application/json")
			m.serveTokenDetectionDetail(c)
			if rec.Code != 200 {
				cancel()
				<-done
				t.Fatalf("concurrent detail: %d", rec.Code)
			}
		}
		reads++
		select {
		case err := <-done:
			if err != nil {
				var persisted TokenMeteringState
				var heads int64
				_ = m.tokenReadStore().First(&persisted).Error
				_ = m.tokenReadStore().Model(&TokenDetectionCurrent{}).Count(&heads).Error
				t.Fatalf("%v; persisted facts=%d heads=%d scan=%d reason=%s", err, persisted.FactCount, heads, persisted.ScanThrough, persisted.LastReason)
			}
			var state TokenMeteringState
			var heads int64
			if err := m.tokenReadStore().First(&state).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil {
				t.Fatal(err)
			}
			if state.FactCount != rows || heads == 0 || state.ScanThrough != start+300 {
				t.Fatalf("workers made no durable progress: facts=%d heads=%d scan=%d", state.FactCount, heads, state.ScanThrough)
			}
			t.Logf("active collection/detection: %d fact rows, %d result heads, %d API cycles, %d collector rounds, %d partial detection rounds resumed", state.FactCount, heads, reads, collectorRounds, budgetYields)
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestTokenRuntimeHealthSampleMatchesRunnableQueue(t *testing.T) {
	m, now, _, normal := detectionFairnessFixtureSize(t, 6)
	defer m.Close()
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	check := func(want int) {
		t.Helper()
		jobs, err := tokenDetectionJobsBounded(m.tokenStore(), normal.SourceEpoch, hash, now, 100, 60)
		if err != nil {
			t.Fatal(err)
		}
		sample, err := tokenDetectionPendingSample(m.tokenReadStore(), normal.SourceEpoch, hash, now, 100)
		if err != nil {
			t.Fatal(err)
		}
		ids := map[int64]bool{}
		for _, job := range jobs {
			ids[job.LogID] = true
		}
		if len(jobs) != want || len(sample) != len(jobs) {
			t.Fatalf("sample/queue size: %d/%d want %d", len(sample), len(jobs), want)
		}
		for _, job := range sample {
			if !ids[job.LogID] {
				t.Fatal("health sample differs from runnable work")
			}
			delete(ids, job.LogID)
		}
		if len(ids) != 0 {
			t.Fatal("health sample omitted work or returned duplicates")
		}
	}
	check(7)
	if err := m.tokenStore().Model(&TokenDetectionRetry{}).Where("source_epoch=?", normal.SourceEpoch).Update("next_at", now+3600).Error; err != nil {
		t.Fatal(err)
	}
	check(1)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=?", normal.SourceEpoch, 4).Update("revision", 2).Error; err != nil {
		t.Fatal(err)
	}
	check(2) // A corrected fact bypasses capacity retry backoff in both paths.
}
