package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/metering"
	"gorm.io/gorm"
)

func TestTokenMeteringLegacyMissingLedgerAndRawIDRepair(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	raw := tokenMeteringRawFixture(`{}`)
	fact, refs := parseTokenMetering(m.cfg, raw, now)
	parsed := tokenMeteringDecode(t, fact)
	parsed.Version, parsed.MissingFields = "token-facts-v6", nil
	fact.SourceFacts, fact.ParserVersion = tokenDetectionJSON(parsed), parsed.Version
	fact.RequestID, fact.MissingFields, fact.PricingSnapshot = "", "", ""
	tokenMeteringSetFactHash(&fact, refs)
	if err := m.tokenStore().Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	knownMissing := fact
	knownMissing.LogID++
	knownMissing.CreatedAt += 300
	knownMissing.MissingFields = tokenMeteringMissingJSON([]tokenMeteringMissingField{{Name: "request_id", Reason: "source_missing"}})
	if err := m.tokenStore().Create(&knownMissing).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if _, err := m.migrateTokenMeteringBatch(context.Background(), now); err != nil {
			t.Fatal(err)
		}
		if tokenMeteringStateRead(t, m).MigrationCursor >= knownMissing.LogID {
			break
		}
	}
	got := tokenMeteringFactRead(t, m, fact.LogID)
	missing := map[string]string{}
	for _, field := range tokenMeteringDecodeMissing(got.MissingFields) {
		missing[field.Name] = field.Reason
	}
	if got.RequestID != "" || got.CustomerNameSnapshot != "" || got.BasePathSnapshot != "" || got.ModelMappingSnapshot != "" {
		t.Fatal("legacy migration fabricated historical metadata")
	}
	if missing["request_id"] != "legacy_source_replay_required" || missing["pricing_snapshot"] != "not_recorded_in_fact" || missing["customer_name_source"] != "not_recorded_in_fact" || missing["cache_tokens"] == "" {
		t.Fatal("legacy field gaps hidden", missing)
	}
	var repairs []struct{ StartAt int64 }
	if err := m.tokenStore().Table("token_metering_source_repairs_v1").Where("source_epoch=?", fact.SourceEpoch).Find(&repairs).Error; err != nil || len(repairs) != 1 || repairs[0].StartAt != fact.CreatedAt/300*300 {
		t.Fatal("raw ID repair must retain one exact source window, excluding known source absence", repairs, err)
	}
	if _, err := m.migrateTokenMeteringBatch(context.Background(), now+1); err != nil {
		t.Fatal(err)
	}
	if after := tokenMeteringFactRead(t, m, fact.LogID); after.Revision != got.Revision || after.MissingFields != got.MissingFields {
		t.Fatal("completed missing ledger was rewritten")
	}
}

func TestTokenMeteringRawIDRepairDoesNotWaitForReportMigration(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	fact := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if err := m.tokenStore().Model(&fact).Updates(map[string]any{
		"request_id": "", "missing_fields": "", "parser_version": "token-facts-v6",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Exec("UPDATE token_report_state_migration_v1 SET cursor_epoch='',cursor_id=0,ready=0 WHERE id=1").Error; err != nil {
		t.Fatal(err)
	}
	changed, _, err := m.migrateTokenMeteringBatchProgress(context.Background(), now)
	if err != nil || changed != 1 || tokenMeteringStateRead(t, m).MigrationCursor != fact.LogID {
		t.Fatal("derived history starved the source repair checkpoint", changed, err)
	}
	var ready bool
	if err := m.tokenStore().Raw("SELECT ready FROM token_report_state_migration_v1 WHERE id=1").Scan(&ready).Error; err != nil || ready {
		t.Fatal("unvisited report migration was marked complete", ready, err)
	}
	var repairs int64
	if err := m.tokenStore().Table("token_metering_source_repairs_v1").Where("source_epoch=? AND start_at=?", fact.SourceEpoch, fact.CreatedAt/300*300).Count(&repairs).Error; err != nil || repairs != 1 {
		t.Fatal("missing raw ID was not durably queued for exact source replay", repairs, err)
	}
	if _, _, err := m.migrateTokenMeteringBatchProgress(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Raw("SELECT ready FROM token_report_state_migration_v1 WHERE id=1").Scan(&ready).Error; err != nil || !ready {
		t.Fatal("source repair starved the derived migration", ready, err)
	}
}

func TestTokenMeteringMigrationWorkerResumesCommittedBudgetPrefix(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	var bytes int64
	for id := int64(1); id <= 24; id++ {
		raw := tokenMeteringRawFixture(`{"usage_semantic":"openai"}`)
		raw.ID = id
		fact, refs := parseTokenMetering(m.cfg, raw, now)
		parsed := tokenMeteringDecode(t, fact)
		parsed.Version = "token-facts-v4"
		parsed.Fields["quota_per_unit"] = metering.Field{State: "derived", Kind: "source_contract_v1", Value: "500000"}
		fact.SourceFacts, fact.ParserVersion = tokenDetectionJSON(parsed), parsed.Version
		tokenMeteringSetFactHash(&fact, refs)
		bytes += fact.StoredBytes
		if err := m.tokenStore().Create(&fact).Error; err != nil {
			t.Fatal(err)
		}
		if len(refs) > 0 {
			if err := m.tokenStore().Create(&refs).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).
		Updates(map[string]any{"fact_count": 24, "fact_bytes": bytes}).Error; err != nil {
		t.Fatal(err)
	}
	var updates atomic.Int64
	if err := m.tokenStore().Callback().Update().Before("gorm:update").Register("migration-prefix-deadline", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" && updates.Add(1) == 9 {
			// The first eight facts and their cursor already committed. The next
			// transaction exhausts the real worker budget and must roll back.
			<-tx.Statement.Context.Done()
			_ = tx.AddError(tx.Statement.Context.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Update().Remove("migration-prefix-deadline"); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.runTokenMeteringMigration(ctx, func() time.Time { return time.Unix(now, 0) })
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("migration worker ignored shutdown")
		}
	}()
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	for {
		var state TokenMeteringState
		if err := m.tokenReadStore().WithContext(deadline).First(&state, "source_epoch=?", m.cfg.TokenFactsSourceEpoch).Error; err != nil {
			t.Fatal("worker did not resume the committed prefix within the next bounded turn", err)
		}
		if state.MigrationCursor == 24 {
			if updates.Load() != 25 {
				t.Fatal("deadline injection or rollback was not exercised", updates.Load())
			}
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("committed migration budget yield entered a long failure backoff")
		case <-time.After(20 * time.Millisecond):
		}
	}
	for id := int64(1); id <= 24; id++ {
		fact := tokenMeteringFactRead(t, m, id)
		if fact.Revision != 2 || tokenMeteringDecode(t, fact).Fields["quota_per_unit"].State != "missing" {
			t.Fatal("resumed worker duplicated changes or retained an unverified unit", id)
		}
	}
}

func TestTokenMeteringMigrationWorkerYieldsAfterSuccessfulBudgetCheckpoint(t *testing.T) {
	tokenMeteringMigrationWorkerYieldCheck(t, false)
}

func TestTokenMeteringMigrationWorkerYieldsAfterRowBudget(t *testing.T) {
	tokenMeteringMigrationWorkerYieldCheck(t, true)
}

func tokenMeteringMigrationWorkerYieldCheck(t *testing.T, rowBudget bool) {
	t.Helper()
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	count, firstCheckpoint := int64(24), int64(1)
	if rowBudget {
		count, firstCheckpoint = 136, 128/tokenMeteringMigrationChunk
	}
	var bytes int64
	for id := int64(1); id <= count; id++ {
		raw := tokenMeteringRawFixture(`{"usage_semantic":"openai"}`)
		raw.ID = id
		fact, refs := parseTokenMetering(m.cfg, raw, now)
		parsed := tokenMeteringDecode(t, fact)
		parsed.Version = "token-facts-v4"
		parsed.Fields["quota_per_unit"] = metering.Field{State: "derived", Kind: "source_contract_v1", Value: "500000"}
		fact.SourceFacts, fact.ParserVersion = tokenDetectionJSON(parsed), parsed.Version
		tokenMeteringSetFactHash(&fact, refs)
		bytes += fact.StoredBytes
		if err := m.tokenStore().Create(&fact).Error; err != nil {
			t.Fatal(err)
		}
		if len(refs) > 0 {
			if err := m.tokenStore().Create(&refs).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).
		Updates(map[string]any{"fact_count": count, "fact_bytes": bytes}).Error; err != nil {
		t.Fatal(err)
	}
	var checkpoints atomic.Int64
	events := make(chan time.Time, 2)
	if err := m.tokenStore().Callback().Update().Before("gorm:update").Register("migration-successful-budget-yield", func(tx *gorm.DB) {
		values, ok := tx.Statement.Dest.(map[string]any)
		if tx.Statement.Table != "token_metering_states" || !ok || values["migration_cursor"] == nil {
			return
		}
		n := checkpoints.Add(1)
		if n == 1 && !rowBudget {
			// Commit the first chunk just before the worker's existing budget
			// ends. The next loop yields with nil error and a durable cursor.
			deadline, ok := tx.Statement.Context.Deadline()
			if !ok {
				_ = tx.AddError(context.DeadlineExceeded)
				return
			}
			if !waitSourceLifecycle(tx.Statement.Context, time.Until(deadline)-180*time.Millisecond) {
				_ = tx.AddError(tx.Statement.Context.Err())
				return
			}
		}
		if n == firstCheckpoint || n == firstCheckpoint+1 {
			events <- time.Now()
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Update().Remove("migration-successful-budget-yield"); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.runTokenMeteringMigration(ctx, func() time.Time { return time.Unix(now, 0) })
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("migration worker ignored shutdown")
		}
	}()
	var observed [2]time.Time
	deadline, stop := context.WithTimeout(context.Background(), 12*time.Second)
	defer stop()
	for i := range observed {
		select {
		case observed[i] = <-events:
		case <-deadline.Done():
			t.Fatal("migration did not resume its committed checkpoint", i)
		}
	}
	if delay := observed[1].Sub(observed[0]); delay < 4*time.Second {
		if rowBudget {
			t.Fatal("row budget checkpoint skipped the reserved writer interval", delay)
		}
		t.Fatal("successful budget checkpoint skipped the reserved writer interval", delay)
	}
	for {
		var state TokenMeteringState
		if err := m.tokenReadStore().WithContext(deadline).First(&state, "source_epoch=?", m.cfg.TokenFactsSourceEpoch).Error; err != nil {
			t.Fatal(err)
		}
		if state.MigrationCursor == count {
			break
		}
		if !waitSourceLifecycle(deadline, 10*time.Millisecond) {
			t.Fatal("migration lost its checkpoint after yielding")
		}
	}
	for id := int64(1); id <= count; id++ {
		fact := tokenMeteringFactRead(t, m, id)
		if fact.Revision != 2 || tokenMeteringDecode(t, fact).Fields["quota_per_unit"].State != "missing" {
			t.Fatal("yielded migration duplicated a fact or retained an unverified unit", id)
		}
	}
}

func TestTokenMeteringMigrationWaitsForBusyWriter(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	raw := tokenMeteringRawFixture(`{"usage_semantic":"openai"}`)
	fact, refs := parseTokenMetering(m.cfg, raw, now)
	parsed := tokenMeteringDecode(t, fact)
	parsed.Version = "token-facts-v4"
	parsed.Fields["quota_per_unit"] = metering.Field{State: "derived", Kind: "source_contract_v1", Value: "500000"}
	fact.SourceFacts, fact.ParserVersion = tokenDetectionJSON(parsed), parsed.Version
	tokenMeteringSetFactHash(&fact, refs)
	if err := m.tokenStore().Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", fact.SourceEpoch).
		Updates(map[string]any{"fact_count": 1, "fact_bytes": fact.StoredBytes}).Error; err != nil {
		t.Fatal(err)
	}
	type result struct {
		changed int
		err     error
	}
	done := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m.tokenLocalWriteMu.Lock()
	go func() {
		n, err := m.migrateTokenMeteringBatch(ctx, now)
		done <- result{n, err}
	}()
	select {
	case got := <-done:
		m.tokenLocalWriteMu.Unlock()
		t.Fatalf("migration abandoned a retained checkpoint instead of waiting for the busy writer: changed=%d err=%v", got.changed, got.err)
	case <-time.After(100 * time.Millisecond):
	}
	m.tokenLocalWriteMu.Unlock()
	select {
	case got := <-done:
		if got.err != nil || got.changed != 1 {
			t.Fatal("migration did not resume after the writer was released", got)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("migration ignored its bounded local deadline")
	}
	stored := tokenMeteringFactRead(t, m, fact.LogID)
	if tokenMeteringDecode(t, stored).Fields["quota_per_unit"].State != "missing" ||
		tokenMeteringStateRead(t, m).MigrationCursor != fact.LogID || stored.LastSeenAt != fact.LastSeenAt {
		t.Fatal("writer handoff lost the migration checkpoint or changed source observation time")
	}
}

func TestTokenMeteringMigrationQueuedCancellationKeepsCheckpoint(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	before := tokenMeteringStateRead(t, m)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()
	m.tokenLocalWriteMu.Lock()
	n, err := m.migrateTokenMeteringBatch(ctx, now)
	m.tokenLocalWriteMu.Unlock()
	var waitErr *tokenLocalWaitError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &waitErr) || n != 0 {
		t.Fatal("queued migration did not observe cancellation", n, err)
	}
	after := tokenMeteringStateRead(t, m)
	if after.MigrationCursor != before.MigrationCursor || after.MigrationVersion != before.MigrationVersion || after.FactCount != before.FactCount {
		t.Fatal("cancelled migration advanced its durable checkpoint")
	}
	if !m.tokenLocalWriteMu.TryLock() {
		t.Fatal("cancelled migration retained a writer slot")
	}
	m.tokenLocalWriteMu.Unlock()
}

func TestTokenMeteringEpochDoesNotSupplyEvidence(t *testing.T) {
	for _, epoch := range []string{"source-a", "nexus-logs-20261027-v1"} {
		cfg := tokenMeteringConfig()
		cfg.TokenFactsSourceEpoch, cfg.TokenFactsUsageAdapter = epoch, "unknown"
		fact, _ := parseTokenMetering(cfg, tokenMeteringRawFixture(`{"claude":false,"model_ratio":1,"group_ratio":1}`), 10)
		p := tokenMeteringDecode(t, fact)
		if p.Adapter != "unknown" || p.TotalInput != nil || p.Semantic != "unsupported" {
			t.Fatalf("epoch granted semantic evidence: %+v", p)
		}
		for _, key := range []string{"quota_per_unit", "currency", "billing_source", "rounding_mode", "effective_model_ratio", "effective_group_ratio", "image", "is_task", "request_rules"} {
			if p.Fields[key].State != "missing" || p.Fields[key].Value != "" {
				t.Fatalf("epoch supplied %s: %+v", key, p.Fields[key])
			}
		}
	}
	// A producer/format name is not a usage semantic. Explicit configured
	// bindings still work, including when the configured adapter is unknown.
	for _, value := range []string{"rc26", "claude", "openai_compatible"} {
		cfg := tokenMeteringConfig()
		fact, _ := parseTokenMetering(cfg, tokenMeteringRawFixture(`{"usage_semantic":"`+value+`"}`), 10)
		if p := tokenMeteringDecode(t, fact); p.TotalInput != nil || p.SemanticReason != "explicit_semantic_unsupported" {
			t.Fatalf("unbound format inferred semantic: %+v", p)
		}
	}
}

func TestTokenMeteringRenormalizeClearsPreviousEvidence(t *testing.T) {
	cfg := tokenMeteringConfig()
	fact, _ := parseTokenMetering(cfg, tokenMeteringRawFixture(`{"usage_semantic":"openai"}`), 10)
	p := tokenMeteringDecode(t, fact)
	if p.TotalInput == nil {
		t.Fatal("explicit source semantic missing")
	}
	p.Fields["usage_semantic"] = metering.Field{State: "null"}
	normalizeTokenMetering(cfg, &p)
	if p.TotalInput != nil || p.UnassignedCacheWrite != nil || p.Semantic != "unsupported" || p.SemanticReason != "explicit_semantic_unusable" {
		t.Fatalf("stale successful interpretation retained: %+v", p)
	}
}

func TestTokenMeteringProtocolGuessRejectedBeforeAndAfterMigration(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":20,"cache_creation_tokens":30}`)
	fact, refs := parseTokenMetering(m.cfg, raw, now)
	p := tokenMeteringDecode(t, fact)
	p.Version, p.Semantic, p.SemanticReason = "token-facts-v5", "anthropic_native", "rc26_claude_adapter"
	p.TotalInput = ptrTokenMetering(150)
	fact.SourceFacts, fact.ParserVersion = tokenDetectionJSON(p), p.Version
	tokenMeteringSetFactHash(&fact, refs)
	if err := m.tokenStore().Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", fact.SourceEpoch).
		Updates(map[string]any{"fact_count": 1, "fact_bytes": fact.StoredBytes}).Error; err != nil {
		t.Fatal(err)
	}
	in, err := tokenDetectionInputOf(fact)
	if err != nil || in.Usage.TotalInput != nil || in.Usage.Semantic != "unsupported" || in.Usage.Fields["cache_tokens"].Value != "20" {
		t.Fatal("legacy protocol guess was consumed as usage evidence", in.Usage, err)
	}
	if changed, err := m.migrateTokenMeteringBatch(context.Background(), now); err != nil || changed != 1 {
		t.Fatal("protocol-guess migration", changed, err)
	}
	got := tokenMeteringFactRead(t, m, fact.LogID)
	p = tokenMeteringDecode(t, got)
	if got.ParserVersion != tokenMeteringParserVersion || p.TotalInput != nil || p.Fields["quota_per_unit"].State != "missing" || p.Fields["cache_creation_tokens"].Value != "30" {
		t.Fatal("migration retained a guess or changed recorded values", p)
	}
}

func TestTokenDetectionRejectsGuessedEvidenceBeforeMigration(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown_semantic", true: "recorded_semantic"}[explicit], func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			fact := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
			parsed := tokenMeteringDecode(t, fact)
			parsed.Version, parsed.SemanticReason = "token-facts-v4", "rc26_source_contract"
			unit := parsed.Fields["quota_per_unit"]
			unit.Kind = "source_contract_v1"
			parsed.Fields["quota_per_unit"] = unit
			if explicit {
				parsed.Fields["usage_semantic"] = metering.Field{State: "valid", Value: "anthropic"}
			} else {
				parsed.Fields["usage_semantic"] = metering.Field{State: "missing"}
			}
			body := tokenDetectionJSON(parsed)
			if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=1").Updates(map[string]any{"source_facts": body, "parser_version": "token-facts-v4"}).Error; err != nil {
				t.Fatal(err)
			}
			n, err := m.runTokenDetectionRound(context.Background(), now)
			if n != 1 || err != nil {
				t.Fatal("legacy request was not evaluated", n, err)
			}
			head, result := detectionHead(t, m, 1)
			if result.Price.Supported || result.Price.QuotaPerUnit != "" || result.ActualQuota == nil {
				t.Fatal("guessed unit became current price evidence", result.Price)
			}
			var revision TokenDetectionRevision
			if err := m.tokenStore().Where("source_epoch=? AND log_id=1 AND revision=?", fact.SourceEpoch, head.Revision).First(&revision).Error; err != nil {
				t.Fatal(err)
			}
			var frozen tokenDetectionFrozen
			if json.Unmarshal([]byte(revision.FrozenJSON), &frozen) != nil || frozen.Input.Usage.Fields["quota_per_unit"].State != "missing" || (frozen.Input.Usage.TotalInput != nil) != explicit {
				t.Fatal("freeze did not distinguish source evidence from guessed fields")
			}
			if tokenMeteringFactRead(t, m, 1).SourceFacts != body {
				t.Fatal("fixture was migrated; consumption-boundary protection was not tested")
			}
			oldHash := tokenDetectionHash([]any{"content-evidence-v2", detectionPolicy()})
			if oldHash == tokenDetectionPolicyHash(detectionPolicy()) {
				t.Fatal("older decisions were not invalidated")
			}
			if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Where("log_id=1").Update("policy_hash", oldHash).Error; err != nil {
				t.Fatal(err)
			}
			if jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), fact.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now+1, 50, 60); err != nil || len(jobs) != 1 || jobs[0].LogID != 1 {
				t.Fatal("old frozen result was treated as current", jobs, err)
			}
		})
	}
}

func TestTokenMeteringMigrationCommitRestartAndAccounting(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	m.cfg.TokenFactsUsageAdapter = "unknown"
	var bytes int64
	for id := int64(1); id <= 24; id++ {
		raw := tokenMeteringRawFixture(`{"usage_semantic":"openai"}`)
		raw.ID = id
		fact, refs := parseTokenMetering(m.cfg, raw, now-10)
		p := tokenMeteringDecode(t, fact)
		p.Version, p.Adapter = "token-facts-v4", "newapi-rc26"
		p.Fields["quota_per_unit"] = metering.Field{State: "derived", Kind: "source_contract_v1", Value: "500000", Reason: "quota_per_usd"}
		body, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		fact.SourceFacts, fact.ParserVersion = string(body), p.Version
		fact.EvidenceJSON, fact.EvidenceSnapshotAt = `{"recorded":"evidence"}`, now-9
		tokenMeteringSetFactHash(&fact, refs)
		bytes += fact.StoredBytes
		if err := m.storeDB.Create(&fact).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&refs).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).
		Updates(map[string]any{"fact_count": 24, "fact_bytes": bytes}).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenMeteringHealthCounts(m.storeDB); err != nil {
		t.Fatal(err)
	}
	// Reproduce v4's existing ledger drift, not only the new per-row delta.
	if err := m.storeDB.Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Update("fact_bytes", bytes+123456).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER fail_second_migration_chunk BEFORE UPDATE OF source_facts ON token_metering_facts
		WHEN NEW.log_id=9 BEGIN SELECT RAISE(ABORT,'injected migration failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	changed, err := m.migrateTokenMeteringBatch(context.Background(), now)
	if err == nil || !strings.Contains(err.Error(), "injected migration failure") || changed != 8 {
		t.Fatalf("committed prefix lost or uncommitted progress counted: %d %v", changed, err)
	}
	state := tokenMeteringStateRead(t, m)
	if state.MigrationCursor != 8 || state.MigrationVersion != tokenMeteringMigrationVersion(m.cfg) {
		t.Fatalf("checkpoint not atomic with committed prefix: %+v", state)
	}
	if fact := tokenMeteringFactRead(t, m, 9); fact.Revision != 1 || fact.ParserVersion != "token-facts-v4" {
		t.Fatal("failed chunk partially committed")
	}
	if err := m.storeDB.Exec("DROP TRIGGER fail_second_migration_chunk").Error; err != nil {
		t.Fatal(err)
	}
	// New worker object: progress must come entirely from the database.
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB}
	changed, err = restarted.migrateTokenMeteringBatch(context.Background(), now+1)
	if err != nil || changed != 16 {
		t.Fatalf("restart failed to resume durable prefix: %d %v", changed, err)
	}
	var actualBytes int64
	if err := m.storeDB.Model(&TokenMeteringFact{}).Select("SUM(stored_bytes)").Scan(&actualBytes).Error; err != nil {
		t.Fatal(err)
	}
	state = tokenMeteringStateRead(t, m)
	if state.FactBytes != actualBytes || state.FactCount != 24 || state.MigrationCursor != 24 {
		t.Fatalf("capacity/checkpoint mismatch: state=%+v actual=%d", state, actualBytes)
	}
	for id := int64(1); id <= 24; id++ {
		fact := tokenMeteringFactRead(t, m, id)
		p := tokenMeteringDecode(t, fact)
		if fact.Revision != 2 || fact.LastSeenAt != now-10 || fact.EvidenceSnapshotAt != now-9 || fact.EvidenceJSON != `{"recorded":"evidence"}` || p.Adapter != "unknown" || p.Fields["quota_per_unit"].State != "missing" {
			t.Fatalf("migration changed evidence or kept fabricated unit: id=%d fact=%+v", id, fact)
		}
		raw := tokenMeteringRawFixture(`{"usage_semantic":"openai"}`)
		raw.ID = id
		collected, _ := parseTokenMetering(m.cfg, raw, now+100)
		if collected.FactHash != fact.FactHash {
			t.Fatal("unchanged source replay would create another revision")
		}
	}
	if changed, err := restarted.migrateTokenMeteringBatch(context.Background(), now+2); err != nil || changed != 0 {
		t.Fatalf("completed pass rewrites missing evidence: %d %v", changed, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if changed, err := restarted.migrateTokenMeteringBatch(ctx, now+3); !errors.Is(err, context.Canceled) || changed != 0 {
		t.Fatalf("cancel ignored: %d %v", changed, err)
	}
}
