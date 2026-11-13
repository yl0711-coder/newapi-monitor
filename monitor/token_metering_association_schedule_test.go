package monitor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func tokenAssociationScheduleFacts(t *testing.T, m *Monitor, first, count, createdAt, now int64, checked bool) {
	t.Helper()
	var facts []TokenMeteringFact
	var references []TokenMeteringRef
	var entries []NginxRequestEvidence
	for offset := int64(0); offset < count; offset++ {
		id := first + offset
		raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0,"cache_creation_tokens":0}`)
		raw.ID, raw.CreatedAt, raw.RequestID = id, createdAt+offset%30, fmt.Sprintf("association-schedule-%d", id)
		fact, refs := parseTokenMetering(m.cfg, raw, now)
		if checked {
			fact.LinkCheckedAt, fact.LinkAttempts, fact.LinkNextAt = now-120, 3, now-60
		}
		facts = append(facts, fact)
		references = append(references, refs...)
		entries = append(entries, NginxRequestEvidence{EventID: fmt.Sprintf("association-schedule-%d", id), EventMS: raw.CreatedAt * 1000,
			HMACKeyID: refs[0].KeyID, OneAPIIDHMAC: refs[0].Hash, Status: 200, Route: "/v1/messages",
			RequestLength: ptrTokenMetering(1000), RequestLengthState: "valid", RequestLengthVersion: "request-length-v1"})
	}
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error {
		if err := tx.CreateInBatches(facts, 100).Error; err != nil {
			return err
		}
		return tx.CreateInBatches(references, 100).Error
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.nginxEvidenceDB.CreateInBatches(entries, 100).Error; err != nil {
		t.Fatal(err)
	}
}

func TestTokenMeteringFirstAssociationPrecedesRecentRechecks(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed_%t", indexed), func(t *testing.T) {
			m, now, _ := tokenMeteringFixture(t)
			defer m.Close()
			m.cfg.TokenProcessingBatchRows = 50
			m.cfg.TokenProcessingPollSeconds = 15
			tokenAssociationScheduleFacts(t, m, 1, 80, now-86400, now, true)
			tokenAssociationScheduleFacts(t, m, 81, 80, now-1800, now, true)
			tokenAssociationScheduleFacts(t, m, 161, 49, now-600, now, false)
			if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=209").Update("link_next_at", now+300).Error; err != nil {
				t.Fatal(err)
			}
			if indexed {
				if err := initTokenMeteringLinkIndex(m.tokenStore()); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := m.linkTokenMeteringBatch(context.Background(), now); err != nil || n != 50 {
				t.Fatal("association turn", n, err)
			}
			var rows []TokenMeteringFact
			if err := m.tokenStore().Order("log_id").Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			var historical, repeated, first int
			for _, f := range rows {
				if f.LinkCheckedAt != now {
					continue
				}
				if f.LinkState != "unique" || f.RequestLength == nil {
					t.Fatal("real evidence was not associated", f.LogID, f.LinkState)
				}
				switch {
				case f.LogID <= 80:
					historical++
				case f.LogID <= 160:
					repeated++
				case f.LogID < 209:
					first++
				default:
					t.Fatal("future backoff was bypassed")
				}
			}
			if historical != 1 || repeated != 9 || first != 40 {
				t.Fatal("reserved work or first-association capacity lost", historical, repeated, first)
			}
			// A later bounded turn must finish the remaining first associations;
			// the future-backoff row is still not eligible.
			if _, err := m.linkTokenMeteringBatch(context.Background(), now+15); err != nil {
				t.Fatal(err)
			}
			var remaining int64
			if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id>=161 AND log_id<209 AND link_checked_at=0").Count(&remaining).Error; err != nil || remaining != 0 {
				t.Fatal("first association suffix did not resume", remaining, err)
			}
		})
	}
}

func TestTokenMeteringAssociationLanesWithContinuousArrivals(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 50, 15
	if err := initTokenMeteringLinkIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	tokenAssociationScheduleFacts(t, m, 1, 320, now-86400, now, true)
	tokenAssociationScheduleFacts(t, m, 321, 160, now-1800, now, true)
	for turn := int64(0); turn < 12; turn++ {
		at := now + turn*60
		first := 10000 + turn*40
		tokenAssociationScheduleFacts(t, m, first, 40, at-600, at, false)
		if n, err := m.linkTokenMeteringBatch(context.Background(), at); err != nil || n != 50 {
			t.Fatal("continuous association turn", turn, n, err)
		}
		var incomplete int64
		if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id>=10000 AND (link_checked_at=0 OR link_state!='unique')").Count(&incomplete).Error; err != nil || incomplete != 0 {
			t.Fatal("new association work accumulates despite spare capacity", turn, incomplete, err)
		}
	}
	var history, rechecks int64
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id<=320 AND link_attempts>3").Count(&history).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id>320 AND log_id<10000 AND link_attempts>3").Count(&rechecks).Error; err != nil {
		t.Fatal(err)
	}
	if history == 0 || history >= 320 || rechecks == 0 || rechecks >= 160 {
		t.Fatal("history and existing recent checks must advance and remain present", history, rechecks)
	}
}

func TestTokenMeteringAssociationIndexUpgradeRollbackAndTinyTurns(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	tokenAssociationScheduleFacts(t, m, 1, 80, now-86400, now, true)
	tokenAssociationScheduleFacts(t, m, 81, 80, now-1800, now, true)
	tokenAssociationScheduleFacts(t, m, 161, 80, now-600, now, false)
	// A legacy NULL check timestamp represents an unobserved request too.
	if err := m.tokenStore().Exec("UPDATE token_metering_facts SET link_checked_at=NULL WHERE log_id=161").Error; err != nil {
		t.Fatal(err)
	}
	before, err := tokenMeteringLinkJobs(m.tokenReadStore(), m.cfg.TokenFactsSourceEpoch, now, 50, 15)
	if err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("index rollback fixture")
	if err := m.tokenStore().Transaction(func(tx *gorm.DB) error {
		if err := initTokenMeteringLinkIndex(tx); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if m.tokenStore().Migrator().HasIndex(&TokenMeteringFact{}, "idx_token_link_recent_stage_v1") {
		t.Fatal("rolled-back index remained installed")
	}
	for n := 0; n < 2; n++ {
		if err := initTokenMeteringLinkIndex(m.tokenStore()); err != nil {
			t.Fatal(err)
		}
	}
	type statement struct {
		sql  string
		args []any
	}
	var queries []statement
	if err := m.tokenStore().Callback().Query().After("gorm:query").Register("association_stage_plan", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "COALESCE(link_checked_at,0)") {
			queries = append(queries, statement{tx.Statement.SQL.String(), append([]any(nil), tx.Statement.Vars...)})
		}
	}); err != nil {
		t.Fatal(err)
	}
	after, err := tokenMeteringLinkJobs(m.tokenReadStore(), m.cfg.TokenFactsSourceEpoch, now, 50, 15)
	_ = m.tokenStore().Callback().Query().Remove("association_stage_plan")
	if err != nil || !reflect.DeepEqual(before, after) || len(queries) != 2 {
		t.Fatal("index upgrade changed candidates or omitted a stage", err, len(queries))
	}
	for _, query := range queries {
		var plan []struct{ Detail string }
		if err := m.tokenStore().Raw("EXPLAIN QUERY PLAN "+query.sql, query.args...).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		indexed := false
		for _, row := range plan {
			if strings.Contains(row.Detail, "SCAN ") || strings.Contains(row.Detail, "TEMP B-TREE") {
				t.Fatal("unbounded association selection", plan)
			}
			indexed = indexed || strings.Contains(row.Detail, "USING INDEX idx_token_link_recent_stage_v1")
		}
		if !indexed {
			t.Fatal("association stage did not use its index", plan)
		}
	}
	seen := map[int64]bool{}
	for turn := int64(0); turn < 3; turn++ {
		jobs, err := tokenMeteringLinkJobs(m.tokenReadStore(), m.cfg.TokenFactsSourceEpoch, now+turn*15, 1, 15)
		if err != nil || len(jobs) != 1 {
			t.Fatal("tiny association turn", len(jobs), err)
		}
		seen[jobs[0].LogID] = true
	}
	var laneSeen [3]bool
	for id := range seen {
		switch {
		case id <= 80:
			laneSeen[0] = true
		case id <= 160:
			laneSeen[1] = true
		default:
			laneSeen[2] = true
		}
	}
	if laneSeen != [3]bool{true, true, true} || len(seen) != 3 {
		t.Fatal("a tiny association turn starved a lane", seen)
	}
}
