package monitor

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Reproduce the live failure: the fifteen-minute range itself contains a
// restart backlog. A descending sample showed some new decisions while the
// unselected middle waited until it became the old ascending edge.
func TestTokenLatestPriorityDrainsArrivalsAndCorrectionsDuringBacklog(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "rebuilding"
		if ready {
			name = "ready"
		}
		t.Run(name, func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			db := m.tokenStore()
			base := detectionFact(t, m, 1, now-800, 3, 100, 0, 0)
			base.LinkRevision = 0
			if err := db.Delete(&TokenMeteringFact{}, "source_epoch=?", base.SourceEpoch).Error; err != nil {
				t.Fatal(err)
			}
			facts := make([]TokenMeteringFact, 0, 1200)
			heads := make([]TokenDetectionCurrent, 0, 600)
			for id := int64(1); id <= 1200; id++ {
				f := base
				f.LogID, f.CreatedAt = id, now-800+id/4
				facts = append(facts, f)
				if id%2 == 0 {
					heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: id, CreatedAt: f.CreatedAt, Revision: 1, FactRevision: f.Revision, PolicyHash: "old-policy", ResultJSON: "{}"})
				}
			}
			for _, rows := range []any{facts, heads} {
				if err := db.CreateInBatches(rows, 128).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := initTokenDetectionReportIndex(db); err != nil {
				t.Fatal(err)
			}
			if err := db.Exec("UPDATE "+tokenPendingMigrationTable+" SET ready=?", ready).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
				t.Fatal(err)
			}
			lease, hash := "latest-priority-fixture", tokenDetectionPolicyHash(detectionPolicy())
			if err := db.Create(&TokenDetectionState{SourceEpoch: base.SourceEpoch, LeaseToken: lease, LeaseUntil: now + 3600}).Error; err != nil {
				t.Fatal(err)
			}
			const arrivals, turns = int64(10), int64(12)
			for turn := int64(0); turn < turns; turn++ {
				at := now + turn*15
				for row := int64(0); row < arrivals; row++ {
					f := base
					f.LogID, f.CreatedAt = 1000000+turn*arrivals+row, at-30
					if err := db.Create(&f).Error; err != nil {
						t.Fatal(err)
					}
				}
				jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), base.SourceEpoch, hash, at, 250, 15)
				if err != nil || len(jobs) < 32 {
					t.Fatal("bounded backlog prefix", len(jobs), err)
				}
				for _, job := range jobs[:32] {
					if err := m.processTokenDetection(context.Background(), job.SourceEpoch, job.LogID, detectionPolicy(), lease, at); err != nil {
						t.Fatal(err)
					}
				}
				// Evidence arrives independently, after the first result. Reuse
				// the normal trigger and revision fence to demand a second decision.
				if err := db.Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id>=1000000 AND link_revision=0 AND EXISTS (SELECT 1 FROM token_detection_currents c WHERE c.source_epoch=token_metering_facts.source_epoch AND c.log_id=token_metering_facts.log_id)", base.SourceEpoch).Update("link_revision", gorm.Expr("link_revision+1")).Error; err != nil {
					t.Fatal(err)
				}
				var pending int64
				if err := db.Table(tokenReportPendingMembers).Where("source_epoch=? AND log_id>=1000000 AND created_at<? AND base_pending=1", base.SourceEpoch, at-60).Count(&pending).Error; err != nil {
					t.Fatal(err)
				}
				if pending > 0 {
					t.Fatalf("turn %d: %d recent identities missed the bounded correction deadline", turn, pending)
				}
			}
			var background int64
			if err := db.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id<1000000 AND policy_hash=?", base.SourceEpoch, hash).Count(&background).Error; err != nil || background == 0 {
				t.Fatal("older queue lost its reserved opportunity", background, err)
			}
			actual, err := tokenPendingProjectionReady(db)
			if err != nil || actual != ready {
				t.Fatal("scheduling changed global readiness", actual, err)
			}
		})
	}
}

func TestTokenLatestPriorityOverlapKeepsUniqueBudget(t *testing.T) {
	var primary, background []TokenMeteringFact
	for id := int64(1); id <= 32; id++ {
		primary = append(primary, TokenMeteringFact{SourceEpoch: "a", LogID: id})
	}
	background = append(background, primary[:16]...)
	background = append(background, TokenMeteringFact{SourceEpoch: "a", LogID: 99}, TokenMeteringFact{SourceEpoch: "b", LogID: 1})
	jobs := tokenDetectionPriorityJobs(primary, background, 32)
	if len(jobs) != 32 || jobs[15].LogID != 16 || jobs[31].LogID != 99 {
		t.Fatal("overlap consumed a reserved unique slot", jobs)
	}
	jobs = tokenDetectionPriorityJobs(primary, background, 40)
	if len(jobs) != 34 || jobs[33].SourceEpoch != "b" {
		t.Fatal("identity must include source epoch and spare capacity must be reused", jobs)
	}
}

// The urgent range must not replace the existing fifteen-minute discovery
// range: reference, policy, coverage and scheduled reviews can change after a
// request leaves its first three minutes while the global projection rebuilds.
func TestTokenLatestPriorityKeepsOlderDependencyRepairsWhileRebuilding(t *testing.T) {
	for _, dependency := range []string{"policy", "reference", "coverage", "due"} {
		t.Run(dependency, func(t *testing.T) {
			m, now, _ := detectionFixture(t)
			defer m.Close()
			f := detectionFact(t, m, 700, now-400, 1, 100, 20, 0)
			detectionRun(t, m, now)
			db := m.tokenStore()
			head, _ := detectionHead(t, m, f.LogID)
			var err error
			switch dependency {
			case "policy":
				err = db.Model(&TokenDetectionCurrent{}).Where("log_id=?", f.LogID).Update("policy_hash", "previous-policy").Error
			case "reference":
				err = db.Model(&TokenDetectionCurrent{}).Where("log_id=?", f.LogID).Update("reference_generation", head.ReferenceGeneration+1).Error
			case "coverage":
				err = db.Model(&TokenDetectionCurrent{}).Where("log_id=?", f.LogID).Update("source_complete", !head.SourceComplete).Error
			case "due":
				err = db.Model(&TokenDetectionCurrent{}).Where("log_id=?", f.LogID).Update("next_at", now-1).Error
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Exec("UPDATE " + tokenPendingMigrationTable + " SET ready=0").Error; err != nil {
				t.Fatal(err)
			}
			jobs, err := tokenDetectionJobsBounded(m.tokenReadStore(), f.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now+2, 250, 15)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, job := range jobs {
				found = found || job.SourceEpoch == f.SourceEpoch && job.LogID == f.LogID
			}
			if !found {
				t.Fatal("three-minute priority hid an existing recent dependency repair", dependency)
			}
			if ready, err := tokenPendingProjectionReady(db); err != nil || ready {
				t.Fatal("dependency repair changed global readiness", ready, err)
			}
		})
	}
}

// Opt-in, read-only selection against the running local queue. No source
// connection, worker, migration, lease claim or fact write is executed.
func TestTokenLatestPriorityReadOnlyLiveQueue(t *testing.T) {
	file := os.Getenv("TOKEN_LATEST_QUEUE_READONLY")
	if file == "" {
		t.Skip("set TOKEN_LATEST_QUEUE_READONLY for a bounded local queue probe")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(sqlite.Open("file:"+file+"?mode=ro"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	var trace strings.Builder
	db.Logger = logger.New(log.New(&trace, "", 0), logger.Config{LogLevel: logger.Info})
	if err := db.Exec("PRAGMA query_only=ON").Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now, started := time.Now().Unix(), time.Now()
	var state TokenDetectionState
	if err := db.WithContext(ctx).Select("source_epoch,policy_hash").Where("source_epoch=?", os.Getenv("TOKEN_LATEST_QUEUE_EPOCH")).First(&state).Error; err != nil {
		t.Fatal(err)
	}
	var proof struct {
		At                                   int64 `json:"at"`
		ReadOnly                             bool  `json:"read_only"`
		QueueReady                           bool  `json:"queue_ready"`
		Selected, Latest, Fresh, Corrections int
		ElapsedMS                            int64 `json:"elapsed_ms"`
		SelectionMS                          int64 `json:"selection_ms"`
		Stage, Error, SQLTrace               string
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		proof.Stage = "readiness"
		proof.QueueReady, err = tokenPendingProjectionReady(tx)
		if err != nil {
			return err
		}
		proof.Stage = "selection"
		selectionStarted := time.Now()
		jobs, e := tokenDetectionJobsBounded(tx, state.SourceEpoch, state.PolicyHash, now, 250, 15)
		proof.SelectionMS = time.Since(selectionStarted).Milliseconds()
		if e != nil {
			return e
		}
		ids := make([]int64, 0, len(jobs))
		seen := map[int64]bool{}
		for _, job := range jobs {
			if seen[job.LogID] {
				t.Fatal("duplicate selected identity")
			}
			seen[job.LogID] = true
			ids = append(ids, job.LogID)
		}
		var rows []struct {
			LogID, CreatedAt         int64
			HeadPresent, BasePending bool
		}
		if len(ids) > 0 {
			proof.Stage = "selected_members"
			// The verification probe is an identity lookup, not a scan of all
			// members for this epoch through a time/range covering index.
			if e := tx.Table(tokenReportPendingMembers+" INDEXED BY sqlite_autoindex_token_report_pending_members_v1_1").Select("log_id,created_at,head_present,base_pending").Where("source_epoch=? AND log_id IN ?", state.SourceEpoch, ids).Scan(&rows).Error; e != nil {
				return e
			}
		}
		proof.Selected = len(rows)
		for _, r := range rows {
			if r.CreatedAt >= now-180 {
				proof.Latest++
			}
			if !r.HeadPresent {
				proof.Fresh++
			} else if r.BasePending {
				proof.Corrections++
			}
		}
		return nil
	})
	if err != nil {
		proof.Error = err.Error()
	} else {
		proof.Stage = "complete"
	}
	proof.At, proof.ReadOnly, proof.ElapsedMS = now, true, time.Since(started).Milliseconds()
	proof.SQLTrace = trace.String()
	if proof.Selected > 250 {
		t.Fatal("row budget exceeded", proof.Selected)
	}
	body, _ := json.MarshalIndent(proof, "", "  ")
	if out := os.Getenv("TOKEN_LATEST_QUEUE_REPORT"); out != "" {
		if err := os.WriteFile(out, append(body, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(body))
}
