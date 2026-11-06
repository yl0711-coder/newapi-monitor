package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func TestGenerationRulesUpgradeRestageAndRecoverWithoutChangingMoney(t *testing.T) {
	_, authorization, _ := usageAuthorityFixture(t)
	f := selectionFixture(t)
	ctx := context.Background()
	owner, _, err := LoadUsageGenerationOwner(ctx, authorization, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	legacy := f.plan // Version before calculation_version was introduced.
	legacyRaw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	oldRequest := generationRequest(legacy, legacyRaw)
	rules, err := json.Marshal(legacy.Funding.Rules)
	if err != nil {
		t.Fatal(err)
	}
	oldRequest.RulesSHA256 = generationHash(append([]byte("local-monthly-draft-v1:"), rules...))
	if err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, legacyRaw, f.raw, false)
	}); err != nil {
		t.Fatal(err)
	}
	selected, err := changeGenerationSelection(ctx, f.receiver, 1, "2026-09", &oldRequest, "")
	if err != nil {
		t.Fatal(err)
	}
	oldJob, err := f.g.tasks.Submit(ctx, oldRequest)
	if err != nil {
		t.Fatal(err)
	}
	worker := testLocalWorker(t, f.g)
	for range 3 {
		if _, err := worker.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	retired, err := f.g.tasks.Get(ctx, owner, oldJob.ID)
	if err != nil || retired.State != monthlytasks.Invalidated || retired.Attempts != 0 || worker.Status().Dispatched != 0 {
		t.Fatal("worker executed obsolete calculation", retired, worker.Status(), err)
	}
	if _, err := f.g.SubmitUsageMonth(ctx, authorization, 1, 1, "2026-09"); PublicUsageGenerationProblem(err).Code != "not_prepared" {
		t.Fatal("old selection could be reactivated", err)
	}
	current := stageFixture(t, f, selected.Head)
	if current.Request.InputSHA256 == oldRequest.InputSHA256 {
		t.Fatal("reused obsolete plan")
	}
	job, err := f.g.SubmitUsageMonth(ctx, authorization, 1, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := f.g.Run(ctx, owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	status, err := f.g.ReadUsageMonthStatus(ctx, authorization, 1, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	checkCustomerStatusJSON(t, status, "pending_verification")
	again, err := f.g.SubmitUsageMonth(ctx, authorization, 1, 1, "2026-09")
	if err != nil || again.ID != job.ID {
		t.Fatal("duplicate recovery created another task", err)
	}
	var keptPlan, keptInput []byte
	if err := f.receiver.QueryRow(`SELECT plan,preparation FROM portal_monthly_generation_inputs WHERE id=?`, oldRequest.InputSHA256).Scan(&keptPlan, &keptInput); err != nil || !bytes.Equal(keptPlan, legacyRaw) || !bytes.Equal(keptInput, f.raw) {
		t.Fatal("recovery changed retained legacy evidence", err)
	}
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("recovery wrote source", err)
	}
}

func TestCustomerGenerationObsoleteRulesNeedPreparationWithoutRetries(t *testing.T) {
	for _, state := range []string{monthlytasks.Queued, monthlytasks.DraftReady} {
		t.Run(state, func(t *testing.T) {
			f := newCustomerStatusFixture(t)
			f.request.RulesSHA256 = strings.Repeat("d", 64)
			selected := f.selectMetadata(t)
			ctx := context.Background()
			job, err := f.g.tasks.Submit(ctx, f.request)
			if err != nil {
				t.Fatal(err)
			}
			if state == monthlytasks.DraftReady {
				_, lease, err := f.g.tasks.Claim(ctx, f.request.Owner, job.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.g.tasks.Complete(ctx, lease, strings.Repeat("f", 64)); err != nil {
					t.Fatal(err)
				}
			}
			before, err := f.g.tasks.Get(ctx, f.request.Owner, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			// Status and rejection must not need the large retained input table.
			if _, err := f.db.Exec(`DROP TABLE portal_monthly_generation_inputs`); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				status, err := f.g.ReadUsageMonthStatus(ctx, f.authRO, 1, 1, "2026-09")
				if err != nil {
					t.Fatal(err)
				}
				checkCustomerStatusJSON(t, status, "needs_preparation")
				if _, err := f.g.SubmitUsageMonth(ctx, f.authRO, 1, 1, "2026-09"); PublicUsageGenerationProblem(err).Code != "not_prepared" {
					t.Fatal("obsolete input attempted loading or admission", err)
				}
			}
			after, err := f.g.tasks.Get(ctx, f.request.Owner, job.ID)
			if err != nil || after != before {
				t.Fatal("poll/submit mutated old task", err)
			}
			current, err := ReadGenerationSelection(ctx, f.db, 1, "2026-09")
			if err != nil || !reflect.DeepEqual(current, selected) {
				t.Fatal("poll/submit changed selection", err)
			}
		})
	}
}
