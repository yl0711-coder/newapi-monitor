package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"gorm.io/gorm"
)

type customerStatusFixture struct {
	g            *Generation
	db           *sql.DB
	auth, authRO *gorm.DB
	path         string
	request      monthlytasks.Request
}

func newCustomerStatusFixture(t *testing.T) customerStatusFixture {
	t.Helper()
	auth, ro, _ := usageAuthorityFixture(t)
	path := filepath.Join(t.TempDir(), "receiver.sqlite")
	db := generationDB(t, path, false)
	if err := InitGenerationReceiver(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := InitGenerationSelections(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	store, err := monthlytasks.New(db)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately no evidence root, funding DB, calculator or generation guard:
	// metadata polling MUST NOT run generation or inspect financial evidence.
	request := usageAuthorityRequest(t, ro)
	request.RulesSHA256 = generationRulesHash(monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000})
	return customerStatusFixture{&Generation{receiver: db, tasks: store}, db, auth, ro, path, request}
}

func (f customerStatusFixture) selectMetadata(t *testing.T) GenerationSelection {
	t.Helper()
	s, err := changeGenerationSelection(context.Background(), f.db, 1, "2026-09", &f.request, "")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func checkCustomerStatusJSON(t *testing.T, status UsageGenerationStatus, want string) {
	t.Helper()
	if status.Month != "2026-09" || status.State != want || status.Message == "" {
		t.Fatal("unexpected customer status", status, want)
	}
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 {
		t.Fatal("customer DTO field set expanded", string(raw))
	}
	for _, key := range []string{"month", "state", "message"} {
		if _, ok := fields[key]; !ok {
			t.Fatal("missing public field", key)
		}
	}
	for _, private := range []string{"source_identity", "input_sha256", "result_sha256", "failure_code", "task_key", "group_id", "member_manifest", "/private/", "draft_ready", "downloadable", "published"} {
		if bytes.Contains(raw, []byte(private)) {
			t.Fatal("internal metadata or false publication leaked", private, string(raw))
		}
	}
}

func TestCustomerGenerationStatusLifecycleReadOnlyAndNoFinancialIO(t *testing.T) {
	f := newCustomerStatusFixture(t)
	ctx := context.Background()
	status, err := f.g.ReadUsageMonthStatus(ctx, f.authRO, 1, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	checkCustomerStatusJSON(t, status, "not_prepared")
	selected := f.selectMetadata(t)
	status, err = f.g.ReadUsageMonthStatus(ctx, f.authRO, 1, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	checkCustomerStatusJSON(t, status, "not_requested")
	task, err := f.g.tasks.Submit(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Exec(`DROP TABLE portal_monthly_generation_inputs`); err != nil {
		t.Fatal(err)
	}
	readonly := generationDB(t, f.path, true)
	reader, err := monthlytasks.New(readonly)
	if err != nil {
		t.Fatal(err)
	}
	g := &Generation{receiver: readonly, tasks: reader}
	check := func(want string) {
		t.Helper()
		before, err := os.ReadFile(f.path)
		if err != nil {
			t.Fatal(err)
		}
		for range 4 {
			out, err := g.ReadUsageMonthStatus(ctx, f.authRO, 1, 1, "2026-09")
			if err != nil {
				t.Fatal(err)
			}
			checkCustomerStatusJSON(t, out, want)
		}
		after, err := os.ReadFile(f.path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("status polling modified receiver", err)
		}
	}
	check("queued")
	_, lease, err := f.g.tasks.Claim(ctx, f.request.Owner, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	check("processing")
	if err := f.g.tasks.Complete(ctx, lease, strings.Repeat("f", 64)); err != nil {
		t.Fatal(err)
	}
	check("pending_verification")
	if err := f.g.tasks.Invalidate(ctx, f.request.Owner, task.ID); err != nil {
		t.Fatal(err)
	}
	check("needs_preparation")
	if _, err := WithdrawGenerationSelection(ctx, f.db, 1, "2026-09", selected.Head); err != nil {
		t.Fatal(err)
	}
	check("needs_preparation")
}

func TestCustomerGenerationStatusFailuresDoNotExposeReasonsOrRetry(t *testing.T) {
	for _, code := range []string{"source_unavailable", "internal_failure", "evidence_incomplete", "reconciliation_failed"} {
		t.Run(code, func(t *testing.T) {
			f := newCustomerStatusFixture(t)
			f.selectMetadata(t)
			ctx := context.Background()
			job, err := f.g.tasks.Submit(ctx, f.request)
			if err != nil {
				t.Fatal(err)
			}
			_, lease, err := f.g.tasks.Claim(ctx, f.request.Owner, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.g.tasks.Fail(ctx, lease, code); err != nil {
				t.Fatal(err)
			}
			before, err := f.g.tasks.Get(ctx, f.request.Owner, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := "needs_review"
			if code == "source_unavailable" || code == "internal_failure" {
				want = "temporarily_unavailable"
			}
			for range 3 {
				out, err := f.g.ReadUsageMonthStatus(ctx, f.authRO, 1, 1, "2026-09")
				if err != nil {
					t.Fatal(err)
				}
				checkCustomerStatusJSON(t, out, want)
			}
			after, err := f.g.tasks.Get(ctx, f.request.Owner, job.ID)
			if err != nil || after != before {
				t.Fatal("polling retried or changed failed task", err)
			}
			if _, err := f.db.Exec(`UPDATE portal_monthly_draft_tasks SET attempts=3`); err != nil {
				t.Fatal(err)
			}
			out, err := f.g.ReadUsageMonthStatus(ctx, f.authRO, 1, 1, "2026-09")
			if err != nil {
				t.Fatal(err)
			}
			checkCustomerStatusJSON(t, out, "needs_review")
		})
	}
}

func TestCustomerGenerationStatusRejectsWrongScopeAndCorruptMetadata(t *testing.T) {
	for _, scenario := range []string{"old_session", "changed_members", "corrupt_task", "corrupt_selection", "missing_registry", "cancelled", "future_month"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCustomerStatusFixture(t)
			f.selectMetadata(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := f.g.tasks.Submit(ctx, f.request); err != nil {
				t.Fatal(err)
			}
			version, month := int64(1), "2026-09"
			switch scenario {
			case "old_session":
				version = 0
			case "changed_members":
				if err := f.auth.Exec(`UPDATE usage_member_controls SET tracked_revision=3 WHERE user_id=201`).Error; err != nil {
					t.Fatal(err)
				}
			case "corrupt_task":
				if _, err := f.db.Exec(`UPDATE portal_monthly_draft_tasks SET descriptor='{}'`); err != nil {
					t.Fatal(err)
				}
			case "corrupt_selection":
				for _, q := range []string{`DROP TRIGGER monthly_generation_selection_no_update`, `UPDATE portal_monthly_generation_selections SET payload='{}'`} {
					if _, err := f.db.Exec(q); err != nil {
						t.Fatal(err)
					}
				}
			case "missing_registry":
				if _, err := f.db.Exec(`DROP TABLE portal_monthly_generation_selections`); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				cancel()
			case "future_month":
				month = "2999-09"
			}
			out, err := f.g.ReadUsageMonthStatus(ctx, f.authRO, 1, version, month)
			if err == nil || out != (UsageGenerationStatus{}) {
				t.Fatal("invalid read returned status", out, err)
			}
		})
	}
	f := newCustomerStatusFixture(t)
	f.selectMetadata(t)
	if _, err := f.g.tasks.Submit(context.Background(), f.request); err != nil {
		t.Fatal(err)
	}
	out, err := f.g.ReadUsageMonthStatus(context.Background(), f.authRO, 2, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	checkCustomerStatusJSON(t, out, "not_prepared")
}

func TestCustomerGenerationStatusRechecksDuringRead(t *testing.T) {
	for _, scenario := range []string{"membership", "source"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCustomerStatusFixture(t)
			selected := f.selectMetadata(t)
			if _, err := f.g.tasks.Submit(context.Background(), f.request); err != nil {
				t.Fatal(err)
			}
			if err := f.auth.Exec(`PRAGMA journal_mode=WAL`).Error; err != nil {
				t.Fatal(err)
			}
			calls := 0
			const name = "monthly_status_change_during_read"
			err := f.authRO.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
				if tx.Statement.Table != "customer_groups" || tx.Error != nil {
					return
				}
				calls++
				if scenario == "membership" && calls == 1 {
					_ = tx.AddError(f.auth.Exec(`UPDATE usage_member_controls SET tracked_revision=3 WHERE user_id=201`).Error)
				}
				if scenario == "source" && calls == 2 {
					_, err := WithdrawGenerationSelection(context.Background(), f.db, 1, "2026-09", selected.Head)
					if err != nil {
						_ = tx.AddError(err)
					}
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = f.authRO.Callback().Query().Remove(name) })
			out, err := f.g.ReadUsageMonthStatus(context.Background(), f.authRO, 1, 1, "2026-09")
			if !errors.Is(err, ErrGenerationChanged) || out != (UsageGenerationStatus{}) {
				t.Fatal("changed scope/source returned stale status", out, err)
			}
		})
	}
}

func TestCustomerGenerationProblemAllowlist(t *testing.T) {
	secret := "mysql://customer:secret@rds/private/raw-evidence.json user_id=201 difference=1.819570"
	for _, err := range []error{
		errors.New(secret), fmt.Errorf("%s: %w", secret, context.DeadlineExceeded),
		fmt.Errorf("%s: %w", secret, ErrGenerationIntegrity), errors.Join(errors.New(secret), ErrGenerationChanged),
		ErrGenerationInput, ErrGenerationSelectionMissing, monthlytasks.ErrNotFound, ErrGenerationCapacity,
		monthlytasks.ErrCooldown, monthlytasks.ErrExhausted, context.Canceled,
	} {
		problem := PublicUsageGenerationProblem(err)
		raw, marshalErr := json.Marshal(problem)
		if marshalErr != nil || problem.Code == "" || problem.Message == "" {
			t.Fatal(problem, marshalErr)
		}
		for _, forbidden := range []string{"secret", "mysql", "rds", "private", "201", "1.819570", "raw-evidence"} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatal("raw error exposed", string(raw))
			}
		}
	}
	if got := PublicUsageGenerationProblem(nil); got != (UsageGenerationProblem{}) {
		t.Fatal("nil error created customer error")
	}
	if got, err := usageGenerationTaskStatus("2026-09", monthlytasks.Task{State: "unknown"}); !errors.Is(err, ErrGenerationIntegrity) || got != (UsageGenerationStatus{}) {
		t.Fatal("unknown task state accepted")
	}
}
