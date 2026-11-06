package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func stageFixture(t *testing.T, f generationFixture, head string) GenerationSelection {
	t.Helper()
	s, err := StageGenerationVersion(context.Background(), f.receiver, f.plan, f.raw, head)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func stagingSelectionCount(t *testing.T, f generationFixture, want int) {
	t.Helper()
	var count int
	if err := f.receiver.QueryRow(`SELECT COUNT(*) FROM portal_monthly_generation_selections`).Scan(&count); err != nil || count != want {
		t.Fatal("selection count", count, want, err)
	}
}

func TestGenerationStagingRestartAndMonthOnlySubmission(t *testing.T) {
	_, authorization, _ := usageAuthorityFixture(t)
	f := selectionFixture(t)
	ctx := context.Background()
	owner, _, err := LoadUsageGenerationOwner(ctx, authorization, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	selected := stageFixture(t, f, "")
	if same := stageFixture(t, f, selected.Head); !reflect.DeepEqual(same, selected) {
		t.Fatal("restaging changed current version")
	}
	generationAdmissionCounts(t, f.receiver, 0, 1)
	stagingSelectionCount(t, f, 1)
	var savedPlan, savedInput []byte
	if err := f.receiver.QueryRow(`SELECT plan,preparation FROM portal_monthly_generation_inputs WHERE id=?`, selected.Request.InputSHA256).Scan(&savedPlan, &savedInput); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(savedInput, f.raw) {
		t.Fatal("staging changed preparation bytes")
	}
	// Reopen the receiver and construct a fresh service. No original plan or
	// preparation is passed to SubmitUsageMonth after restart.
	receiver := generationDB(t, f.receiverPath, false)
	selector, err := GenerationSelector(generationDB(t, f.receiverPath, true))
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewUsageGenerationGuard(authorization, selector)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewGeneration(receiver, f.funding, f.root, guard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	task, err := restarted.SubmitUsageMonth(ctx, authorization, 1, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	again, err := restarted.SubmitUsageMonth(ctx, authorization, 1, 1, "2026-09")
	if err != nil || !reflect.DeepEqual(again, task) {
		t.Fatal("duplicate submission created another task", err)
	}
	artifact, err := restarted.Run(ctx, owner, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	ready, err := restarted.SubmitUsageMonth(ctx, authorization, 1, 1, "2026-09")
	if err != nil || ready.ID != task.ID || ready.State != monthlytasks.DraftReady {
		t.Fatal("completed task not reused", err)
	}
	cached, err := restarted.Run(ctx, owner, ready.ID)
	if err != nil || !reflect.DeepEqual(artifact, cached) {
		t.Fatal("restart/cache changed draft", err)
	}
	generationAdmissionCounts(t, f.receiver, 1, 1)
	stagingSelectionCount(t, f, 1)
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("source database modified", err)
	}
}

func TestGenerationStagingAtomicWriteFailures(t *testing.T) {
	for _, table := range []string{"portal_monthly_generation_inputs", "portal_monthly_generation_selections"} {
		t.Run(table, func(t *testing.T) {
			f := selectionFixture(t)
			ctx := context.Background()
			first := stageFixture(t, f, "")
			f.plan.Funding.SourceIdentity = strings.Repeat("d", 64)
			if _, err := f.receiver.Exec(`CREATE TRIGGER staging_failure BEFORE INSERT ON ` + table + ` BEGIN SELECT RAISE(ABORT,'synthetic staging failure'); END`); err != nil {
				t.Fatal(err)
			}
			out, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, first.Head)
			if err == nil || out.Head != "" {
				t.Fatal("failed staging returned success", err)
			}
			generationAdmissionCounts(t, f.receiver, 0, 1)
			stagingSelectionCount(t, f, 1)
			current, err := ReadGenerationSelection(ctx, f.receiver, 1, "2026-09")
			if err != nil || current.Head != first.Head {
				t.Fatal("failed staging changed selection", err)
			}
			if _, err := f.receiver.Exec(`DROP TRIGGER staging_failure`); err != nil {
				t.Fatal(err)
			}
			stageFixture(t, f, first.Head)
			generationAdmissionCounts(t, f.receiver, 0, 2)
			stagingSelectionCount(t, f, 2)
		})
	}
}

func TestGenerationStagingConcurrentSwitchRetainsOnlyWinner(t *testing.T) {
	f := selectionFixture(t)
	first := stageFixture(t, f, "")
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, identity := range []string{"d", "e"} {
		wg.Add(1)
		go func(identity string) {
			defer wg.Done()
			p := f.plan
			p.Funding.SourceIdentity = strings.Repeat(identity, 64)
			_, err := StageGenerationVersion(context.Background(), f.receiver, p, f.raw, first.Head)
			results <- err
		}(identity)
	}
	wg.Wait()
	close(results)
	success, conflicts := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrGenerationSelectionConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal("concurrent staging did not serialize", success, conflicts)
	}
	generationAdmissionCounts(t, f.receiver, 0, 2)
	stagingSelectionCount(t, f, 2)
}

func TestGenerationStagingCommitFailureRollsBackInputAndSelection(t *testing.T) {
	f := selectionFixture(t)
	ctx := context.Background()
	first := stageFixture(t, f, "")
	f.receiver.SetMaxOpenConns(1)
	for _, query := range []string{
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE staging_parent(id INTEGER PRIMARY KEY)`,
		`CREATE TABLE staging_deferred(parent_id INTEGER REFERENCES staging_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER staging_commit_failure AFTER INSERT ON portal_monthly_generation_selections BEGIN INSERT INTO staging_deferred(parent_id) VALUES(99); END`,
	} {
		if _, err := f.receiver.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	f.plan.Funding.SourceIdentity = strings.Repeat("d", 64)
	out, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, first.Head)
	if err == nil || out.Head != "" {
		t.Fatal("failed commit returned new selection", err)
	}
	generationAdmissionCounts(t, f.receiver, 0, 1)
	stagingSelectionCount(t, f, 1)
	current, err := ReadGenerationSelection(ctx, f.receiver, 1, "2026-09")
	if err != nil || current.Head != first.Head {
		t.Fatal("half commit visible", err)
	}
	if _, err := f.receiver.Exec(`DROP TRIGGER staging_commit_failure`); err != nil {
		t.Fatal(err)
	}
	stageFixture(t, f, first.Head)
	generationAdmissionCounts(t, f.receiver, 0, 2)
}

func TestGenerationStagingNeverRepairsEstablishedOrRevokedInputs(t *testing.T) {
	for _, scenario := range []string{"metadata_only", "task_without_input", "invalidated_task", "missing_selected_input", "corrupt_selected_input"} {
		t.Run(scenario, func(t *testing.T) {
			f := selectionFixture(t)
			ctx := context.Background()
			head := ""
			wantInputs, wantTasks, wantSelections := 0, 0, 0
			wantErr := ErrGenerationIntegrity
			switch scenario {
			case "metadata_only":
				s := selectFixture(t, f, "")
				head = s.Head
				wantSelections = 1
			case "task_without_input":
				p, raw, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.g.tasks.Submit(ctx, generationRequest(p, raw)); err != nil {
					t.Fatal(err)
				}
				wantTasks = 1
			case "invalidated_task":
				task := generationSubmit(t, f)
				if err := f.g.tasks.Invalidate(ctx, task.Request.Owner, task.ID); err != nil {
					t.Fatal(err)
				}
				wantTasks, wantInputs, wantErr = 1, 1, ErrGenerationChanged
			case "missing_selected_input":
				s := stageFixture(t, f, "")
				head = s.Head
				wantSelections = 1
				for _, query := range []string{`DROP TRIGGER monthly_generation_no_delete`, `DELETE FROM portal_monthly_generation_inputs`} {
					if _, err := f.receiver.Exec(query); err != nil {
						t.Fatal(err)
					}
				}
			case "corrupt_selected_input":
				s := stageFixture(t, f, "")
				head = s.Head
				wantInputs, wantSelections = 1, 1
				for _, query := range []string{`DROP TRIGGER monthly_generation_no_replace`, `UPDATE portal_monthly_generation_inputs SET preparation='{}'`} {
					if _, err := f.receiver.Exec(query); err != nil {
						t.Fatal(err)
					}
				}
			}
			out, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, head)
			if !errors.Is(err, wantErr) || out.Head != "" {
				t.Fatal("established state repaired or reactivated", scenario, err)
			}
			generationAdmissionCounts(t, f.receiver, wantTasks, wantInputs)
			stagingSelectionCount(t, f, wantSelections)
		})
	}
}

func TestGenerationStagingCapacityAndStaleHeadLeaveNoOrphans(t *testing.T) {
	f := selectionFixture(t)
	ctx := context.Background()
	current := stageFixture(t, f, "")
	f.plan.Funding.SourceIdentity = strings.Repeat("d", 64)
	if _, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, ""); !errors.Is(err, ErrGenerationSelectionConflict) {
		t.Fatal(err)
	}
	generationAdmissionCounts(t, f.receiver, 0, 1)
	for i := 1; i < generationRecordLimit; i++ {
		f.plan.Funding.SourceIdentity = fmt.Sprintf("%064x", i)
		current = stageFixture(t, f, current.Head)
	}
	f.plan.Funding.SourceIdentity = strings.Repeat("e", 64)
	if out, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, current.Head); !errors.Is(err, ErrGenerationCapacity) || out.Head != "" {
		t.Fatal("input capacity bypassed", err)
	}
	generationAdmissionCounts(t, f.receiver, 0, generationRecordLimit)
	stagingSelectionCount(t, f, generationRecordLimit)
	got, err := ReadGenerationSelection(ctx, f.receiver, 1, "2026-09")
	if err != nil || got.Head != current.Head {
		t.Fatal("capacity failure switched current", err)
	}
	if _, err := WithdrawGenerationSelection(ctx, f.receiver, 1, "2026-09", current.Head); err != nil {
		t.Fatal("capacity blocks withdrawal", err)
	}
}

func TestGenerationStagingSubmitRejectsMissingAndWrongScope(t *testing.T) {
	for _, scenario := range []string{"unselected", "withdrawn", "missing_input", "corrupt_input", "foreign_org", "stale_session", "wrong_month"} {
		t.Run(scenario, func(t *testing.T) {
			_, authorization, _ := usageAuthorityFixture(t)
			f := selectionFixture(t)
			ctx := context.Background()
			owner, _, err := LoadUsageGenerationOwner(ctx, authorization, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			f.plan.Owner = owner
			gid, version, month := int64(1), int64(1), "2026-09"
			if scenario != "unselected" {
				selected := stageFixture(t, f, "")
				switch scenario {
				case "withdrawn":
					if _, err := WithdrawGenerationSelection(ctx, f.receiver, 1, month, selected.Head); err != nil {
						t.Fatal(err)
					}
				case "missing_input":
					for _, q := range []string{`DROP TRIGGER monthly_generation_no_delete`, `DELETE FROM portal_monthly_generation_inputs`} {
						if _, err := f.receiver.Exec(q); err != nil {
							t.Fatal(err)
						}
					}
				case "corrupt_input":
					for _, q := range []string{`DROP TRIGGER monthly_generation_no_replace`, `UPDATE portal_monthly_generation_inputs SET preparation='{}'`} {
						if _, err := f.receiver.Exec(q); err != nil {
							t.Fatal(err)
						}
					}
				case "foreign_org":
					gid = 2
				case "stale_session":
					version = 0
				case "wrong_month":
					month = "2026-08"
				}
			}
			out, err := f.g.SubmitUsageMonth(ctx, authorization, gid, version, month)
			if err == nil || out != (monthlytasks.Task{}) {
				t.Fatal("invalid admission returned task", err)
			}
			var count int
			if err := f.receiver.QueryRow(`SELECT COUNT(*) FROM portal_monthly_draft_tasks`).Scan(&count); err != nil || count != 0 {
				t.Fatal("rejected request created task", err)
			}
		})
	}
}

func TestGenerationStagingRechecksChangesDuringAdmission(t *testing.T) {
	for _, scenario := range []string{"withdrawn", "member_revision", "temporarily_unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			authorization, ro, _ := usageAuthorityFixture(t)
			f := selectionFixture(t)
			ctx := context.Background()
			owner, _, err := LoadUsageGenerationOwner(ctx, ro, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			f.plan.Owner = owner
			selected := stageFixture(t, f, "")
			// Inject between the service's initial guard and task admission.
			f.g.verifyCurrent = func(ctx context.Context, _ monthlytasks.Request) error {
				switch scenario {
				case "withdrawn":
					_, err := WithdrawGenerationSelection(ctx, f.receiver, 1, "2026-09", selected.Head)
					return err
				case "member_revision":
					return authorization.Exec(`UPDATE usage_member_controls SET tracked_revision=3 WHERE user_id=201`).Error
				default:
					_, err := f.receiver.Exec(`DROP TABLE portal_monthly_generation_selections`)
					return err
				}
			}
			out, err := f.g.SubmitUsageMonth(ctx, ro, 1, 1, "2026-09")
			if err == nil || out != (monthlytasks.Task{}) {
				t.Fatal("changed scope returned task", err)
			}
			want := monthlytasks.Invalidated
			if scenario == "temporarily_unavailable" {
				want = monthlytasks.Queued
				if errors.Is(err, ErrGenerationChanged) {
					t.Fatal("unavailability treated as revocation")
				}
			}
			var state string
			if err := f.receiver.QueryRow(`SELECT state FROM portal_monthly_draft_tasks`).Scan(&state); err != nil || state != want {
				t.Fatal("wrong task state", state, want, err)
			}
		})
	}
}
