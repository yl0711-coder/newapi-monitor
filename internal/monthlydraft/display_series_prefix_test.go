package monthlydraft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// A bill needs its fixed beginning and predecessors, never later months.
// Missing or broken August/September material must not block a complete July
// bill or consume/reset its recovery budget. Earlier missing material remains
// covered by TestAutomaticDisplaySeriesMissingPredecessorDoesNotSpendPublishRetries.
func TestAutomaticDisplaySeriesEarlierMonthIgnoresUnrelatedLaterSources(t *testing.T) {
	for _, kind := range []string{"withdrawn", "corrupt"} {
		t.Run(kind, func(t *testing.T) {
			f := newDisplaySeriesFixture(t)
			ctx := context.Background()
			before, err := automaticDisplayDependencyFingerprint(ctx, f.g, f.ranges[0], f.requests[0])
			if err != nil {
				t.Fatal(err)
			}
			for i := 1; i < len(f.requests); i++ {
				if kind == "withdrawn" {
					selection, err := ReadGenerationSelection(ctx, f.g.receiver, 1, f.requests[i].Month)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := WithdrawGenerationSelection(ctx, f.g.receiver, 1, f.requests[i].Month, selection.Head); err != nil {
						t.Fatal(err)
					}
				} else {
					input, err := monitor.DecodeMonthlyPreparationInput(f.inputs[i])
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(input.Sources[0].Path, []byte("deliberately broken unrelated future synthetic source"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			after, err := automaticDisplayDependencyFingerprint(ctx, f.g, f.ranges[0], f.requests[0])
			if err != nil || after != before {
				t.Fatal("later source changed an earlier bill dependency snapshot", err)
			}
			shorter := f.ranges[0]
			shorter.Through = "2026-07"
			short, err := automaticDisplayDependencyFingerprint(ctx, f.g, shorter, f.requests[0])
			if err != nil || short != before {
				t.Fatal("extending the allowed end changed the actual July dependencies", err)
			}
			job, err := f.g.Submit(ctx, f.plans[0], f.inputs[0])
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
			if err != nil {
				t.Fatal(err)
			}
			if worked, err := w.step(ctx); err != nil || !worked || w.Status().Published != 1 {
				t.Fatal("later source prevented an otherwise complete earlier bill", worked, err, w.Status())
			}
			displaySeriesCount(t, f, 1)
			var record monitor.PortalMonthlyBill
			if err := f.auth.First(&record, "group_id=1 AND month='2026-07'").Error; err != nil {
				t.Fatal(err)
			}
			var attempts int
			if err := f.g.receiver.QueryRow("SELECT attempts FROM portal_monthly_publication_jobs WHERE task_id=? AND state='saved'", job.ID).Scan(&attempts); err != nil || attempts != 1 {
				t.Fatal("earlier bill spent unnecessary publication attempts", attempts, err)
			}
			files, err := filepath.Glob(filepath.Join(f.g.root.Name(), "display-source-*.json"))
			if err != nil || len(files) != 1 {
				t.Fatal("earlier bill prepared unneeded later source money", len(files), err)
			}
		})
	}
}

func TestAutomaticDisplaySeriesRequiredScopeKeepsUncertainLaterPublication(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	ctx := context.Background()
	publish := displaySeriesPublisher(f.auth)
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, func(ctx context.Context, req monthlytasks.Request, raw []byte, p *monitor.PreparedMonthlyDisplayPublication) error {
		if err := publish(ctx, req, raw, p); err != nil {
			return err
		}
		return ErrGenerationBusy // simulate main-store commit before journal acknowledgement
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := f.g.Submit(ctx, f.plans[2], f.inputs[2])
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(ctx); err != nil || !worked {
		t.Fatal("could not exercise uncertain publication", worked, err)
	}
	displaySeriesCount(t, f, 1)
	var frozen monitor.PortalMonthlyBill
	if err := f.auth.First(&frozen, "group_id=1 AND month='2026-09'").Error; err != nil {
		t.Fatal(err)
	}
	var state string
	if err := f.g.receiver.QueryRow("SELECT state FROM portal_monthly_publication_jobs WHERE task_id=?", job.ID).Scan(&state); err != nil || state == "saved" {
		t.Fatal("fixture did not retain an unacknowledged publication", state, err)
	}
	scope, err := f.g.automaticDisplayRequiredScope(ctx, f.ranges[0], f.requests[0])
	if err != nil || scope.Through != "2026-09" {
		t.Fatal("uncertain later publication was dropped from earlier-month checks", scope, err)
	}
	selection, err := ReadGenerationSelection(ctx, f.g.receiver, 1, "2026-08")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WithdrawGenerationSelection(ctx, f.g.receiver, 1, "2026-08", selection.Head); err != nil {
		t.Fatal(err)
	}
	if _, err := automaticDisplayDependencyFingerprint(ctx, f.g, f.ranges[0], f.requests[0]); !errors.Is(err, ErrGenerationSelectionMissing) {
		t.Fatal("missing bridge behind an issued later bill was ignored", err)
	}
	var after monitor.PortalMonthlyBill
	if err := f.auth.First(&after, "group_id=1 AND month='2026-09'").Error; err != nil || after != frozen {
		t.Fatal("range inspection changed the issued later bill", err)
	}
	// A bad request/owner or a cancelled source inspection never produces a
	// smaller, apparently usable prefix as a fallback.
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	cancel()
	if scope, err := f.g.automaticDisplayRequiredScope(ctx, f.ranges[0], f.requests[0]); err == nil || scope != (AutomaticDisplaySeriesRange{}) {
		t.Fatal("cancelled inspection returned a usable scope", scope, err)
	}
}
