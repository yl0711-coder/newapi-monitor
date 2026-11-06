// Package monthlydraft connects local audited funding observations to the
// existing pure reconciliation calculator. It is not wired into a production
// entrypoint, HTTP route, scheduler, source connection or publication workflow.
package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type Review struct {
	Mode           string                           `json:"mode"`
	Funding        monthlysource.FundingMoneyReview `json:"funding"`
	Reconciliation monitor.MonthlyPreparationReport `json:"reconciliation"`
}

var draftSlots = make(chan struct{}, 1)
var ErrDraftBusy = errors.New("observed monthly draft reconciliation already running")

// ReconcileObservedMonth adds an exact per-row funding check, never replaces
// independent movement coverage, balances, usage or actual refund evidence.
// Sources referenced by raw still need file integrity/semantic verification:
// the pure calculator's "not_checked_by_pure_calculator" warning is preserved.
// This returns internal drafts only, not bills a customer can publish/download.
func ReconcileObservedMonth(ctx context.Context, request monthlysource.FundingMonthRequest, windows []monthlysource.FundingWindowObservation, raw []byte, now time.Time) (Review, error) {
	if err := ctx.Err(); err != nil {
		return Review{}, err
	}
	select {
	case draftSlots <- struct{}{}:
		defer func() { <-draftSlots }()
	default:
		return Review{}, ErrDraftBusy
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return Review{}, err
	}
	// The adapter deliberately accepts one month and the exact account scope,
	// not unrelated historical periods or extra organization members.
	if len(input.Accounts) != len(request.UserIDs) {
		return Review{}, errors.New("draft accounts differ from observed account scope")
	}
	accounts := make(map[int64]monitor.MonthlyPreparationAccount, len(input.Accounts))
	for _, a := range input.Accounts {
		if len(a.Months) != 1 || a.Months[0] != request.Month {
			return Review{}, errors.New("draft must select exactly the observed month")
		}
		accounts[a.UserID] = a
	}
	for _, id := range request.UserIDs {
		if _, ok := accounts[id]; !ok {
			return Review{}, errors.New("draft contains foreign or missing account")
		}
	}
	funding, err := monthlysource.ReviewFundingMoney(ctx, request, windows)
	if err != nil {
		return Review{}, err
	}
	blocked := make(map[int64]string)
	for _, observed := range funding.Accounts {
		if observed.Status != "observed_month" {
			blocked[observed.UserID] = "funding_" + observed.Status
			continue
		}
		if !sameMovements(accounts[observed.UserID], observed, funding.Windows.From, funding.Windows.To) {
			blocked[observed.UserID] = "funding_movements_mismatch"
		}
	}
	if err := ctx.Err(); err != nil {
		return Review{}, err
	}
	report, err := monitor.PrepareMonthlyReconciliation(raw, now)
	if err != nil {
		return Review{}, err
	}
	for i, a := range report.Accounts {
		if reason := blocked[a.UserID]; reason != "" {
			// Do not retain a calculated amount/anchor on a failed funding check.
			// Other accounts remain independently eligible for their own checks.
			report.Accounts[i] = monitor.MonthlyPreparationAccountReport{UserID: a.UserID, GroupID: a.GroupID, Username: a.Username,
				Error:  "资金逐笔核对未通过；未生成金额候选",
				Months: []monitor.MonthlyPreparationRow{{Month: request.Month, Status: reason, Reason: "资金观察范围不完整、仍有待核验项或与制单流水不同；未补零或调平", Sources: []string{}}},
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return Review{}, err
	}
	return Review{Mode: "local_observed_month_draft_not_publication", Funding: funding, Reconciliation: report}, nil
}

func sameMovements(candidate monitor.MonthlyPreparationAccount, observed monthlysource.FundingMoneyAccount, from, to int64) bool {
	want := make(map[int64]monthlysource.FundingObservedMovement, len(observed.Movements))
	for _, m := range observed.Movements {
		want[m.LogID] = m
	}
	for _, m := range candidate.Movements {
		if m.At < from || m.At >= to {
			continue // prior movements may be required by the independent baseline
		}
		x, ok := want[m.ID]
		if !ok || m.UserID != observed.UserID || m.At != x.At || m.DeltaQuota != x.DeltaQuota {
			return false
		}
		delete(want, m.ID) // duplicates cannot match twice
	}
	return len(want) == 0
}
