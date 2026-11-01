package monthlysource

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"time"
)

var fundingMoneySlots = make(chan struct{}, 1)
var ErrFundingMoneyBusy = errors.New("monthly funding money review already running")

// These are internal observation reports, NOT financial completeness evidence,
// wallet-write confirmations, publication permission, or customer-facing DTOs.
type FundingMoneyReview struct {
	Mode     string                `json:"mode"`
	Windows  FundingMonthReview    `json:"windows"`
	Accounts []FundingMoneyAccount `json:"accounts"`
}

type FundingMoneyAccount struct {
	UserID    int64                     `json:"user_id"`
	Status    string                    `json:"status"`
	Totals    *FundingMoneyTotals       `json:"totals,omitempty"`
	Movements []FundingObservedMovement `json:"movements,omitempty"`
}

type FundingMoneyTotals struct {
	IncreaseQuota int64 `json:"increase_quota"`
	DecreaseQuota int64 `json:"decrease_quota"` // signed; never absolute/rounded
}

type FundingObservedMovement struct {
	LogID        int64  `json:"log_id"`
	At           int64  `json:"at"`
	DeltaQuota   int64  `json:"delta_quota"`
	WindowIndex  int    `json:"window_index"`
	PageID       string `json:"page_id"`
	SourceDigest string `json:"source_digest"`
	Kind         string `json:"kind"`
	Method       string `json:"method"`
}

// ReviewFundingMoney recomputes selection from opaque audited observations,
// never from editable report flags. Only wholly selected account-months expose
// observed totals; incomplete months do not turn missing data into zero. Even
// an empty stable month is NOT proof that no unlogged wallet changes occurred.
// No DB access, writes, automatic coverage assertion or bill creation occurs.
func ReviewFundingMoney(parent context.Context, request FundingMonthRequest, windows []FundingWindowObservation) (FundingMoneyReview, error) {
	if err := parent.Err(); err != nil {
		return FundingMoneyReview{}, err
	}
	select {
	case fundingMoneySlots <- struct{}{}:
		defer func() { <-fundingMoneySlots }()
	default:
		return FundingMoneyReview{}, ErrFundingMoneyBusy
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	month, err := ReviewFundingMonth(ctx, request, windows)
	if err != nil {
		return FundingMoneyReview{}, err
	}
	// Month validation already bounded and checked these immutable objects.
	byScan := make(map[string]*fundingWindowObservationData, len(windows))
	for _, w := range windows {
		byScan[w.data.scan.ID] = w.data
	}
	r := FundingMoneyReview{Mode: "observed_funding_not_wallet_proof", Windows: month, Accounts: make([]FundingMoneyAccount, len(month.Accounts))}
	byUser := make(map[int64]int, len(month.Accounts))
	for i, account := range month.Accounts {
		status := "incomplete_observation"
		if account.SelectedSeconds == month.To-month.From && len(month.MissingWindows) == 0 {
			status = "observed_month"
		}
		r.Accounts[i] = FundingMoneyAccount{UserID: account.UserID, Status: status}
		byUser[account.UserID] = i
	}
	// Walk source records once, not once per account. Preserve per-row evidence
	// identity and both sides of transfers; do not net across customers.
	for wi, version := range month.Windows {
		w := byScan[version.NewerScanID]
		for id, record := range w.newerRecords {
			if err := ctx.Err(); err != nil {
				return FundingMoneyReview{}, err
			}
			i, scoped := byUser[record.target]
			if !scoped || r.Accounts[i].Status != "observed_month" || record.status != FundingMovement {
				continue
			}
			if !record.targetKnown || !record.hasDelta || record.delta == 0 || record.at < version.From || record.at >= version.To || !validDigest(record.pageID) || !validDigest(record.digest) {
				return FundingMoneyReview{}, errors.New("audited movement missing exact amount, scope or source identity")
			}
			r.Accounts[i].Movements = append(r.Accounts[i].Movements, FundingObservedMovement{LogID: id, At: record.at, DeltaQuota: record.delta, WindowIndex: wi, PageID: record.pageID, SourceDigest: record.digest, Kind: record.kind, Method: record.method})
		}
	}
	for i := range r.Accounts {
		if err := ctx.Err(); err != nil {
			return FundingMoneyReview{}, err
		}
		a := &r.Accounts[i]
		if a.Status != "observed_month" {
			continue
		}
		sort.Slice(a.Movements, func(i, j int) bool {
			if a.Movements[i].At != a.Movements[j].At {
				return a.Movements[i].At < a.Movements[j].At
			}
			return a.Movements[i].LogID < a.Movements[j].LogID
		})
		increase, decrease := new(big.Int), new(big.Int)
		for _, m := range a.Movements {
			total := increase
			if m.DeltaQuota < 0 {
				total = decrease
			}
			total.Add(total, big.NewInt(m.DeltaQuota))
		}
		if !increase.IsInt64() || !decrease.IsInt64() {
			a.Status, a.Movements = "amount_out_of_range", nil
			continue
		}
		a.Totals = &FundingMoneyTotals{IncreaseQuota: increase.Int64(), DecreaseQuota: decrease.Int64()}
	}
	if err := ctx.Err(); err != nil {
		return FundingMoneyReview{}, err
	}
	return r, nil
}
