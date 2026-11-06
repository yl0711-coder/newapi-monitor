package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"
)

const walletHistorySummaryLimit = 8 << 20

// Process-local audited month, not a JSON report/approval supplied by callers.
// Source files, current authority and immutable file digests still have to be
// pinned by the composition layer before any historical input can be admitted.
type WalletHistoryMonth struct{ data *walletHistoryMonthData }

func (WalletHistoryMonth) MarshalJSON() ([]byte, error) {
	return nil, errors.New("wallet history months must be rebuilt from original captures")
}

func (o *WalletHistoryMonth) UnmarshalJSON([]byte) error {
	if o != nil {
		*o = WalletHistoryMonth{}
	}
	return errors.New("wallet history month reports are not original evidence")
}

type walletHistoryEndpoint struct {
	id, user int64
	// Funding rows cannot share an ID with a consumption/refund endpoint.
	funding bool
}

type walletHistoryMonthAccount struct {
	UserID                        int64
	ConsumeQuota, RefundQuota     int64
	ConsumeRecords, RefundRecords int64
	FirstActivityAt               int64
	Movements                     []FundingObservedMovement
	Tokens                        []TokenUsageMonthRow
	TokenSHA256                   string
}

type walletHistoryMonthData struct {
	scope        OrganizationMonthScope
	selectionSHA string
	from, to     int64
	accounts     []walletHistoryMonthAccount
	endpoints    []walletHistoryEndpoint
	bytes        int
}

// ReviewCapturedWalletHistoryMonth reads only retained LOCAL originals. It
// requires complete funding, user totals AND matching raw token archives for
// every account. No supplied totals, reconstructed snapshots or report flags
// are accepted. It does not query upstream, migrate, write, select or publish.
func ReviewCapturedWalletHistoryMonth(parent context.Context, db *sql.DB, selection []byte, tokens []CapturedTokenArchive) (WalletHistoryMonth, error) {
	var empty WalletHistoryMonth
	if parent == nil || db == nil {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	checkCtx, checkCancel := context.WithTimeout(ctx, 2*time.Second)
	err := registeredWalletSQLite(checkCtx, db)
	checkCancel()
	if err != nil {
		return empty, err
	}
	now := time.Now()
	s, err := canonicalCapturedUsageIndex(selection, now)
	if err != nil || len(tokens) != len(s.Tokens) || len(s.WalletIDs) != 2 || len(s.Funding) == 0 || len(s.Funding) > 31 {
		return empty, errors.Join(ErrTokenMonthCaptureIntegrity, err)
	}
	month, _ := time.ParseInLocation("2006-01", s.Scope.Funding.Month, fundingMonthZone)
	data := &walletHistoryMonthData{scope: s.Scope, selectionSHA: digest(selection), from: month.Unix(), to: month.AddDate(0, 1, 0).Unix(), accounts: []walletHistoryMonthAccount{}, endpoints: []walletHistoryEndpoint{}}
	// Current wallet observations retain only their collection meaning. They
	// must be intact, but are never relabeled as month opening/closing balances.
	var older WalletObservation
	for i, id := range s.WalletIDs {
		o, err := capturedWalletForScope(ctx, db, id, s.Scope)
		if err != nil {
			return empty, err
		}
		if i == 0 {
			older = o
		} else if id == s.WalletIDs[0] || o.StartedAtMS-older.CompletedAtMS < 300000 {
			return empty, ErrWalletMonthCaptureIntegrity
		}
	}
	windows := make([]FundingWindowObservation, 0, len(s.Funding))
	for _, v := range s.Funding {
		o, err := ReviewFundingWindow(ctx, db, v.OlderScanID, v.NewerScanID)
		if err != nil {
			return empty, err
		}
		windows = append(windows, o)
	}
	funding, err := ReviewFundingMoney(ctx, s.Scope.Funding, windows)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(funding.Windows.Windows, s.Funding) || len(funding.Windows.MissingWindows) != 0 || len(funding.Accounts) != len(s.Tokens) {
		return empty, ErrFundingMonthCaptureIntegrity
	}
	tokenBytes := 0
	for i, selected := range s.Tokens {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		cash := funding.Accounts[i]
		if cash.UserID != selected.UserID || cash.Status != "observed_month" || cash.Totals == nil || tokens[i].UserID != selected.UserID {
			return empty, ErrFundingMonthCaptureIntegrity
		}
		tokenBytes += len(tokens[i].Raw)
		if tokenBytes > tokenMonthCaptureBytes {
			return empty, ErrTokenMonthCaptureBudget
		}
		archive, token, err := DecodeTokenUsageMonthArchive(ctx, tokens[i].Raw, now)
		if err != nil {
			return empty, err
		}
		if archive.SelectionSHA256 != data.selectionSHA || archive.SourceIdentity != s.Scope.Funding.SourceIdentity || archive.Rules != s.Scope.Funding.Rules || archive.Month != s.Scope.Funding.Month || archive.UserID != selected.UserID || !slices.Equal(token.Sources, selected.Parents) || len(archive.Pairs) != len(selected.Usage) {
			return empty, ErrTokenMonthCaptureIntegrity
		}
		// Tie every raw token parent to its ACTUAL stored user-total projection,
		// not just a matching monthly sum from another capture/window.
		for j, pair := range archive.Pairs {
			for phase, o := range []TokenUsageWindowObservation{pair.Older, pair.Newer} {
				projection, err := UsageObservationFromToken(o)
				if err != nil {
					return empty, err
				}
				raw, err := usageWindowBytes(projection)
				if err != nil {
					return empty, err
				}
				want := selected.Usage[j].OlderID
				if phase == 1 {
					want = selected.Usage[j].NewerID
				}
				if digest(raw) != want {
					return empty, ErrTokenMonthCaptureIntegrity
				}
			}
		}
		request := s.Scope.Funding
		request.UserIDs = []int64{selected.UserID}
		usage, err := ReviewUsageMonth(ctx, db, request, selected.Usage)
		if err != nil {
			return empty, err
		}
		report := usage.Report()
		if len(report.Accounts) != 1 || report.From != data.from || report.To != data.to {
			return empty, ErrTokenMonthCaptureIntegrity
		}
		account := report.Accounts[0]
		if account.UserID != selected.UserID || account.Totals == nil || len(account.MissingWindows) != 0 || account.MatchingSeconds != data.to-data.from || account.ObservedSeconds != data.to-data.from {
			return empty, ErrTokenMonthCaptureIntegrity
		}
		if len(account.Totals.Sources) > 1 {
			return empty, ErrTokenMonthCaptureIntegrity
		}
		a := walletHistoryMonthAccount{UserID: selected.UserID, Movements: slices.Clone(cash.Movements), Tokens: slices.Clone(token.Rows), TokenSHA256: digest(tokens[i].Raw)}
		for _, total := range account.Totals.Sources {
			if total.BillingSource != "wallet" {
				return empty, ErrTokenMonthCaptureIntegrity
			}
			a.ConsumeQuota, a.RefundQuota = total.ConsumeQuota, total.RefundQuota
			a.ConsumeRecords, a.RefundRecords = total.ConsumeRecords, total.RefundRecords
		}
		if a.ConsumeQuota != token.ConsumeQuota || a.RefundQuota != token.RefundQuota {
			return empty, ErrTokenMonthCaptureIntegrity
		}
		for _, window := range account.Windows {
			if !window.Matching || len(window.Issues) != 0 {
				return empty, ErrTokenMonthCaptureIntegrity
			}
			for _, row := range window.Rows {
				if row.BillingSource != "wallet" {
					return empty, ErrTokenMonthCaptureIntegrity
				}
				if a.FirstActivityAt == 0 || row.FirstAt < a.FirstActivityAt {
					a.FirstActivityAt = row.FirstAt
				}
				data.endpoints = append(data.endpoints, walletHistoryEndpoint{id: row.MinID, user: a.UserID}, walletHistoryEndpoint{id: row.MaxID, user: a.UserID})
			}
		}
		for _, m := range a.Movements {
			if a.FirstActivityAt == 0 || m.At < a.FirstActivityAt {
				a.FirstActivityAt = m.At
			}
			data.endpoints = append(data.endpoints, walletHistoryEndpoint{id: m.LogID, user: a.UserID, funding: true})
		}
		raw, err := json.Marshal(a)
		if err != nil {
			return empty, err
		}
		data.bytes += len(raw)
		if data.bytes > walletHistorySummaryLimit {
			return empty, ErrTokenMonthCaptureBudget
		}
		data.accounts = append(data.accounts, a)
	}
	data.bytes += len(data.endpoints) * 24
	if data.bytes > walletHistorySummaryLimit {
		return empty, ErrTokenMonthCaptureBudget
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return WalletHistoryMonth{data: data}, nil
}
