package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"slices"
	"sort"
	"time"
)

// A closing point derived from two actual same-view originals. It is not a
// raw historical snapshot and cannot be reconstructed from an editable report.
type WalletHistoryBoundary struct {
	scope    OrganizationMonthScope
	accounts []WalletTailBoundary
}

func (WalletHistoryBoundary) MarshalJSON() ([]byte, error) {
	return nil, errors.New("wallet history boundary must be rebuilt from original tail captures")
}
func (b *WalletHistoryBoundary) UnmarshalJSON([]byte) error {
	if b != nil {
		*b = WalletHistoryBoundary{}
	}
	return errors.New("wallet history boundary reports are not original evidence")
}

func ReviewCapturedWalletHistoryBoundary(ctx context.Context, db *sql.DB, selection []byte) (WalletHistoryBoundary, error) {
	var empty WalletHistoryBoundary
	if ctx == nil || db == nil {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	s, err := canonicalCapturedUsageIndex(selection, time.Now())
	if err != nil {
		return empty, err
	}
	tails, err := ReadSelectedWalletTails(ctx, db, selection)
	if err != nil {
		return empty, err
	}
	out := WalletHistoryBoundary{scope: s.Scope, accounts: make([]WalletTailBoundary, 0, len(tails.Pairs))}
	for i, pair := range tails.Pairs {
		point, err := DeriveWalletTailBoundary(pair.Older, pair.Newer)
		if err != nil || point.UserID() != s.Scope.Funding.UserIDs[i] || point.Boundary() != tails.Boundary {
			return empty, errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		out.accounts = append(out.accounts, point)
	}
	return out, ctx.Err()
}

// Report is diagnostic derived ledger arithmetic, not a statement or a source
// wallet snapshot. Any nonzero registration-month origin residual remains
// explicit for the existing historical-resolution policy, NEVER folded into
// consumption/refunds or silently made an observed opening balance.
type WalletHistoryReport struct {
	SourceIdentity          string
	GroupID                 int64
	FromMonth, ThroughMonth string
	Boundary                int64
	Accounts                []WalletHistoryAccount
}

type WalletHistoryAccount struct {
	UserID, RegisteredAt                int64
	OriginBalanceQuota, OriginUsedQuota int64
	OriginRequiresResolution            bool
	ClosingOriginalSHA256               [2]string
	Months                              []WalletHistoryAccountMonth
}

type WalletHistoryAccountMonth struct {
	Month, SelectionSHA256, TokenSHA256      string
	OpeningBalanceQuota, ClosingBalanceQuota int64
	OpeningUsedQuota, ClosingUsedQuota       int64
	ConsumeQuota, RefundQuota                int64
	ConsumeRecords, RefundRecords            int64
	Movements                                []FundingObservedMovement
	Tokens                                   []TokenUsageMonthRow
}

type WalletHistory struct {
	report *WalletHistoryReport
	scope  OrganizationMonthScope
}

func (WalletHistory) MarshalJSON() ([]byte, error) {
	return nil, errors.New("serialize wallet history Report only; it is not admission evidence")
}
func (h *WalletHistory) UnmarshalJSON([]byte) error {
	if h != nil {
		*h = WalletHistory{}
	}
	return errors.New("wallet histories must be recalculated from audited originals")
}

func (h WalletHistory) Report() WalletHistoryReport {
	if h.report == nil {
		return WalletHistoryReport{}
	}
	r := *h.report
	r.Accounts = slices.Clone(r.Accounts)
	for i := range r.Accounts {
		a := &r.Accounts[i]
		a.Months = slices.Clone(a.Months)
		for j := range a.Months {
			a.Months[j].Movements = slices.Clone(a.Months[j].Movements)
			a.Months[j].Tokens = slices.Clone(a.Months[j].Tokens)
		}
	}
	return r
}

// Identity/range binding only, not permission to publish or a decision about
// origin residuals. Admission must still recheck CURRENT complete authority.
func (h WalletHistory) MatchesCollectionScope(scope OrganizationMonthScope) bool {
	if h.report == nil || scope.Funding.Month < h.report.FromMonth || scope.Funding.Month > h.report.ThroughMonth {
		return false
	}
	_, err := canonicalFundingCaptureScope(scope, time.Now())
	return err == nil && sameWalletHistoryScope(h.scope, scope)
}

// Pure LOCAL composition of source-audited months and a separately audited
// closing point. Requires every month from the earliest actual registration
// month through that boundary. Ordering is canonical; no absent-month zeros,
// cross-account netting, float conversion, or publication permission exists.
func DeriveWalletHistory(parent context.Context, closing WalletHistoryBoundary, months []WalletHistoryMonth) (WalletHistory, error) {
	return deriveWalletHistory(parent, closing, months, false)
}

// Diagnostic source arithmetic ONLY. A customer-beneficial opposite residual
// may produce a negative DERIVED origin counter; keep it explicit instead of
// clamping it or changing logs. Original counters remain nonnegative. The
// ordinary DeriveWalletHistory path stays strict, and this is not permission
// to normalize, select or publish a bill or to claim a historical incident date.
func ReviewWalletHistoryOriginResidual(parent context.Context, closing WalletHistoryBoundary, months []WalletHistoryMonth) (WalletHistory, error) {
	return deriveWalletHistory(parent, closing, months, true)
}

func deriveWalletHistory(parent context.Context, closing WalletHistoryBoundary, months []WalletHistoryMonth, residualReview bool) (WalletHistory, error) {
	var empty WalletHistory
	if parent == nil || len(closing.accounts) == 0 || len(closing.accounts) != len(closing.scope.Funding.UserIDs) || len(months) == 0 || len(months) > 120 {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	if err := parent.Err(); err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if _, err := canonicalFundingCaptureScope(closing.scope, time.Now()); err != nil {
		return empty, err
	}
	ordered := slices.Clone(months)
	for _, m := range ordered {
		if m.data == nil {
			return empty, ErrWalletMonthCaptureIntegrity
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].data.from < ordered[j].data.from })
	boundary, first := closing.accounts[0].Boundary(), int64(0)
	for i, point := range closing.accounts {
		if point.UserID() != closing.scope.Funding.UserIDs[i] || point.SourceIdentity() != closing.scope.Funding.SourceIdentity || point.Boundary() != boundary || point.RegisteredAt() <= 0 || point.UsedQuota() < 0 {
			return empty, ErrWalletMonthCaptureIntegrity
		}
		at := walletTailMonthStart(point.RegisteredAt() * 1000)
		if first == 0 || at < first {
			first = at
		}
	}
	cursor, usedBytes := first, 0
	seen := map[int64]struct {
		user, from int64
		funding    bool
	}{}
	for _, month := range ordered {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		d := month.data
		if d.from != cursor || d.to <= d.from || d.to > boundary || !sameWalletHistoryScope(d.scope, closing.scope) || len(d.accounts) != len(closing.accounts) {
			return empty, ErrWalletMonthCaptureIntegrity
		}
		usedBytes += d.bytes
		if usedBytes > walletHistorySummaryLimit {
			return empty, ErrTokenMonthCaptureBudget
		}
		for _, endpoint := range d.endpoints {
			if old, ok := seen[endpoint.id]; ok && (old.user != endpoint.user || old.from != d.from || old.funding || endpoint.funding) {
				return empty, ErrWalletMonthCaptureIntegrity
			}
			seen[endpoint.id] = struct {
				user, from int64
				funding    bool
			}{endpoint.user, d.from, endpoint.funding}
		}
		cursor = d.to
	}
	if cursor != boundary {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	r := WalletHistoryReport{SourceIdentity: closing.scope.Funding.SourceIdentity, GroupID: closing.scope.GroupID, FromMonth: ordered[0].data.scope.Funding.Month, ThroughMonth: ordered[len(ordered)-1].data.scope.Funding.Month, Boundary: boundary, Accounts: []WalletHistoryAccount{}}
	for index, point := range closing.accounts {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		originalsA, originalsB := point.OriginalHashes()
		a := WalletHistoryAccount{UserID: point.UserID(), RegisteredAt: point.RegisteredAt(), ClosingOriginalSHA256: [2]string{originalsA, originalsB}, Months: make([]WalletHistoryAccountMonth, len(ordered))}
		balance, used := point.BalanceQuota(), point.UsedQuota()
		negativeDerived := false
		for i := len(ordered) - 1; i >= 0; i-- {
			d := ordered[i].data
			row := d.accounts[index]
			if row.UserID != a.UserID || row.FirstActivityAt != 0 && row.FirstActivityAt < a.RegisteredAt {
				return empty, ErrWalletMonthCaptureIntegrity
			}
			var opBalance, opUsed int64
			var err error
			if residualReview {
				opBalance, opUsed, err = walletHistoryReverseForReview(balance, used, row, true)
			} else {
				opBalance, opUsed, err = walletHistoryReverse(balance, used, row)
			}
			if err != nil {
				return empty, err
			}
			negativeDerived = negativeDerived || opUsed < 0
			// A later-created member cannot inherit another account's origin.
			// Even zero-price observed requests count as pre-registration activity.
			zeroOrigin := opBalance == 0 && opUsed == 0 && balance == 0 && used == 0
			residualOnly := residualReview && oppositeWalletHistoryResidual(opBalance, opUsed) && opBalance == balance && opUsed == used
			if d.to <= a.RegisteredAt && (row.FirstActivityAt != 0 || !zeroOrigin && !residualOnly) {
				return empty, ErrWalletMonthCaptureIntegrity
			}
			a.Months[i] = WalletHistoryAccountMonth{Month: d.scope.Funding.Month, SelectionSHA256: d.selectionSHA, TokenSHA256: row.TokenSHA256, OpeningBalanceQuota: opBalance, ClosingBalanceQuota: balance, OpeningUsedQuota: opUsed, ClosingUsedQuota: used, ConsumeQuota: row.ConsumeQuota, RefundQuota: row.RefundQuota, ConsumeRecords: row.ConsumeRecords, RefundRecords: row.RefundRecords, Movements: slices.Clone(row.Movements), Tokens: slices.Clone(row.Tokens)}
			balance, used = opBalance, opUsed
		}
		a.OriginBalanceQuota, a.OriginUsedQuota = balance, used
		a.OriginRequiresResolution = balance != 0 || used != 0
		if negativeDerived && (!residualReview || !oppositeWalletHistoryResidual(balance, used)) {
			return empty, ErrWalletMonthCaptureIntegrity
		}
		// Re-run forward arithmetic and every adjacent balance/counter edge.
		// This is an arithmetic check, not a second independent source proof.
		for i, row := range a.Months {
			if row.OpeningBalanceQuota != balance || row.OpeningUsedQuota != used {
				return empty, ErrWalletMonthCaptureIntegrity
			}
			var closingBalance, closingUsed int64
			var err error
			if residualReview {
				closingBalance, closingUsed, err = walletHistoryForwardForReview(balance, used, ordered[i].data.accounts[index], true)
			} else {
				closingBalance, closingUsed, err = walletHistoryForward(balance, used, ordered[i].data.accounts[index])
			}
			if err != nil || closingBalance != row.ClosingBalanceQuota || closingUsed != row.ClosingUsedQuota {
				return empty, errors.Join(ErrWalletMonthCaptureIntegrity, err)
			}
			balance, used = closingBalance, closingUsed
		}
		if balance != point.BalanceQuota() || used != point.UsedQuota() {
			return empty, ErrWalletMonthCaptureIntegrity
		}
		r.Accounts = append(r.Accounts, a)
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return WalletHistory{report: &r, scope: closing.scope}, nil
}

func sameWalletHistoryScope(a, b OrganizationMonthScope) bool {
	a.Funding.Month, b.Funding.Month = "", ""
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func walletHistoryReverse(balance, used int64, row walletHistoryMonthAccount) (int64, int64, error) {
	return walletHistoryReverseForReview(balance, used, row, false)
}

func walletHistoryReverseForReview(balance, used int64, row walletHistoryMonthAccount, residualReview bool) (int64, int64, error) {
	b, u := big.NewInt(balance), big.NewInt(used)
	for _, m := range row.Movements {
		b.Sub(b, big.NewInt(m.DeltaQuota))
	}
	b.Add(b, big.NewInt(row.ConsumeQuota))
	b.Sub(b, big.NewInt(row.RefundQuota))
	u.Sub(u, big.NewInt(row.ConsumeQuota))
	u.Add(u, big.NewInt(row.RefundQuota))
	return walletHistoryExactForReview(b, u, residualReview)
}

func walletHistoryForward(balance, used int64, row walletHistoryMonthAccount) (int64, int64, error) {
	return walletHistoryForwardForReview(balance, used, row, false)
}

func walletHistoryForwardForReview(balance, used int64, row walletHistoryMonthAccount, residualReview bool) (int64, int64, error) {
	b, u := big.NewInt(balance), big.NewInt(used)
	for _, m := range row.Movements {
		b.Add(b, big.NewInt(m.DeltaQuota))
	}
	b.Sub(b, big.NewInt(row.ConsumeQuota))
	b.Add(b, big.NewInt(row.RefundQuota))
	u.Add(u, big.NewInt(row.ConsumeQuota))
	u.Sub(u, big.NewInt(row.RefundQuota))
	return walletHistoryExactForReview(b, u, residualReview)
}

func walletHistoryExact(balance, used *big.Int) (int64, int64, error) {
	if !balance.IsInt64() || !used.IsInt64() || used.Sign() < 0 {
		return 0, 0, ErrWalletMonthCaptureIntegrity
	}
	return balance.Int64(), used.Int64(), nil
}

func walletHistoryExactForReview(balance, used *big.Int, residualReview bool) (int64, int64, error) {
	if !residualReview {
		return walletHistoryExact(balance, used)
	}
	if !balance.IsInt64() || !used.IsInt64() {
		return 0, 0, ErrWalletMonthCaptureIntegrity
	}
	return balance.Int64(), used.Int64(), nil
}

func oppositeWalletHistoryResidual(balance, used int64) bool {
	return balance > 0 && used < 0 && new(big.Int).Add(big.NewInt(balance), big.NewInt(used)).Sign() == 0
}
