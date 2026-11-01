package monthlysource

// Local retained-original review, not a current source snapshot, a complete
// wallet tail, a month-boundary balance, or permission to publish a statement.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"time"
)

var ErrWalletTailPrefixReview = errors.New("retained wallet tail prefix requires review")

type WalletTailPrefixReport struct {
	Status               string               `json:"status"`
	SourceIdentity       string               `json:"source_identity"`
	UserID               int64                `json:"user_id"`
	Kind                 string               `json:"kind"`
	Boundary             int64                `json:"boundary"`
	RegisteredAt         int64                `json:"registered_at"`
	StablePages          int                  `json:"stable_pages"`
	After                WalletTailPageCursor `json:"after"`
	ConsumeQuota         int64                `json:"consume_quota"`
	RefundQuota          int64                `json:"refund_quota"`
	FundingDeltaQuota    int64                `json:"funding_delta_quota"`
	ConsumeRecords       int64                `json:"consume_records"`
	RefundRecords        int64                `json:"refund_records"`
	FundingRecords       int64                `json:"funding_records"`
	OlderTraversalSHA256 string               `json:"older_traversal_sha256"`
	NewerTraversalSHA256 string               `json:"newer_traversal_sha256"`
}

// The report is diagnostic. An editable report cannot be decoded into the
// source-owned review, and the review cannot be passed to old wallet gates.
type WalletTailPrefixReview struct {
	report *WalletTailPrefixReport
	series walletTailPageSeries
}

func (WalletTailPrefixReview) MarshalJSON() ([]byte, error) { return nil, ErrWalletTailPrefixReview }
func (p *WalletTailPrefixReview) UnmarshalJSON([]byte) error {
	if p != nil {
		*p = WalletTailPrefixReview{}
	}
	return ErrWalletTailPrefixReview
}
func (p WalletTailPrefixReview) Report() WalletTailPrefixReport {
	if p.report == nil {
		return WalletTailPrefixReport{}
	}
	return *p.report
}

// Identity binding only. Current authorization and final source consistency
// still have to be checked by any future generation consumer.
func (p WalletTailPrefixReview) MatchesCaptureScope(key string, scope OrganizationMonthScope) bool {
	if p.report == nil || p.series.JobKey != key || p.series.ScopeSHA != registeredCaptureScopeSHA(scope) {
		return false
	}
	_, err := canonicalFundingCaptureScope(scope, time.Now())
	return err == nil
}

type tailPrefixAmounts struct {
	consume, refund, funding    *big.Int
	consumeN, refundN, fundingN int64
	resolved                    bool
}

type tailPrefixPage struct {
	index       WalletTailPageIndex
	semanticSHA string
	end         bool
	amounts     tailPrefixAmounts
}

type auditedTailTraversal struct {
	pages                       []tailPrefixPage
	firstMS, lastMS, registered int64
	sha                         string
}

func tailPrefixPageAmounts(o WalletTailPageObservation) tailPrefixAmounts {
	a := tailPrefixAmounts{consume: new(big.Int), refund: new(big.Int), funding: new(big.Int), resolved: true}
	for _, row := range o.Usage {
		if row.BillingSource != "wallet" {
			a.resolved = false
		}
		if row.Type == 2 {
			a.consume.Add(a.consume, big.NewInt(row.Quota))
			a.consumeN += row.Records
		} else {
			a.refund.Add(a.refund, big.NewInt(row.Quota))
			a.refundN += row.Records
		}
	}
	for _, row := range o.Funding {
		m := NormalizeFundingLog(row, o.Request.Wallet.Rules)
		switch m.Status {
		case FundingMovement:
			if m.TargetUserID == o.Request.Wallet.UserID {
				if m.DeltaQuota == nil || row.CreatedAt < o.User.RegisteredAt {
					a.resolved = false
				} else {
					a.funding.Add(a.funding, big.NewInt(*m.DeltaQuota))
					a.fundingN++
				}
			}
		case FundingNonWallet:
		case FundingReview:
			if m.TargetUserID == 0 || m.TargetUserID == o.Request.Wallet.UserID {
				a.resolved = false
			}
		default:
			a.resolved = false
		}
	}
	return a
}

// Audit ALL rows, not only first/last recovery checkpoints. Keep compact page
// fingerprints/amounts, not both passes' complete funding bodies in memory.
func auditTailPageTraversal(ctx context.Context, tx *sql.Tx, s walletTailPageSeries, now time.Time) (auditedTailTraversal, error) {
	var empty auditedTailTraversal
	rows, err := tx.QueryContext(ctx, `SELECT seq,id,previous_id,started_at_ms,completed_at_ms,
CASE WHEN typeof(payload)='blob' AND length(payload)<=2097152 THEN payload END
FROM portal_monthly_tail_page_journal WHERE series=? ORDER BY seq LIMIT 4097`, s.id())
	if err != nil {
		return empty, err
	}
	defer rows.Close()
	out := auditedTailTraversal{}
	previous, after, size := "", WalletTailPageCursor{}, 0
	seenFunding := map[int64]bool{}
	h := sha256.New()
	enc := json.NewEncoder(h)
	ended := false
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		var seq int
		var id, parent string
		var started, completed int64
		var raw []byte
		if err := rows.Scan(&seq, &id, &parent, &started, &completed, &raw); err != nil {
			return empty, err
		}
		if ended || seq != len(out.pages) || seq >= walletTailJournalPages || parent != previous || !validDigest(id) || digest(raw) != id ||
			completed > now.UnixMilli() || len(raw) > walletTailBytesLimit || size+len(raw) > walletTailStoreBytesLimit {
			return empty, ErrWalletMonthCaptureIntegrity
		}
		var o WalletTailPageObservation
		if json.Unmarshal(raw, &o) != nil {
			return empty, ErrWalletMonthCaptureIntegrity
		}
		canonical, err := walletTailPageBytes(o)
		r := o.Request
		r.After = WalletTailPageCursor{}
		if err != nil || !bytes.Equal(canonical, raw) || r != s.Request || o.Request.After != after || o.StartedAtMS != started || o.CompletedAtMS != completed ||
			seq > 0 && (o.User.RegisteredAt != out.registered || started < out.lastMS) {
			return empty, errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		if seq == 0 {
			out.firstMS, out.registered = started, o.User.RegisteredAt
		}
		if completed-out.firstMS > (24 * time.Hour).Milliseconds() {
			return empty, ErrWalletTailCaptureCapacity
		}
		for _, k := range o.FundingKeys {
			if seenFunding[k.ID] {
				return empty, ErrWalletMonthCaptureIntegrity
			}
			seenFunding[k.ID] = true
		}
		// Wallet values and physical capture times may legitimately move between
		// passes. Compare actual page metadata and log content, not those values.
		semantic, err := json.Marshal(struct {
			Index   WalletTailPageIndex
			Usage   []UsageWindowRow
			Funding []FundingLog
			Keys    []WalletTailPageCursor
			Other   int
		}{o.Index, o.Usage, o.Funding, o.FundingKeys, o.OtherUserFundingRecords})
		if err != nil {
			return empty, err
		}
		out.pages = append(out.pages, tailPrefixPage{index: o.Index, semanticSHA: digest(semantic), end: o.ViewEndReached, amounts: tailPrefixPageAmounts(o)})
		if err := enc.Encode([]any{seq, id, parent}); err != nil {
			return empty, err
		}
		out.lastMS, previous, after, size, ended = completed, id, o.Index.Next, size+len(raw), o.ViewEndReached
	}
	if err := rows.Err(); err != nil {
		return empty, err
	}
	if err := rows.Close(); err != nil {
		return empty, err
	}
	if len(out.pages) == 0 || !ended {
		return empty, ErrWalletCaptureBusy
	}
	out.sha = hex.EncodeToString(h.Sum(nil))
	return out, nil
}

// ReviewWalletTailPagePrefix only reads a previously initialized private
// receiver. It reads NO production/source connection and writes no database.
// Boundary is the ORIGINAL Beijing month boundary, not a retimed observation.
func ReviewWalletTailPagePrefix(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, userID int64, kind string, boundary int64) (WalletTailPrefixReview, error) {
	return reviewWalletTailPagePrefix(parent, receiver, key, scope, userID, kind, boundary, time.Now)
}

func reviewWalletTailPagePrefix(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, userID int64, kind string, boundary int64, clock func() time.Time) (WalletTailPrefixReview, error) {
	var empty WalletTailPrefixReview
	if parent == nil || receiver == nil || clock == nil || !fundingScopeHas(scope.Funding.UserIDs, userID) || (kind != "user_usage" && kind != "global_funding") {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	now := clock()
	month, err := time.ParseInLocation("2006-01", scope.Funding.Month, fundingMonthZone)
	if err != nil || boundary <= 0 || boundary < month.AddDate(0, 1, 0).Unix() || boundary > walletTailMonthStart(now.UnixMilli()) || walletTailMonthStart(boundary*1000) != boundary {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	if err := validateTailPageJournal(ctx, receiver); err != nil {
		return empty, err
	}
	tx, err := receiver.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	plan, err := loadFundingMonthCapturePlan(ctx, tx, key, scope, now)
	if err != nil {
		return empty, err
	}
	var records int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(length(payload)),0) FROM portal_monthly_tail_page_journal`).Scan(&records, &size); err != nil {
		return empty, err
	}
	if records > walletTailJournalPages || size < 0 || size > walletTailStoreBytesLimit {
		return empty, ErrWalletTailCaptureCapacity
	}
	s := walletTailPageSeries{JobKey: key, PlanSHA: registeredCapturePlanSHA(plan), ScopeSHA: registeredCaptureScopeSHA(scope),
		Request: WalletTailPageRequest{Wallet: WalletTailRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: userID, Boundary: boundary}, Kind: kind}}
	a, err := auditTailPageTraversal(ctx, tx, s, now)
	if err != nil {
		return empty, err
	}
	s.Pass = 1
	b, err := auditTailPageTraversal(ctx, tx, s, now)
	if err != nil {
		return empty, err
	}
	if a.registered != b.registered || b.firstMS-a.lastMS < (5*time.Minute).Milliseconds() {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	r := &WalletTailPrefixReport{Status: "no_closed_prefix", SourceIdentity: s.Request.Wallet.SourceIdentity, UserID: userID, Kind: kind, Boundary: boundary,
		RegisteredAt: a.registered, OlderTraversalSHA256: a.sha, NewerTraversalSHA256: b.sha}
	var consume, refund, funds big.Int
	for i, p := range a.pages {
		// The previous pass's terminal page is still a moving suffix, even if
		// the second pass happened to return the same IDs. Always re-read it.
		if p.end {
			break
		}
		if i >= len(b.pages) || b.pages[i].end || p.semanticSHA != b.pages[i].semanticSHA {
			r.Status = "changed_prefix"
			break
		}
		if !p.amounts.resolved || !b.pages[i].amounts.resolved {
			return empty, ErrWalletTailPrefixReview
		}
		r.StablePages++
		r.After = p.index.Next
		r.ConsumeRecords += p.amounts.consumeN
		r.RefundRecords += p.amounts.refundN
		r.FundingRecords += p.amounts.fundingN
		consume.Add(&consume, p.amounts.consume)
		refund.Add(&refund, p.amounts.refund)
		funds.Add(&funds, p.amounts.funding)
		if r.Status != "changed_prefix" {
			r.Status = "stable_prefix"
		}
	}
	if !consume.IsInt64() || !refund.IsInt64() || !funds.IsInt64() {
		return empty, ErrWalletTailPrefixReview
	}
	r.ConsumeQuota, r.RefundQuota, r.FundingDeltaQuota = consume.Int64(), refund.Int64(), funds.Int64()
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return WalletTailPrefixReview{report: r, series: s}, nil
}
