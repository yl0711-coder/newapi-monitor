package monthlysource

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

// This is an eligibility check only. It does not create a new capture, attest
// historical cutoffs, combine different accounts' reads or approve a bill.
const TokenWalletReuseMode = "original_token_wallet_views_not_capture_or_bill_approval"
const tokenWalletReuseGapMS int64 = 300000

var ErrTokenWalletReuseIneligible = errors.New("token wallet view does not satisfy independent wallet observation constraints")

type TokenWalletReuseView struct {
	ParentSHA256  string `json:"parent_sha256"`
	WalletSHA256  string `json:"wallet_sha256"`
	StartedAtMS   int64  `json:"started_at_ms"`
	CompletedAtMS int64  `json:"completed_at_ms"`
}

type TokenWalletReuseRejection struct {
	ParentSHA256 string `json:"parent_sha256"`
	Reason       string `json:"reason"`
}

type TokenWalletReuseReport struct {
	Version        int                         `json:"version"`
	Mode           string                      `json:"mode"`
	Status         string                      `json:"status"`
	ArchiveSHA256  string                      `json:"archive_sha256"`
	SourceIdentity string                      `json:"source_identity"`
	UserID         int64                       `json:"user_id"`
	Month          string                      `json:"month"`
	Observations   int                         `json:"observations"`
	Eligible       int                         `json:"eligible"`
	MinimumGapMS   int64                       `json:"minimum_gap_ms"`
	Selected       []TokenWalletReuseView      `json:"selected"`
	Rejected       []TokenWalletReuseRejection `json:"rejected"`
}

// WalletObservationFromToken preserves the original transaction's identity,
// complete timing interval and exact wallet integers. Never move timestamps or
// extend the EXISTING wallet timeout to make a slow token capture fit. Parent
// digest must remain bound to the original archive in any later consumer.
func WalletObservationFromToken(parent TokenUsageWindowObservation, expectedParentSHA256 string) (WalletObservation, error) {
	var empty WalletObservation
	if !validDigest(expectedParentSHA256) || ValidateTokenUsageWindowObservation(parent) != nil {
		return empty, ErrTokenWalletReuseIneligible
	}
	raw, err := json.Marshal(parent)
	if err != nil || digest(raw) != expectedParentSHA256 {
		return empty, ErrTokenWalletReuseIneligible
	}
	u := parent.Wallet
	if !u.Exists || u.UserID != parent.Request.UserID || u.BalanceQuota == nil || u.UsedQuota == nil {
		return empty, ErrTokenWalletReuseIneligible
	}
	// Detach caller-owned pointer values. Negative balances are valid; a missing
	// balance or counter is never manufactured as zero.
	balance, used := *u.BalanceQuota, *u.UsedQuota
	u.BalanceQuota, u.UsedQuota = &balance, &used
	out := WalletObservation{Version: 1, Basis: walletObservationBasis, SourceIdentity: parent.Request.SourceIdentity,
		StartedAtMS: parent.StartedAtMS, CompletedAtMS: parent.CompletedAtMS, Users: []WalletUser{u}}
	if ValidateWalletObservation(out) != nil {
		return empty, ErrTokenWalletReuseIneligible
	}
	return out, nil
}

// ReviewTokenWalletReuse re-audits the FULL original token-month archive before
// selecting two candidate views. No JSON "ready" flag replaces original data.
// Reports omit balances, names, tokens and credentials. A ready result is only
// about source reuse; it is not an input selection or financial authorization.
func ReviewTokenWalletReuse(parent context.Context, raw []byte, expectedSHA256 string, now time.Time) (TokenWalletReuseReport, error) {
	var empty TokenWalletReuseReport
	if parent == nil || !validDigest(expectedSHA256) || len(raw) == 0 || len(raw) > TokenUsageArchiveLimit {
		return empty, errors.New("invalid token wallet reuse scope")
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if digest(raw) != expectedSHA256 {
		return empty, errors.New("token wallet reuse archive digest differs")
	}
	archive, _, err := DecodeTokenUsageMonthArchive(ctx, raw, now)
	if err != nil {
		return empty, err
	}
	report := TokenWalletReuseReport{Version: 1, Mode: TokenWalletReuseMode, Status: "insufficient_eligible_views", ArchiveSHA256: expectedSHA256,
		SourceIdentity: archive.SourceIdentity, UserID: archive.UserID, Month: archive.Month, Observations: 2 * len(archive.Pairs), MinimumGapMS: tokenWalletReuseGapMS,
		Selected: []TokenWalletReuseView{}, Rejected: []TokenWalletReuseRejection{}}
	candidates := []TokenWalletReuseView{}
	seenParent := map[string]bool{}
	for _, pair := range archive.Pairs {
		for _, p := range []TokenUsageWindowObservation{pair.Older, pair.Newer} {
			if err := ctx.Err(); err != nil {
				return empty, err
			}
			body, err := json.Marshal(p)
			if err != nil {
				return empty, err
			}
			id := digest(body)
			if seenParent[id] {
				return empty, errors.New("duplicate token capture cannot supply two wallet views")
			}
			seenParent[id] = true
			wallet, err := WalletObservationFromToken(p, id)
			reason := ""
			switch {
			case p.CompletedAtMS-p.StartedAtMS > walletCaptureTimeout.Milliseconds():
				reason = "exceeds_existing_wallet_capture_budget"
			case err != nil:
				reason = "wallet_values_unavailable"
			}
			if reason != "" {
				report.Rejected = append(report.Rejected, TokenWalletReuseRejection{ParentSHA256: id, Reason: reason})
				continue
			}
			walletRaw, err := walletObservationBytes(wallet)
			if err != nil {
				return empty, err
			}
			wid := digest(walletRaw)
			candidates = append(candidates, TokenWalletReuseView{ParentSHA256: id, WalletSHA256: wid, StartedAtMS: p.StartedAtMS, CompletedAtMS: p.CompletedAtMS})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.CompletedAtMS != b.CompletedAtMS {
			return a.CompletedAtMS < b.CompletedAtMS
		}
		if a.StartedAtMS != b.StartedAtMS {
			return a.StartedAtMS < b.StartedAtMS
		}
		return a.ParentSHA256 < b.ParentSHA256
	})
	// Choose one stable original parent for an identical wallet view even when
	// archive pair order changes. Another query/window name is not a new read.
	unique := make([]TokenWalletReuseView, 0, len(candidates))
	seenWallet := map[string]bool{}
	for _, candidate := range candidates {
		if seenWallet[candidate.WalletSHA256] {
			report.Rejected = append(report.Rejected, TokenWalletReuseRejection{ParentSHA256: candidate.ParentSHA256, Reason: "same_wallet_view_not_independent"})
			continue
		}
		seenWallet[candidate.WalletSHA256] = true
		unique = append(unique, candidate)
	}
	candidates = unique
	sort.Slice(report.Rejected, func(i, j int) bool { return report.Rejected[i].ParentSHA256 < report.Rejected[j].ParentSHA256 })
	report.Eligible = len(candidates)
	if len(candidates) >= 2 {
		// Earliest-completing eligible read, then latest-completing eligible read.
		// A different window/log name or parent hash is not independence proof.
		first := candidates[0]
		report.Status = "insufficient_separation"
		for i := len(candidates) - 1; i > 0; i-- {
			last := candidates[i]
			if last.StartedAtMS-first.CompletedAtMS >= tokenWalletReuseGapMS && last.CompletedAtMS/1000 != first.CompletedAtMS/1000 {
				report.Selected = []TokenWalletReuseView{first, last}
				report.Status = "two_original_views_available_not_boundary_proof"
				break
			}
		}
	}
	return report, ctx.Err()
}
