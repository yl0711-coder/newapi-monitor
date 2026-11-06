package monthlysource

import (
	"encoding/json"
	"errors"
	"math/big"
	"time"
)

// WalletTailBoundary is an opaque calculation, not a retained wallet original
// or publication permission. Callers cannot JSON-decode assertions into it.
// Its hashes identify the two originals that later history preparation must
// retain and re-audit; matching calculations alone do not prove all history.
type WalletTailBoundary struct {
	userID, registeredAt, boundary, balance, used int64
	source, olderSHA, newerSHA                    string
}

func (b WalletTailBoundary) UserID() int64          { return b.userID }
func (b WalletTailBoundary) RegisteredAt() int64    { return b.registeredAt }
func (b WalletTailBoundary) Boundary() int64        { return b.boundary }
func (b WalletTailBoundary) BalanceQuota() int64    { return b.balance }
func (b WalletTailBoundary) UsedQuota() int64       { return b.used }
func (b WalletTailBoundary) SourceIdentity() string { return b.source }
func (b WalletTailBoundary) OriginalHashes() (string, string) {
	return b.olderSHA, b.newerSHA
}

// DeriveWalletTailBoundary independently recalculates BOTH originals. Wallets
// may have moved between views; the inferred closed boundary must not move.
// It does not sleep, access a DB, alter usage or relabel physical capture times.
func DeriveWalletTailBoundary(older, newer WalletTailObservation) (WalletTailBoundary, error) {
	var empty WalletTailBoundary
	if err := ValidateWalletTailObservation(older); err != nil {
		return empty, err
	}
	if err := ValidateWalletTailObservation(newer); err != nil {
		return empty, err
	}
	if older.Request != newer.Request || older.User.RegisteredAt != newer.User.RegisteredAt ||
		newer.StartedAtMS < older.CompletedAtMS || newer.StartedAtMS-older.CompletedAtMS < (5*time.Minute).Milliseconds() {
		return empty, errors.New("wallet tail requires independent separated views of the same scope")
	}
	aBalance, aUsed, err := deriveWalletTail(older)
	if err != nil {
		return empty, err
	}
	bBalance, bUsed, err := deriveWalletTail(newer)
	if err != nil {
		return empty, err
	}
	if aBalance != bBalance || aUsed != bUsed {
		return empty, errors.New("wallet tail boundary changed between independent views")
	}
	aRaw, err := json.Marshal(older)
	if err != nil {
		return empty, err
	}
	bRaw, err := json.Marshal(newer)
	if err != nil {
		return empty, err
	}
	return WalletTailBoundary{userID: older.Request.UserID, registeredAt: older.User.RegisteredAt,
		boundary: older.Request.Boundary, balance: aBalance, used: aUsed, source: older.Request.SourceIdentity,
		olderSHA: digest(aRaw), newerSHA: digest(bRaw)}, nil
}

func deriveWalletTail(o WalletTailObservation) (int64, int64, error) {
	net := new(big.Int)
	for _, row := range o.Usage {
		// Unknown legacy sources and subscription usage require separate reviewed
		// semantics. Even zero rows are not permission to assume wallet-only.
		if row.BillingSource != "wallet" {
			return 0, 0, errors.New("wallet tail usage billing source requires evidence")
		}
		if row.Type == 2 {
			net.Add(net, big.NewInt(row.Quota))
		} else {
			net.Sub(net, big.NewInt(row.Quota))
		}
	}
	funds := new(big.Int)
	for _, row := range o.Funding {
		movement := NormalizeFundingLog(row, o.Request.Rules)
		switch movement.Status {
		case FundingMovement:
			if movement.TargetUserID == o.Request.UserID {
				if movement.DeltaQuota == nil || row.CreatedAt < o.User.RegisteredAt {
					return 0, 0, errors.New("wallet tail movement lacks a valid registered target")
				}
				funds.Add(funds, big.NewInt(*movement.DeltaQuota))
			}
		case FundingNonWallet:
			// Reviewed exact producer actions only, not a substring exemption.
		case FundingReview:
			if movement.TargetUserID == 0 || movement.TargetUserID == o.Request.UserID {
				return 0, 0, errors.New("wallet tail contains unresolved funding")
			}
		default:
			return 0, 0, errors.New("wallet tail contains an unexpected funding type")
		}
	}
	balance := new(big.Int).Sub(big.NewInt(*o.User.BalanceQuota), funds)
	balance.Add(balance, net)
	used := new(big.Int).Sub(big.NewInt(*o.User.UsedQuota), net)
	if !balance.IsInt64() || !used.IsInt64() || used.Sign() < 0 {
		return 0, 0, errors.New("wallet tail derived balance or counter exceeds exact quota bounds")
	}
	if o.User.RegisteredAt >= o.Request.Boundary && (balance.Sign() != 0 || used.Sign() != 0) {
		return 0, 0, errors.New("wallet tail contradicts registration boundary")
	}
	return balance.Int64(), used.Int64(), nil
}
