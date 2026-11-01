package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

// Source-derived intermediate, NOT an AutomaticScope or permission to publish.
// In particular, an unresolved historical origin remains explicit. Actual
// physical capture milliseconds are retained; no month-end snapshot is forged.
type automaticHistoryContextInput struct {
	input     automaticHistoryInput
	directory []byte
	accounts  []automaticHistoryViewAccount
}

type automaticHistoryViewAccount struct {
	UserID                                int64
	Username                              string
	RegisteredAt, CompletedAtMS           int64
	OriginalSHA256                        [2]string
	Month                                 string
	BalanceQuota, UsedQuota               int64
	MonthConsumeQuota, MonthRefundQuota   int64
	BridgeConsumeQuota, BridgeRefundQuota int64
	Funding                               []monthlysource.FundingObservedMovement
	OriginBalanceQuota, OriginUsedQuota   int64
	OriginRequiresResolution              bool
}

func (automaticHistoryContextInput) MarshalJSON() ([]byte, error) {
	return nil, errors.New("history context input is not historical scope approval")
}
func (a *automaticHistoryContextInput) UnmarshalJSON([]byte) error {
	if a != nil {
		*a = automaticHistoryContextInput{}
	}
	return errors.New("history context input must be rebuilt from original files")
}

func (g *Generation) prepareAutomaticHistoryContextInput(parent context.Context, authority *gorm.DB, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest) (automaticHistoryContextInput, error) {
	return g.prepareAutomaticHistoryContextInputWithPolicy(parent, authority, ref, req, nil)
}

func (g *Generation) prepareAutomaticHistoryContextInputWithPolicy(parent context.Context, authority *gorm.DB, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest, policy *monthlysource.MonthlyObservationFile) (automaticHistoryContextInput, error) {
	var empty automaticHistoryContextInput
	if parent == nil {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	directory, err := captureAutomaticHistoryDirectory(ctx, authority, req.Owner)
	if err != nil {
		return empty, err
	}
	input, err := g.prepareAutomaticHistoryInputWithPolicy(ctx, ref, req, policy)
	if err != nil {
		return empty, err
	}
	d, err := decodeAutomaticHistoryDirectory(directory, req.Owner)
	if err != nil {
		return empty, err
	}
	accounts, err := projectAutomaticHistoryView(ctx, input.originals, d, req.Month)
	if err != nil {
		return empty, err
	}
	after, err := captureAutomaticHistoryDirectory(ctx, authority, req.Owner)
	if err != nil {
		return empty, err
	}
	if !bytes.Equal(directory, after) {
		return empty, ErrGenerationChanged
	}
	if err := g.verifyFiles(ctx, input.originals.sources); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return automaticHistoryContextInput{input: input, directory: directory, accounts: accounts}, nil
}

func projectAutomaticHistoryView(ctx context.Context, audited auditedAutomaticHistory, directory automaticHistoryDirectory, month string) ([]automaticHistoryViewAccount, error) {
	if ctx == nil || !audited.history.MatchesCollectionScope(audited.scope) || month != audited.scope.Funding.Month ||
		directory.Owner.GroupID != audited.scope.GroupID || directory.Owner.AuthVersion != audited.scope.AuthVersion ||
		directory.Owner.MemberManifest != audited.scope.MemberManifest || directory.Owner.MemberFingerprint != audited.scope.MemberFingerprint {
		return nil, ErrGenerationEvidence
	}
	r := audited.history.Report()
	if len(directory.Members) != len(r.Accounts) || len(audited.tails.Pairs) != len(r.Accounts) {
		return nil, ErrGenerationEvidence
	}
	out := make([]automaticHistoryViewAccount, 0, len(r.Accounts))
	seen := map[int64]bool{}
	for i, a := range r.Accounts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pair := audited.tails.Pairs[i]
		view := pair.Newer
		if directory.Members[i].UserID != a.UserID || view.User.UserID != a.UserID || view.User.RegisteredAt != a.RegisteredAt ||
			view.Request.Boundary != r.Boundary || view.CompletedAtMS > time.Now().UnixMilli() || view.User.BalanceQuota == nil || view.User.UsedQuota == nil {
			return nil, ErrGenerationEvidence
		}
		olderRaw, err := json.Marshal(pair.Older)
		if err != nil {
			return nil, err
		}
		newerRaw, err := json.Marshal(view)
		if err != nil {
			return nil, err
		}
		if a.ClosingOriginalSHA256 != ([2]string{generationHash(olderRaw), generationHash(newerRaw)}) {
			return nil, ErrGenerationIntegrity
		}
		v := automaticHistoryViewAccount{UserID: a.UserID, Username: directory.Members[i].Username, RegisteredAt: a.RegisteredAt,
			CompletedAtMS: view.CompletedAtMS, OriginalSHA256: a.ClosingOriginalSHA256, Month: month, BalanceQuota: *view.User.BalanceQuota, UsedQuota: *view.User.UsedQuota,
			OriginBalanceQuota: a.OriginBalanceQuota, OriginUsedQuota: a.OriginUsedQuota, OriginRequiresResolution: a.OriginRequiresResolution, Funding: []monthlysource.FundingObservedMovement{}}
		consume, refund := new(big.Int), new(big.Int)
		var target *monthlysource.WalletHistoryAccountMonth
		for j := range a.Months {
			m := &a.Months[j]
			if m.Month == month {
				target = m
				v.MonthConsumeQuota, v.MonthRefundQuota = m.ConsumeQuota, m.RefundQuota
			}
			if m.Month > month {
				consume.Add(consume, big.NewInt(m.ConsumeQuota))
				refund.Add(refund, big.NewInt(m.RefundQuota))
			}
			v.Funding = append(v.Funding, m.Movements...)
		}
		if target == nil {
			return nil, ErrGenerationEvidence
		}
		for _, row := range view.Usage {
			if row.BillingSource != "wallet" {
				return nil, ErrGenerationEvidence
			}
			switch row.Type {
			case 2:
				consume.Add(consume, big.NewInt(row.Quota))
			case 6:
				refund.Add(refund, big.NewInt(row.Quota))
			default:
				return nil, ErrGenerationEvidence
			}
		}
		for _, row := range view.Funding {
			m := monthlysource.NormalizeFundingLog(row, view.Request.Rules)
			if m.Status == monthlysource.FundingNonWallet {
				continue
			}
			if m.Status == monthlysource.FundingReview && m.TargetUserID != 0 && m.TargetUserID != a.UserID {
				continue
			}
			if m.Status != monthlysource.FundingMovement || m.DeltaQuota == nil {
				return nil, ErrGenerationEvidence
			}
			if m.TargetUserID != a.UserID {
				continue
			}
			v.Funding = append(v.Funding, monthlysource.FundingObservedMovement{LogID: m.LogID, At: m.At, DeltaQuota: *m.DeltaQuota, SourceDigest: m.SourceDigest, Kind: m.Kind, Method: m.Method})
		}
		if !consume.IsInt64() || !refund.IsInt64() || len(v.Funding) > 25000 {
			return nil, ErrGenerationCapacity
		}
		v.BridgeConsumeQuota, v.BridgeRefundQuota = consume.Int64(), refund.Int64()
		net := new(big.Int)
		end, err := time.ParseInLocation("2006-01", month, time.FixedZone("Usage-UTC+8", 8*3600))
		if err != nil {
			return nil, ErrGenerationInput
		}
		end = end.AddDate(0, 1, 0)
		for _, m := range v.Funding {
			if m.LogID <= 0 || seen[m.LogID] {
				return nil, ErrGenerationIntegrity
			}
			seen[m.LogID] = true
			if m.At >= end.Unix() {
				net.Add(net, big.NewInt(m.DeltaQuota))
			}
		}
		balance := new(big.Int).Sub(big.NewInt(v.BalanceQuota), net)
		balance.Add(balance, consume)
		balance.Sub(balance, refund)
		used := new(big.Int).Sub(big.NewInt(v.UsedQuota), consume)
		used.Add(used, refund)
		if balance.Cmp(big.NewInt(target.ClosingBalanceQuota)) != 0 || used.Cmp(big.NewInt(target.ClosingUsedQuota)) != 0 {
			return nil, ErrGenerationEvidence
		}
		sort.Slice(v.Funding, func(i, j int) bool {
			if v.Funding[i].At != v.Funding[j].At {
				return v.Funding[i].At < v.Funding[j].At
			}
			return v.Funding[i].LogID < v.Funding[j].LogID
		})
		out = append(out, v)
	}
	raw, err := json.Marshal(out)
	if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
		return nil, ErrGenerationCapacity
	}
	return out, ctx.Err()
}
