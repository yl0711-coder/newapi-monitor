package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const walletReuseSourceID = "observed-wallet-reuse"
const walletReuseOriginID = "observed-wallet-reuse-origin"

func (g *Generation) walletReuseMaterialSources(ctx context.Context, m monthlysource.MonthlyObservationMaterial) ([]monitor.MonthlyPreparationSource, error) {
	reuse := *m.WalletReuse
	raw, err := g.readPrivatePreparationFile(ctx, reuse, monthlysource.WalletReuseMaterialLimit)
	if err != nil {
		return nil, err
	}
	var meta monthlysource.WalletReuseMaterial
	if json.Unmarshal(raw, &meta) != nil {
		return nil, ErrGenerationInput
	}
	canonical, err := json.Marshal(meta)
	if err != nil || !bytes.Equal(raw, canonical) || meta.Version != 1 || meta.Mode != monthlysource.WalletReuseMaterialMode {
		return nil, ErrGenerationInput
	}
	originalRaw, err := g.readPrivatePreparationFile(ctx, meta.OriginalMaterial, monthlysource.WalletReuseMaterialLimit)
	if err != nil {
		return nil, err
	}
	original, err := monthlysource.DecodeWalletReuseOrigin(ctx, originalRaw, meta.OriginalMaterial)
	if err != nil {
		return nil, err
	}
	// Source handoff is an explicit overlay ONLY for wallet selection. No
	// funding/usage window, rule, account or database may change underneath it.
	m.WalletReuse = nil
	m.WalletIDs = original.WalletIDs
	body, err := json.Marshal(m)
	if err != nil || !bytes.Equal(body, originalRaw) {
		return nil, ErrGenerationEvidence
	}
	return []monitor.MonthlyPreparationSource{
		{ID: walletReuseSourceID, Path: reuse.Path, SHA256: reuse.SHA256},
		{ID: walletReuseOriginID, Path: meta.OriginalMaterial.Path, SHA256: meta.OriginalMaterial.SHA256},
	}, nil
}

func (g *Generation) selectedReusedWalletBalances(ctx context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput, selection *WalletObservationSelection) ([]walletInputBalance, string, error) {
	var source monitor.MonthlyPreparationSource
	for _, s := range input.Sources {
		if s.ID == selection.ReuseSourceID {
			source = s
		}
	}
	raw, err := g.readPrivatePreparationFile(ctx, monthlysource.MonthlyObservationFile{Path: source.Path, SHA256: source.SHA256}, monthlysource.WalletReuseMaterialLimit)
	if err != nil {
		return nil, "", err
	}
	var meta monthlysource.WalletReuseMaterial
	if json.Unmarshal(raw, &meta) != nil || meta.GroupID != p.Owner.GroupID || meta.Month != p.Funding.Month || meta.SourceIdentity != p.Funding.SourceIdentity || len(meta.Accounts) != len(p.Funding.UserIDs) {
		return nil, "", ErrGenerationEvidence
	}
	find := func(ref monthlysource.MonthlyObservationFile) bool {
		for _, s := range input.Sources {
			if s.Path == ref.Path && s.SHA256 == ref.SHA256 {
				return true
			}
		}
		return false
	}
	if !find(meta.OriginalMaterial) {
		return nil, "", ErrGenerationEvidence
	}
	originalRaw, err := g.readPrivatePreparationFile(ctx, meta.OriginalMaterial, monthlysource.WalletReuseMaterialLimit)
	if err != nil {
		return nil, "", err
	}
	archives := map[int64][]byte{}
	budget := int64(len(raw) + len(originalRaw))
	for i, a := range meta.Accounts {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if a.UserID != p.Funding.UserIDs[i] || !find(a.Archive) || budget >= generationEvidenceLimit {
			return nil, "", ErrGenerationEvidence
		}
		// The archive is also the exact per-user token source selected by this
		// plan, not a similarly named independent file or another account's view.
		matched := false
		for _, t := range p.TokenUsageSources {
			if t.UserID != a.UserID {
				continue
			}
			for _, s := range input.Sources {
				if s.ID == t.SourceID && s.Path == a.Archive.Path && s.SHA256 == a.Archive.SHA256 {
					matched = true
				}
			}
		}
		if !matched {
			return nil, "", ErrGenerationEvidence
		}
		b, err := g.readPrivatePreparationFile(ctx, a.Archive, min(int64(monthlysource.TokenUsageArchiveLimit), generationEvidenceLimit-budget))
		if err != nil {
			return nil, "", err
		}
		budget += int64(len(b))
		archives[a.UserID] = b
	}
	verified, err := monthlysource.VerifyWalletReuseMaterial(ctx, raw, originalRaw, archives, time.Now())
	if err != nil {
		return nil, "", err
	}
	// Exact group/month/full account scope was checked above, then the manifest
	// was regenerated from original sources. No per-account wall clock is merged.
	month, err := time.ParseInLocation("2006-01", p.Funding.Month, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		return nil, "", ErrGenerationInput
	}
	out := []walletInputBalance{}
	for _, a := range verified.Accounts {
		archive, _, err := monthlysource.DecodeTokenUsageMonthArchive(ctx, archives[a.UserID], time.Now())
		if err != nil {
			return nil, "", err
		}
		parents := map[string]monthlysource.TokenUsageWindowObservation{}
		for _, pair := range archive.Pairs {
			for _, o := range []monthlysource.TokenUsageWindowObservation{pair.Older, pair.Newer} {
				b, err := json.Marshal(o)
				if err != nil {
					return nil, "", err
				}
				parents[generationHash(b)] = o
			}
		}
		seenSeconds := map[int64]bool{}
		for _, chosen := range a.Selected {
			o, exists := parents[chosen.ParentSHA256]
			if !exists {
				return nil, "", ErrGenerationEvidence
			}
			wallet, err := monthlysource.WalletObservationFromToken(o, chosen.ParentSHA256)
			if err != nil {
				return nil, "", err
			}
			b, err := json.Marshal(wallet)
			if err != nil || generationHash(b) != chosen.WalletSHA256 {
				return nil, "", ErrGenerationIntegrity
			}
			at := wallet.CompletedAtMS / 1000
			if at < month.AddDate(0, 1, 0).Unix() || seenSeconds[at] || len(wallet.Users) != 1 || wallet.Users[0].UserID != a.UserID {
				return nil, "", ErrGenerationEvidence
			}
			seenSeconds[at] = true
			u := wallet.Users[0]
			out = append(out, walletInputBalance{ID: chosen.WalletSHA256, Balance: monitor.MonthlyPreparationBalance{UserID: a.UserID, At: at, BalanceQuota: *u.BalanceQuota, UsedQuota: *u.UsedQuota, TimeBasis: "collection_observation",
				Source: selection.ReuseSourceID + "#token-wallet:" + chosen.WalletSHA256 + ":" + chosen.ParentSHA256 + ":user:" + strconv.FormatInt(a.UserID, 10)}})
		}
	}
	// Keep representation stable and distinct from any standalone DB wallet IDs.
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, "", err
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if bytes.Equal(encoded, []byte("[]")) {
		return nil, "", ErrGenerationEvidence
	}
	return out, generationHash(encoded), nil
}
