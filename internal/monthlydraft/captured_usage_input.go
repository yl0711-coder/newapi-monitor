package monthlydraft

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func planHasUsage(p GenerationPlan) bool {
	return len(p.UsageWindows) != 0 || p.CapturedUsageSourceID != ""
}

// Read only the pinned original index. No new pathname, roster, amount or
// persisted partition authority is inferred from customer input.
func (g *Generation) capturedUsageSelection(ctx context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) (monthlysource.CapturedMonthSelection, string, []byte, error) {
	var empty monthlysource.CapturedMonthSelection
	source, err := tokenSource(input, p.CapturedUsageSourceID)
	if err != nil {
		return empty, "", nil, err
	}
	raw, err := g.readPrivatePreparationFile(ctx, monthlysource.MonthlyObservationFile{Path: source.Path, SHA256: source.SHA256}, 2<<20)
	if err != nil {
		return empty, "", nil, err
	}
	var s monthlysource.CapturedMonthSelection
	if json.Unmarshal(raw, &s) != nil {
		return empty, "", nil, ErrGenerationIntegrity
	}
	owner := p.Owner
	wallets := slices.Clone(s.WalletIDs)
	slices.Sort(wallets)
	if s.Key != (SourcePreparationRequest{Owner: owner, Month: p.Funding.Month}).Key() || s.Scope.GroupID != owner.GroupID || s.Scope.AuthVersion != owner.AuthVersion || s.Scope.MemberFingerprint != owner.MemberFingerprint || s.Scope.MemberManifest != owner.MemberManifest || !reflect.DeepEqual(s.Scope.Funding, p.Funding) || !reflect.DeepEqual(s.Funding, p.Windows) || p.WalletObservations == nil || !reflect.DeepEqual(wallets, p.WalletObservations.IDs) || len(s.Tokens) != len(p.TokenUsageSources) {
		return empty, "", nil, ErrGenerationEvidence
	}
	for i, token := range s.Tokens {
		if token.UserID != p.TokenUsageSources[i].UserID {
			return empty, "", nil, ErrGenerationEvidence
		}
	}
	return s, source.SHA256, raw, nil
}

func (g *Generation) reviewPlanUsage(ctx context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) (monthlysource.UsageMonthObservation, error) {
	if p.CapturedUsageSourceID == "" {
		return monthlysource.ReviewUsageMonth(ctx, g.funding, p.Funding, p.UsageWindows)
	}
	s, sha, raw, err := g.capturedUsageSelection(ctx, p, input)
	if err != nil {
		return monthlysource.UsageMonthObservation{}, err
	}
	parts, err := monthlysource.PartitionCapturedUsageIndex(ctx, raw, time.Now())
	if err != nil {
		return monthlysource.UsageMonthObservation{}, err
	}
	return monthlysource.ReviewPartitionedCapturedUsage(ctx, g.funding, sha, s.Scope, parts)
}

func capturedArchiveReferencesMatch(a monthlysource.TokenUsageMonthArchive, selected monthlysource.CapturedTokenSelection, sha string) bool {
	if a.SelectionSHA256 != sha || a.UserID != selected.UserID || len(a.Pairs) != len(selected.Usage) || len(selected.Parents) != len(a.Pairs)*2 {
		return false
	}
	for i, pair := range a.Pairs {
		for phase, o := range []monthlysource.TokenUsageWindowObservation{pair.Older, pair.Newer} {
			raw, err := json.Marshal(o)
			if err != nil || generationHash(raw) != selected.Parents[i*2+phase] {
				return false
			}
			u, err := monthlysource.UsageObservationFromToken(o)
			if err != nil {
				return false
			}
			body, err := json.Marshal(u)
			id := selected.Usage[i].OlderID
			if phase == 1 {
				id = selected.Usage[i].NewerID
			}
			if err != nil || generationHash(body) != id {
				return false
			}
		}
	}
	return true
}
