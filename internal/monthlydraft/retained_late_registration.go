package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Select original registration proof for a member born AFTER this closed month.
// Never creates a balance, changes its timestamp or treats missing history as
// zero. A member registered inside the chosen month still needs first-month
// balance evidence; this function does not relax that separate requirement.
func (g *Generation) retainedLateMemberRegistrations(ctx context.Context, funding *FundingSourcePreparation, req SourcePreparationRequest,
	history monitor.MonthlyPreparationInput, from, end int64) (map[int64]*monitor.MonthlyBundleRegistration, error) {
	if ctx == nil || g == nil || funding == nil || funding.source == nil || from <= 0 || end <= from {
		return nil, ErrGenerationInput
	}
	current, err := funding.scope(ctx, req)
	if err != nil {
		return nil, err
	}
	accounts := map[int64]monitor.MonthlyPreparationAccount{}
	for _, account := range history.Accounts {
		accounts[account.UserID] = account
	}
	var ids []int64
	for _, id := range current.Funding.UserIDs {
		a, exists := accounts[id]
		if !exists || a.GroupID != req.Owner.GroupID || a.CounterBasis != "net_consumption" || a.Baseline.At <= 0 {
			return nil, ErrGenerationEvidence
		}
		if a.Baseline.At <= from {
			continue
		}
		if !futureZeroOrigin(a, end) {
			return nil, ErrGenerationEvidence
		}
		ids = append(ids, id)
	}
	result := map[int64]*monitor.MonthlyBundleRegistration{}
	if len(ids) == 0 {
		return result, ctx.Err() // ordinary history: no additional source lookup
	}
	// Fresh bounded SELECT-only metadata, not cached enrollment or first usage.
	// Financial proof remains the separately retained ORIGINAL signup record.
	rows, err := monthlysource.ReadRegistrationTimes(ctx, funding.source, funding.sourceIdentity, ids)
	if err != nil {
		if errors.Is(err, monthlysource.ErrRegistrationBusy) {
			err = errors.Join(ErrGenerationBusy, err)
		}
		return nil, err
	}
	if len(rows) != len(ids) {
		return nil, ErrGenerationEvidence
	}
	reader := retainedRegistrationReader{g: g, sources: history.Sources, cache: map[string][]byte{}}
	for i, row := range rows {
		if row.UserID != ids[i] || row.At < end || accounts[row.UserID].Baseline.At > row.At {
			return nil, ErrGenerationEvidence
		}
		registration, err := reader.selectOriginal(ctx, accounts[row.UserID], row.At, current.Funding.Rules)
		if err != nil {
			return nil, err
		}
		registration.NoEarlierActivitySource = observationMaterialSourceID + "#captured-empty-pre-registration:" + req.Month + ":user:" + strconv.FormatInt(row.UserID, 10)
		result[row.UserID] = registration
	}
	if _, err := funding.scope(ctx, req); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

type retainedRegistrationReader struct {
	g       *Generation
	sources []monitor.MonthlyPreparationSource
	cache   map[string][]byte
	bytes   int
}

func (r *retainedRegistrationReader) read(ctx context.Context, id string) ([]byte, error) {
	if body, exists := r.cache[id]; exists {
		return body, ctx.Err()
	}
	if len(r.cache) >= 32 {
		return nil, ErrGenerationCapacity
	}
	source, err := tokenSource(monitor.MonthlyPreparationInput{Sources: r.sources}, id)
	if err != nil {
		return nil, err
	}
	body, err := r.g.readPrivatePreparationFile(ctx, monthlysource.MonthlyObservationFile{Path: source.Path, SHA256: source.SHA256}, 1<<20)
	if err != nil {
		return nil, err
	}
	if len(body) > monitor.MonthlyPreparationInputLimit-r.bytes {
		return nil, ErrGenerationCapacity
	}
	r.bytes += len(body)
	r.cache[id] = body
	return body, nil
}

func (r *retainedRegistrationReader) selectOriginal(ctx context.Context, a monitor.MonthlyPreparationAccount, createdAt int64, rules monthlysource.FundingRules) (*monitor.MonthlyBundleRegistration, error) {
	var selected *monitor.MonthlyBundleRegistration
	for _, snapshot := range a.Snapshots {
		if snapshot.At < createdAt && (snapshot.BalanceQuota != 0 || snapshot.UsedQuota != 0) {
			return nil, ErrGenerationEvidence
		}
	}
	for _, m := range a.Movements {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if m.UserID != a.UserID || m.At < createdAt {
			return nil, ErrGenerationEvidence
		}
		if m.DeltaQuota <= 0 {
			continue
		}
		id, pointer, found := strings.Cut(m.Source, "#")
		if !found || !validScopeSourceID(id) || !strings.HasPrefix(pointer, "/") {
			// Other later funding can use its original database-row locator.
			// It is not a signup JSON row, and cannot supply registration proof.
			continue
		}
		body, err := r.read(ctx, id)
		if err != nil {
			return nil, err
		}
		raw, err := registrationJSONPointer(body, pointer)
		if err != nil {
			return nil, err
		}
		var log monthlysource.FundingLog
		if json.Unmarshal(raw, &log) != nil {
			return nil, ErrGenerationEvidence
		}
		kind := monthlysource.NormalizeFundingLog(log, rules)
		if kind.Status != monthlysource.FundingMovement || kind.TargetUserID != a.UserID || kind.LogID != m.ID || kind.At != m.At || kind.DeltaQuota == nil || *kind.DeltaQuota != m.DeltaQuota {
			return nil, ErrGenerationEvidence
		}
		if kind.Kind != "gift" || kind.Method != "注册赠送" {
			continue
		}
		registration := monitor.MonthlyBundleRegistration{At: log.CreatedAt, EventID: log.ID, Source: m.Source}
		if _, err := registrationFromOriginal(body, pointer, a.UserID, registration, rules); err != nil || selected != nil {
			return nil, ErrGenerationEvidence
		}
		selected = &registration
	}
	if selected == nil {
		return nil, ErrGenerationEvidence
	}
	return selected, ctx.Err()
}
