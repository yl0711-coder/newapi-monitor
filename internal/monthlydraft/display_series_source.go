package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// A server-owned range: fixed configuration in legacy mode, or the authenticated
// registration window in explicit registered mode. Never infer origin from a
// customer's chosen month, completed task or earliest available financial input.
type AutomaticDisplaySeriesRange struct {
	GroupID        int64  `json:"group_id"`
	SourceIdentity string `json:"source_identity"`
	From           string `json:"from"`
	Through        string `json:"through"`
}

type automaticDisplayPublication struct {
	evidence []byte
	series   *monitor.PreparedMonthlyDisplayPublication
	binding  []byte
}

type AutomaticDisplaySeriesPreparer func(context.Context, monthlytasks.Request, []byte) (automaticDisplayPublication, error)

var automaticDisplaySeriesSlots = make(chan struct{}, 1)

// Construction reads no data, starts nothing and registers no routes. Calls run
// in the background worker, not in bill GET/download. Predecessors are prepared
// without submitting customer tasks or publishing extra customer bills.
func NewSelectedAutomaticDisplaySeries(g *Generation, ranges []AutomaticDisplaySeriesRange) (AutomaticDisplaySeriesPreparer, error) {
	return newSelectedAutomaticDisplaySeries(g, ranges, nil)
}

func newSelectedAutomaticDisplaySeries(g *Generation, ranges []AutomaticDisplaySeriesRange, resolve func(context.Context, AutomaticDisplaySeriesRange, monthlytasks.Request) (AutomaticDisplaySeriesRange, error)) (AutomaticDisplaySeriesPreparer, error) {
	owned, err := automaticDisplaySeriesRanges(g, ranges, false)
	if err != nil {
		return nil, err
	}
	return newAutomaticDisplaySeriesPreparer(g, func(ctx context.Context, request monthlytasks.Request) (AutomaticDisplaySeriesRange, error) {
		scope, exists := owned[request.Owner.GroupID]
		if !exists || request.SourceIdentity != scope.SourceIdentity || request.Month < scope.From || request.Month > scope.Through {
			return AutomaticDisplaySeriesRange{}, ErrGenerationInput
		}
		if resolve != nil {
			return resolve(ctx, scope, request)
		}
		return scope, nil
	})
}

// Validation is shared by the legacy fixed ranges and the explicit registered
// worker. Empty ranges are allowed ONLY by the latter; they are not authority.
func automaticDisplaySeriesRanges(g *Generation, ranges []AutomaticDisplaySeriesRange, allowEmpty bool) (map[int64]AutomaticDisplaySeriesRange, error) {
	if g == nil || g.readOnly || g.root == nil || g.receiver == nil || g.verifyCurrent == nil || (!allowEmpty && len(ranges) == 0) || len(ranges) > 32 {
		return nil, ErrGenerationInput
	}
	owned := map[int64]AutomaticDisplaySeriesRange{}
	for _, scope := range ranges {
		zone := time.FixedZone("Usage-UTC+8", 8*3600)
		from, e1 := time.ParseInLocation("2006-01", scope.From, zone)
		through, e2 := time.ParseInLocation("2006-01", scope.Through, zone)
		months := (through.Year()-from.Year())*12 + int(through.Month()-from.Month()) + 1
		if !validSelectionScope(scope.GroupID, scope.From) || !validSelectionScope(scope.GroupID, scope.Through) || e1 != nil || e2 != nil || months < 1 || months > 120 ||
			!generationHex(scope.SourceIdentity, 32) || owned[scope.GroupID].GroupID != 0 || through.AddDate(0, 1, 0).After(time.Now()) {
			return nil, ErrGenerationInput
		}
		owned[scope.GroupID] = scope
	}
	return owned, nil
}

// Only the worker supplies this private resolver. It cannot be supplied by a
// customer, and every result still passes the existing prefix and money gates.
func newAutomaticDisplaySeriesPreparer(g *Generation, lookup func(context.Context, monthlytasks.Request) (AutomaticDisplaySeriesRange, error)) (AutomaticDisplaySeriesPreparer, error) {
	return newAutomaticDisplaySeriesPreparerWithPlan(g, lookup, monitor.PrepareMonthlyDisplayPublication)
}

type automaticDisplayPlanFactory func(context.Context, monitor.MonthlyDisplaySeriesScope, []monitor.MonthlyDisplayPublicationSource, time.Time) (*monitor.PreparedMonthlyDisplayPublication, error)

func newAutomaticDisplaySeriesPreparerWithPlan(g *Generation, lookup func(context.Context, monthlytasks.Request) (AutomaticDisplaySeriesRange, error), prepare automaticDisplayPlanFactory) (AutomaticDisplaySeriesPreparer, error) {
	if g == nil || g.readOnly || g.root == nil || g.receiver == nil || g.verifyCurrent == nil || lookup == nil || prepare == nil {
		return nil, ErrGenerationInput
	}
	return func(parent context.Context, request monthlytasks.Request, original []byte) (automaticDisplayPublication, error) {
		if parent == nil {
			return automaticDisplayPublication{}, ErrGenerationInput
		}
		if err := parent.Err(); err != nil {
			return automaticDisplayPublication{}, err
		}
		select {
		case automaticDisplaySeriesSlots <- struct{}{}:
			defer func() { <-automaticDisplaySeriesSlots }()
		default:
			return automaticDisplayPublication{}, ErrGenerationBusy
		}
		ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
		defer cancel()
		if len(original) == 0 || len(original) > 2<<20 {
			return automaticDisplayPublication{}, ErrGenerationInput
		}
		scope, err := lookup(ctx, request)
		if err != nil {
			return automaticDisplayPublication{}, err
		}
		// Keep the fixed origin. Unrequested later months are not dependencies;
		// already attempted later publications remain transaction constraints.
		scope, err = g.automaticDisplayRequiredScope(ctx, scope, request)
		if err != nil {
			return automaticDisplayPublication{}, err
		}
		if err := g.current(ctx, request); err != nil {
			return automaticDisplayPublication{}, err
		}
		// Snapshot the complete required prefix first. A missing predecessor
		// cannot leave a partial plan or shift its start to the target month.
		var selected []monthlytasks.Request
		var retainedFiles []monitor.MonthlyPreparationSource
		seenFiles := map[string]string{}
		var sourceBytes int64
		inputBytes := 0
		from, _ := time.Parse("2006-01", scope.From)
		for at := from; at.Format("2006-01") <= scope.Through; at = at.AddDate(0, 1, 0) {
			selection, err := ReadGenerationSelection(ctx, g.receiver, scope.GroupID, at.Format("2006-01"))
			if err != nil {
				return automaticDisplayPublication{}, err
			}
			if selection.Request == nil {
				return automaticDisplayPublication{}, ErrGenerationSelectionMissing
			}
			if selection.Request.Owner.GroupID != request.Owner.GroupID || selection.Request.SourceIdentity != scope.SourceIdentity || (selection.Request.Month == request.Month && *selection.Request != request) {
				return automaticDisplayPublication{}, ErrGenerationChanged
			}
			selected = append(selected, *selection.Request)
			saved, err := g.loadRetainedGeneration(ctx, *selection.Request, time.Now())
			if err != nil {
				return automaticDisplayPublication{}, err
			}
			inputBytes += len(saved.planRaw) + len(saved.inputRaw)
			if inputBytes > monitor.MonthlyPreparationInputLimit {
				return automaticDisplayPublication{}, ErrGenerationCapacity
			}
			for _, source := range saved.input.Sources {
				if digest, seen := seenFiles[source.Path]; seen {
					if digest != source.SHA256 {
						return automaticDisplayPublication{}, ErrGenerationIntegrity
					}
					continue
				}
				rel, err := g.relativeSource(source.Path)
				if err != nil {
					return automaticDisplayPublication{}, err
				}
				info, err := g.root.Lstat(rel)
				if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
					return automaticDisplayPublication{}, ErrGenerationIntegrity
				}
				if info.Size() > generationEvidenceLimit-sourceBytes {
					return automaticDisplayPublication{}, ErrGenerationCapacity
				}
				sourceBytes += info.Size()
				seenFiles[source.Path] = source.SHA256
				retainedFiles = append(retainedFiles, source)
			}
		}
		var sources []monitor.MonthlyDisplayPublicationSource
		var target []byte
		size := 0
		for _, sourceRequest := range selected {
			// A retained historical owner/revision is not the live target's
			// authorization. Keep the historical proof immutable while gating
			// the operation on the current target owner and exact source heads.
			local := *g
			local.verifyCurrent = func(ctx context.Context, selected monthlytasks.Request) error {
				if selected != sourceRequest {
					return ErrGenerationChanged
				}
				return g.currentAutomaticDisplaySource(ctx, request, selected)
			}
			source, err := local.automaticDisplaySource(ctx, sourceRequest)
			if err != nil {
				return automaticDisplayPublication{}, err
			}
			size += len(source.Evidence)
			if size > monitor.MonthlyPreparationInputLimit {
				return automaticDisplayPublication{}, ErrGenerationCapacity
			}
			if sourceRequest.Month == request.Month {
				// Only the original retained review timestamp may differ from the
				// newly completed task's timestamp. No monetary/source rewrite.
				if !sameAutomaticDisplayEvidence(original, source.Evidence) {
					return automaticDisplayPublication{}, ErrGenerationEvidence
				}
				target = bytes.Clone(source.Evidence)
			}
			sources = append(sources, source)
		}
		if err := g.verifyFiles(ctx, retainedFiles); err != nil {
			return automaticDisplayPublication{}, err
		}
		prepared, err := prepare(ctx, monitor.MonthlyDisplaySeriesScope{GroupID: scope.GroupID, SourceIdentity: scope.SourceIdentity, From: scope.From, Through: scope.Through}, sources, time.Now())
		if err != nil {
			return automaticDisplayPublication{}, err
		}
		for _, sourceRequest := range selected {
			if err := g.currentAutomaticDisplaySource(ctx, request, sourceRequest); err != nil {
				return automaticDisplayPublication{}, err
			}
		}
		binding, err := prepared.BindingSHA256(request.Month)
		if err != nil {
			return automaticDisplayPublication{}, err
		}
		raw, err := json.Marshal(struct {
			Mode, EvidenceSHA256, DisplaySHA256 string
		}{"fixed-origin-publication-v1", generationHash(target), binding})
		if err != nil || ctx.Err() != nil {
			return automaticDisplayPublication{}, errors.Join(err, ctx.Err())
		}
		return automaticDisplayPublication{evidence: target, series: prepared, binding: raw}, nil
	}, nil
}

// Current customer authorization is checked through the target request. A
// predecessor is server-selected historical evidence, not a new customer task:
// compare its original organization/source/rules and immutable selected version
// without replacing its old owner, names, members or reviewed timestamp.
func (g *Generation) currentAutomaticDisplaySource(ctx context.Context, target, source monthlytasks.Request) error {
	if source.Owner.GroupID != target.Owner.GroupID || source.SourceIdentity != target.SourceIdentity || !currentGenerationRules(source) {
		return ErrGenerationChanged
	}
	if source.Month == target.Month && source != target {
		return ErrGenerationChanged
	}
	if err := g.current(ctx, target); err != nil {
		return err
	}
	selected, err := ReadGenerationSelection(ctx, g.receiver, source.Owner.GroupID, source.Month)
	if err != nil {
		return err
	}
	if selected.Request == nil || *selected.Request != source {
		return ErrGenerationChanged
	}
	return ctx.Err()
}

func sameAutomaticDisplayEvidence(a, b []byte) bool {
	var first, second monitor.MonthlyBillEvidence
	if json.Unmarshal(a, &first) != nil || json.Unmarshal(b, &second) != nil {
		return false
	}
	first.ReviewedAt, second.ReviewedAt = 0, 0
	x, err := json.Marshal(first)
	y, e2 := json.Marshal(second)
	return err == nil && e2 == nil && bytes.Equal(x, y)
}

// Re-audit selected original sources even on cache hits. Cached financial JSON
// is never authority to skip file, usage, funding, wallet or token verification.
func (g *Generation) auditAutomaticDisplaySource(ctx context.Context, request monthlytasks.Request, reviewedAt int64) (monitor.MonthlyDisplayPublicationSource, error) {
	var empty monitor.MonthlyDisplayPublicationSource
	if err := g.current(ctx, request); err != nil {
		return empty, err
	}
	saved, err := g.loadRetainedGeneration(ctx, request, time.Now())
	if err != nil {
		return empty, err
	}
	scoped, release, err := g.bindObservationStore(ctx, saved.plan, saved.input)
	if err != nil {
		return empty, err
	}
	defer release()
	if err := scoped.verifyFiles(ctx, saved.input.Sources); err != nil {
		return empty, err
	}
	if _, err := scoped.auditUsageInput(ctx, saved.plan, saved.input); err != nil {
		return empty, err
	}
	if _, err := scoped.auditWalletInput(ctx, saved.plan, saved.input); err != nil {
		return empty, err
	}
	if _, err := scoped.auditTokenInput(ctx, saved.plan, saved.input); err != nil {
		return empty, err
	}
	scope, err := scoped.selectedRetainedScope(ctx, saved.plan, saved.input)
	if err != nil {
		return empty, err
	}
	windows, err := scoped.auditWindows(ctx, saved.plan)
	if err != nil {
		return empty, err
	}
	meta, err := scoped.observedAutomaticContext(ctx, saved.plan, saved.inputRaw, windows, scope)
	if err != nil {
		return empty, err
	}
	bundle, err := prepareRetainedContextBundle(ctx, saved.plan, saved.inputRaw, meta, time.Now())
	if err != nil || bundle.ReadyForReview != 1 || bundle.Blocked != 0 || len(bundle.Entries) != 1 || bundle.Entries[0].Candidate == nil {
		return empty, errors.Join(ErrGenerationEvidence, err)
	}
	candidate, err := automaticCorrectedCandidate(*bundle.Entries[0].Candidate, meta.Corrections, saved.inputRaw)
	if err != nil {
		return empty, err
	}
	raw, err := monitor.PrepareAutomaticMonthlyBill(candidate, reviewedAt, time.Now())
	if err != nil {
		return empty, err
	}
	if err := scoped.verifyFiles(ctx, saved.input.Sources); err != nil {
		return empty, err
	}
	if err := g.current(ctx, request); err != nil {
		return empty, err
	}
	names := map[int64]string{}
	for _, member := range scope.Group.Members {
		names[member.UserID] = member.Username
	}
	return monitor.MonthlyDisplayPublicationSource{Evidence: raw, Account: scope.Group.Name, Names: names}, ctx.Err()
}
