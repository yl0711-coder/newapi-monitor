package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// PreparedInputInbox is private operator configuration, NEVER a customer DTO.
// Write a complete canonical JSON file by atomic rename. Every entry names a
// completed automatic/raw or retained-normalized package inside EvidenceRoot and
// pins its manifest. Revision is an explicit retry activation, not authority.
// An empty Entries list pauses future intake; an already running transaction
// may finish. Stop/drain the Deployment to cancel and join in-flight intake.
type PreparedInputInbox struct {
	Version int                       `json:"version"`
	Entries []PreparedInputInboxEntry `json:"entries"`
}

type PreparedInputInboxEntry struct {
	GroupID        int64  `json:"group_id"`
	Month          string `json:"month"`
	Revision       uint64 `json:"revision"`
	Directory      string `json:"directory"`
	ManifestSHA256 string `json:"manifest_sha256"`
	ExpectedHead   string `json:"expected_head"`
}

type preparationInboxProgress struct {
	entry    PreparedInputInboxEntry
	attempts int
	status   string
	next     time.Time
}

// Owned by one loop. No financial/source payloads are retained in this map.
type preparationInbox struct {
	name, sourceID string
	states         map[GenerationTarget]*preparationInboxProgress
	cursor         int
	indexFailure   string
}

const preparationInboxInterval = 30 * time.Second

var preparationInboxSlots = make(chan struct{}, 1)

func preparationInboxName(name string) bool {
	if len(name) == 0 || len(name) > 128 || name == "." || name == ".." || strings.HasPrefix(name, ".") {
		return false
	}
	for _, ch := range name {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
			return false
		}
	}
	return true
}

func decodePreparationInbox(raw []byte, scope *generationScope) (PreparedInputInbox, error) {
	var index PreparedInputInbox
	if scope == nil || scope.registered || len(raw) == 0 || len(raw) > 64<<10 || json.Unmarshal(raw, &index) != nil {
		return index, ErrGenerationInput
	}
	canonical, err := json.Marshal(index)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) || index.Version != 1 || index.Entries == nil || len(index.Entries) > 32 {
		return index, ErrGenerationInput
	}
	seen := make(map[GenerationTarget]bool)
	for _, entry := range index.Entries {
		target := GenerationTarget{GroupID: entry.GroupID, Month: entry.Month}
		if !scope.allows(entry.GroupID, entry.Month) || !validSelectionScope(entry.GroupID, entry.Month) || seen[target] || entry.Revision == 0 ||
			!preparationInboxName(entry.Directory) || !generationHex(entry.ManifestSHA256, 32) || (entry.ExpectedHead != "" && !generationHex(entry.ExpectedHead, 32)) {
			return index, ErrGenerationInput
		}
		seen[target] = true
	}
	return index, nil
}

func (p *preparationInbox) serve(ctx context.Context, d *Deployment) {
	for ctx.Err() == nil {
		p.step(ctx, d, time.Now(), d.receivePreparedInput)
		timer := time.NewTimer(preparationInboxInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// At most ONE entry per tick; round-robin avoids a bad target starving others.
// Busy/deadline failures retry at most three times, then pause. Other failures
// pause immediately. Restart resets in-memory retry budgets, never selection
// history. A successful/paused entry is not retried until its explicit entry
// (including Revision) changes, or the service is deliberately restarted.
func (p *preparationInbox) step(ctx context.Context, d *Deployment, now time.Time,
	receive func(context.Context, PreparedInputInboxEntry, string) (PreparationResult, error)) {
	raw, err := readArchiveFile(ctx, d.g.root, p.name, 64<<10)
	var index PreparedInputInbox
	if err == nil {
		index, err = decodePreparationInbox(raw, d.runtime.scope)
	}
	if err != nil {
		code := "invalid_index"
		if errors.Is(err, os.ErrNotExist) {
			code = "waiting_for_index"
		}
		if ctx.Err() == nil && p.indexFailure != code {
			slog.Warn("月度账单资料接入等待", "code", code)
		}
		p.indexFailure = code
		return // no target is touched on a partially written/invalid index
	}
	p.indexFailure = ""
	previous := p.states
	p.states = make(map[GenerationTarget]*preparationInboxProgress, len(index.Entries))
	for _, entry := range index.Entries {
		target := GenerationTarget{GroupID: entry.GroupID, Month: entry.Month}
		state := previous[target]
		if state == nil || state.entry != entry {
			state = &preparationInboxProgress{entry: entry}
		}
		p.states[target] = state
	}
	for offset := 0; offset < len(index.Entries); offset++ {
		i := (p.cursor + offset) % len(index.Entries)
		entry := index.Entries[i]
		state := p.states[GenerationTarget{GroupID: entry.GroupID, Month: entry.Month}]
		if state.status == "selected" || state.status == "paused" || now.Before(state.next) {
			continue
		}
		p.cursor = (i + 1) % len(index.Entries)
		started := time.Now()
		result, err := receive(ctx, entry, p.sourceID)
		if ctx.Err() != nil && !result.Applied {
			return // shutdown is not a source failure
		}
		state.attempts++
		code := "selected"
		if err == nil && result.Applied {
			state.status = "selected"
		} else if state.attempts < 3 && (errors.Is(err, ErrGenerationBusy) || errors.Is(err, context.DeadlineExceeded)) {
			state.status, code = "retry_wait", "temporarily_busy"
			state.next = now.Add(time.Since(started) + time.Duration(state.attempts*state.attempts)*preparationInboxInterval)
		} else {
			state.status, code = "paused", "check_required"
			if errors.Is(err, ErrGenerationSelectionConflict) {
				code = "selection_conflict"
			}
		}
		// Only allowlisted metadata: never log raw SQL errors, paths, money,
		// customer data, authority hashes or evidence contents.
		slog.Info("月度账单资料接入状态", "group_id", entry.GroupID, "month", entry.Month,
			"status", state.status, "code", code, "attempts", state.attempts)
		return
	}
}

func (d *Deployment) receivePreparedInput(parent context.Context, entry PreparedInputInboxEntry, scopeSource string) (PreparationResult, error) {
	ctx, done, err := d.runtime.beginPreparation(parent, entry.GroupID, entry.Month)
	if err != nil {
		return PreparationResult{}, err
	}
	defer done()
	select {
	case preparationInboxSlots <- struct{}{}:
		defer func() { <-preparationInboxSlots }()
	default:
		return PreparationResult{}, ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	planRaw, input, plan, err := d.readPreparedPackage(ctx, entry, scopeSource)
	if err != nil {
		return PreparationResult{}, err
	}
	selected, err := ReadGenerationSelection(ctx, d.receiver, entry.GroupID, entry.Month)
	if err != nil {
		return PreparationResult{}, err
	}
	head := entry.ExpectedHead
	request := generationRequest(plan, planRaw)
	if selected.Request != nil && *selected.Request == request {
		// Recovery after commit/restart: recheck idempotently at the EXACT
		// current request. A withdrawn/different request cannot take this path.
		head = selected.Head
	} else if selected.Head != head {
		return PreparationResult{}, ErrGenerationSelectionConflict
	}
	return d.PrepareMonthlyInput(ctx, planRaw, input, head, true)
}

func (d *Deployment) readPreparedPackage(ctx context.Context, entry PreparedInputInboxEntry, scopeSource string) ([]byte, []byte, GenerationPlan, error) {
	fail := func(err error) ([]byte, []byte, GenerationPlan, error) {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return nil, nil, GenerationPlan{}, err
	}
	if !preparationInboxName(entry.Directory) || !generationHex(entry.ManifestSHA256, 32) || !validScopeSourceID(scopeSource) {
		return fail(ErrGenerationInput)
	}
	st, err := d.g.root.Lstat(entry.Directory)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return fail(ErrGenerationIntegrity)
	}
	root, err := d.g.root.OpenRoot(entry.Directory)
	if err != nil {
		return fail(ErrGenerationIntegrity)
	}
	defer root.Close()
	current, err := root.Stat(".")
	if err != nil || !os.SameFile(st, current) {
		return fail(ErrGenerationIntegrity)
	}
	manifestRaw, err := readArchiveFile(ctx, root, "manifest.json", 64<<10)
	if err != nil || generationHash(manifestRaw) != entry.ManifestSHA256 {
		return fail(ErrGenerationIntegrity)
	}
	var manifest automaticPreparationManifest
	if json.Unmarshal(manifestRaw, &manifest) != nil {
		return fail(ErrGenerationIntegrity)
	}
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(manifestRaw, canonical) || manifest.Version != 1 || manifest.Mode != automaticPreparationMode ||
		(manifest.PreparationKind != "" && manifest.PreparationKind != retainedPreparationKind) ||
		manifest.Owner.GroupID != entry.GroupID || manifest.Month != entry.Month {
		return fail(ErrGenerationIntegrity)
	}
	planRaw, err := readArchiveFile(ctx, root, "plan.json", generationPlanLimit)
	if err != nil || generationHash(planRaw) != manifest.PlanSHA256 {
		return fail(ErrGenerationIntegrity)
	}
	input, err := readArchiveFile(ctx, root, "input.json", monitor.MonthlyPreparationInputLimit)
	if err != nil || generationHash(input) != manifest.PreparationSHA256 {
		return fail(ErrGenerationIntegrity)
	}
	scope, err := readArchiveFile(ctx, root, "scope.json", monitor.MonthlyPreparationInputLimit)
	if err != nil || generationHash(scope) != manifest.ScopeSHA256 {
		return fail(ErrGenerationIntegrity)
	}
	var plan GenerationPlan
	if json.Unmarshal(planRaw, &plan) != nil || plan.Owner != manifest.Owner || plan.Funding.Month != manifest.Month ||
		plan.Funding.SourceIdentity != manifest.SourceIdentity || plan.AutomaticScopeSourceID != scopeSource {
		return fail(ErrGenerationIntegrity)
	}
	_, canonicalPlan, parsedInput, err := canonicalGeneration(plan, input, time.Now())
	if err != nil || !bytes.Equal(planRaw, canonicalPlan) {
		return fail(ErrGenerationIntegrity)
	}
	scopeMatched := false
	for _, source := range parsedInput.Sources {
		if source.ID == scopeSource {
			rel, err := d.g.relativeSource(source.Path)
			if err != nil || rel != entry.Directory+"/scope.json" || source.SHA256 != manifest.ScopeSHA256 {
				return fail(ErrGenerationIntegrity)
			}
			scopeMatched = true
		}
	}
	current, err = d.g.root.Lstat(entry.Directory)
	if !scopeMatched || err != nil || !os.SameFile(st, current) || current.Mode().Perm()&0077 != 0 {
		return fail(ErrGenerationIntegrity)
	}
	// Subsequent shared preparation verifies the canonical plan, referenced
	// original scope hash/path and ALL financial evidence before selecting.
	return planRaw, input, plan, ctx.Err()
}
