package monthlydraft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// VerifyGenerationCurrent must check live authorization AND the selected source
// version, honoring ctx. Return ErrGenerationChanged only for a proven change;
// transient read failures must not be mistaken for revocation. No production
// implementation is supplied here; tests use explicit local synthetic policy.
type VerifyGenerationCurrent func(context.Context, monthlytasks.Request) error

type Generation struct {
	receiver, funding *sql.DB
	tasks             *monthlytasks.Store
	root              *os.Root
	verifyCurrent     VerifyGenerationCurrent
	calculate         func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error)
	readOnly          bool                              // operator inspection must never invalidate or claim tasks
	observationSource *monitor.MonthlyPreparationSource // scoped read-only handle; never set on the shared Generation
	observationFile   os.FileInfo
}

var generationSlots = make(chan struct{}, 1)

// Retained SQLite evidence may exceed 32 MiB. Stream it within the unchanged
// TOTAL 128 MiB budget rather than loading whole files or raising total I/O.
const generationEvidenceLimit int64 = 128 << 20

// NewGeneration checks local SQLite file identities and opens a private evidence
// DIRECTORY. Both SQLite handles are caller-owned; funding should be
// opened mode=ro. All writes target the separate local receiver. No routes,
// workers or initialization start automatically. Close after active calls end.
func NewGeneration(receiver, funding *sql.DB, evidenceRoot string, current VerifyGenerationCurrent) (*Generation, error) {
	if receiver == nil || funding == nil || receiver == funding || current == nil || !filepath.IsAbs(evidenceRoot) ||
		filepath.Clean(evidenceRoot) != evidenceRoot || evidenceRoot == string(filepath.Separator) {
		return nil, ErrGenerationInput
	}
	st, err := os.Lstat(evidenceRoot)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return nil, ErrGenerationInput
	}
	receiverFile, err := generationDatabaseFile(receiver)
	if err != nil {
		return nil, err
	}
	fundingFile, err := generationDatabaseFile(funding)
	if err != nil || os.SameFile(receiverFile, fundingFile) {
		return nil, ErrGenerationInput
	}
	root, err := os.OpenRoot(evidenceRoot)
	if err != nil {
		return nil, err
	}
	tasks, err := monthlytasks.New(receiver)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	return &Generation{receiver: receiver, funding: funding, tasks: tasks, root: root, verifyCurrent: current, calculate: ReconcileObservedMonth}, nil
}

func (g *Generation) Close() error { return g.root.Close() }

func generationDatabaseFile(db *sql.DB) (os.FileInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var main string
	for rows.Next() {
		var sequence int
		var name, path string
		if err := rows.Scan(&sequence, &name, &path); err != nil {
			return nil, err
		}
		if name == "main" {
			main = path
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(main) {
		return nil, ErrGenerationInput // No volatile :memory: receiver or source.
	}
	info, err := os.Stat(main)
	if err != nil || !info.Mode().IsRegular() {
		return nil, ErrGenerationInput
	}
	return info, nil
}

func (g *Generation) current(ctx context.Context, request monthlytasks.Request) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.verifyCurrent(ctx, request); err != nil {
		return err
	}
	if !currentGenerationRules(request) {
		// A worker may invalidate this obsolete draft task, but it must not
		// claim it, increment attempts or run old calculations again.
		return errors.Join(ErrGenerationChanged, ErrGenerationRulesChanged)
	}
	return ctx.Err()
}

func (g *Generation) currentTask(ctx context.Context, task monthlytasks.Task) error {
	err := g.current(ctx, task.Request)
	if errors.Is(err, ErrGenerationChanged) && !g.readOnly {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		return errors.Join(err, g.tasks.Invalidate(cleanup, task.Request.Owner, task.ID))
	}
	return err
}

func (g *Generation) Submit(ctx context.Context, plan GenerationPlan, preparation []byte) (monthlytasks.Task, error) {
	if g.readOnly {
		return monthlytasks.Task{}, ErrGenerationInput
	}
	// The stored canonical envelope is the restart source, not the caller's
	// later mutations of slices, editable result JSON or current directory.
	preparation = bytes.Clone(preparation)
	p, raw, input, err := canonicalGeneration(plan, preparation, time.Now())
	if err != nil {
		return monthlytasks.Task{}, err
	}
	if !tokenInputPresent(p, input) {
		return monthlytasks.Task{}, ErrGenerationInput
	}
	for _, source := range input.Sources {
		if _, err := g.relativeSource(source.Path); err != nil {
			return monthlytasks.Task{}, err
		}
	}
	request := generationRequest(p, raw)
	if err := g.current(ctx, request); err != nil {
		return monthlytasks.Task{}, err
	}
	return g.tasks.SubmitWithInputs(ctx, request, func(ctx context.Context, tx *sql.Tx, existing bool) error {
		return retainGenerationPlan(ctx, tx, raw, preparation, existing)
	})
}

// Run produces an INTERNAL fully reconciled draft for exactly one organization
// and month. It never publishes, debits/refunds, alters source rows or returns
// a partial organization's bill. A successful draft is not an attestation that
// the asserted normalized balance evidence is semantically complete.
func (g *Generation) Run(parent context.Context, owner monthlytasks.Owner, id string) (artifact GenerationArtifact, err error) {
	select {
	case generationSlots <- struct{}{}:
		defer func() { <-generationSlots }()
	default:
		return artifact, ErrGenerationBusy
	}
	ctx, timeout := context.WithTimeout(parent, 2*time.Minute)
	defer timeout()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	task, err := g.tasks.Get(ctx, owner, id)
	if err != nil {
		return artifact, err
	}
	if err := g.currentTask(ctx, task); err != nil {
		return artifact, err
	}
	ready := task.State == monthlytasks.DraftReady
	if g.readOnly && !ready {
		return artifact, ErrGenerationEvidence
	}
	var lease monthlytasks.Lease
	stopHeartbeat := func() {}
	handoff := false
	if !ready {
		task, lease, err = g.tasks.Claim(ctx, owner, id)
		if err != nil {
			return artifact, err
		}
		stopHeartbeat = g.heartbeat(ctx, cancel, lease)
		defer stopHeartbeat()
		defer func() {
			if err != nil && !handoff {
				cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
				defer stop()
				code := "internal_failure"
				if errors.Is(err, ErrGenerationEvidence) {
					code = "reconciliation_failed"
				} else if errors.Is(err, ErrGenerationIntegrity) {
					code = "evidence_incomplete"
				}
				// Failure to persist cleanup leaves a leased task for bounded
				// recovery. It never turns the original failure into success.
				_ = g.tasks.Fail(cleanup, lease, code)
			}
		}()
	}
	saved, err := g.loadRetainedGeneration(ctx, task.Request, time.Now())
	if err != nil {
		return GenerationArtifact{}, err
	}
	scoped, releaseSource, err := g.bindObservationStore(ctx, saved.plan, saved.input)
	if err != nil {
		return GenerationArtifact{}, err
	}
	g = scoped // Keep the receiver available to deferred failure cleanup on bind errors.
	defer releaseSource()
	if err := g.verifyFiles(ctx, saved.input.Sources); err != nil {
		return GenerationArtifact{}, err
	}
	windows, err := g.auditWindows(ctx, saved.plan)
	if err != nil {
		return GenerationArtifact{}, err
	}
	usageDigest, err := g.auditUsageInput(ctx, saved.plan, saved.input)
	if err != nil {
		return GenerationArtifact{}, err
	}
	walletDigest, err := g.auditWalletInput(ctx, saved.plan, saved.input)
	if err != nil {
		return GenerationArtifact{}, err
	}
	tokenDigest, err := g.auditTokenInput(ctx, saved.plan, saved.input)
	if err != nil {
		return GenerationArtifact{}, err
	}
	if len(saved.artifact) != 0 {
		// Cached money is reusable only against the same freshly audited
		// funding and selected usage observations, including scope/ordering. This
		// doesn't rerun wallet arithmetic or apply financial movements.
		funding, err := monthlysource.ReviewFundingMoney(ctx, saved.plan.Funding, windows)
		if err != nil {
			return GenerationArtifact{}, err
		}
		artifact, err = decodeGenerationArtifact(saved)
		if err != nil {
			return GenerationArtifact{}, err
		}
		encoded, err := json.Marshal(funding)
		if err != nil || generationHash(encoded) != artifact.FundingSHA256 || usageDigest != artifact.UsageSHA256 || walletDigest != artifact.WalletSHA256 || tokenDigest != artifact.TokenSHA256 {
			return GenerationArtifact{}, ErrGenerationIntegrity
		}
		proof, err := g.selectedGenerationReview(ctx, saved.plan, saved.inputRaw, windows, artifact.Reconciliation)
		if err != nil {
			return GenerationArtifact{}, err
		}
		want, wantErr := json.Marshal(artifact.ScopeReview)
		got, gotErr := json.Marshal(proof)
		if wantErr != nil || gotErr != nil || !bytes.Equal(want, got) {
			return GenerationArtifact{}, ErrGenerationIntegrity
		}
	} else {
		if ready { // A ready marker without its durable draft is not recoverable success.
			return GenerationArtifact{}, ErrGenerationIntegrity
		}
		review, err := g.calculate(ctx, saved.plan.Funding, windows, saved.inputRaw, time.Now())
		if err != nil {
			return GenerationArtifact{}, err
		}
		proof, err := g.selectedGenerationReview(ctx, saved.plan, saved.inputRaw, windows, review.Reconciliation)
		if err != nil {
			return GenerationArtifact{}, err
		}
		if !generationArtifactReconciled(saved.plan, review.Reconciliation, proof) {
			return GenerationArtifact{}, ErrGenerationEvidence
		}
		funding, err := json.Marshal(review.Funding)
		if err != nil {
			return GenerationArtifact{}, err
		}
		review.Reconciliation.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
		artifact = GenerationArtifact{Version: 1, Mode: "local_generation_draft_not_publication", PlanSHA256: task.Request.InputSHA256,
			FundingSHA256: generationHash(funding), UsageSHA256: usageDigest, WalletSHA256: walletDigest, TokenSHA256: tokenDigest, Reconciliation: review.Reconciliation, ScopeReview: proof}
		saved.artifact, err = json.Marshal(artifact)
		if err != nil || len(saved.artifact) > generationArtifactLimit {
			return GenerationArtifact{}, ErrGenerationCapacity
		}
		saved.digest = generationHash(saved.artifact)
	}
	// Detect replaced evidence and policy changes across long audit/calculation.
	if err := g.verifyFiles(ctx, saved.input.Sources); err != nil {
		return GenerationArtifact{}, err
	}
	stopHeartbeat()
	if cause := context.Cause(ctx); cause != nil {
		return GenerationArtifact{}, cause
	}
	if !ready {
		if err := g.tasks.Renew(ctx, lease); err != nil {
			return GenerationArtifact{}, err
		}
	}
	if err := g.currentTask(ctx, task); err != nil {
		return GenerationArtifact{}, err
	}
	if !ready {
		if err := saveGenerationArtifact(ctx, g.receiver, task.Request.InputSHA256, saved.artifact); err != nil {
			return GenerationArtifact{}, err
		}
		// Artifact commit and task completion are separate durable steps. A
		// failed/ambiguous completion preserves the artifact and running task.
		// Recovery re-audits input, reuses this draft and fences the old lease.
		handoff = true
		if err := g.tasks.Complete(ctx, lease, saved.digest); err != nil {
			return GenerationArtifact{}, err
		}
	}
	if err := g.currentTask(ctx, task); err != nil {
		return GenerationArtifact{}, err
	}
	final, err := g.tasks.Get(ctx, owner, id)
	if err != nil || final.State != monthlytasks.DraftReady || final.ResultSHA256 != saved.digest {
		return GenerationArtifact{}, errors.Join(ErrGenerationIntegrity, err)
	}
	return artifact, nil
}

func (g *Generation) heartbeat(ctx context.Context, cancel context.CancelCauseFunc, lease monthlytasks.Lease) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(8 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := g.tasks.Renew(ctx, lease); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(stop); <-done }) }
}

func (g *Generation) auditWindows(ctx context.Context, p GenerationPlan) ([]monthlysource.FundingWindowObservation, error) {
	if err := g.requireObservationStore(p); err != nil {
		return nil, err
	}
	windows := make([]monthlysource.FundingWindowObservation, 0, len(p.Windows))
	rows := 0
	for _, w := range p.Windows {
		observed, err := monthlysource.ReviewFundingWindow(ctx, g.funding, w.OlderScanID, w.NewerScanID)
		if err != nil {
			return nil, err
		}
		c := observed.Comparison()
		if c.OlderHeadID != w.OlderHeadID || c.NewerHeadID != w.NewerHeadID || c.From != w.From || c.To != w.To || c.SourceIdentity != p.Funding.SourceIdentity {
			return nil, ErrGenerationIntegrity
		}
		rows += c.OlderRows + c.NewerRows
		if rows > 100000 {
			return nil, ErrGenerationCapacity
		}
		windows = append(windows, observed)
	}
	return windows, nil
}

func decodeGenerationArtifact(s generationRetained) (GenerationArtifact, error) {
	var a GenerationArtifact
	if json.Unmarshal(s.artifact, &a) != nil {
		return a, ErrGenerationIntegrity
	}
	canonical, err := json.Marshal(a)
	if err != nil || !bytes.Equal(canonical, s.artifact) || a.Version != 1 || a.Mode != "local_generation_draft_not_publication" ||
		a.PlanSHA256 != generationHash(s.planRaw) || !generationHex(a.FundingSHA256, 32) || !generationArtifactReconciled(s.plan, a.Reconciliation, a.ScopeReview) ||
		(!planHasUsage(s.plan) && a.UsageSHA256 != "") || (planHasUsage(s.plan) && !generationHex(a.UsageSHA256, 32)) ||
		(s.plan.WalletObservations == nil && a.WalletSHA256 != "") || (s.plan.WalletObservations != nil && !generationHex(a.WalletSHA256, 32)) ||
		(len(s.plan.TokenUsageSources) == 0 && a.TokenSHA256 != "") || (len(s.plan.TokenUsageSources) != 0 && !generationHex(a.TokenSHA256, 32)) ||
		a.Reconciliation.SourceIntegrity != "retained_files_sha256_verified_not_fact_attestation" {
		return GenerationArtifact{}, ErrGenerationIntegrity
	}
	return a, nil
}

func (g *Generation) relativeSource(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", ErrGenerationInput
	}
	rel, err := filepath.Rel(g.root.Name(), path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", ErrGenerationInput
	}
	return rel, nil
}

func (g *Generation) verifyFiles(parent context.Context, sources []monitor.MonthlyPreparationSource) error {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	var planned int64
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := g.relativeSource(source.Path)
		if err != nil {
			return errors.Join(ErrGenerationIntegrity, err)
		}
		info, err := g.root.Lstat(rel)
		if err != nil || !info.Mode().IsRegular() {
			return ErrGenerationIntegrity
		}
		if info.Size() < 0 || info.Size() > generationEvidenceLimit-planned {
			return ErrGenerationCapacity
		}
		planned += info.Size()
	}
	var total int64
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := g.verifyFile(ctx, source, generationEvidenceLimit-total)
		if err != nil {
			if errors.Is(err, ErrGenerationCapacity) {
				return ErrGenerationCapacity
			}
			return errors.Join(ErrGenerationIntegrity, err)
		}
		total += n
	}
	return g.verifyBoundObservationStore(ctx, sources)
}

func (g *Generation) verifyFile(ctx context.Context, source monitor.MonthlyPreparationSource, remaining int64) (int64, error) {
	rel, err := g.relativeSource(source.Path)
	if err != nil {
		return 0, err
	}
	before, err := g.root.Lstat(rel)
	if err != nil || !before.Mode().IsRegular() {
		return 0, ErrGenerationIntegrity
	}
	if remaining < 0 || before.Size() > remaining {
		return 0, ErrGenerationCapacity
	}
	// Root fences path traversal; NOFOLLOW/NONBLOCK also prevent a final-path
	// swap into a symlink, FIFO or device from hanging source verification.
	f, err := g.root.OpenFile(rel, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return 0, ErrGenerationIntegrity
	}
	h := sha256.New()
	buffer := make([]byte, 64<<10)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		// Read at most one byte beyond the remaining budget to detect growth
		// between stat and read without allowing a further full-buffer read.
		readSize := int64(len(buffer))
		if remaining-size+1 < readSize {
			readSize = remaining - size + 1
		}
		n, err := f.Read(buffer[:readSize])
		size += int64(n)
		if size > remaining {
			return 0, ErrGenerationCapacity
		}
		_, _ = h.Write(buffer[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	after, err := f.Stat()
	if err != nil {
		return 0, err
	}
	current, err := g.root.Lstat(rel)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(after, current) || size != before.Size() || size != after.Size() ||
		!before.ModTime().Equal(after.ModTime()) || hex.EncodeToString(h.Sum(nil)) != source.SHA256 {
		return 0, ErrGenerationIntegrity
	}
	return size, nil
}
