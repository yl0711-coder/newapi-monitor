package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type generationFixture struct {
	g                        *Generation
	receiver, funding        *sql.DB
	root, receiverPath, path string
	plan                     GenerationPlan
	input                    monitor.MonthlyPreparationInput
	raw                      []byte
	sourceHash               string
}

func generationDB(t *testing.T, path string, readOnly bool) *sql.DB {
	t.Helper()
	mode := "rwc"
	if readOnly {
		mode = "ro"
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode="+mode+"&_pragma=busy_timeout(500)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func generationFixtureNew(t *testing.T) generationFixture {
	t.Helper()
	req, windows, input, path := draftFixtureRetained(t)
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	// This retained synthetic source is NOT an automatic semantic attestation.
	// The test asserts that the artifact explicitly retains that limitation.
	body, err := json.Marshal(input.Accounts)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "independent-evidence.json")
	if err := os.WriteFile(file, body, 0600); err != nil {
		t.Fatal(err)
	}
	input.Sources[0].Path, input.Sources[0].SHA256 = file, generationHash(body)
	raw := draftJSON(t, input)
	plan := GenerationPlan{Version: 1, Owner: monthlytasks.Owner{GroupID: 1, AuthVersion: 1,
		MemberFingerprint: generationMemberHash(req.UserIDs), MemberManifest: strings.Repeat("b", 64)}, Funding: req, PreparationSHA256: generationHash(raw)}
	for _, o := range windows {
		c := o.Comparison()
		plan.Windows = append(plan.Windows, monthlysource.FundingMonthWindowVersion{From: c.From, To: c.To,
			OlderScanID: c.OlderScanID, NewerScanID: c.NewerScanID, OlderHeadID: c.OlderHeadID, NewerHeadID: c.NewerHeadID})
	}
	receiverPath := filepath.Join(root, "receiver.sqlite")
	receiver, funding := generationDB(t, receiverPath, false), generationDB(t, path, true)
	if err := InitGenerationReceiver(context.Background(), receiver); err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(receiver, funding, root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return generationFixture{g, receiver, funding, root, receiverPath, path, plan, input, raw, generationHash(source)}
}

func generationSubmit(t *testing.T, f generationFixture) monthlytasks.Task {
	t.Helper()
	j, err := f.g.Submit(context.Background(), f.plan, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func assertGenerationAmounts(t *testing.T, a GenerationArtifact, knownRequests ...int64) {
	t.Helper()
	if a.Mode != "local_generation_draft_not_publication" || a.Reconciliation.SourceIntegrity != "retained_files_sha256_verified_not_fact_attestation" {
		t.Fatal("internal draft promoted to published or attested bill")
	}
	want := []monitor.MonthlyPreparationAmounts{
		{Opening: "1000.000000", Increase: "10.000000", Decrease: "-800.000000", Consume: "3.000000", Refund: "1.000000", Closing: "208.000000"},
		{Opening: "1000.000000", Increase: "800.000000", Decrease: "0.000000", Consume: "3.000000", Refund: "1.000000", Closing: "1798.000000"},
	}
	if len(a.Reconciliation.Accounts) != 2 {
		t.Fatal("partial organization")
	}
	for i, member := range a.Reconciliation.Accounts {
		row := member.Months[0]
		requestsMatch := row.Requests == nil
		if len(knownRequests) > 0 {
			requestsMatch = row.Requests != nil && *row.Requests == knownRequests[0]
		}
		if row.Amounts == nil || *row.Amounts != want[i] || !requestsMatch || row.Tokens != nil {
			t.Fatal("money or unknown auxiliary stats changed", member)
		}
	}
}

func TestGenerationEndToEndReopenAndImmutableScope(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	j := generationSubmit(t, f)
	shuffled := f.plan
	shuffled.Funding.UserIDs = []int64{202, 201}
	shuffled.Windows = append([]monthlysource.FundingMonthWindowVersion(nil), f.plan.Windows...)
	for i, k := 0, len(shuffled.Windows)-1; i < k; i, k = i+1, k-1 {
		shuffled.Windows[i], shuffled.Windows[k] = shuffled.Windows[k], shuffled.Windows[i]
	}
	reused, err := f.g.Submit(ctx, shuffled, f.raw)
	if err != nil || !reflect.DeepEqual(j, reused) || !bytes.Equal(f.raw, draftJSON(t, f.input)) {
		t.Fatal("canonical request dedup or caller isolation", err)
	}
	artifact, err := f.g.Run(ctx, j.Request.Owner, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	if err := f.g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := generationDB(t, f.receiverPath, false)
	g, err := NewGeneration(reopened, f.funding, f.root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		return Review{}, errors.New("completed draft must not rerun wallet calculation")
	}
	after, err := g.Run(ctx, j.Request.Owner, j.ID)
	if err != nil || !reflect.DeepEqual(artifact, after) {
		t.Fatal("durable draft changed or recalculated", err)
	}
	badOwner := j.Request.Owner
	badOwner.AuthVersion++
	if _, err := g.Run(ctx, badOwner, j.ID); !errors.Is(err, monthlytasks.ErrNotFound) {
		t.Fatal("foreign authorization read draft", err)
	}
	before, err := os.ReadFile(f.path)
	if err != nil || generationHash(before) != f.sourceHash {
		t.Fatal("funding receiver changed", err)
	}
	for _, query := range []string{
		`UPDATE portal_monthly_generation_inputs SET preparation='{}'`,
		`UPDATE portal_monthly_generation_inputs SET artifact='{}'`,
		`DELETE FROM portal_monthly_generation_inputs`,
	} {
		if _, err := reopened.Exec(query); err == nil {
			t.Fatal("immutable evidence was mutable", query)
		}
	}
	if err := os.WriteFile(f.input.Sources[0].Path, []byte("changed after draft completed"), 0600); err != nil {
		t.Fatal(err)
	}
	if a, err := g.Run(ctx, j.Request.Owner, j.ID); !errors.Is(err, ErrGenerationIntegrity) || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("cached draft bypassed source file verification", err)
	}
}

func TestGenerationArtifactCommitBeforeTaskCompletionRecovery(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	j := generationSubmit(t, f)
	if _, err := f.receiver.Exec(`CREATE TRIGGER simulate_completion_failure BEFORE UPDATE ON portal_monthly_draft_tasks
WHEN NEW.state='draft_ready' BEGIN SELECT RAISE(ABORT,'synthetic interrupted completion'); END`); err != nil {
		t.Fatal(err)
	}
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); err == nil || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("interrupted completion returned success", err)
	}
	got, err := f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || got.State != monthlytasks.Running || got.ResultSHA256 != "" {
		t.Fatal("failed completion advanced task", got, err)
	}
	saved, err := loadGeneration(ctx, f.receiver, j.Request, time.Now())
	if err != nil || len(saved.artifact) == 0 {
		t.Fatal("durable artifact lost", err)
	}
	// Advance this synthetic lease beyond expiry without sleeping. This is
	// white-box time simulation; OS process-exit fencing is tested in tasks.
	if _, err := f.receiver.Exec(`DROP TRIGGER simulate_completion_failure;
UPDATE portal_monthly_draft_tasks SET created_at=created_at-60000,updated_at=updated_at-60000,started_at=started_at-60000,lease_until=lease_until-60000`); err != nil {
		t.Fatal(err)
	}
	if err := f.g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(f.receiver, f.funding, f.root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		return Review{}, errors.New("recovery must reuse committed artifact")
	}
	a, err := g.Run(ctx, j.Request.Owner, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, a)
	got, err = g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || got.Attempts != 2 || got.State != monthlytasks.DraftReady || got.ResultSHA256 != saved.digest {
		t.Fatal("recovery duplicated/overwrote draft", got, err)
	}
}

func TestGenerationDoesNotPublishPartialOrganizationOrReplacedEvidence(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	input := draftCopy(t, f.input)
	input.Accounts[0].Snapshots[0].BalanceQuota++
	f.raw, f.plan.PreparationSHA256 = draftJSON(t, input), generationHash(draftJSON(t, input))
	j := generationSubmit(t, f)
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); !errors.Is(err, ErrGenerationEvidence) || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("single bad member returned partial successful bill", err)
	}
	failed, err := f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || failed.State != monthlytasks.Failed || failed.FailureCode != "reconciliation_failed" {
		t.Fatal(failed, err)
	}
	f.raw = draftJSON(t, f.input)
	f.plan.PreparationSHA256 = generationHash(f.raw)
	j = generationSubmit(t, f)
	original, err := os.ReadFile(f.input.Sources[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.input.Sources[0].Path, []byte("changed same path"), 0600); err != nil {
		t.Fatal(err)
	}
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); !errors.Is(err, ErrGenerationIntegrity) || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("replaced evidence used", err)
	}
	saved, err := loadGeneration(ctx, f.receiver, j.Request, time.Now())
	if err != nil || len(saved.artifact) != 0 {
		t.Fatal("failed verification retained successful draft", err)
	}
	if got := generationSubmit(t, f); got.ID != j.ID || got.State != monthlytasks.Failed || got.Attempts != 1 {
		t.Fatal("refresh silently retried failed generation", got)
	}
	if err := os.WriteFile(f.input.Sources[0].Path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.g.tasks.Retry(ctx, j.Request.Owner, j.ID); !errors.Is(err, monthlytasks.ErrCooldown) {
		t.Fatal("generation retry skipped cooldown", err)
	}
	// White-box clock advancement on synthetic FAILED metadata only. The
	// amounts, plan and source pages stay unchanged.
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_draft_tasks SET created_at=created_at-31000,
updated_at=updated_at-31000,retry_at=retry_at-31000 WHERE id=?`, j.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.g.tasks.Retry(ctx, j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	a, err := f.g.Run(ctx, j.Request.Owner, j.ID)
	if err != nil {
		t.Fatal("restored evidence cannot recover", err)
	}
	assertGenerationAmounts(t, a)
}

func TestGenerationCurrentPolicyBeforeAndAfterWork(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	j := generationSubmit(t, f)
	calculate := f.g.calculate
	f.g.calculate = func(ctx context.Context, request monthlysource.FundingMonthRequest, windows []monthlysource.FundingWindowObservation, raw []byte, now time.Time) (Review, error) {
		r, err := calculate(ctx, request, windows, raw, now)
		f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
		return r, err
	}
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); !errors.Is(err, ErrGenerationChanged) || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("scope changed during calculation", err)
	}
	state, err := f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || state.State != monthlytasks.Invalidated {
		t.Fatal("changed scope still usable", state, err)
	}
	// A transient authorization read is not proof of revocation. It must
	// return no draft, without converting the queued task into invalidated.
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return nil }
	f.plan.Owner.AuthVersion++
	j = generationSubmit(t, f)
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error {
		return errors.New("synthetic temporary local read failure")
	}
	if _, err := f.g.Run(ctx, j.Request.Owner, j.ID); err == nil {
		t.Fatal("unavailable authorization was allowed")
	}
	state, err = f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || state.State != monthlytasks.Queued || state.Attempts != 0 {
		t.Fatal("transient read revoked scope or started work", state, err)
	}
}

func TestGenerationPlanValidationAndLocalFileBoundary(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	for name, mutate := range map[string]func(*GenerationPlan){
		"foreign owner":    func(p *GenerationPlan) { p.Owner.GroupID++ },
		"foreign members":  func(p *GenerationPlan) { p.Funding.UserIDs = []int64{201, 999} },
		"duplicate member": func(p *GenerationPlan) { p.Funding.UserIDs = []int64{201, 201} },
		"wrong month":      func(p *GenerationPlan) { p.Funding.Month = "2026-08" },
		"missing interval": func(p *GenerationPlan) { p.Windows = p.Windows[1:] },
		"wrong digest":     func(p *GenerationPlan) { p.PreparationSHA256 = strings.Repeat("a", 64) },
		"wrong rules":      func(p *GenerationPlan) { p.Funding.Rules.QuotaPerUnit++ },
	} {
		t.Run(name, func(t *testing.T) {
			p := f.plan
			mutate(&p)
			if _, err := f.g.Submit(ctx, p, f.raw); err == nil {
				t.Fatal("invalid generation plan persisted")
			}
		})
	}
	input := draftCopy(t, f.input)
	input.Sources[0].Path = filepath.Join(filepath.Dir(f.root), "outside.json")
	raw := draftJSON(t, input)
	p := f.plan
	p.PreparationSHA256 = generationHash(raw)
	if _, err := f.g.Submit(ctx, p, raw); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("source escaped private root", err)
	}
	symlink := filepath.Join(f.root, "symlink.json")
	if err := os.Symlink(f.input.Sources[0].Path, symlink); err != nil {
		t.Fatal(err)
	}
	source := f.input.Sources[0]
	source.Path = symlink
	if err := f.g.verifyFiles(ctx, []monitor.MonthlyPreparationSource{source}); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("symlink source accepted", err)
	}
	if _, err := NewGeneration(f.receiver, f.receiver, f.root, f.g.verifyCurrent); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("same receiver/source handle allowed", err)
	}
	alias := generationDB(t, f.path, false)
	if _, err := NewGeneration(alias, f.funding, f.root, f.g.verifyCurrent); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("distinct handles to same source file allowed", err)
	}
	if _, err := NewGeneration(f.receiver, f.funding, f.root, nil); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("missing current authorization/version guard")
	}
}

func TestGenerationPinnedHeadsAndEvidenceChangeDuringWork(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	bad := f.plan
	bad.Windows = append([]monthlysource.FundingMonthWindowVersion(nil), bad.Windows...)
	bad.Windows[0].NewerHeadID = strings.Repeat("0", 64)
	j, err := f.g.Submit(ctx, bad, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); !errors.Is(err, ErrGenerationIntegrity) || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("different retained head accepted", err)
	}
	j = generationSubmit(t, f)
	calculate := f.g.calculate
	f.g.calculate = func(ctx context.Context, request monthlysource.FundingMonthRequest, windows []monthlysource.FundingWindowObservation, raw []byte, now time.Time) (Review, error) {
		r, err := calculate(ctx, request, windows, raw, now)
		if err == nil {
			err = os.WriteFile(f.input.Sources[0].Path, []byte("source changed during wallet calculation"), 0600)
		}
		return r, err
	}
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); !errors.Is(err, ErrGenerationIntegrity) || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("source replacement during calculation passed", err)
	}
	saved, err := loadGeneration(ctx, f.receiver, j.Request, time.Now())
	if err != nil || len(saved.artifact) != 0 {
		t.Fatal("source changed but successful draft persisted", err)
	}
}

func TestGenerationCancellationBusyAndFailedArtifactWrite(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	j := generationSubmit(t, f)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := f.g.Run(canceled, j.Request.Owner, j.ID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	generationSlots <- struct{}{}
	_, err := f.g.Run(ctx, j.Request.Owner, j.ID)
	<-generationSlots
	if !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("unbounded generation concurrency", err)
	}
	if _, err := f.receiver.Exec(`CREATE TRIGGER simulate_artifact_failure BEFORE UPDATE ON portal_monthly_generation_inputs
BEGIN SELECT RAISE(ABORT,'synthetic result storage failure'); END`); err != nil {
		t.Fatal(err)
	}
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); err == nil || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("failed artifact write returned success", err)
	}
	saved, err := loadGeneration(ctx, f.receiver, j.Request, time.Now())
	if err != nil || len(saved.artifact) != 0 {
		t.Fatal("partial draft leaked", err)
	}
	state, err := f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || state.State != monthlytasks.Failed || state.ResultSHA256 != "" {
		t.Fatal("failed write state", state, err)
	}
}

func TestGenerationPostCompletionRevocationAndCorruptCache(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	j := generationSubmit(t, f)
	calls := 0
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error {
		calls++
		if calls == 3 { // First run: before claim, before completion, after completion.
			return ErrGenerationChanged
		}
		return nil
	}
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); !errors.Is(err, ErrGenerationChanged) || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("revoked scope returned a completed draft", err)
	}
	state, err := f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || state.State != monthlytasks.Invalidated || state.ResultSHA256 != "" {
		t.Fatal("post-completion revocation", state, err)
	}
	f.plan.Owner.AuthVersion++
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return nil }
	j = generationSubmit(t, f)
	if _, err := f.g.Run(ctx, j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	// Explicit corruption injection in the isolated receiver, bypassing the
	// production immutability trigger only in this test.
	if _, err := f.receiver.Exec(`DROP TRIGGER monthly_generation_no_replace;
UPDATE portal_monthly_generation_inputs SET artifact='{}' WHERE id=?`, j.Request.InputSHA256); err != nil {
		t.Fatal(err)
	}
	if a, err := f.g.Run(ctx, j.Request.Owner, j.ID); !errors.Is(err, ErrGenerationIntegrity) || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("corrupted cache was trusted", err)
	}
}

func TestGenerationHeartbeatStopsAfterLeaseLoss(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	j := generationSubmit(t, f)
	_, lease, err := f.g.tasks.Claim(ctx, j.Request.Owner, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	work, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := f.g.heartbeat(work, cancel, lease)
	defer stop()
	if err := f.g.tasks.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-work.Done():
		if !errors.Is(context.Cause(work), monthlytasks.ErrLeaseLost) {
			t.Fatal("heartbeat did not propagate lease loss", context.Cause(work))
		}
	case <-time.After(12 * time.Second):
		t.Fatal("lost lease left generation running")
	}
	stop() // idempotent shutdown, no detached renewal goroutine
}

func TestGenerationReceiverBoundAndUninitializedFailure(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	// Terminal tasks remain retained; admission must not evict them or leave
	// a new queued task when the input receiver is at its record limit.
	for i := range generationRecordLimit {
		f.plan.Owner.AuthVersion = int64(i + 1)
		j := generationSubmit(t, f)
		if err := f.g.tasks.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
			t.Fatal(err)
		}
	}
	f.plan.Owner.AuthVersion++
	if _, err := f.g.Submit(ctx, f.plan, f.raw); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("unbounded retained manifests", err)
	}
	var count int
	if err := f.receiver.QueryRow(`SELECT COUNT(*) FROM portal_monthly_generation_inputs`).Scan(&count); err != nil || count != generationRecordLimit {
		t.Fatal("receiver size limit", count, err)
	}
	if err := f.receiver.QueryRow(`SELECT COUNT(*) FROM portal_monthly_draft_tasks`).Scan(&count); err != nil || count != generationRecordLimit {
		t.Fatal("capacity rejection left an unusable queued task", count, err)
	}
	if err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return generationCapacity(ctx, tx, 0, generationReceiverLimit)
	}); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("receiver byte bound", err)
	}
	blank := generationDB(t, filepath.Join(f.root, "uninitialized.sqlite"), false)
	// Open/identify the synthetic SQLite file without feature initialization.
	if _, err := blank.Exec(`CREATE TABLE unrelated (value INTEGER)`); err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(blank, f.funding, f.root, f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err := g.Submit(ctx, f.plan, f.raw); err == nil {
		t.Fatal("generation automatically initialized receiver")
	}
}
