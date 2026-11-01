package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func capturedSpoolTest(t *testing.T, p *WalletSourcePreparation) *CapturedSourceSpool {
	t.Helper()
	s, err := NewCapturedSourceSpool(p, privateCapturedOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(s.directory)
	if err != nil || len(entries) != 0 {
		t.Fatal("constructor wrote files", err)
	}
	return s
}

func capturedSpoolAttemptNames(t *testing.T, s *CapturedSourceSpool, req SourcePreparationRequest) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.directory, s.jobName(req)))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestCapturedSourceSpoolCreatesReusesAndDefersNextStage(t *testing.T) {
	f, _, p, req, driver := capturedFilesFixture(t)
	s := capturedSpoolTest(t, p)
	calls := 0
	var selected CapturedSourceFiles
	next := func(_ context.Context, got SourcePreparationRequest, files CapturedSourceFiles) (bool, error) {
		calls++
		if got != req {
			t.Fatal("wrong owner/month")
		}
		selected = files
		return false, nil // Sealed observations are NOT enough for publication.
	}
	step, err := s.Before(next)
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := step(t.Context(), req); err != nil || ready || calls != 0 {
		t.Fatal("new seal crossed stage boundary", ready, calls, err)
	}
	if got := capturedSpoolAttemptNames(t, s, req); !reflect.DeepEqual(got, []string{"seal-1"}) {
		t.Fatal(got)
	}
	restarted, err := NewCapturedSourceSpool(p, s.directory)
	if err != nil {
		t.Fatal(err)
	}
	step, err = restarted.Before(next)
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := step(t.Context(), req); err != nil || ready || calls != 1 || selected.Material.SHA256 == "" {
		t.Fatal("restart did not reuse", calls, err)
	}
	again, wrote, err := restarted.Advance(t.Context(), req)
	if err != nil || wrote || !reflect.DeepEqual(again, selected) {
		t.Fatal("duplicate request rewrote source", err)
	}
	// Source identity is part of the local key, not an editable customer alias.
	before := s.jobName(req)
	p.funding.sourceIdentity = strings.Repeat("c", 64)
	if s.jobName(req) == before {
		t.Fatal("source not included in key")
	}
	if driver.queries.Load() != 0 || driver.writes.Load() != 0 || driver.pings.Load() != 0 {
		t.Fatal("local seal hit upstream")
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

func TestCapturedSourceSpoolRetryPersistsAndStopsAtThree(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	s := capturedSpoolTest(t, p)
	writes := 0
	fail := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
		writes++
		if err := writeArchiveFile(ctx, root, name, raw); err != nil {
			return err
		}
		return errors.New("synthetic disk interruption")
	}
	var original []byte
	for attempt := 1; attempt <= 3; attempt++ {
		var err error
		s, err = NewCapturedSourceSpool(p, s.directory)
		if err != nil {
			t.Fatal(err)
		}
		files, wrote, err := s.advance(t.Context(), req, fail)
		if err == nil || wrote || files.Material.SHA256 != "" || writes != attempt {
			t.Fatal("failed work retried within one step", attempt, writes, err)
		}
		if got := len(capturedSpoolAttemptNames(t, s, req)); got != attempt {
			t.Fatal("restart reset attempt budget", got)
		}
		first, err := os.ReadFile(filepath.Join(s.directory, s.jobName(req), "seal-1", "selection.json"))
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 1 {
			original = first
		} else if !bytes.Equal(original, first) {
			t.Fatal("failed evidence overwritten")
		}
	}
	for i := 0; i < 2; i++ {
		if _, _, err := s.advance(t.Context(), req, fail); !errors.Is(err, ErrGenerationCapacity) {
			t.Fatal("attempt cap ignored", err)
		}
	}
	if writes != 3 || len(capturedSpoolAttemptNames(t, s, req)) != 3 {
		t.Fatal("unbounded retries")
	}
}

func TestCapturedSourceSpoolFirstWriteFailureAlsoExhaustsDurableBudget(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	s := capturedSpoolTest(t, p)
	calls := 0
	fail := func(context.Context, *os.Root, string, []byte) error {
		calls++
		return errors.New("synthetic first-write failure")
	}
	for i := 0; i < 3; i++ {
		var err error
		s, err = NewCapturedSourceSpool(p, s.directory)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.advance(t.Context(), req, fail); err == nil {
			t.Fatal("first write unexpectedly succeeded")
		}
	}
	if _, _, err := s.advance(t.Context(), req, fail); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("empty failure retried indefinitely", err)
	}
	if calls != 3 || len(capturedSpoolAttemptNames(t, s, req)) != 3 {
		t.Fatal("empty directories reset attempt count", calls)
	}
}

func TestCapturedSourceSpoolCrashCountsAttemptAndPreservesPartial(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	s := capturedSpoolTest(t, p)
	job := filepath.Join(s.directory, s.jobName(req))
	if err := os.Mkdir(job, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(job, "seal-1"), 0700); err != nil {
		t.Fatal(err)
	}
	// A crash immediately after mkdir consumes the reserved attempt too.
	ctx, cancel := context.WithCancel(t.Context())
	interrupt := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
		if err := writeArchiveFile(ctx, root, name, raw); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	}
	if _, _, err := s.advance(ctx, req, interrupt); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	first, err := os.ReadFile(filepath.Join(job, "seal-2", "selection.json"))
	if err != nil {
		t.Fatal(err)
	}
	files, wrote, err := s.Advance(t.Context(), req)
	if err != nil || !wrote || filepath.Base(filepath.Dir(files.Material.Path)) != "seal-3" {
		t.Fatal("partial attempt not isolated", files, err)
	}
	retained, err := os.ReadFile(filepath.Join(job, "seal-2", "selection.json"))
	if err != nil || !bytes.Equal(first, retained) {
		t.Fatal("partial original lost", err)
	}
	if len(capturedSpoolAttemptNames(t, s, req)) != 3 {
		t.Fatal("extra attempt created")
	}
}

func TestCapturedSourceSpoolCannotReplaceDamagedCompleteOrRevokedScope(t *testing.T) {
	_, auth, p, req, _ := capturedFilesFixture(t)
	s := capturedSpoolTest(t, p)
	files, _, err := s.Advance(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.Material.Path, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Advance(t.Context(), req); err == nil {
		t.Fatal("damaged completed evidence hidden by new attempt")
	}
	if len(capturedSpoolAttemptNames(t, s, req)) != 1 {
		t.Fatal("replaced completed source")
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Advance(t.Context(), req); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("revoked owner reused source", err)
	}
	if len(capturedSpoolAttemptNames(t, s, req)) != 1 {
		t.Fatal("revocation wrote new attempt")
	}
}

func TestCapturedSourceSpoolRootIsolationLocksAndUnreadyNoWrites(t *testing.T) {
	for _, scenario := range []string{"root_moved", "lock", "unready", "wrong_owner"} {
		t.Run(scenario, func(t *testing.T) {
			var p *WalletSourcePreparation
			var req SourcePreparationRequest
			if scenario == "unready" {
				f, _, _, wallet, _ := walletPreparationFixture(t)
				p = wallet
				req = SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
			} else {
				_, _, p, req, _ = capturedFilesFixture(t)
			}
			s := capturedSpoolTest(t, p)
			switch scenario {
			case "root_moved":
				if err := os.Rename(s.directory, s.directory+"-retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(s.directory, 0700); err != nil {
					t.Fatal(err)
				}
			case "lock":
				root, lock, err := openCapturedOutput(s.directory)
				if err != nil {
					t.Fatal(err)
				}
				defer root.Close()
				defer lock.Close()
			case "wrong_owner":
				req.Owner.GroupID++
			}
			files, wrote, err := s.Advance(t.Context(), req)
			if err == nil || wrote || files.Material.SHA256 != "" {
				t.Fatal("invalid operation accepted", err)
			}
			if scenario == "lock" && !errors.Is(err, ErrGenerationBusy) {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(s.directory)
			if err != nil || len(entries) != 0 {
				t.Fatal("invalid operation allocated output", err)
			}
		})
	}
}

func TestCapturedSourceSpoolCapacityCountsFailuresAndAllowsReuse(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	s := capturedSpoolTest(t, p)
	files, _, err := s.Advance(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	root, err := openArchiveRoot(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	inventory, err := inspectCapturedSpool(t.Context(), root)
	if err != nil || inventory.jobs != 1 || inventory.bytes <= 0 {
		t.Fatal("inventory did not count retained bytes", inventory, err)
	}
	s.limit = inventory.bytes // No free capacity, existing complete input still works.
	got, wrote, err := s.Advance(t.Context(), req)
	if err != nil || wrote || got != files {
		t.Fatal("full spool prevented read-only reuse", err)
	}
	// Source scope is immutable; filling other slots must not overwrite this job.
	for i := 0; i < capturedSpoolJobs-1; i++ {
		name := "job-" + generationHash([]byte("synthetic other request "+stringID(int64(i))))
		if err := os.Mkdir(filepath.Join(s.directory, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	inventory, err = inspectCapturedSpool(t.Context(), root)
	if err != nil || inventory.jobs != capturedSpoolJobs {
		t.Fatal(inventory, err)
	}
	if got, _, err := s.Advance(t.Context(), req); err != nil || got != files {
		t.Fatal("slot limit prevented existing reuse", err)
	}
	// A partial attempt's files occupy the SAME capacity, not free ignored bytes.
	partial := filepath.Join(s.directory, "job-"+generationHash([]byte("synthetic other request 0")), "seal-1")
	if err := os.Mkdir(partial, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partial, "selection.json"), bytes.Repeat([]byte("x"), 8192), 0600); err != nil {
		t.Fatal(err)
	}
	after, err := inspectCapturedSpool(t.Context(), root)
	if err != nil || after.bytes < inventory.bytes+8192 {
		t.Fatal("failure bytes ignored", inventory, after, err)
	}
	if got, _, err := s.Advance(t.Context(), req); err != nil || got != files {
		t.Fatal("another job's incomplete content affected this evidence", err)
	}
}

func TestCapturedSourceSpoolConcurrentOwnersDoNotDuplicateAttempt(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	first := capturedSpoolTest(t, p)
	second, err := NewCapturedSourceSpool(p, first.directory)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		files CapturedSourceFiles
		wrote bool
		err   error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for _, s := range []*CapturedSourceSpool{first, second} {
		go func(s *CapturedSourceSpool) {
			<-start
			files, wrote, err := s.Advance(t.Context(), req)
			results <- outcome{files, wrote, err}
		}(s)
	}
	close(start)
	writes := 0
	var selected CapturedSourceFiles
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			if !errors.Is(r.err, ErrGenerationBusy) {
				t.Fatal(r.err)
			}
			continue
		}
		if selected.Material.SHA256 != "" && selected != r.files {
			t.Fatal("concurrent work chose different sources")
		}
		selected = r.files
		if r.wrote {
			writes++
		}
	}
	if writes != 1 || selected.Material.SHA256 == "" || len(capturedSpoolAttemptNames(t, first, req)) != 1 {
		t.Fatal("duplicate or lost output", "writes", writes, "material", selected.Material.SHA256, "attempts", capturedSpoolAttemptNames(t, first, req))
	}
}

func TestCapturedSourceSpoolCapacityBeforeAllocatingAnotherJob(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	for _, scenario := range []string{"bytes", "jobs"} {
		t.Run(scenario, func(t *testing.T) {
			s := capturedSpoolTest(t, p)
			want := 0
			if scenario == "bytes" {
				s.limit = 4 << 20
			} else {
				want = capturedSpoolJobs
				for i := 0; i < capturedSpoolJobs; i++ {
					if err := os.Mkdir(filepath.Join(s.directory, "job-"+generationHash([]byte(stringID(int64(i))))), 0700); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, _, err := s.Advance(t.Context(), req); !errors.Is(err, ErrGenerationCapacity) {
				t.Fatal("capacity bypassed", err)
			}
			entries, err := os.ReadDir(s.directory)
			if err != nil || len(entries) != want {
				t.Fatal("capacity failure allocated files", err)
			}
		})
	}
}

func TestCapturedSourceSpoolQueueCannotApproveOnlyASeal(t *testing.T) {
	f, _, p, req, _ := capturedFilesFixture(t)
	s := capturedSpoolTest(t, p)
	calls := 0
	step, err := ComposeCapturedSourcePreparation(p, s, func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (bool, error) {
		calls++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewControlledLocalRuntime(f.g, p.funding.authorization, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: req.Owner.GroupID, Month: req.Month}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.EnableOnDemandPreparation(t.Context(), p.funding.authorization, func(_ context.Context, got SourcePreparationRequest) error {
		if got != req {
			return ErrGenerationChanged
		}
		return nil
	}, step); err != nil {
		t.Fatal(err)
	}
	if _, err := r.preparation.SubmitMonthly(t.Context(), req.Owner.GroupID, req.Owner.AuthVersion, req.Month); err != nil {
		t.Fatal(err)
	}
	if names := capturedSpoolAttemptNames(t, s, req); len(names) != 0 {
		t.Fatal("HTTP allocated output")
	}
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "queued" || steps != 1 || failures != 0 || calls != 0 {
		t.Fatal("new source prematurely advanced", state, steps, failures, calls)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET next_at=0`); err != nil {
		t.Fatal(err)
	}
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, steps, failures = preparationState(t, f.receiver)
	if state != "failed" || steps != 2 || failures != 1 || calls != 1 {
		t.Fatal("seal bypassed actual selection", state, steps, failures, calls)
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

func TestCapturedDatabaseBudgetKeepsTransientAndFinalBounds(t *testing.T) {
	for _, available := range []int64{0, 1 << 20, 5 << 20, 16 << 20, generationEvidenceLimit, generationEvidenceLimit + 1} {
		for _, tokenBytes := range []int{0, 1 << 20, 8 << 20} {
			a := monthlysource.CapturedMonthAssembly{Selection: []byte("synthetic index"), Tokens: []monthlysource.CapturedTokenArchive{{UserID: 201, Raw: make([]byte, tokenBytes)}}}
			budget, err := capturedDatabaseBudget(a, available)
			if err != nil {
				if !errors.Is(err, ErrGenerationCapacity) {
					t.Fatal(err)
				}
				continue
			}
			metadata := int64(len(a.Selection)) + (4 << 20)
			if budget > 64<<20 || budget < 128<<10 || budget%4096 != 0 || 2*budget+metadata > available || budget+int64(tokenBytes)+metadata > available {
				t.Fatal("reservation widened", available, tokenBytes, budget)
			}
		}
	}
}
