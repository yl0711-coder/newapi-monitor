package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

// New authority revisions, but the SAME independently captured synthetic source
// observations. This is test setup, never an authority/attempt reset API.
func capturedSpoolRevision(t *testing.T, auth *gorm.DB, p *WalletSourcePreparation, base SourcePreparationRequest, revision int64) SourcePreparationRequest {
	t.Helper()
	if revision == base.Owner.AuthVersion {
		return base
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=?", base.Owner.GroupID).Update("portal_auth_ver", revision).Error; err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(t.Context(), auth, base.Owner.GroupID, revision)
	if err != nil {
		t.Fatal(err)
	}
	req := SourcePreparationRequest{Owner: owner, Month: base.Month}
	scope, err := p.funding.scope(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(t.Context(), p.funding.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	// Funding scan identities bind the authority revision too. Build complete
	// synthetic empty-funding traversals for EACH new scope, not fake a ready
	// marker or borrow a checkpoint belonging to another immutable plan.
	for i, scan := range plan.Scans {
		at := time.Now().Add(-time.Hour).UnixMilli() + int64(i)*1000
		page := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100000000, EndReached: true, Rows: []monthlysource.FundingLog{}, StartedAtMS: at, CompletedAtMS: at + 1}
		if _, err := monthlysource.SaveFundingPage(t.Context(), p.funding.capture, page); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		`INSERT INTO portal_monthly_token_capture_slots(job_key,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload) SELECT ?,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload FROM portal_monthly_token_capture_slots WHERE job_key=?`,
		`INSERT INTO portal_monthly_wallet_capture_slots(job_key,ordinal,observation_id) SELECT ?,ordinal,observation_id FROM portal_monthly_wallet_capture_slots WHERE job_key=?`,
	} {
		if _, err := p.funding.capture.Exec(q, req.Key(), base.Key()); err != nil {
			t.Fatal(err)
		}
	}
	return req
}

func capturedSpoolFileSnapshot(t *testing.T, directory string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[path] = generationHash(raw)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func capturedRetainedInventory(t *testing.T, s *CapturedSourceSpool) capturedSpoolInventory {
	t.Helper()
	root, lock, err := openCapturedOutput(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	defer lock.Close()
	i, err := inspectCapturedSpoolPolicy(t.Context(), root, s.retainCompleted)
	if err != nil {
		t.Fatal(err)
	}
	return i
}

func TestCapturedSpoolRetentionThirtyThreeActualSealsRestartAndNoSourceIO(t *testing.T) {
	f, auth, wallet, base, driver := capturedFilesFixture(t)
	legacy := capturedSpoolTest(t, wallet)
	for i := int64(1); i <= capturedSpoolJobs; i++ {
		req := capturedSpoolRevision(t, auth, wallet, base, i)
		files, wrote, err := legacy.Advance(t.Context(), req)
		if err != nil || !wrote || files.Material.SHA256 == "" {
			t.Fatal("actual initial seal", i, wrote, err)
		}
	}
	before := capturedSpoolFileSnapshot(t, legacy.directory)
	if len(before) != capturedSpoolJobs*5 {
		t.Fatal("expected all 32 genuine two-account seals", len(before))
	}
	req := capturedSpoolRevision(t, auth, wallet, base, capturedSpoolJobs+1)
	if _, _, err := legacy.Advance(t.Context(), req); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("legacy 32-job policy changed", err)
	}
	retained, err := NewRetainedCapturedSourceSpool(wallet, legacy.directory)
	if err != nil {
		t.Fatal(err)
	}
	i := capturedRetainedInventory(t, retained)
	if i.jobs != 32 || i.closedJobs != 32 || !retained.canAllocateJob(i) || i.bytes <= 0 {
		t.Fatal("closed originals still occupy all live slots", i)
	}
	// Competing handles must still create exactly ONE new attempt. No source
	// query/connection or old financial/source rewrite is allowed.
	other, err := NewRetainedCapturedSourceSpool(wallet, legacy.directory)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		files CapturedSourceFiles
		wrote bool
		err   error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, s := range []*CapturedSourceSpool{retained, other} {
		go func(s *CapturedSourceSpool) {
			<-start
			files, wrote, err := s.Advance(t.Context(), req)
			results <- result{files, wrote, err}
		}(s)
	}
	close(start)
	writes := 0
	var selected CapturedSourceFiles
	for range 2 {
		r := <-results
		if r.err != nil {
			if !errors.Is(r.err, ErrGenerationBusy) {
				t.Fatal(r.err)
			}
			continue
		}
		if r.wrote {
			writes++
		}
		if selected.Material.SHA256 != "" && selected != r.files {
			t.Fatal("concurrent seal identities differ")
		}
		selected = r.files
	}
	if writes != 1 || selected.Material.SHA256 == "" || !reflect.DeepEqual(capturedSpoolAttemptNames(t, retained, req), []string{"seal-1"}) {
		t.Fatal("new allocation was lost/duplicated", writes)
	}
	restarted, err := NewRetainedCapturedSourceSpool(wallet, retained.directory)
	if err != nil {
		t.Fatal(err)
	}
	again, wrote, err := restarted.Advance(t.Context(), req)
	if err != nil || wrote || again != selected {
		t.Fatal("restart did not revalidate original seal", err)
	}
	i = capturedRetainedInventory(t, restarted)
	if i.jobs != 33 || i.closedJobs != 33 {
		t.Fatal("completed jobs lost across restart", i)
	}
	after := capturedSpoolFileSnapshot(t, retained.directory)
	for path, hash := range before {
		if after[path] != hash {
			t.Fatal("an original source was removed/moved/changed", path)
		}
	}
	if driver.queries.Load() != 0 || driver.writes.Load() != 0 || driver.pings.Load() != 0 {
		t.Fatal("retention performed upstream I/O")
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

// Capacity-only sentinels: not financially valid or accepted source material.
// They let us exhaust every metadata boundary without synthesizing money.
func capturedSpoolSentinel(t *testing.T, directory, key string, closed bool) string {
	t.Helper()
	job := filepath.Join(directory, "job-"+generationHash([]byte(key)))
	if err := os.Mkdir(job, 0700); err != nil {
		t.Fatal(err)
	}
	if closed {
		seal := filepath.Join(job, "seal-1")
		if err := os.Mkdir(seal, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(seal, "materials.json"), []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return job
}

func TestCapturedSpoolRetentionLiveTotalAndByteLimits(t *testing.T) {
	_, _, wallet, req, _ := capturedFilesFixture(t)
	for _, kind := range []string{"live", "total", "total_overflow", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			s, err := NewRetainedCapturedSourceSpool(wallet, privateCapturedOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			if kind == "bytes" {
				s.limit = 4 << 20 // original byte reserve, unchanged by retention
			} else {
				n := capturedSpoolRetainedJobs
				if kind == "live" {
					n = capturedSpoolJobs + 1
				}
				if kind == "total_overflow" {
					n++
				}
				for i := range n {
					capturedSpoolSentinel(t, s.directory, kind+stringID(int64(i)), kind != "live" || i == 0)
				}
			}
			before := capturedSpoolFileSnapshot(t, s.directory)
			if _, _, err := s.Advance(t.Context(), req); !errors.Is(err, ErrGenerationCapacity) {
				t.Fatal("retained mode bypassed resource bound", kind, err)
			}
			if after := capturedSpoolFileSnapshot(t, s.directory); !reflect.DeepEqual(before, after) {
				t.Fatal("capacity failure changed originals")
			}
		})
	}
}

func TestCapturedSpoolRetentionBadCompleteCannotApproveOrReseal(t *testing.T) {
	_, _, wallet, req, _ := capturedFilesFixture(t)
	s, err := NewRetainedCapturedSourceSpool(wallet, privateCapturedOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	files, _, err := s.Advance(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.Material.Path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	before := capturedSpoolFileSnapshot(t, s.directory)
	i := capturedRetainedInventory(t, s)
	if i.closedJobs != 1 {
		t.Fatal("test did not exercise a closed allocation marker")
	}
	for range 2 {
		if files, wrote, err := s.Advance(t.Context(), req); err == nil || wrote || files != (CapturedSourceFiles{}) {
			t.Fatal("closed allocation marker treated as evidence", err)
		}
	}
	if after := capturedSpoolFileSnapshot(t, s.directory); !reflect.DeepEqual(before, after) || !reflect.DeepEqual(capturedSpoolAttemptNames(t, s, req), []string{"seal-1"}) {
		t.Fatal("bad completed original replaced/retried")
	}
}

func TestCapturedSpoolRetentionFailureAttemptsAndUnsafePathsRemainProtected(t *testing.T) {
	for _, kind := range []string{"three_failures", "symlink", "public", "hardlink", "gap", "two_markers"} {
		t.Run(kind, func(t *testing.T) {
			_, _, wallet, req, _ := capturedFilesFixture(t)
			s, err := NewRetainedCapturedSourceSpool(wallet, privateCapturedOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			if kind == "three_failures" {
				for range capturedSpoolAttempts {
					_, _, err := s.advance(t.Context(), req, func(context.Context, *os.Root, string, []byte) error {
						return errors.New("synthetic failed first write")
					})
					if err == nil {
						t.Fatal("synthetic interruption did not fail")
					}
				}
				s, err = NewRetainedCapturedSourceSpool(wallet, s.directory)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := s.Advance(t.Context(), req); !errors.Is(err, ErrGenerationCapacity) {
					t.Fatal("restart reset failure count", err)
				}
				if len(capturedSpoolAttemptNames(t, s, req)) != capturedSpoolAttempts {
					t.Fatal("attempt directories lost")
				}
				if i := capturedRetainedInventory(t, s); i.closedJobs != 0 || i.jobs != 1 {
					t.Fatal("failed attempts treated as completed", i)
				}
				return
			}
			job := capturedSpoolSentinel(t, s.directory, kind, true)
			switch kind {
			case "symlink":
				if err := os.Symlink(job, filepath.Join(s.directory, "job-"+strings.Repeat("a", 64))); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(filepath.Join(job, "seal-1", "materials.json"), 0644); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(filepath.Join(job, "seal-1", "materials.json"), filepath.Join(job, "seal-1", "extra")); err != nil {
					t.Fatal(err)
				}
			case "gap", "two_markers":
				other := "seal-2"
				if kind == "gap" {
					other = "seal-3"
				}
				if err := os.Mkdir(filepath.Join(job, other), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(job, other, "materials.json"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			root, err := openArchiveRoot(s.directory)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			i, err := inspectCapturedSpoolPolicy(t.Context(), root, true)
			if kind == "gap" || kind == "two_markers" {
				if err != nil || i.closedJobs != 0 || i.jobs != 1 {
					t.Fatal("ambiguous original got a free live slot", i, err)
				}
			} else if err == nil {
				t.Fatal("unsafe path accepted", kind)
			}
		})
	}
}

func TestCapturedSpoolRetentionCountsAllFailureBytesAndReusesAtFullLimit(t *testing.T) {
	_, _, wallet, req, _ := capturedFilesFixture(t)
	s, err := NewRetainedCapturedSourceSpool(wallet, privateCapturedOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	files, _, err := s.Advance(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	before := capturedRetainedInventory(t, s)
	other := capturedSpoolSentinel(t, s.directory, "unrelated-failed-attempt", false)
	if err := os.Mkdir(filepath.Join(other, "seal-1"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "seal-1", "selection.json"), bytes.Repeat([]byte("x"), 8192), 0600); err != nil {
		t.Fatal(err)
	}
	after := capturedRetainedInventory(t, s)
	if after.bytes < before.bytes+8192 || after.closedJobs != 1 || after.jobs != 2 {
		t.Fatal("failed/closed original bytes not fully charged", before, after)
	}
	s.limit = after.bytes // No extra free byte; existing exact reuse still works.
	again, wrote, err := s.Advance(t.Context(), req)
	if err != nil || wrote || again != files {
		t.Fatal("full byte limit blocked safe reuse", err)
	}
}

func TestCapturedSpoolRetentionPrivateConfigActuallyControlsStartupStage(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "legacy"
		if enabled {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			addCapturedHistoryReconstruction(t, &f)
			cfg, ref := capturedPreparationConfigFixture(t, f)
			cfg.RetainCompletedSeals = enabled
			raw := materialPreparationJSON(t, cfg)
			if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			ref.SHA256 = generationHash(raw)
			for i := range capturedSpoolJobs {
				capturedSpoolSentinel(t, f.spool.directory, "startup-other-closed-"+stringID(int64(i)), true)
			}
			before := capturedSpoolFileSnapshot(t, f.spool.directory)
			d := f.open(t)
			if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
				t.Fatal(err)
			}
			if d.runtime.Phase() != "disabled" {
				t.Fatal("constructor started work")
			}
			if after := capturedSpoolFileSnapshot(t, f.spool.directory); !reflect.DeepEqual(before, after) {
				t.Fatal("startup wrote spool")
			}
			// Run only the actual composed background preparation step. No
			// customer request or registration lookup manufactures source work.
			step := d.runtime.preparation.dependencies.step
			ready, err := step(t.Context(), f.req)
			if !enabled {
				if !errors.Is(err, ErrGenerationCapacity) || ready {
					t.Fatal("legacy config ignored 32-job limit", ready, err)
				}
				return
			}
			if err != nil || ready {
				t.Fatal("opt-in stage failed to seal without approving bill", ready, err)
			}
			if got := capturedSpoolAttemptNames(t, f.spool, f.req); !reflect.DeepEqual(got, []string{"seal-1"}) {
				t.Fatal("startup policy not used by real background step", got)
			}
			// The following stage must normalize and select the original source
			// under its unchanged financial checks, not just report a free slot.
			ready, err = step(t.Context(), f.req)
			if !errors.Is(err, ErrGenerationBusy) || ready {
				t.Fatal("disabled runtime admitted financial selection", ready, err)
			}
			if err := d.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			ready, err = step(t.Context(), f.req)
			if err != nil || !ready {
				t.Fatal("actual source handoff failed after opt-in seal", ready, err)
			}
			selected, err := ReadGenerationSelection(t.Context(), d.receiver, f.req.Owner.GroupID, f.req.Month)
			if err != nil || selected.Request == nil {
				t.Fatal("financially validated selection missing", err)
			}
			saved, err := loadGeneration(t.Context(), d.receiver, *selected.Request, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			for _, account := range saved.input.Accounts {
				if account.Baseline.BalanceQuota != 500000000 || account.Snapshots[0].BalanceQuota != 499000000 {
					t.Fatal("capacity policy changed original money")
				}
			}
			after := capturedSpoolFileSnapshot(t, f.spool.directory)
			for path, digest := range before {
				if after[path] != digest {
					t.Fatal("startup stage modified an unrelated retained original", path)
				}
			}
		})
	}
}

func TestCapturedSpoolRetentionCustomerClickGeneratesOriginalMoneyAndDownload(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		name := "legacy_material"
		if partitioned {
			name = "partitioned_material"
		}
		t.Run(name, func(t *testing.T) {
			f := newCapturedHistoryFixture(t)
			addCapturedHistoryReconstruction(t, &f)
			f.wallet.funding.partitionUsage = partitioned
			var err error
			f.spool, err = NewRetainedCapturedSourceSpool(f.wallet, f.spool.directory)
			if err != nil {
				t.Fatal(err)
			}
			for i := range capturedSpoolJobs {
				capturedSpoolSentinel(t, f.spool.directory, "customer-unrelated-closed-"+stringID(int64(i)), true)
			}
			before := capturedSpoolFileSnapshot(t, f.spool.directory)
			// Actual first-click script -> HTTP -> preparation -> normalization
			// -> selected plan -> worker -> saved member/token money -> XLSX.
			// The original helper independently expects 2000 opening, 1996
			// closing, and each account consume 3/refund 1, not a ready flag.
			testCapturedHistoryPublicationAdapter(t, f, true, true, true, true)
			after := capturedSpoolFileSnapshot(t, f.spool.directory)
			for path, hash := range before {
				if after[path] != hash {
					t.Fatal("customer generation changed unrelated retained originals")
				}
			}
			if !reflect.DeepEqual(capturedSpoolAttemptNames(t, f.spool, f.req), []string{"seal-1"}) {
				t.Fatal("customer generated duplicate seals")
			}
		})
	}
}
