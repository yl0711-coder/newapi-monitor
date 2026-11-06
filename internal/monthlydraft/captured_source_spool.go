package monthlydraft

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const capturedSpoolJobs = 32
const capturedSpoolAttempts = 3
const capturedSpoolRetainedJobs = 256 // opt-in TOTAL, including the 32 live jobs

// CapturedSourceSpool owns no connections and starts no loop. Its dedicated,
// existing private directory is server configuration, never customer input.
// Files (including failures) are retained; no eviction or financial selection.
type CapturedSourceSpool struct {
	wallet            *WalletSourcePreparation
	directory         string
	identity          os.FileInfo
	limit             int64
	retainCompleted   bool // closed seals retain original paths, not live allocation slots
	historyDependency bool // data-only core seals; never upgrade an old financial seal in place
}

func NewCapturedSourceSpool(wallet *WalletSourcePreparation, directory string) (*CapturedSourceSpool, error) {
	return newCapturedSourceSpool(wallet, directory, false)
}

func newCapturedSourceSpool(wallet *WalletSourcePreparation, directory string, retainCompleted bool) (*CapturedSourceSpool, error) {
	if wallet == nil || wallet.funding == nil || wallet.funding.capture == nil || wallet.funding.authorization == nil {
		return nil, ErrGenerationInput
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil || resolved != directory {
		return nil, ErrGenerationInput
	}
	root, err := openArchiveRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	info, err := root.Stat(".")
	if err != nil {
		return nil, err
	}
	if err := capturedSpoolPrivate(info, true); err != nil {
		return nil, err
	}
	return &CapturedSourceSpool{wallet: wallet, directory: directory, identity: info, limit: generationEvidenceLimit, retainCompleted: retainCompleted}, nil
}

// NewRetainedCapturedSourceSpool opts into separate live/closed job accounting.
// It writes nothing, moves/deletes no files and opens no source connection.
// A completion marker closes seal allocation, NOT financial validation: every
// reuse still runs the original complete source/authority/hash checks.
func NewRetainedCapturedSourceSpool(wallet *WalletSourcePreparation, directory string) (*CapturedSourceSpool, error) {
	return newCapturedSourceSpool(wallet, directory, true)
}

func (s *CapturedSourceSpool) canAllocateJob(i capturedSpoolInventory) bool {
	if !s.retainCompleted {
		return i.jobs < capturedSpoolJobs
	}
	return i.jobs < capturedSpoolRetainedJobs && i.jobs-i.closedJobs < capturedSpoolJobs
}

func (s *CapturedSourceSpool) jobName(req SourcePreparationRequest) string {
	// The request already binds group, month, auth and complete roster. Pin the
	// source identity too: another source cannot reuse this directory by name.
	identity := req.Key() + "/" + s.wallet.funding.sourceIdentity
	if s.wallet.funding.registeredOrigins {
		identity += "/registered-origins-v1" // separate format; never upgrade an old seal in place
	}
	if s.wallet.funding.walletTails {
		identity += "/wallet-tail-v1"
	}
	if s.historyDependency {
		identity += "/history-source-dependency-v1"
	}
	return "job-" + generationHash([]byte(identity))
}

// Advance makes at most ONE new local seal attempt. completedNow is false for
// a revalidated existing seal, true for actual new work; neither approves a bill.
// Outer on-demand scheduling supplies cooldown. Persistent directories cap
// attempts across restarts and repeated customer clicks, not an in-memory map.
func (s *CapturedSourceSpool) Advance(ctx context.Context, req SourcePreparationRequest) (files CapturedSourceFiles, completedNow bool, err error) {
	return s.advance(ctx, req, writeArchiveFile)
}

func (s *CapturedSourceSpool) advance(parent context.Context, req SourcePreparationRequest,
	writeFile func(context.Context, *os.Root, string, []byte) error) (CapturedSourceFiles, bool, error) {
	var empty CapturedSourceFiles
	if parent == nil || s == nil || s.wallet == nil || s.identity == nil || writeFile == nil || s.limit <= 0 || s.limit > generationEvidenceLimit {
		return empty, false, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	// Cheap authorization/metadata admission before re-reading an entire month
	// that cannot fit. This is only an early refusal, never financial approval;
	// the original audit and locked allocation checks below remain mandatory.
	if _, err := s.wallet.funding.scope(ctx, req); err != nil {
		return empty, false, err
	}
	root, lock, err := openCapturedOutput(s.directory)
	if err != nil {
		return empty, false, err
	}
	defer root.Close()
	defer lock.Close()
	current, err := root.Stat(".")
	if err != nil || !os.SameFile(s.identity, current) {
		return empty, false, ErrGenerationIntegrity
	}
	inventory, err := inspectCapturedSpoolPolicy(ctx, root, s.retainCompleted)
	if err != nil {
		return empty, false, err
	}
	job := s.jobName(req)
	_, statErr := root.Lstat(job)
	newJob := os.IsNotExist(statErr)
	if statErr != nil && !newJob {
		return empty, false, statErr
	}
	if newJob {
		if !s.canAllocateJob(inventory) || s.limit-inventory.bytes-(64<<10) <= 4<<20 {
			return empty, false, ErrGenerationCapacity
		}
	}
	// Hold the SAME nonblocking spool lock from capacity inspection through
	// audit and seal. Otherwise a competing caller can pass preflight, start
	// an audit AFTER this caller reserved seal-1, and make its inner audit
	// return Busy: mere contention would consume a durable failure attempt.
	// No SQLite transaction or source connection is held across file work.
	// Full original verification still precedes EVERY output allocation.
	if _, err := s.wallet.AssembleCapturedSources(ctx, req); err != nil {
		return empty, false, err
	}
	if err := automaticPreparationRootCurrent(root, s.directory); err != nil {
		return empty, false, err
	}
	if newJob {
		if err := root.Mkdir(job, 0700); err != nil {
			return empty, false, err
		}
		if err := syncArchiveDirectory(root); err != nil {
			return empty, false, err
		}
	}
	jobRoot, err := root.OpenRoot(job)
	if err != nil {
		return empty, false, err
	}
	defer jobRoot.Close()
	last, complete, err := inspectCapturedJob(jobRoot)
	if err != nil {
		return empty, false, err
	}
	if complete != "" {
		// A bad COMPLETE marker must fail closed, never trigger replacement by
		// a different attempt whose amounts merely look acceptable.
		files, err := s.wallet.SealCapturedSources(ctx, req, filepath.Join(s.directory, job, complete))
		if err != nil {
			return empty, false, err
		}
		if err := s.current(root, jobRoot, job); err != nil {
			return empty, false, err
		}
		return files, false, nil
	}
	// Allocation itself consumes the attempt, including a crash before the
	// first byte. Reusing an empty failed directory would bypass the durable
	// retry limit whenever the first write consistently fails.
	attempt := last + 1
	if attempt > capturedSpoolAttempts {
		return empty, false, ErrGenerationCapacity
	}
	// Reserve directory/allocation overhead as well as all bytes retained by
	// other jobs and failed attempts. No completed output needs new capacity.
	available := s.limit - inventory.bytes - (64 << 10)
	if available <= 4<<20 {
		return empty, false, ErrGenerationCapacity
	}
	name := "seal-" + strconv.Itoa(attempt)
	if err := jobRoot.Mkdir(name, 0700); err != nil {
		return empty, false, err
	}
	if err := syncArchiveDirectory(jobRoot); err != nil {
		return empty, false, err
	}
	if err := s.current(root, jobRoot, job); err != nil {
		return empty, false, err
	}
	files, err := s.wallet.sealCapturedSourcesBounded(ctx, req, filepath.Join(s.directory, job, name), available, writeFile)
	if err != nil {
		return empty, false, err
	}
	if err := s.current(root, jobRoot, job); err != nil {
		return empty, false, err
	}
	return files, true, nil
}

// Before leaves a completed new seal for the NEXT worker step, avoiding a
// second expensive stage in the same twenty-second budget. The trusted next
// stage receives only server-derived file references. The existing queue must
// still independently verify a real source selection before accepting true.
func (s *CapturedSourceSpool) Before(next func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (bool, error)) (SourcePreparationStep, error) {
	if s == nil || next == nil {
		return nil, ErrGenerationInput
	}
	return func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		files, completed, err := s.Advance(ctx, req)
		if err != nil || completed {
			return false, err
		}
		if _, err := s.wallet.funding.scope(ctx, req); err != nil {
			return false, err
		}
		return next(ctx, req, files)
	}, nil
}

func (s *CapturedSourceSpool) current(root, jobRoot *os.Root, job string) error {
	if err := automaticPreparationRootCurrent(root, s.directory); err != nil {
		return err
	}
	return automaticPreparationRootCurrent(jobRoot, filepath.Join(s.directory, job))
}

// ComposeCapturedSourcePreparation starts no routes, flags or workers. All
// three private capture stores must have been explicitly initialized. Only a
// trusted historical preparation/selection stage may be supplied as next; the
// runtime still rejects true without an independently validated selection.
func ComposeCapturedSourcePreparation(wallet *WalletSourcePreparation, spool *CapturedSourceSpool,
	next func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (bool, error)) (SourcePreparationStep, error) {
	if wallet == nil || spool == nil || spool.wallet != wallet {
		return nil, ErrGenerationInput
	}
	token, err := NewTokenSourcePreparation(wallet.funding)
	if err != nil {
		return nil, err
	}
	step, err := spool.Before(next)
	if err != nil {
		return nil, err
	}
	if wallet.funding.walletTails {
		step, err = walletTailSourceBefore(wallet.funding, step, false)
		if err != nil {
			return nil, err
		}
	}
	for _, wrap := range []func(SourcePreparationStep) (SourcePreparationStep, error){wallet.Before, token.Before, wallet.funding.Before, wallet.WarmFirst} {
		step, err = wrap(step)
		if err != nil {
			return nil, err
		}
	}
	if wallet.funding.walletTails {
		step, err = walletTailSourceBefore(wallet.funding, step, true)
		if err != nil {
			return nil, err
		}
	}
	if wallet.funding.registeredOrigins {
		step, err = registeredSourceBefore(wallet.funding, step)
		if err != nil {
			return nil, err
		}
	}
	return step, nil
}

func inspectCapturedJob(root *os.Root) (last int, complete string, err error) {
	gap := false
	for i := 1; i <= capturedSpoolAttempts; i++ {
		name := "seal-" + strconv.Itoa(i)
		if _, e := root.Lstat(name); os.IsNotExist(e) {
			gap = true
			continue
		} else if e != nil {
			return 0, "", e
		}
		if gap {
			return 0, "", ErrGenerationIntegrity
		}
		last = i
		child, e := root.OpenRoot(name)
		if e != nil {
			return 0, "", e
		}
		_, markerErr := child.Lstat("materials.json")
		closeErr := child.Close()
		if closeErr != nil {
			return 0, "", closeErr
		}
		if markerErr == nil {
			if complete != "" {
				return 0, "", ErrGenerationIntegrity
			}
			complete = name
		} else if !os.IsNotExist(markerErr) {
			return 0, "", markerErr
		}
	}
	return last, complete, nil
}

type capturedSpoolInventory struct {
	jobs       int
	closedJobs int
	bytes      int64
}

// Bounded metadata only: no payload/hash reads, no source network, no deleting.
// Other jobs' ordinary damaged content is counted, not interpreted as this job's
// evidence. Symlinks, public paths and aliases fail before any allocation.
func inspectCapturedSpool(ctx context.Context, root *os.Root) (capturedSpoolInventory, error) {
	return inspectCapturedSpoolPolicy(ctx, root, false)
}

func inspectCapturedSpoolPolicy(ctx context.Context, root *os.Root, retainCompleted bool) (capturedSpoolInventory, error) {
	var out capturedSpoolInventory
	var visit func(string, int) error
	visit = func(path string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := root.Lstat(path)
		if err != nil {
			return err
		}
		if err := capturedSpoolPrivate(info, depth < 3); err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		if info.Size() < 0 || stat.Blocks < 0 {
			return ErrGenerationIntegrity
		}
		// Saturate, never let a damaged/oversized spool wrap the byte counter.
		if stat.Blocks > generationEvidenceLimit/512 || info.Size() > generationEvidenceLimit {
			out.bytes = generationEvidenceLimit + 1
		} else {
			occupied := max(info.Size(), stat.Blocks*512)
			if occupied > generationEvidenceLimit-out.bytes {
				out.bytes = generationEvidenceLimit + 1
			} else {
				out.bytes += occupied
			}
		}
		if depth == 3 {
			return nil
		}
		f, err := root.Open(path)
		if err != nil {
			return err
		}
		limit := 256
		if depth == 0 {
			limit = capturedSpoolJobs
			if retainCompleted {
				limit = capturedSpoolRetainedJobs
			}
		} else if depth == 1 {
			limit = capturedSpoolAttempts
		}
		entries, readErr := f.ReadDir(limit + 1)
		if errors.Is(readErr, io.EOF) {
			readErr = nil
		}
		if err := errors.Join(readErr, f.Close()); err != nil {
			return err
		}
		if len(entries) > limit {
			return ErrGenerationCapacity
		}
		for _, entry := range entries {
			name := entry.Name()
			if depth == 0 {
				if !strings.HasPrefix(name, "job-") || !generationHex(strings.TrimPrefix(name, "job-"), 32) {
					return ErrGenerationIntegrity
				}
				out.jobs++
			} else if depth == 1 && name != "seal-1" && name != "seal-2" && name != "seal-3" {
				return ErrGenerationIntegrity
			}
			if err := visit(filepath.Join(path, name), depth+1); err != nil {
				return err
			}
			if depth == 0 && retainCompleted {
				job, err := root.OpenRoot(name)
				if err != nil {
					return err
				}
				_, closed, inspectErr := inspectCapturedJob(job)
				if err := job.Close(); err != nil {
					return err
				}
				// Advance NEVER allocates another attempt once a complete marker
				// exists. Only that mechanical fact releases a live slot here.
				// Gaps/ambiguous markers stay charged as live; unsafe file paths
				// already failed above. Corrupt contents are NEVER bill evidence.
				if inspectErr == nil && closed != "" {
					out.closedJobs++
				}
			}
		}
		return nil
	}
	err := visit(".", 0)
	return out, err
}

func capturedSpoolPrivate(info os.FileInfo, directory bool) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 || info.IsDir() != directory || (!directory && (!info.Mode().IsRegular() || stat.Nlink != 1)) {
		return ErrGenerationIntegrity
	}
	return nil
}
