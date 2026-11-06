package main

// Bounded observation schedule only. It does not authorize customer bills,
// infer wallet boundaries, collect funding pages, or initialize Monitor data.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type observationMonthPlan struct {
	Version int                               `json:"version"`
	GroupID int64                             `json:"group_id"`
	Funding monthlysource.FundingMonthRequest `json:"funding"`
}

type observationMonthJob struct {
	Name    string
	Request observationRequest
	Newer   bool
}

type observationMonthProgress struct {
	Mode           string `json:"mode"`
	PlanSHA256     string `json:"plan_sha256"`
	GroupID        int64  `json:"group_id"`
	Month          string `json:"month"`
	Captures       int    `json:"captures"`
	Completed      int    `json:"completed"`
	Phase          string `json:"phase"`
	Next           string `json:"next,omitempty"`
	NextEligibleMS int64  `json:"next_eligible_unix_ms,omitempty"`
	Captured       bool   `json:"captured"`
	IndexSHA256    string `json:"index_sha256,omitempty"`
}

type observationMonthState struct {
	Progress observationMonthProgress
	Plan     observationMonthPlan
	Jobs     []observationMonthJob
	Refs     map[string]assemblyCapture
	Next     *observationMonthJob
}

func runObservationMonth(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "private canonical parent directory")
	name := fs.String("batch", "", "one observation batch name")
	if args[0] == "init-observation-month" {
		group := fs.Int64("group-id", 0, "one explicitly selected organization")
		month := fs.String("month", "", "one closed UTC+8 month")
		source := fs.String("source-identity", "", "independently verified source digest")
		users := fs.String("user-ids", "", "exact account IDs")
		organization := fs.String("organization-scope", "", "optional sealed current roster instead of manual group/month/source/users")
		organizationSHA := fs.String("expected-organization", "", "exact current roster file SHA256")
		create := fs.Bool("create", false, "write only a NEW local schedule; default checks only")
		if fs.Parse(args[1:]) != nil || fs.NArg() != 0 {
			return errInput
		}
		gid, funding, err := organizationInitScope(*organization, *organizationSHA, *group, *month, *source, *users)
		if err != nil {
			return err
		}
		slices.Sort(funding.UserIDs) // Stable identity, same account set; duplicates remain invalid.
		p := observationMonthPlan{Version: 1, GroupID: gid, Funding: funding}
		r, err := initializeObservationMonth(ctx, *workspace, *name, p, *create)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(r)
	}
	expected := fs.String("expected-plan", "", "exact immutable observation schedule digest")
	if args[0] == "plan-observation-month" {
		create := fs.Bool("create-index", false, "write a private capture index only after all captures are rechecked")
		if fs.Parse(args[1:]) != nil || fs.NArg() != 0 || !validSHA(*expected) {
			return errInput
		}
		r, err := sealObservationMonthIndex(ctx, *workspace, *name, *expected, *create)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(r)
	}
	connection := fs.String("connection-file", "", "private loopback source configuration; read ONLY with -capture")
	capture := fs.Bool("capture", false, "one bounded capture, no retries or background loop")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 || !validSHA(*expected) ||
		(args[0] == "inspect-observation-month" && *capture) || (*capture && *connection == "") {
		return errInput
	}
	r, err := advanceObservationMonth(ctx, *workspace, *name, *expected, *connection, *capture, captureObservation, time.Now)
	if r.Mode == "" {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(r))
}

func observationMonthJobs(ctx context.Context, p observationMonthPlan) ([]observationMonthJob, error) {
	if ctx == nil || p.Version != 1 || p.GroupID <= 0 || len(p.Funding.UserIDs) == 0 {
		return nil, errInput
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	month, err := time.ParseInLocation("2006-01", p.Funding.Month, time.FixedZone("UTC+8", 8*3600))
	if err != nil || month.Format("2006-01") != p.Funding.Month || month.Year() < 2020 || month.AddDate(0, 1, 0).After(time.Now()) {
		return nil, errInput
	}
	base, err := monthlysource.ReviewFundingMonth(ctx, p.Funding, nil)
	if err != nil || len(base.Accounts) != len(p.Funding.UserIDs) || (base.To-base.From)/86400*int64(len(base.Accounts)) > 4096 {
		return nil, errInput
	}
	for i, account := range base.Accounts {
		if account.UserID != p.Funding.UserIDs[i] {
			return nil, errInput
		}
	}
	jobs := make([]observationMonthJob, 0, 2*((base.To-base.From)/86400*int64(len(base.Accounts))+1))
	for _, phase := range []string{"older", "newer"} {
		index := 0
		for at := base.From; at < base.To; at += 86400 {
			for _, id := range p.Funding.UserIDs {
				r := observationRequest{Version: 1, Kind: "observe-token-usage", SourceIdentity: p.Funding.SourceIdentity,
					UserIDs: []int64{id}, From: at, To: at + 86400}
				if err := r.validate(time.Now()); err != nil {
					return nil, err
				}
				jobs = append(jobs, observationMonthJob{fmt.Sprintf("token-%04d-%s", index, phase), r, phase == "newer"})
				index++
			}
		}
		jobs = append(jobs, observationMonthJob{Name: "wallet-" + phase, Newer: phase == "newer",
			Request: observationRequest{Version: 1, Kind: "observe-wallet", SourceIdentity: p.Funding.SourceIdentity, UserIDs: slices.Clone(p.Funding.UserIDs)}})
	}
	return jobs, nil
}

func initializeObservationMonth(ctx context.Context, workspace, name string, p observationMonthPlan, create bool) (observationMonthProgress, error) {
	jobs, err := observationMonthJobs(ctx, p)
	if err != nil {
		return observationMonthProgress{}, err
	}
	root, output, err := jobPath(workspace, name)
	if err != nil || root != workspace {
		return observationMonthProgress{}, errInput
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		return observationMonthProgress{}, errInput
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return observationMonthProgress{}, err
	}
	r := observationMonthProgress{Mode: "checked_observation_schedule_no_source_or_files", PlanSHA256: hash(raw), GroupID: p.GroupID,
		Month: p.Funding.Month, Captures: len(jobs), Phase: "not_created"}
	if !create {
		return r, nil
	}
	lock, err := workspaceLock(root, true, true)
	if err != nil {
		return observationMonthProgress{}, err
	}
	defer lock.Close()
	if err := ctx.Err(); err != nil {
		return observationMonthProgress{}, err
	}
	if err := os.Mkdir(output, 0700); err != nil {
		return observationMonthProgress{}, err
	}
	// Separate parent/batch locks avoid recursively locking observeOne's lock.
	inner, err := workspaceLock(output, true, true)
	if err != nil {
		return observationMonthProgress{}, err
	}
	if err := inner.Close(); err != nil {
		return observationMonthProgress{}, err
	}
	pending := filepath.Join(output, "observation-month.pending.json")
	if err := exclusiveFile(pending, raw); err != nil {
		return observationMonthProgress{}, err
	}
	if err := ctx.Err(); err != nil {
		return observationMonthProgress{}, err
	}
	if err := publishNewFile(pending, filepath.Join(output, "observation-month.json")); err != nil {
		return observationMonthProgress{}, err
	}
	r.Mode, r.Phase = "created_observation_schedule_not_captured", "older"
	return r, errors.Join(syncPath(output), syncPath(root))
}

// Read a bounded, independently validated sealed capture, not arbitrary rows
// or caller-provided completed flags. The original request must match the job.
func observationMonthCapture(root string, job observationMonthJob, now time.Time) (assemblyCapture, int64, int64, int, error) {
	dir := filepath.Join(root, job.Name)
	marker, err := readAssemblyManifest(dir)
	if err != nil {
		return assemblyCapture{}, 0, 0, 0, err
	}
	ref := assemblyCapture{Directory: dir, ManifestSHA256: hash(marker)}
	raw, err := readAssemblyCapture(ref, job.Request.Kind)
	if err != nil {
		return ref, 0, 0, 0, err
	}
	want, _ := json.Marshal(job.Request)
	got, err := privateRead(filepath.Join(dir, "request.json"), 4096)
	if err != nil || !bytes.Equal(want, got) {
		return ref, 0, 0, 0, errInput
	}
	var stamp struct {
		StartedAtMS   int64 `json:"started_at_ms"`
		CompletedAtMS int64 `json:"completed_at_ms"`
	}
	if json.Unmarshal(raw, &stamp) != nil || stamp.StartedAtMS <= 0 || stamp.CompletedAtMS > now.UnixMilli() {
		return ref, 0, 0, 0, errInput
	}
	return ref, stamp.StartedAtMS, stamp.CompletedAtMS, len(raw), nil
}

// Caller holds the parent workspace lock; the inner shared lock prevents
// accepting half-written single observations. No source connection is opened.
func inspectObservationMonth(ctx context.Context, root, expected string, now time.Time) (observationMonthState, error) {
	var state observationMonthState
	if _, err := privateInfo(root, true); err != nil {
		return state, err
	}
	anchored, err := os.OpenRoot(root)
	if err != nil {
		return state, err
	}
	defer anchored.Close()
	lock, err := workspaceLock(root, false, false)
	if err != nil {
		return state, err
	}
	defer lock.Close()
	if _, err := os.Lstat(filepath.Join(root, "observation-month.pending.json")); !os.IsNotExist(err) {
		return state, errInput
	}
	raw, err := privateRead(filepath.Join(root, "observation-month.json"), 65536)
	if err != nil || !validSHA(expected) || hash(raw) != expected || json.Unmarshal(raw, &state.Plan) != nil {
		return state, errInput
	}
	canonical, err := json.Marshal(state.Plan)
	if err != nil || !bytes.Equal(raw, canonical) {
		return state, errInput
	}
	state.Jobs, err = observationMonthJobs(ctx, state.Plan)
	if err != nil {
		return state, err
	}
	state.Progress = observationMonthProgress{Mode: "observation_progress_not_bill_approval", PlanSHA256: expected,
		GroupID: state.Plan.GroupID, Month: state.Plan.Funding.Month, Captures: len(state.Jobs), Phase: "captured_needs_assembly_and_audit"}
	state.Refs = make(map[string]assemblyCapture)
	allowed := map[string]bool{"observation-month.json": true, ".funding-capture.lock": true, "observation-index.json": true}
	for _, job := range state.Jobs {
		allowed[job.Name] = true
	}
	dir, err := anchored.Open(".")
	if err != nil {
		return state, err
	}
	entries, readErr := dir.ReadDir(len(allowed) + 1)
	closeErr := dir.Close()
	if (readErr != nil && !errors.Is(readErr, io.EOF)) || closeErr != nil || len(entries) > len(allowed) {
		return state, errInput
	}
	for _, entry := range entries {
		if !allowed[entry.Name()] {
			return state, errInput
		}
	}
	var olderEnd, latestEnd int64
	var bytesRead int
	for i, job := range state.Jobs {
		if err := ctx.Err(); err != nil {
			return state, err
		}
		_, statErr := privateInfo(filepath.Join(root, job.Name), true)
		if os.IsNotExist(statErr) {
			if state.Next == nil {
				copyJob := state.Jobs[i]
				state.Next = &copyJob
			}
			continue
		}
		if statErr != nil || state.Next != nil {
			return state, errInput // Reject gaps/forks, never skip corrupt work.
		}
		ref, started, ended, size, err := observationMonthCapture(root, job, now)
		if err != nil {
			return state, err
		}
		bytesRead += size
		if bytesRead > 32<<20 || (latestEnd != 0 && started-latestEnd < 2000) || (job.Newer && started-olderEnd < 300000) {
			return state, errInput
		}
		state.Refs[job.Name] = ref
		state.Progress.Completed++
		latestEnd = max(latestEnd, ended)
		if !job.Newer {
			olderEnd = max(olderEnd, ended)
		}
	}
	if state.Next != nil {
		r := &state.Progress
		r.Phase, r.Next, r.NextEligibleMS = "older", state.Next.Name, latestEnd+2000
		if latestEnd == 0 {
			r.NextEligibleMS = 0
		}
		if state.Next.Newer {
			r.Phase, r.NextEligibleMS = "newer", max(r.NextEligibleMS, olderEnd+300000)
		}
		if r.NextEligibleMS > now.UnixMilli() {
			r.Phase = "waiting_for_capture_interval"
		}
	}
	if _, err := os.Lstat(filepath.Join(root, "observation-index.json")); !os.IsNotExist(err) {
		want, indexErr := observationMonthIndex(state)
		stored, readErr := privateRead(filepath.Join(root, "observation-index.json"), 2<<20)
		if err != nil || indexErr != nil || readErr != nil || !bytes.Equal(want, stored) {
			return state, errInput
		}
		state.Progress.IndexSHA256 = hash(stored)
	}
	return state, observationRootCurrent(anchored, root)
}

// No daemon/retry loop. Even an explicit capture invocation performs at most
// one bounded read, preserving the existing source SQL/index/timeout limits.
func advanceObservationMonth(parent context.Context, workspace, name, expected, connection string, capture bool,
	collect observationCapture, clock func() time.Time) (observationMonthProgress, error) {
	var empty observationMonthProgress
	if parent == nil || collect == nil || clock == nil || !validSHA(expected) || (capture && connection == "") {
		return empty, errInput
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	workspacePath, root, err := jobPath(workspace, name)
	if err != nil || workspacePath != workspace {
		return empty, errInput
	}
	lock, err := workspaceLock(workspacePath, capture, false)
	if err != nil {
		return empty, err
	}
	defer lock.Close()
	if _, err := privateInfo(root, true); err != nil {
		return empty, err
	}
	anchored, err := os.OpenRoot(root)
	if err != nil {
		return empty, err
	}
	defer anchored.Close()
	before, err := inspectObservationMonth(ctx, root, expected, clock())
	if err != nil {
		return empty, err
	}
	if !capture || before.Next == nil || before.Progress.NextEligibleMS > clock().UnixMilli() {
		return before.Progress, observationRootCurrent(anchored, root)
	}
	if err := observationRootCurrent(anchored, root); err != nil {
		return before.Progress, err
	}
	before.Progress.Mode = "one_observation_capture_attempted_no_retry"
	result, err := observeOne(ctx, root, before.Next.Name, connection, before.Next.Request, true, collect)
	if err != nil {
		return before.Progress, err // No retry/advance; a sealed result is rechecked next time.
	}
	if !result.Complete {
		return before.Progress, errInput
	}
	if err := observationRootCurrent(anchored, root); err != nil {
		return before.Progress, err
	}
	after, err := inspectObservationMonth(ctx, root, expected, clock())
	if err != nil {
		return before.Progress, err
	}
	after.Progress.Captured = true
	return after.Progress, observationRootCurrent(anchored, root)
}
