package main

// Offline, data-only assembly. Captured records retain their original IDs and
// payloads. No Monitor import, credentials, source capture or customer approval.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type assemblyFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type assemblyCapture struct {
	Directory      string `json:"directory"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type assemblyUsagePair struct {
	Older assemblyCapture `json:"older"`
	Newer assemblyCapture `json:"newer"`
}

type observationAssemblyPlan struct {
	Version     int                 `json:"version"`
	FundingPlan assemblyFile        `json:"funding_plan"`
	FundingDB   assemblyFile        `json:"funding_database"`
	Usage       []assemblyUsagePair `json:"usage"`
	TokenUsage  []assemblyUsagePair `json:"token_usage,omitempty"`
	Wallet      []assemblyCapture   `json:"wallet"`
}

// Selection is a private source selection, not a GenerationPlan, publication
// authority, proof of settled boundaries or historical directory membership.
type observationAssemblySelection struct {
	Version          int                                       `json:"version"`
	GroupID          int64                                     `json:"group_id"`
	Funding          monthlysource.FundingMonthRequest         `json:"funding"`
	FundingWindows   []monthlysource.FundingMonthWindowVersion `json:"funding_windows"`
	UsageWindows     []monthlysource.UsageWindowVersion        `json:"usage_windows"`
	WalletIDs        []string                                  `json:"wallet_ids"`
	TokenProjections []assemblyTokenProjection                 `json:"token_projections,omitempty"`
}

// Preserve the actual parent capture digest as well as the projected user-view
// digest. A token capture is never counted as two independent observations.
type assemblyTokenProjection struct {
	ParentSHA256 string `json:"parent_sha256"`
	UsageSHA256  string `json:"usage_sha256"`
}

type observationAssemblyReview struct {
	Funding monthlysource.FundingMoneyReview  `json:"funding"`
	Usage   monthlysource.UsageMonthReport    `json:"usage"`
	Wallet  []monthlysource.WalletObservation `json:"wallet"`
}

type observationAssemblyReceipt struct {
	Version         int    `json:"version"`
	Mode            string `json:"mode"`
	PlanSHA256      string `json:"plan_sha256"`
	SelectionSHA256 string `json:"selection_sha256"`
	ReviewSHA256    string `json:"review_sha256"`
	DatabaseSHA256  string `json:"database_sha256,omitempty"`
	DatabaseBytes   int64  `json:"database_bytes,omitempty"`
	FundingPages    int    `json:"funding_pages"`
	FundingRows     int    `json:"funding_observation_rows"`
	UsageCaptures   int    `json:"usage_captures"`
	WalletCaptures  int    `json:"wallet_captures"`
}

func runObservationAssembly(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("assemble-observations", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "existing private output workspace")
	name := fs.String("output", "", "new output directory; never overwritten")
	path := fs.String("plan", "", "private observation assembly plan")
	expected := fs.String("expected-plan", "", "explicit assembly plan SHA256")
	create := fs.Bool("create-output", false, "write NEW local evidence; default validates in memory only")
	if fs.Parse(args) != nil || fs.NArg() != 0 || !validSHA(*expected) {
		return errInput
	}
	raw, err := privateRead(*path, 2<<20)
	var p observationAssemblyPlan
	if err != nil || hash(raw) != *expected || json.Unmarshal(raw, &p) != nil {
		return errInput
	}
	r, err := assembleObservations(ctx, *workspace, *name, p, raw, *create)
	if err != nil {
		return err // No partial success receipt. Incomplete output is retained.
	}
	return json.NewEncoder(out).Encode(r)
}

func assembleObservations(ctx context.Context, workspace, name string, p observationAssemblyPlan, raw []byte, create bool) (observationAssemblyReceipt, error) {
	fail := observationAssemblyReceipt{}
	if ctx == nil {
		return fail, errInput
	}
	canonical, err := json.Marshal(p)
	if err != nil || !bytes.Equal(raw, canonical) || p.Version != 1 || len(p.Usage)+len(p.TokenUsage) == 0 || len(p.Usage)+len(p.TokenUsage) > 4096 || len(p.Wallet) == 0 || len(p.Wallet) > 64 || !validSHA(p.FundingDB.SHA256) {
		return fail, errInput
	}
	root, output, err := jobPath(workspace, name)
	if err != nil {
		return fail, err
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		return fail, errInput
	}
	fundingPlan, _, err := readMonthPlan(p.FundingPlan.Path, p.FundingPlan.SHA256)
	if err != nil {
		return fail, err
	}
	windows, err := validateMonthAuditPlan(ctx, fundingPlan)
	if err != nil {
		return fail, err
	}
	if err := sealedAssemblySource(ctx, p.FundingDB); err != nil {
		return fail, err
	}
	source, err := openReceiver(ctx, p.FundingDB.Path, false)
	if err != nil {
		return fail, err
	}
	defer source.Close()
	// Preflight only mutates a private in-memory receiver, never a source file.
	memory, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return fail, err
	}
	memory.SetMaxOpenConns(1)
	defer memory.Close()
	selection, before, pages, rows, err := fillObservationAssembly(ctx, source, memory, fundingPlan, windows, p)
	if err != nil {
		return fail, err
	}
	selectionRaw, err := json.Marshal(selection)
	if err != nil {
		return fail, err
	}
	reviewRaw, err := json.Marshal(before)
	if err != nil || len(reviewRaw) > 8<<20 {
		return fail, errInput
	}
	if err := sealedAssemblySource(ctx, p.FundingDB); err != nil {
		return fail, err
	}
	r := observationAssemblyReceipt{Version: 1, Mode: "checked_observation_assembly_no_output", PlanSHA256: hash(raw),
		SelectionSHA256: hash(selectionRaw), ReviewSHA256: hash(reviewRaw), FundingPages: pages, FundingRows: rows,
		UsageCaptures: len(selection.UsageWindows) * 2, WalletCaptures: len(selection.WalletIDs)}
	if !create {
		return r, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return fail, err
	}
	// Only a fresh directory is ever written. Failures leave recoverable partial
	// evidence without a completion marker; a retry may not overwrite it.
	if err := os.Mkdir(output, 0700); err != nil {
		return fail, err
	}
	if err := exclusiveFile(filepath.Join(output, "plan.json"), raw); err != nil {
		return fail, err
	}
	pending := filepath.Join(output, "observations.pending.sqlite")
	if err := exclusiveFile(pending, nil); err != nil {
		return fail, err
	}
	target, err := openReceiver(ctx, pending, true)
	if err != nil {
		return fail, err
	}
	selected, after, copiedPages, copiedRows, copyErr := fillObservationAssembly(ctx, source, target, fundingPlan, windows, p)
	err = errors.Join(copyErr, target.Close())
	if err != nil || copiedPages != pages || copiedRows != rows || !reflect.DeepEqual(selected, selection) || !reflect.DeepEqual(before, after) {
		return fail, errors.Join(errInput, err)
	}
	// Re-open the actual output read-only and reconstruct all reports. An INSERT
	// receipt or successful file copy is insufficient for sealing the database.
	target, err = openReceiver(ctx, pending, false)
	if err != nil {
		return fail, err
	}
	again, auditErr := reviewObservationAssembly(ctx, target, fundingPlan, windows, selection, pages, rows)
	err = errors.Join(auditErr, target.Close())
	if err != nil || !reflect.DeepEqual(before, again) {
		return fail, errors.Join(errInput, err)
	}
	if err := sealedAssemblySource(ctx, p.FundingDB); err != nil {
		return fail, err
	}
	if err := syncPath(pending); err != nil {
		return fail, err
	}
	r.DatabaseSHA256, r.DatabaseBytes, err = hashAssemblyDatabase(ctx, pending)
	if err != nil {
		return fail, err
	}
	for _, file := range []struct {
		name string
		raw  []byte
	}{{"selection.json", selectionRaw}, {"review.json", reviewRaw}} {
		if err := exclusiveFile(filepath.Join(output, file.name), file.raw); err != nil {
			return fail, err
		}
	}
	if err := publishNewFile(pending, filepath.Join(output, "observations.sqlite")); err != nil {
		return fail, err
	}
	if err := errors.Join(ctx.Err(), syncPath(output)); err != nil {
		return fail, err
	}
	r.Mode = "assembled_observations_not_bill_approval"
	seal, err := json.Marshal(r)
	if err != nil {
		return fail, err
	}
	marker := filepath.Join(output, "assembly.pending.json")
	if err := exclusiveFile(marker, seal); err != nil {
		return fail, err
	}
	if err := ctx.Err(); err != nil {
		return fail, err
	}
	if err := publishNewFile(marker, filepath.Join(output, "assembly.json")); err != nil {
		return fail, err
	}
	return r, errors.Join(syncPath(output), syncPath(root))
}

func sealedAssemblySource(ctx context.Context, f assemblyFile) error {
	if !filepath.IsAbs(f.Path) || filepath.Clean(f.Path) != f.Path || !validSHA(f.SHA256) {
		return errInput
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(f.Path + suffix); !os.IsNotExist(err) {
			return errInput
		}
	}
	got, _, err := hashAssemblyDatabase(ctx, f.Path)
	if err != nil || got != f.SHA256 {
		return errors.Join(errInput, err)
	}
	return nil
}

func fillObservationAssembly(ctx context.Context, source, target *sql.DB, plan monthAuditPlan, windows []monthAuditWindow, p observationAssemblyPlan) (observationAssemblySelection, observationAssemblyReview, int, int, error) {
	var empty observationAssemblySelection
	var noReview observationAssemblyReview
	if _, err := target.ExecContext(ctx, "PRAGMA page_size=4096"); err != nil {
		return empty, noReview, 0, 0, err
	}
	var limit int
	if err := target.QueryRowContext(ctx, "PRAGMA max_page_count=32768").Scan(&limit); err != nil || limit != 32768 {
		return empty, noReview, 0, 0, errInput
	}
	for _, initialize := range []func(context.Context, *sql.DB) error{monthlysource.InitFundingPageStore, monthlysource.InitUsageWindowStore, monthlysource.InitWalletObservationStore} {
		if err := initialize(ctx, target); err != nil {
			return empty, noReview, 0, 0, err
		}
	}
	selection := observationAssemblySelection{Version: 1, GroupID: plan.GroupID, Funding: plan.Funding}
	var pages, records int
	var size int64
	for _, w := range windows {
		selection.FundingWindows = append(selection.FundingWindows, w.Version)
		v := w.Version
		for _, scan := range [][2]string{{v.OlderScanID, v.OlderHeadID}, {v.NewerScanID, v.NewerHeadID}} {
			n, rows, bytes, err := copySelectedScan(ctx, source, target, scan[0], scan[1], (96<<20)-size, 100000-records)
			if err != nil {
				return empty, noReview, 0, 0, err
			}
			pages, records, size = pages+n, records+rows, size+bytes
		}
	}
	// The sealed source may not hide extra, unselected financial records.
	if _, err := reviewAssembledMonth(ctx, source, plan, windows, pages, records); err != nil {
		return empty, noReview, 0, 0, err
	}
	seen := map[string]bool{}
	var captureBytes int
	load := func(ref assemblyCapture, kind string) ([]byte, error) {
		raw, err := readAssemblyCapture(ref, kind)
		if err != nil || seen[hash(raw)] {
			return nil, errors.Join(errInput, err)
		}
		captureBytes += len(raw)
		if captureBytes > 32<<20 {
			return nil, errInput
		}
		seen[hash(raw)] = true
		return raw, nil
	}
	for _, pair := range p.Usage {
		var ids [2]string
		for i, ref := range []assemblyCapture{pair.Older, pair.Newer} {
			raw, err := load(ref, "observe-usage")
			var observation monthlysource.UsageWindowObservation
			if err != nil || json.Unmarshal(raw, &observation) != nil {
				return empty, noReview, 0, 0, errors.Join(errInput, err)
			}
			id, err := monthlysource.SaveUsageWindowObservation(ctx, target, observation)
			if err != nil || id != hash(raw) {
				return empty, noReview, 0, 0, errors.Join(errInput, err)
			}
			ids[i] = id
		}
		selection.UsageWindows = append(selection.UsageWindows, monthlysource.UsageWindowVersion{OlderID: ids[0], NewerID: ids[1]})
	}
	for _, pair := range p.TokenUsage {
		var ids [2]string
		for i, ref := range []assemblyCapture{pair.Older, pair.Newer} {
			raw, err := load(ref, "observe-token-usage")
			var parent monthlysource.TokenUsageWindowObservation
			if err != nil || json.Unmarshal(raw, &parent) != nil {
				return empty, noReview, 0, 0, errors.Join(errInput, err)
			}
			observation, err := monthlysource.UsageObservationFromToken(parent)
			if err != nil {
				return empty, noReview, 0, 0, errors.Join(errInput, err)
			}
			id, err := monthlysource.SaveUsageWindowObservation(ctx, target, observation)
			if err != nil {
				return empty, noReview, 0, 0, errors.Join(errInput, err)
			}
			ids[i] = id
			selection.TokenProjections = append(selection.TokenProjections, assemblyTokenProjection{ParentSHA256: hash(raw), UsageSHA256: id})
		}
		selection.UsageWindows = append(selection.UsageWindows, monthlysource.UsageWindowVersion{OlderID: ids[0], NewerID: ids[1]})
	}
	for _, ref := range p.Wallet {
		raw, err := load(ref, "observe-wallet")
		var observation monthlysource.WalletObservation
		if err != nil || json.Unmarshal(raw, &observation) != nil {
			return empty, noReview, 0, 0, errors.Join(errInput, err)
		}
		id, err := monthlysource.SaveWalletObservation(ctx, target, observation)
		if err != nil || id != hash(raw) {
			return empty, noReview, 0, 0, errors.Join(errInput, err)
		}
		selection.WalletIDs = append(selection.WalletIDs, id)
	}
	sort.Slice(selection.UsageWindows, func(i, j int) bool { return selection.UsageWindows[i].OlderID < selection.UsageWindows[j].OlderID })
	slices.Sort(selection.WalletIDs)
	sort.Slice(selection.TokenProjections, func(i, j int) bool {
		return selection.TokenProjections[i].ParentSHA256 < selection.TokenProjections[j].ParentSHA256
	})
	review, err := reviewObservationAssembly(ctx, target, plan, windows, selection, pages, records)
	return selection, review, pages, records, err
}

func readAssemblyCapture(ref assemblyCapture, kind string) ([]byte, error) {
	if !filepath.IsAbs(ref.Directory) || filepath.Clean(ref.Directory) != ref.Directory || !validSHA(ref.ManifestSHA256) {
		return nil, errInput
	}
	if _, err := privateInfo(ref.Directory, true); err != nil {
		return nil, err
	}
	manifestRaw, err := readAssemblyManifest(ref.Directory)
	var marker observationReceipt
	if err != nil || hash(manifestRaw) != ref.ManifestSHA256 || json.Unmarshal(manifestRaw, &marker) != nil || marker.Version != 1 || !marker.Complete || marker.Mode != "retained_private_observation_not_bill_approval" || marker.Directory != ref.Directory {
		return nil, errInput
	}
	canonical, err := json.Marshal(marker)
	if err != nil || !bytes.Equal(manifestRaw, canonical) {
		return nil, errInput
	}
	requestRaw, err := privateRead(filepath.Join(ref.Directory, "request.json"), 4096)
	var request observationRequest
	if err != nil || hash(requestRaw) != marker.RequestSHA256 || json.Unmarshal(requestRaw, &request) != nil || request.Kind != kind || request.validate(time.Now()) != nil {
		return nil, errInput
	}
	canonical, err = json.Marshal(request)
	if err != nil || !bytes.Equal(requestRaw, canonical) {
		return nil, errInput
	}
	limit := int64(65536)
	if kind == "observe-token-usage" {
		limit = monthlysource.TokenUsageObservationLimit
	}
	raw, err := privateRead(filepath.Join(ref.Directory, "observation.json"), limit)
	if err != nil || hash(raw) != marker.ObservationSHA256 || validateObservation(raw, request) != nil {
		return nil, errInput
	}
	return raw, nil
}

// observeOne seals by linking manifest.pending.json to manifest.json and
// intentionally retains both names. Accept only that exact two-name inode,
// not an arbitrary hardlink or a generally relaxed privateRead contract.
func readAssemblyManifest(directory string) ([]byte, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if err := observationRootCurrent(root, directory); err != nil {
		return nil, err
	}
	before, err := root.Lstat("manifest.json")
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0077 != 0 || before.Size() <= 0 || before.Size() > 4096 {
		return nil, errInput
	}
	st, ok := before.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) || (st.Nlink != 1 && st.Nlink != 2) {
		return nil, errInput
	}
	if st.Nlink == 2 {
		pending, err := root.Lstat("manifest.pending.json")
		if err != nil || !pending.Mode().IsRegular() || !os.SameFile(before, pending) {
			return nil, errInput
		}
	}
	f, err := root.OpenFile("manifest.json", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, errInput
	}
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	after, statErr := f.Stat()
	current, pathErr := root.Lstat("manifest.json")
	if err != nil || statErr != nil || pathErr != nil || !os.SameFile(before, after) || !os.SameFile(before, current) ||
		int64(len(raw)) != before.Size() || after.Size() != before.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, errInput
	}
	last, ok := current.Sys().(*syscall.Stat_t)
	if !ok || last.Nlink != st.Nlink || current.Mode() != before.Mode() {
		return nil, errInput
	}
	if st.Nlink == 2 {
		pending, err := root.Lstat("manifest.pending.json")
		if err != nil || !os.SameFile(current, pending) {
			return nil, errInput
		}
	}
	return raw, observationRootCurrent(root, directory)
}

func reviewObservationAssembly(ctx context.Context, db *sql.DB, plan monthAuditPlan, windows []monthAuditWindow, selection observationAssemblySelection, pages, rows int) (observationAssemblyReview, error) {
	var empty observationAssemblyReview
	funding, err := reviewAssembledMonth(ctx, db, plan, windows, pages, rows)
	if err != nil {
		return empty, err
	}
	for _, a := range funding.Accounts {
		if a.Status != "observed_month" || a.Totals == nil {
			return empty, errInput
		}
	}
	usage, err := monthlysource.ReviewUsageMonth(ctx, db, plan.Funding, selection.UsageWindows)
	if err != nil {
		return empty, err
	}
	report := usage.Report()
	for _, a := range report.Accounts {
		if a.Totals == nil || a.ObservedSeconds != report.To-report.From || a.MatchingSeconds != report.To-report.From || len(a.MissingWindows) != 0 {
			return empty, errInput
		}
		for _, total := range a.Totals.Sources {
			if total.BillingSource != "wallet" {
				return empty, errInput // Do not silently normalize unknown sources.
			}
		}
	}
	wallet := make([]monthlysource.WalletObservation, 0, len(selection.WalletIDs))
	seen := map[[2]int64]bool{}
	for _, id := range selection.WalletIDs {
		o, err := monthlysource.LoadWalletObservation(ctx, db, id)
		if err != nil || o.SourceIdentity != plan.Funding.SourceIdentity || len(o.Users) != len(plan.Funding.UserIDs) || o.CompletedAtMS/1000 < report.To || o.CompletedAtMS > time.Now().UnixMilli() {
			return empty, errors.Join(errInput, err)
		}
		for i, u := range o.Users {
			key := [2]int64{u.UserID, o.CompletedAtMS / 1000}
			if u.UserID != plan.Funding.UserIDs[i] || !u.Exists || u.BalanceQuota == nil || u.UsedQuota == nil || seen[key] {
				return empty, errInput
			}
			seen[key] = true
		}
		wallet = append(wallet, o)
	}
	var usageCount, walletCount int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM portal_monthly_usage_observations").Scan(&usageCount); err != nil {
		return empty, err
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM portal_monthly_wallet_observations").Scan(&walletCount); err != nil || usageCount != len(selection.UsageWindows)*2 || walletCount != len(selection.WalletIDs) {
		return empty, errors.Join(errInput, err)
	}
	return observationAssemblyReview{Funding: funding, Usage: report, Wallet: wallet}, ctx.Err()
}
