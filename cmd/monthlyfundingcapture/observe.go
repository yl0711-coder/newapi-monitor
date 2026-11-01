package main

// Explicit, single bounded source observation. This entrypoint does not turn
// archived query JSON into captures, infer monthly boundaries, initialize a
// database, select generation input or publish bills.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type observationRequest struct {
	Version        int     `json:"version"`
	Kind           string  `json:"kind"`
	SourceIdentity string  `json:"source_identity"`
	UserIDs        []int64 `json:"user_ids"`
	From           int64   `json:"from"`
	To             int64   `json:"to"`
}

type observationReceipt struct {
	Version           int    `json:"version"`
	Mode              string `json:"mode"`
	RequestSHA256     string `json:"request_sha256"`
	ObservationSHA256 string `json:"observation_sha256,omitempty"`
	Directory         string `json:"directory,omitempty"`
	Complete          bool   `json:"complete"`
}

type observationCapture func(context.Context, string, observationRequest) ([]byte, error)

// Preserve the cause for cancellation handling, without returning connection
// addresses, credentials or driver messages to the operator-facing output.
type observationTransportFailure struct{ cause error }

func (e *observationTransportFailure) Error() string {
	return "observation transport preparation failed"
}
func (e *observationTransportFailure) Unwrap() error { return e.cause }

// SSH + MySQL authentication may need several round trips. This budget is
// transport-only: the separate five-second observation and two-second SQL
// ceilings remain unchanged, within each caller's original outer deadline.
const observationTransportTimeout = 4 * time.Second

func prepareObservationTransport(parent context.Context, connect func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, observationTransportTimeout)
	defer cancel()
	if err := connect(ctx); err != nil {
		return &observationTransportFailure{cause: err}
	}
	return nil
}

func runObservation(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "existing private canonical directory")
	name := fs.String("output", "", "new private output directory name")
	source := fs.String("source-identity", "", "independently verified MySQL server/database digest")
	users := fs.String("user-ids", "", "explicit user IDs; usage requires exactly one")
	from := fs.Int64("from", 0, "usage inclusive Unix second")
	to := fs.Int64("to", 0, "usage exclusive Unix second; at most one day")
	connection := fs.String("connection-file", "", "private loopback-only connection JSON; only read with -capture")
	capture := fs.Bool("capture", false, "perform ONE bounded read-only capture and retain original bytes locally")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 {
		return errInput
	}
	ids, err := parseIDs(*users)
	if err != nil || (*capture && *connection == "") {
		return errInput
	}
	r := observationRequest{Version: 1, Kind: args[0], SourceIdentity: *source, UserIDs: ids, From: *from, To: *to}
	result, err := observeOne(ctx, *workspace, *name, *connection, r, *capture, captureObservation)
	if result.Mode == "" {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(result))
}

func (r observationRequest) validate(now time.Time) error {
	if r.Version != 1 || !validSHA(r.SourceIdentity) || len(r.UserIDs) == 0 || len(r.UserIDs) > monthlysource.WalletBatchLimit {
		return errInput
	}
	seen := map[int64]bool{}
	for _, id := range r.UserIDs {
		if id <= 0 || seen[id] {
			return errInput
		}
		seen[id] = true
	}
	switch r.Kind {
	case "observe-usage", "observe-token-usage":
		if len(r.UserIDs) != 1 || r.From <= 0 || r.To <= r.From || r.To-r.From > 86400 || r.To > now.Unix()-60 {
			return errInput
		}
	case "observe-wallet":
		if r.From != 0 || r.To != 0 {
			return errInput
		}
	default:
		return errInput
	}
	return nil
}

func usageObservationRequest(r observationRequest) monthlysource.UsageWindowRequest {
	return monthlysource.UsageWindowRequest{SourceIdentity: r.SourceIdentity,
		Rules:  monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000},
		UserID: r.UserIDs[0], From: r.From, To: r.To}
}

func captureObservation(ctx context.Context, connection string, r observationRequest) ([]byte, error) {
	cfg, err := readConnection(connection)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if r.Kind == "observe-token-usage" {
		// Establish the transport before starting the separately bounded
		// observation. A cold SSH/MySQL handshake is not a business query and
		// must not consume most of the existing five-second observation budget.
		// Connection preparation has its own four-second ceiling, no retry, and
		// the original command deadline still bounds the whole operation.
		if err := prepareObservationTransport(ctx, db.PingContext); err != nil {
			return nil, err
		}
		o, err := monthlysource.CaptureTokenUsageWindow(ctx, db, usageObservationRequest(r))
		if err != nil {
			return nil, err
		}
		return json.Marshal(o)
	}
	if r.Kind == "observe-usage" {
		o, err := monthlysource.CaptureUsageWindow(ctx, db, usageObservationRequest(r))
		if err != nil {
			return nil, err
		}
		return json.Marshal(o)
	}
	o, err := monthlysource.CaptureWallet(ctx, db, r.SourceIdentity, r.UserIDs)
	if err != nil {
		return nil, err
	}
	return json.Marshal(o)
}

func validateObservation(raw []byte, r observationRequest) error {
	limit := 65536
	if r.Kind == "observe-token-usage" {
		limit = monthlysource.TokenUsageObservationLimit
	}
	if len(raw) == 0 || len(raw) > limit {
		return errInput
	}
	var canonical []byte
	if r.Kind == "observe-token-usage" {
		var o monthlysource.TokenUsageWindowObservation
		if json.Unmarshal(raw, &o) != nil || monthlysource.ValidateTokenUsageWindowObservation(o) != nil || o.Request != usageObservationRequest(r) {
			return errInput
		}
		canonical, _ = json.Marshal(o)
	} else if r.Kind == "observe-usage" {
		var o monthlysource.UsageWindowObservation
		if json.Unmarshal(raw, &o) != nil || monthlysource.ValidateUsageWindowObservation(o) != nil || o.Request != usageObservationRequest(r) {
			return errInput
		}
		canonical, _ = json.Marshal(o)
	} else {
		var o monthlysource.WalletObservation
		if json.Unmarshal(raw, &o) != nil || monthlysource.ValidateWalletObservation(o) != nil || o.SourceIdentity != r.SourceIdentity || len(o.Users) != len(r.UserIDs) {
			return errInput
		}
		ids := map[int64]bool{}
		for _, id := range r.UserIDs {
			ids[id] = true
		}
		for _, u := range o.Users {
			if !ids[u.UserID] {
				return errInput
			}
			delete(ids, u.UserID)
		}
		if len(ids) != 0 {
			return errInput
		}
		canonical, _ = json.Marshal(o)
	}
	if !bytes.Equal(raw, canonical) {
		return errInput // Reject duplicate/unknown fields and noncanonical bytes.
	}
	return nil
}

func observeOne(ctx context.Context, workspace, name, connection string, r observationRequest, capture bool, collect observationCapture) (result observationReceipt, err error) {
	return observeOneWithWrite(ctx, workspace, name, connection, r, capture, collect, writeObservationFile)
}

func writeObservationFile(dir *os.Root, name string, b []byte) error {
	f, err := dir.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(b)
	return errors.Join(writeErr, f.Sync(), f.Close())
}

func syncObservationRoot(dir *os.Root) error {
	f, err := dir.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func observationRootCurrent(root *os.Root, path string) error {
	opened, err := root.Stat(".")
	current, pathErr := privateInfo(path, true)
	if err != nil || pathErr != nil || !os.SameFile(opened, current) {
		return errInput
	}
	return nil
}

func verifyObservationFile(root *os.Root, name string, want []byte) error {
	before, err := root.Lstat(name)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0077 != 0 || before.Size() != int64(len(want)) {
		return errInput
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) {
		return errInput
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return errInput
	}
	raw, err := io.ReadAll(io.LimitReader(f, int64(len(want))+1))
	after, statErr := f.Stat()
	current, pathErr := root.Lstat(name)
	if err != nil || statErr != nil || pathErr != nil || !os.SameFile(opened, current) || !os.SameFile(opened, after) ||
		!current.Mode().IsRegular() || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || !bytes.Equal(raw, want) {
		return errInput
	}
	return nil
}

func observeOneWithWrite(ctx context.Context, workspace, name, connection string, r observationRequest, capture bool, collect observationCapture, writeFile func(*os.Root, string, []byte) error) (result observationReceipt, err error) {
	if ctx == nil || collect == nil || writeFile == nil || r.validate(time.Now()) != nil {
		return result, errInput
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	rootPath, output, err := jobPath(workspace, name)
	if err != nil || rootPath != workspace {
		return result, errInput
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return result, err
	}
	defer root.Close()
	if _, err := root.Lstat(name); !os.IsNotExist(err) {
		return result, errInput
	}
	requestRaw, err := json.Marshal(r)
	if err != nil {
		return result, err
	}
	result = observationReceipt{Version: 1, Mode: "checked_local_output_no_source_connection", RequestSHA256: hash(requestRaw)}
	if !capture {
		return result, nil // No credentials, locks, output files or source connection.
	}
	lock, err := workspaceLock(rootPath, true, true)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err := observationRootCurrent(root, rootPath); err != nil {
		return result, err
	}
	if _, err := root.Lstat(name); !os.IsNotExist(err) {
		return result, errInput
	}
	result.Mode = "capture_attempted_no_complete_output"
	raw, err := collect(ctx, connection, r)
	if err != nil {
		return result, err
	}
	if err := validateObservation(raw, r); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := observationRootCurrent(root, rootPath); err != nil {
		return result, err
	}
	if err := root.Mkdir(name, 0700); err != nil {
		return result, err
	}
	result.Mode, result.Directory = "incomplete_private_observation_do_not_use", output
	dir, err := root.OpenRoot(name)
	if err != nil {
		return result, err
	}
	defer dir.Close()
	write := func(name string, b []byte) error {
		return writeFile(dir, name, b)
	}
	if err := write("request.json", requestRaw); err != nil {
		return result, err
	}
	if err := write("observation.json", raw); err != nil {
		return result, err
	}
	complete := result
	complete.Mode, complete.Complete, complete.ObservationSHA256 = "retained_private_observation_not_bill_approval", true, hash(raw)
	seal, err := json.Marshal(complete)
	if err != nil {
		return result, err
	}
	if err := write("manifest.pending.json", seal); err != nil {
		return result, err
	}
	// Read all payloads again AFTER the final write, not only immediately after
	// each file. A late write failure/corruption must not get a completion seal.
	for name, want := range map[string][]byte{"request.json": requestRaw, "observation.json": raw, "manifest.pending.json": seal} {
		if err := verifyObservationFile(dir, name, want); err != nil {
			return result, err
		}
	}
	// Confirm the named directory is still the one holding our open handles.
	before, e1 := dir.Stat(".")
	after, e2 := root.Lstat(name)
	if e1 != nil || e2 != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
		return result, errInput
	}
	if err := observationRootCurrent(root, rootPath); err != nil {
		return result, err
	}
	if err := errors.Join(syncObservationRoot(dir), syncObservationRoot(root)); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := dir.Link("manifest.pending.json", "manifest.json"); err != nil {
		return result, err
	}
	result = complete // A subsequent sync failure must not claim zero side effects.
	return result, errors.Join(syncObservationRoot(dir), observationRootCurrent(root, rootPath), ctx.Err())
}
