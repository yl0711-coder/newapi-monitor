package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

const assemblyDatabaseLimit int64 = 128 << 20

// Local evidence receipt only. No authorization, preparation/balance evidence,
// source completeness attestation or publication permission is manufactured.
type monthAssemblyReceipt struct {
	Version        int    `json:"version"`
	Mode           string `json:"mode"`
	GroupID        int64  `json:"group_id"`
	Month          string `json:"month"`
	PlanSHA256     string `json:"plan_sha256"`
	ReviewSHA256   string `json:"review_sha256"`
	DatabaseSHA256 string `json:"database_sha256,omitempty"`
	DatabaseBytes  int64  `json:"database_bytes,omitempty"`
	Pages          int    `json:"pages,omitempty"`
	Rows           int    `json:"observation_rows,omitempty"` // both passes, never money totals
}

func runMonthAssembly(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("assemble-month", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "", "existing private capture workspace")
	planPath := fs.String("plan", "", "private canonical month plan")
	expected := fs.String("expected-plan", "", "pinned month plan SHA256")
	name := fs.String("output", "", "new private output directory name within workspace")
	create := fs.Bool("create-output", false, "create and verify new local evidence; default audit only")
	if fs.Parse(args) != nil || fs.NArg() != 0 || !jobName.MatchString(*name) {
		return errInput
	}
	p, raw, err := readMonthPlan(*planPath, *expected)
	if err != nil {
		return err
	}
	r, err := assembleMonth(ctx, *workspace, *name, p, raw, *create)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(r)
}

func assembleMonth(ctx context.Context, workspace, name string, p monthAuditPlan, raw []byte, create bool) (monthAssemblyReceipt, error) {
	fail := monthAssemblyReceipt{}
	canonical, err := json.Marshal(p)
	if err != nil || string(raw) != string(canonical) {
		return fail, errInput
	}
	windows, err := validateMonthAuditPlan(ctx, p)
	if err != nil {
		return fail, err
	}
	root, output, err := jobPath(workspace, name)
	if err != nil {
		return fail, err
	}
	lock, err := workspaceLock(root, create, false)
	if err != nil {
		return fail, err
	}
	defer lock.Close()
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		return fail, errInput
	}
	before, err := auditMonthLocked(ctx, root, p, windows)
	if err != nil {
		return fail, err
	}
	for _, account := range before.Accounts {
		if account.Status != "observed_month" || account.Totals == nil {
			return fail, errors.New("unresolved monthly observations; audit before assembly")
		}
	}
	reviewRaw, err := json.Marshal(before)
	if err != nil || len(reviewRaw) > 8<<20 {
		return fail, errInput
	}
	r := monthAssemblyReceipt{Version: 1, Mode: "checked_local_assembly_no_output", GroupID: p.GroupID, Month: p.Funding.Month, PlanSHA256: hash(raw), ReviewSHA256: hash(reviewRaw)}
	if !create {
		return r, nil
	}
	if err := ctx.Err(); err != nil {
		return fail, err
	}
	// Exclusive directory creation: failed output is retained, never removed,
	// repaired, resumed as a different version, or overwritten on a retry.
	if err := os.Mkdir(output, 0700); err != nil {
		return fail, err
	}
	if err := exclusiveFile(filepath.Join(output, "month-plan.json"), raw); err != nil {
		return fail, err
	}
	pending := filepath.Join(output, "funding.pending.sqlite")
	if err := exclusiveFile(pending, nil); err != nil {
		return fail, err
	}
	db, err := openReceiver(ctx, pending, true)
	if err != nil {
		return fail, err
	}
	r.Pages, r.Rows, err = copyMonth(ctx, root, p, windows, db)
	err = errors.Join(err, db.Close())
	if err != nil {
		return fail, err
	}
	// Reopen the actual assembled file read-only; don't trust an in-memory
	// result from the original sources or the successful INSERT receipts.
	db, err = openReceiver(ctx, pending, false)
	if err != nil {
		return fail, err
	}
	after, err := reviewAssembledMonth(ctx, db, p, windows, r.Pages, r.Rows)
	err = errors.Join(err, db.Close())
	if err != nil || !reflect.DeepEqual(before, after) {
		return fail, errors.Join(errInput, err)
	}
	if err := ctx.Err(); err != nil {
		return fail, err
	}
	if err := syncPath(pending); err != nil {
		return fail, err
	}
	r.DatabaseSHA256, r.DatabaseBytes, err = hashAssemblyDatabase(ctx, pending)
	if err != nil {
		return fail, err
	}
	if err := exclusiveFile(filepath.Join(output, "review.json"), reviewRaw); err != nil {
		return fail, err
	}
	if err := publishNewFile(pending, filepath.Join(output, "funding.sqlite")); err != nil {
		return fail, err
	}
	if err := syncPath(output); err != nil {
		return fail, err
	}
	if err := ctx.Err(); err != nil {
		return fail, err
	}
	r.Mode = "assembled_local_evidence_not_bill_approval"
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
	if err := errors.Join(syncPath(output), syncPath(root)); err != nil {
		return fail, err
	}
	return r, nil
}

func publishNewFile(pending, final string) error {
	// Link does not replace an existing target. Only our own staging filename
	// is unlinked; file contents remain recoverable under the final filename.
	if err := os.Link(pending, final); err != nil {
		return err
	}
	return os.Remove(pending)
}

func copyMonth(ctx context.Context, root string, p monthAuditPlan, windows []monthAuditWindow, target *sql.DB) (int, int, error) {
	if _, err := target.ExecContext(ctx, "PRAGMA page_size=4096"); err != nil {
		return 0, 0, err
	}
	var limit int
	if err := target.QueryRowContext(ctx, "PRAGMA max_page_count=32768").Scan(&limit); err != nil || limit != 32768 {
		return 0, 0, errInput
	}
	if err := monthlysource.InitFundingPageStore(ctx, target); err != nil {
		return 0, 0, err
	}
	var pages, records int
	var size int64
	for _, w := range windows {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		// Validate the selected manifest and full source index again at copy time.
		if _, err := auditMonthWindow(ctx, root, p, w); err != nil {
			return 0, 0, err
		}
		db, err := openReceiver(ctx, filepath.Join(root, w.Job, "funding.sqlite"), false)
		if err != nil {
			return 0, 0, err
		}
		v := w.Version
		for _, scan := range [][2]string{{v.OlderScanID, v.OlderHeadID}, {v.NewerScanID, v.NewerHeadID}} {
			var n, rows int
			var bytes int64
			n, rows, bytes, err = copySelectedScan(ctx, db, target, scan[0], scan[1], (96<<20)-size, 100000-records)
			pages, records, size = pages+n, records+rows, size+bytes
			if err != nil {
				break
			}
		}
		err = errors.Join(err, db.Close())
		if err != nil {
			return 0, 0, err
		}
	}
	return pages, records, nil
}

func copySelectedScan(ctx context.Context, source, target *sql.DB, scanID, head string, remainingBytes int64, remainingRows int) (int, int, int64, error) {
	if source == nil || target == nil || source == target || !validSHA(head) {
		return 0, 0, 0, errInput
	}
	rows, err := source.QueryContext(ctx, `SELECT id,sequence,CASE WHEN length(payload)<=2097152 THEN payload ELSE NULL END FROM portal_monthly_funding_pages WHERE scan_id=? ORDER BY sequence LIMIT 1001`, scanID)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	var pages, records int
	var size int64
	var last string
	var ended bool
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return 0, 0, 0, err
		}
		var id string
		var seq int
		var raw []byte
		if err := rows.Scan(&id, &seq, &raw); err != nil {
			return 0, 0, 0, err
		}
		size += int64(len(raw))
		var p monthlysource.FundingPage
		if ended || seq != pages+1 || seq > 1000 || len(raw) == 0 || size > 32<<20 || size > remainingBytes || hash(raw) != id || json.Unmarshal(raw, &p) != nil || p.Scan.ID != scanID || p.PreviousID != last {
			return 0, 0, 0, errInput
		}
		canonical, err := json.Marshal(p)
		if err != nil || string(canonical) != string(raw) {
			return 0, 0, 0, errInput
		}
		records += len(p.Rows)
		if records > 20000 || records > remainingRows {
			return 0, 0, 0, errInput
		}
		saved, err := monthlysource.SaveFundingPage(ctx, target, p)
		if err != nil || saved != id {
			return 0, 0, 0, errors.Join(errInput, err)
		}
		pages++
		last, ended = id, p.EndReached
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, err
	}
	if pages == 0 || !ended || last != head {
		return 0, 0, 0, errInput
	}
	return pages, records, size, nil
}

func reviewAssembledMonth(ctx context.Context, db *sql.DB, p monthAuditPlan, windows []monthAuditWindow, pages, records int) (monthlysource.FundingMoneyReview, error) {
	fail := monthlysource.FundingMoneyReview{}
	var actualPages, actualRecords int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM portal_monthly_funding_pages").Scan(&actualPages); err != nil {
		return fail, err
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM portal_monthly_funding_records").Scan(&actualRecords); err != nil {
		return fail, err
	}
	if actualPages != pages || actualRecords != records {
		return fail, errInput
	}
	observations := make([]monthlysource.FundingWindowObservation, 0, len(windows))
	for _, w := range windows {
		v := w.Version
		o, err := monthlysource.ReviewFundingWindow(ctx, db, v.OlderScanID, v.NewerScanID)
		if err != nil {
			return fail, err
		}
		c := o.Comparison()
		if c.OlderHeadID != v.OlderHeadID || c.NewerHeadID != v.NewerHeadID {
			return fail, errInput
		}
		observations = append(observations, o)
	}
	return monthlysource.ReviewFundingMoney(ctx, p.Funding, observations)
}

func hashAssemblyDatabase(ctx context.Context, path string) (string, int64, error) {
	before, err := privateInfo(path, false)
	if err != nil || before.Size() <= 0 || before.Size() > assemblyDatabaseLimit {
		return "", 0, errInput
	}
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return "", 0, errInput
	}
	h := sha256.New()
	buffer := make([]byte, 64<<10)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		n, err := f.Read(buffer)
		size += int64(n)
		if size > assemblyDatabaseLimit {
			return "", 0, errInput
		}
		_, _ = h.Write(buffer[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", 0, err
		}
	}
	after, err := privateInfo(path, false)
	if err != nil || !os.SameFile(before, after) || size != before.Size() || size != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return "", 0, errInput
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}
