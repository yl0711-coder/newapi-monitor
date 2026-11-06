package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"
)

// CopyCapturedMonthObservations exports only the selected original funding
// chains, user-total projections and wallet views to an EMPTY private SQLite.
// Token originals remain in the returned canonical token archives. It does not
// copy another job, source connection, task, authority or customer bill table.
// A failure leaves an incomplete destination, never a financial approval.
// The caller must close, seal and fingerprint this NEW file before handoff.
func CopyCapturedMonthObservations(parent context.Context, source, destination *sql.DB, key string, scope OrganizationMonthScope) (CapturedMonthAssembly, error) {
	return CopyCapturedMonthObservationsBounded(parent, source, destination, key, scope, 64<<20)
}

// CopyCapturedMonthObservationsBounded may LOWER the original physical output
// limit to fit a private spool's remaining space. It cannot raise the 64 MiB
// limit, change original records or turn a partial copy into usable evidence.
func CopyCapturedMonthObservationsBounded(parent context.Context, source, destination *sql.DB, key string, scope OrganizationMonthScope, databaseBytes int64) (result CapturedMonthAssembly, err error) {
	return copyCapturedMonthObservationsBounded(parent, source, destination, key, scope, databaseBytes, false, false)
}

// Explicit new handoff path. Existing callers continue to copy version 1
// daily captures only. Both modes retain the same physical output limit.
func CopyRecoveredCapturedMonthObservationsBounded(parent context.Context, source, destination *sql.DB, key string, scope OrganizationMonthScope, databaseBytes int64) (CapturedMonthAssembly, error) {
	return copyCapturedMonthObservationsBounded(parent, source, destination, key, scope, databaseBytes, true, false)
}

func CopyPartitionedCapturedMonthObservationsBounded(parent context.Context, source, destination *sql.DB, key string, scope OrganizationMonthScope, databaseBytes int64, recovered bool) (CapturedMonthAssembly, error) {
	return copyCapturedMonthObservationsBounded(parent, source, destination, key, scope, databaseBytes, recovered, true)
}

func copyCapturedMonthObservationsBounded(parent context.Context, source, destination *sql.DB, key string, scope OrganizationMonthScope, databaseBytes int64, recovered, partitioned bool) (result CapturedMonthAssembly, err error) {
	defer func() {
		var coded interface{ Code() int }
		if errors.As(err, &coded) && coded.Code()&255 == 13 { // SQLite FULL, not missing money.
			err = errors.Join(ErrTokenMonthCaptureBudget, err)
		}
	}()
	var empty CapturedMonthAssembly
	if parent == nil || source == nil || destination == nil || source == destination {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	if databaseBytes < 4096 || databaseBytes > 64<<20 {
		return empty, ErrTokenMonthCaptureBudget
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	assembled, err := buildCapturedMonthSelection(ctx, source, key, scope, recovered, partitioned)
	if err != nil {
		return empty, err
	}
	var selection CapturedMonthSelection
	if err := json.Unmarshal(assembled.Selection, &selection); err != nil {
		return empty, err
	}
	var tables int
	if err := destination.QueryRowContext(ctx, `SELECT count(*) FROM main.sqlite_master`).Scan(&tables); err != nil {
		return empty, err
	}
	if tables != 0 {
		return empty, ErrTokenMonthCaptureIntegrity // includes distinct handles to source
	}
	// Bound physical output too, not only the payload after it has hit disk.
	// A fresh 4096-byte-page DB can use at most 64 MiB; the enclosing package
	// has its own combined DB/token-file budget before a completion marker.
	for _, q := range []string{`PRAGMA page_size=4096`, `PRAGMA max_page_count=` + strconv.FormatInt(databaseBytes/4096, 10), `PRAGMA journal_mode=DELETE`, `PRAGMA synchronous=FULL`} {
		if _, err := destination.ExecContext(ctx, q); err != nil {
			return empty, err
		}
	}
	for _, init := range []func(context.Context, *sql.DB) error{InitFundingPageStore, InitUsageWindowStore, InitWalletObservationStore} {
		if err := init(ctx, destination); err != nil {
			return empty, err
		}
	}
	for _, w := range selection.Funding {
		for _, pair := range [][2]string{{w.OlderScanID, w.OlderHeadID}, {w.NewerScanID, w.NewerHeadID}} {
			if err := copyCapturedFundingChain(ctx, source, destination, pair[0], pair[1]); err != nil {
				return empty, err
			}
		}
	}
	if partitioned {
		if err := copyPartitionedUsageRows(ctx, source, destination, selection); err != nil {
			return empty, err
		}
	} else {
		for _, account := range selection.Tokens {
			for _, w := range account.Usage {
				for _, id := range []string{w.OlderID, w.NewerID} {
					o, err := LoadUsageWindowObservation(ctx, source, id)
					if err != nil {
						return empty, err
					}
					got, err := SaveUsageWindowObservation(ctx, destination, o)
					if err != nil || got != id {
						return empty, captureCopyError(ctx, err)
					}
				}
			}
		}
	}
	for _, id := range selection.WalletIDs {
		o, err := LoadWalletObservation(ctx, source, id)
		if err != nil {
			return empty, err
		}
		got, err := SaveWalletObservation(ctx, destination, o)
		if err != nil || got != id {
			return empty, captureCopyError(ctx, err)
		}
	}
	verify := func() error { return VerifyCapturedObservationSelection(ctx, destination, assembled.Selection) }
	if partitioned {
		verify = func() error {
			parts, err := PartitionCapturedUsageIndex(ctx, assembled.Selection, time.Now())
			if err != nil {
				return err
			}
			return VerifyPartitionedCapturedObservationSelection(ctx, destination, digest(assembled.Selection), scope, parts)
		}
	}
	if err := verify(); err != nil {
		return empty, err
	}
	current, err := buildCapturedMonthSelection(ctx, source, key, scope, recovered, partitioned)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(current, assembled) {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return assembled, nil
}

// One bounded destination transaction instead of thousands of fsyncs. Source
// originals and indexed columns are revalidated per row; output is private,
// empty and physically capped by the enclosing copier. No network is accessed.
func copyPartitionedUsageRows(ctx context.Context, source, destination *sql.DB, selection CapturedMonthSelection) error {
	tx, err := destination.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	used := 0
	for _, a := range selection.Tokens {
		for _, w := range a.Usage {
			for _, id := range []string{w.OlderID, w.NewerID} {
				o, err := LoadUsageWindowObservation(ctx, source, id)
				if err != nil {
					return err
				}
				raw, err := usageWindowBytes(o)
				if err != nil || digest(raw) != id {
					return captureCopyError(ctx, err)
				}
				used += len(raw)
				if used > usageMonthBytesLimit {
					return ErrTokenMonthCaptureBudget
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_usage_observations(id,source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?)`, id, o.Request.SourceIdentity, o.Request.UserID, o.Request.From, o.Request.To, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
					return err
				}
				stored, err := loadUsageWindow(ctx, tx, id)
				if err != nil || !bytes.Equal(stored, raw) {
					return captureCopyError(ctx, err)
				}
			}
		}
	}
	return tx.Commit()
}

func captureCopyError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	return ErrTokenMonthCaptureIntegrity
}

func copyCapturedFundingChain(ctx context.Context, source, destination *sql.DB, scan, head string) error {
	// Walk the pinned head backwards; never select a newly appended head or
	// export unrelated scans. Save in original sequence using the existing
	// chain/dedup validator. Per-scan bounds match the original funding audit.
	pages := []FundingPage{}
	size := 0
	for id := head; id != ""; {
		if err := ctx.Err(); err != nil {
			return err
		}
		p, sequence, raw, err := loadFundingStored(ctx, source, false, id)
		if err != nil {
			return err
		}
		size += len(raw)
		if p.Scan.ID != scan || sequence <= 0 || sequence > 1000 || len(pages) >= 1000 || size > fundingAuditBytesLimit {
			return ErrFundingMonthCaptureIntegrity
		}
		pages = append(pages, p)
		id = p.PreviousID
	}
	if len(pages) == 0 {
		return ErrFundingMonthCaptureIntegrity
	}
	for i := len(pages) - 1; i >= 0; i-- {
		if _, err := SaveFundingPage(ctx, destination, pages[i]); err != nil {
			return err
		}
	}
	latest, err := LoadLatestFundingPage(ctx, destination, scan)
	if err != nil {
		return err
	}
	raw, err := fundingPageBytes(latest)
	if err != nil || digest(raw) != head {
		return captureCopyError(ctx, err)
	}
	return nil
}

// VerifyCapturedObservationSelection audits the selected records in a sealed
// copy or a private in-progress destination. Its caller MUST pin the original
// selection digest and authority separately. This is not historical balance,
// membership or permission to select/publish a customer bill.
func VerifyCapturedObservationSelection(parent context.Context, db *sql.DB, raw []byte) error {
	return verifyCapturedObservationSelection(parent, db, raw, nil)
}

// Explicit new reader; legacy callers do not silently accept larger captures.
// Reassembly is identity only. ALL usage projections, original funding and
// wallet records still pass the existing audits. The caller separately pins a
// sealed file and authority; this does not verify raw token archives or history.
func VerifyPartitionedCapturedObservationSelection(parent context.Context, db *sql.DB, originalSHA256 string, scope OrganizationMonthScope, archive PartitionedCapturedUsage) error {
	if parent == nil || db == nil {
		return ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	raw, err := ReassembleCapturedUsageIndex(ctx, archive, time.Now())
	if err != nil {
		return err
	}
	usage, err := ReviewPartitionedCapturedUsage(ctx, db, originalSHA256, scope, archive)
	if err != nil {
		return err
	}
	report := usage.Report()
	return verifyCapturedObservationSelection(ctx, db, raw, &report)
}

func verifyCapturedObservationSelection(parent context.Context, db *sql.DB, raw []byte, partitioned *UsageMonthReport) error {
	if parent == nil || db == nil || len(raw) == 0 || len(raw) > 2<<20 {
		return ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	var s CapturedMonthSelection
	if json.Unmarshal(raw, &s) != nil {
		return ErrTokenMonthCaptureIntegrity
	}
	canonical, err := json.Marshal(s)
	recovered := s.Version == 2 && s.Mode == RecoveredCapturedMonthSelectionMode
	legacy := s.Version == 1 && s.Mode == CapturedMonthSelectionMode
	if err != nil || !bytes.Equal(raw, canonical) || (!legacy && !recovered) || !validDigest(s.Key) || !validDigest(s.PlanSHA256) || len(s.WalletIDs) != 2 || len(s.Funding) == 0 || len(s.Funding) > 31 || len(s.Tokens) != len(s.Scope.Funding.UserIDs) {
		return ErrTokenMonthCaptureIntegrity
	}
	if _, err := canonicalFundingCaptureScope(s.Scope, time.Now()); err != nil {
		return err
	}
	windows := make([]FundingWindowObservation, 0, len(s.Funding))
	for _, w := range s.Funding {
		o, err := ReviewFundingWindow(ctx, db, w.OlderScanID, w.NewerScanID)
		if err != nil {
			return err
		}
		windows = append(windows, o)
	}
	funding, err := ReviewFundingMoney(ctx, s.Scope.Funding, windows)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(funding.Windows.Windows, s.Funding) || len(funding.Windows.MissingWindows) != 0 || len(funding.Accounts) != len(s.Scope.Funding.UserIDs) {
		return ErrFundingMonthCaptureIntegrity
	}
	for _, a := range funding.Accounts {
		if a.Status != "observed_month" || a.Totals == nil {
			return ErrFundingMonthCaptureIntegrity
		}
	}
	var older WalletObservation
	for i, id := range s.WalletIDs {
		o, err := capturedWalletForScope(ctx, db, id, s.Scope)
		if err != nil {
			return err
		}
		if i == 0 {
			older = o
		} else if id == s.WalletIDs[0] || o.StartedAtMS-older.CompletedAtMS < 300000 {
			return ErrWalletMonthCaptureIntegrity
		}
	}
	windowLimit, combined := 31, 0
	if recovered {
		windowLimit = 31 * 16
	}
	for i, token := range s.Tokens {
		combined += len(token.Usage)
		if token.UserID != s.Scope.Funding.UserIDs[i] || len(token.Usage) == 0 || len(token.Usage) > windowLimit || len(token.Parents) != 2*len(token.Usage) || partitioned == nil && recovered && combined > 4096 {
			return ErrTokenMonthCaptureIntegrity
		}
		for _, id := range token.Parents {
			if !validDigest(id) {
				return ErrTokenMonthCaptureIntegrity
			}
		}
		r := s.Scope.Funding
		r.UserIDs = []int64{token.UserID}
		if recovered {
			if err := verifyRecoveredWindowShape(ctx, db, r, token.Usage); err != nil {
				return err
			}
		}
		var account UsageMonthAccountReport
		var from, to int64
		if partitioned == nil {
			usage, err := ReviewUsageMonth(ctx, db, r, token.Usage)
			if err != nil {
				return err
			}
			report := usage.Report()
			if len(report.Accounts) != 1 {
				return ErrTokenMonthCaptureIntegrity
			}
			account, from, to = report.Accounts[0], report.From, report.To
		} else {
			if len(partitioned.Accounts) != len(s.Tokens) {
				return ErrTokenMonthCaptureIntegrity
			}
			account, from, to = partitioned.Accounts[i], partitioned.From, partitioned.To
		}
		if account.UserID != token.UserID || account.Totals == nil || len(account.MissingWindows) != 0 || account.MatchingSeconds != to-from {
			return ErrTokenMonthCaptureIntegrity
		}
	}
	return ctx.Err()
}
