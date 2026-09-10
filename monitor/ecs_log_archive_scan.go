package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
)

const (
	ecsArchivePhaseBudget     = 8 * time.Second
	ecsArchivePhaseEntries    = 32
	ecsArchiveDiscoveryPages  = 16
	ecsArchiveCheckpointBytes = 256 << 10
	ecsArchiveFailureBudget   = 2 * time.Second
	ecsArchiveStepReserve     = time.Second
)

// Discovery and ready retries have independent budgets: neither a long source
// chain nor a slow page can monopolize the poll. All writes are local Monitor
// bookkeeping; objects, source identities and fact idempotency stay unchanged.
func (r *ecsArchiveReplayer) scan(ctx context.Context, reader ecsarchive.Reader, binding string, now time.Time) error {
	sum := sha256.Sum256([]byte(binding))
	binding = hex.EncodeToString(sum[:])
	var state ECSLogArchiveScan
	err := r.m.storeDB.WithContext(ctx).First(&state, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		state = ECSLogArchiveScan{ID: 1, Binding: binding}
	} else if err != nil {
		return err
	}
	if state.Binding != binding {
		return errors.New("archive recovery destination changed; explicit migration required")
	}
	discovery, cancel := context.WithTimeout(ctx, ecsArchivePhaseBudget)
	scanErr := r.scanPages(discovery, reader, &state, now)
	cancel()
	retry, cancel := context.WithTimeout(ctx, ecsArchivePhaseBudget)
	retryErr := r.retryReady(retry, reader, now)
	cancel()
	if err := errors.Join(scanErr, retryErr); err != nil {
		code := "archive_recovery_unavailable"
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			code = "archive_scan_interrupted"
		}
		return errors.Join(err, r.scanFailure(ctx, state, code, now))
	}
	state.LastError = ""
	if err := r.saveScan(ctx, state); err != nil {
		return errors.Join(err, r.scanFailure(ctx, state, "archive_checkpoint_unavailable", now))
	}
	return nil
}

// Already verified receipts need no S3 GET and must not consume the download
// budget. Bound listing/SQLite work separately so a large historical prefix
// cannot starve new sources or turn a sweep into an unbounded background job.
type archiveDiscoveryReader struct {
	ecsarchive.Reader
	gets int
}

func (r *archiveDiscoveryReader) Get(ctx context.Context, key string) (ecsarchive.Object, error) {
	r.gets++
	return r.Reader.Get(ctx, key)
}

func (r *ecsArchiveReplayer) scanPages(ctx context.Context, reader ecsarchive.Reader, state *ECSLogArchiveScan, now time.Time) error {
	bounded := &archiveDiscoveryReader{Reader: reader}
	for page := 0; page < ecsArchiveDiscoveryPages; page++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if bounded.gets >= ecsArchivePhaseEntries || archivePhaseYield(ctx, page) {
			return nil
		}
		if err := r.scanPage(ctx, bounded, state, now); err != nil {
			return err
		}
		if state.PageJSON != "" || state.NextToken == "" {
			return nil // Partial saved page, or one complete sweep; never loop back.
		}
	}
	return nil
}

// Persist the exact observed page BEFORE reading objects. Saving an index into
// a freshly relisted, mutable page could skip entries inserted by active tasks.
func (r *ecsArchiveReplayer) scanPage(ctx context.Context, reader *archiveDiscoveryReader, state *ECSLogArchiveScan, now time.Time) error {
	var page ecsarchive.Page
	if state.PageJSON == "" {
		if state.PageIndex != 0 {
			return errors.New("archive checkpoint missing page")
		}
		var err error
		page, err = reader.List(ctx, state.NextToken)
		if err != nil {
			return err
		}
		if err := validArchivePage(page, state.NextToken); err != nil {
			return err
		}
		encoded, err := json.Marshal(page)
		if err != nil || len(encoded) > ecsArchiveCheckpointBytes {
			return errors.New("archive checkpoint exceeds budget")
		}
		state.PageJSON = string(encoded)
		if err := r.saveScan(ctx, *state); err != nil {
			return err
		}
	} else if len(state.PageJSON) > ecsArchiveCheckpointBytes || json.Unmarshal([]byte(state.PageJSON), &page) != nil {
		return errors.New("invalid archive checkpoint")
	}
	if err := validArchivePage(page, state.NextToken); err != nil {
		return err
	}
	if state.PageIndex < 0 || state.PageIndex > len(page.Entries) {
		return errors.New("invalid archive checkpoint index")
	}
	for n := 0; state.PageIndex < len(page.Entries) && reader.gets < ecsArchivePhaseEntries; n++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if archivePhaseYield(ctx, n) {
			break
		}
		if err := r.scanEntry(ctx, reader, page.Entries[state.PageIndex], now); err != nil {
			return err
		}
		// A crash before this checkpoint repeats the entry, not its fact count.
		state.PageIndex++
		state.LastProgress = now.Unix()
		if err := r.saveScan(ctx, *state); err != nil {
			return err
		}
	}
	if state.PageIndex == len(page.Entries) {
		state.NextToken, state.LastSuccess = page.Next, now.Unix()
		state.PageJSON, state.PageIndex = "", 0
		return r.saveScan(ctx, *state)
	}
	return nil
}

// Avoid starting another network read at the very end of a healthy phase.
// Always allow the first item so short caller deadlines can still progress.
func archivePhaseYield(ctx context.Context, processed int) bool {
	deadline, limited := ctx.Deadline()
	return processed > 0 && limited && time.Until(deadline) < ecsArchiveStepReserve
}

func validArchivePage(page ecsarchive.Page, token string) error {
	if len(page.Entries) > ecsarchive.PageSize || len(page.Next) > 4096 || page.Next != "" && page.Next == token {
		return errors.New("archive pagination invalid")
	}
	return nil
}

func (r *ecsArchiveReplayer) scanEntry(ctx context.Context, reader ecsarchive.Reader, entry ecsarchive.Entry, now time.Time) error {
	if _, _, _, _, err := ecsarchive.ParseKey(r.prefix, entry.Key); err != nil {
		return r.record(ctx, ecsarchive.Object{Key: entry.Key, ETag: entry.ETag, Modified: entry.Modified}, ecsArchiveReplayResult{Status: 422})
	}
	if entry.ETag == "" || entry.Modified.IsZero() || entry.Size < 1 || entry.Size > ecsarchive.MaxObject {
		return errors.New("invalid archive listing metadata")
	}
	var receipt ECSLogArchiveReceipt
	err := r.m.storeDB.WithContext(ctx).First(&receipt, "object_key = ?", entry.Key).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if err == nil && (receipt.ETag != entry.ETag || receipt.ArchiveAt != entry.Modified.Unix()) {
		return errors.New("archive immutability violated")
	}
	if err == nil && (receipt.Status == 200 || receipt.Status == 409 || receipt.Status == 410 || receipt.Status == 422) {
		return nil
	}
	if err == nil && receipt.Status == 425 && receipt.PreviousVerified {
		if status := r.dependency(ctx, receipt.Node, receipt.Lane, receipt.PreviousHash); status == 425 {
			return nil // No repeated S3 GET for a known, still-blocked chain.
		} else if status != 200 {
			return errors.New("archive dependency lookup unavailable")
		}
	}
	o, err := reader.Get(ctx, entry.Key)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return r.record(ctx, ecsarchive.Object{Key: entry.Key, ETag: entry.ETag, Modified: entry.Modified}, ecsArchiveReplayResult{Status: 503})
	}
	if o.Key != entry.Key || o.ETag != entry.ETag || !o.Modified.Equal(entry.Modified) || int64(len(o.Body)) != entry.Size {
		return errors.New("archive changed between list and read")
	}
	result := r.replay(ctx, o, now)
	return r.record(ctx, o, result)
}

func (r *ecsArchiveReplayer) saveScan(ctx context.Context, state ECSLogArchiveScan) error {
	return r.m.infraAssetWrite(ctx, 3*time.Second, func(tx *gorm.DB) error { return tx.Save(&state).Error })
}
func (r *ecsArchiveReplayer) scanFailure(ctx context.Context, state ECSLogArchiveScan, code string, now time.Time) error {
	state.LastFailure, state.LastError = now.Unix(), code
	// Cancellation must remain visible. Detach ONLY this small diagnostic write,
	// never AWS reads or ingestion, and retain a strict independent deadline.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), ecsArchiveFailureBudget)
	defer cancel()
	if err := r.saveScan(cleanup, state); err != nil {
		return err
	}
	return errors.New(code)
}
