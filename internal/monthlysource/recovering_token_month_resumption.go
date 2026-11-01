package monthlysource

// Bounded, source-free composition of existing root resumptions. A caller must
// durably pin this plan BEFORE applying it, and keep the source owner stopped.
import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"
)

type RecoveringTokenMonthResumePlan struct {
	Version        int                       `json:"version"`
	JobKey         string                    `json:"job_key"`
	Scope          OrganizationMonthScope    `json:"scope"`
	SnapshotSHA256 string                    `json:"snapshot_sha256"`
	Roots          []AdaptiveTokenResumePlan `json:"roots"`
}

// Fingerprint original money/projections, bindings and splits, NOT the fault
// cursor that the pinned root plans intentionally advance. Never synthesize a
// missing daily observation from its children.
func recoveringResumeSnapshot(ctx context.Context, db adaptiveTokenReader, s recoveringTokenInventory) (string, error) {
	type root struct {
		Ordinal  int                        `json:"ordinal"`
		ID       string                     `json:"id"`
		Created  int64                      `json:"created"`
		Leaves   []UsageWindowRequest       `json:"leaves"`
		Evidence []adaptiveTokenEvidenceRef `json:"evidence"`
	}
	pins := struct {
		Plan  FundingMonthCapturePlan `json:"plan"`
		Old   map[int]string          `json:"old"`
		Roots []root                  `json:"roots"`
	}{Plan: s.plan, Old: map[int]string{}, Roots: []root{}}
	for i := range s.old {
		o, err := loadTokenCaptureSlot(ctx, db, s.plan, i)
		if err != nil {
			return "", err
		}
		raw, err := json.Marshal(o)
		if err != nil {
			return "", err
		}
		pins.Old[i] = digest(raw)
	}
	keys := make([]int, 0, len(s.days))
	for i := range s.days {
		keys = append(keys, i)
	}
	sort.Ints(keys)
	for _, i := range keys {
		d := s.days[i]
		if err := verifyAdaptiveResumptionOriginals(ctx, db, d.state); err != nil {
			return "", err
		}
		pins.Roots = append(pins.Roots, root{d.ordinal, d.state.id, d.state.created, d.state.leaves, adaptiveEvidenceRefs(d.state)})
	}
	raw, err := json.Marshal(pins)
	if err != nil {
		return "", err
	}
	return digest(raw), nil
}

func recoveringResumeInventory(ctx context.Context, db *sql.DB, key string, scope OrganizationMonthScope, now time.Time) (recoveringTokenInventory, string, error) {
	var empty recoveringTokenInventory
	if err := ValidateRecoveringTokenMonthStore(ctx, db); err != nil {
		return empty, "", err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return empty, "", err
	}
	defer func() { _ = tx.Rollback() }()
	plan, err := loadFundingMonthCapturePlan(ctx, tx, key, scope, now)
	if err != nil {
		return empty, "", err
	}
	s, err := loadRecoveringTokenInventoryMode(ctx, tx, plan, now, true)
	if err != nil {
		return empty, "", err
	}
	sha, err := recoveringResumeSnapshot(ctx, tx, s)
	if err != nil {
		return empty, "", err
	}
	if err := tx.Commit(); err != nil {
		return empty, "", err
	}
	return s, sha, nil
}

func PreviewRecoveringTokenMonthResumption(parent context.Context, db *sql.DB, key string, scope OrganizationMonthScope) (RecoveringTokenMonthResumePlan, error) {
	return previewRecoveringMonthResume(parent, db, key, scope, time.Now())
}

func previewRecoveringMonthResume(parent context.Context, db *sql.DB, key string, scope OrganizationMonthScope, now time.Time) (RecoveringTokenMonthResumePlan, error) {
	var empty RecoveringTokenMonthResumePlan
	if parent == nil || db == nil {
		return empty, ErrAdaptiveTokenIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	s, sha, err := recoveringResumeInventory(ctx, db, key, scope, now)
	if err != nil {
		return empty, err
	}
	if len(s.days) == 0 {
		return empty, ErrAdaptiveTokenResumeDenied
	}
	p := RecoveringTokenMonthResumePlan{1, key, scope, sha, []AdaptiveTokenResumePlan{}}
	keys := make([]int, 0, len(s.days))
	for i := range s.days {
		keys = append(keys, i)
	}
	sort.Ints(keys)
	for _, i := range keys {
		d := s.days[i]
		for _, f := range d.state.faults {
			if f.Terminal || (f.Code != "retryable_transport" && f.Code != "retryable_source") {
				return empty, ErrAdaptiveTokenResumeDenied
			}
		}
		if len(d.state.faults) == 3 {
			r, err := previewAdaptiveTokenResume(ctx, db, key, d.state.root.Request, now)
			if err != nil {
				return empty, err
			}
			p.Roots = append(p.Roots, r)
		} else if d.state.epoch > 0 && !d.progress.Complete {
			// A restored queue must not invent a new plan after losing the
			// intent that authorized a still-active root epoch.
			return empty, ErrAdaptiveTokenResumeDenied
		}
	}
	if len(p.Roots) == 0 {
		return empty, ErrAdaptiveTokenResumeDenied
	}
	return p, nil
}

// Every step is idempotent under the SAME pinned plan. Partial progress leaves
// the queue stopped; caller-owned intent enables restart without new budgets.
func ApplyRecoveringTokenMonthResumption(parent context.Context, db *sql.DB, p RecoveringTokenMonthResumePlan) error {
	return applyRecoveringMonthResume(parent, db, p, time.Now())
}

// Read-only validation also accepts a partly applied pinned plan. It never
// constructs a replacement plan from already advanced root epochs.
func ValidateRecoveringTokenMonthResumption(parent context.Context, db *sql.DB, p RecoveringTokenMonthResumePlan) error {
	if parent == nil || db == nil {
		return ErrAdaptiveTokenIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	return checkRecoveringMonthResume(ctx, db, p, time.Now())
}

func checkRecoveringMonthResume(ctx context.Context, db *sql.DB, p RecoveringTokenMonthResumePlan, now time.Time) error {
	if ctx == nil || db == nil || p.Version != 1 || !validDigest(p.JobKey) || !validDigest(p.SnapshotSHA256) || len(p.Roots) == 0 || len(p.Roots) > adaptiveTokenJobs {
		return ErrAdaptiveTokenIntegrity
	}
	s, sha, err := recoveringResumeInventory(ctx, db, p.JobKey, p.Scope, now)
	if err != nil {
		return err
	}
	if sha != p.SnapshotSHA256 {
		return ErrAdaptiveTokenResumeChanged
	}
	bound := map[string]recoveringTokenDay{}
	for _, d := range s.days {
		bound[d.state.id] = d
	}
	seen := map[string]bool{}
	for _, r := range p.Roots {
		d, ok := bound[r.RootID]
		if !ok || seen[r.RootID] || r.JobKey != p.JobKey || r.Request != d.state.root.Request {
			return ErrAdaptiveTokenIntegrity
		}
		seen[r.RootID] = true
		if r.Ordinal > d.state.epoch {
			expected, err := previewAdaptiveTokenResume(ctx, db, p.JobKey, r.Request, now)
			if err != nil {
				return err
			}
			if expected != r {
				return ErrAdaptiveTokenResumeChanged
			}
		} else {
			var raw []byte
			var receipt adaptiveTokenResumeReceipt
			if err := db.QueryRowContext(ctx, `SELECT payload FROM monthly_adaptive_token_resumptions WHERE root_id=? AND ordinal=?`, r.RootID, r.Ordinal).Scan(&raw); err != nil {
				return err
			}
			if json.Unmarshal(raw, &receipt) != nil || receipt.Plan != r {
				return ErrAdaptiveTokenResumeChanged
			}
			if len(d.state.faults) >= 3 {
				return ErrAdaptiveTokenStopped
			}
		}
	}
	for _, d := range s.days {
		if !seen[d.state.id] && len(d.state.faults) >= 3 {
			return ErrAdaptiveTokenStopped
		}
		for _, f := range d.state.faults {
			if f.Terminal || (f.Code != "retryable_transport" && f.Code != "retryable_source") {
				return ErrAdaptiveTokenResumeDenied
			}
		}
	}
	return ctx.Err()
}

func applyRecoveringMonthResume(parent context.Context, db *sql.DB, p RecoveringTokenMonthResumePlan, now time.Time) error {
	if parent == nil || db == nil || p.Version != 1 || !validDigest(p.JobKey) || !validDigest(p.SnapshotSHA256) || len(p.Roots) == 0 || len(p.Roots) > adaptiveTokenJobs {
		return ErrAdaptiveTokenIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	if err := checkRecoveringMonthResume(ctx, db, p, now); err != nil {
		return err
	}
	// Check ALL planned cooldowns before any append (not just the first root).
	for _, r := range p.Roots {
		if now.UnixMilli() < r.EarliestMS {
			return ErrAdaptiveTokenResumeCooldown
		}
	}
	for _, r := range p.Roots {
		if _, err := applyAdaptiveTokenResume(ctx, db, r, now); err != nil {
			return err
		}
	}
	return checkRecoveringMonthResume(ctx, db, p, now)
}
