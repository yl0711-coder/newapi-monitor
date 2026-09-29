//go:build unix

package monitor

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// Shared progress for finite authorized handoff. Execution entry points perform
// separate local/live validation; neither connects to an external data source.
type financeGiftAuthorizedProgress struct {
	TaskID            string
	Status            string
	AuthorizedTargets int
	MatchedTargets    int
	CommittedTargets  int
	RowsUpdated       int
}

func (m *Monitor) validateGiftLocalExecution() error {
	if !m.cfg.LocalSnapshotOnly || !m.cfg.FinanceGiftHandoffApprovalEnabled || m.cfg.ProdDSN != "" || m.cfg.NewAPIBaseURL != "" || m.prodDB != nil ||
		validateFinanceGiftPreviewSettings(m.cfg) != nil || m.storeDB == nil || m.usageFactsStore() == nil || m.storeDB == m.usageFactsStore() || sameStorePath(m.cfg.StorePath, m.cfg.UsageFactsStorePath) {
		return errors.New("authorized handoff execution requires isolated local snapshot stores")
	}
	return nil
}

func (m *Monitor) loadGiftExecutionAuthorization(ctx context.Context, id string) (financeGiftHandoffAuthorization, financeGiftAuthorizationPayload, error) {
	row, p, err := readFinanceGiftAuthorization(ctx, m.storeDB, id)
	if err != nil {
		return row, p, err
	}
	if row.RevokedAt != 0 || p.ApprovedAt > time.Now().Unix() || p.ExpiresAt <= time.Now().Unix() || p.SourcePlan != m.cfg.FinanceGiftHandoffPreviewSHA256 || p.ReceiverBinding != financeGiftReceiverBinding(m.cfg) {
		return row, p, errors.New("authorization revoked, expired or configuration changed")
	}
	return row, p, nil
}

func selectFinanceGiftAuthorizedEvidence(p financeGiftAuthorizationPayload, input financeGiftConfiguredEvidence) ([]financeGiftScopeTarget, [][]FinanceGiftBoundaryEvent, error) {
	if input.sourceHash != p.SourceSnapshot {
		return nil, nil, errors.New("authorized source snapshot changed")
	}
	byTarget := make(map[financeGiftScopeTarget]int, len(input.plan.Targets))
	for i, target := range input.plan.Targets {
		byTarget[target] = i
	}
	targets := make([]financeGiftScopeTarget, 0, len(p.Targets))
	verified := make([][]FinanceGiftBoundaryEvent, 0, len(p.Targets))
	for _, approved := range p.Targets {
		i, ok := byTarget[approved.financeGiftScopeTarget]
		if !ok || len(input.verified[i]) != approved.EvidenceRows {
			return nil, nil, errors.New("authorized target does not match source plan")
		}
		targets = append(targets, approved.financeGiftScopeTarget)
		verified = append(verified, input.verified[i])
	}
	return targets, verified, nil
}

func (m *Monitor) runFinanceGiftAuthorizedLocal(parent context.Context, id string, wait func(context.Context, time.Duration) error) (financeGiftAuthorizedProgress, error) {
	if err := m.validateGiftLocalExecution(); err != nil {
		return financeGiftAuthorizedProgress{TaskID: id, Status: "rejected"}, err
	}
	return m.runFinanceGiftAuthorized(parent, id, wait, m.cfg.FinanceGiftHandoffLocalExecutionEnabled)
}

// Only validated entry points may call this core. Provisioning is scoped to an
// explicit execution attempt, never a preview, status read or normal startup.
func (m *Monitor) runFinanceGiftAuthorized(parent context.Context, id string, wait func(context.Context, time.Duration) error, provisionLedger bool) (financeGiftAuthorizedProgress, error) {
	result := financeGiftAuthorizedProgress{TaskID: id, Status: "rejected"}
	if !m.financeGiftPreviewMu.TryLock() {
		return result, errors.New("handoff already in use")
	}
	defer m.financeGiftPreviewMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, financeGiftHandoffRunTimeout)
	defer cancel()
	row, p, err := m.loadGiftExecutionAuthorization(ctx, id)
	if err != nil {
		return result, err
	}
	lockCtx, lockCancel := context.WithTimeout(ctx, financeGiftHandoffWriteBudget)
	lock, err := lockFinanceGiftLocalReceiver(lockCtx, m.usageFactsStore(), m.cfg.UsageFactsStorePath)
	lockCancel()
	if err != nil {
		return result, err
	}
	defer lock.Close()
	input, err := loadFinanceGiftConfiguredEvidence(ctx, m.cfg)
	if err != nil {
		return result, err
	}
	targets, verified, err := selectFinanceGiftAuthorizedEvidence(p, input)
	if err != nil {
		return result, err
	}
	// Never provision before the fixed authorization and evidence are verified.
	// Existing offline callers retain their missing-ledger rejection.
	if provisionLedger {
		setupCtx, setupCancel := context.WithTimeout(ctx, financeGiftHandoffWriteBudget)
		err := ensureFinanceGiftHandoffLedger(setupCtx, m.usageFactsStore())
		setupCancel()
		if err != nil {
			return result, err
		}
	}
	// Check all authorized targets and previous commit proofs before writing any.
	result, err = inspectFinanceGiftAuthorizedProgress(ctx, m.usageFactsStore(), id, p, targets, verified)
	if err != nil || result.Status == "complete" {
		return result, err
	}
	if err := wait(ctx, financeGiftScopeBatchInterval); err != nil {
		return result, err
	}
	guard := func(ctx context.Context, index int) (int, error) {
		current, _, err := m.loadGiftExecutionAuthorization(ctx, id)
		if err == nil && current.PayloadHash != row.PayloadHash {
			return 0, errors.New("authorization changed during execution")
		}
		return p.Targets[index].RowsToUpdate, err
	}
	_, err = executeFinanceGiftHandoffGuarded(ctx, m.usageFactsStore(), id, targets, verified, p.MaxHours, wait, func(financeGiftScopeBatchEntry) error { return nil }, guard)
	// Progress is derived from durable facts/commits, never an optimistic main
	// DB flag. A canceled caller can inspect it afresh on the next invocation.
	progress, inspectErr := inspectFinanceGiftAuthorizedProgress(ctx, m.usageFactsStore(), id, p, targets, verified)
	if inspectErr != nil {
		return financeGiftAuthorizedProgress{TaskID: id, Status: "progress_unavailable", AuthorizedTargets: len(targets)}, errors.Join(err, inspectErr)
	}
	return progress, err
}

func inspectFinanceGiftAuthorizedProgress(parent context.Context, db *gorm.DB, id string, p financeGiftAuthorizationPayload, targets []financeGiftScopeTarget, verified [][]FinanceGiftBoundaryEvent) (financeGiftAuthorizedProgress, error) {
	result := financeGiftAuthorizedProgress{TaskID: id, Status: "pending", AuthorizedTargets: len(targets)}
	ctx, cancel := context.WithTimeout(parent, financeGiftHandoffWriteBudget)
	defer cancel()
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var commits []financeGiftHandoffCommit
		if err := tx.Where("job=?", id).Order("\"index\"").Limit(len(targets) + 1).Find(&commits).Error; err != nil {
			return err
		}
		if len(commits) > len(targets) {
			return errors.New("commit count exceeds authorization")
		}
		byIndex := make(map[int]financeGiftScopeBatchEntry, len(commits))
		for _, commit := range commits {
			entry, err := decodeFinanceGiftAuthorizedCommit(commit, p)
			if err != nil {
				return err
			}
			byIndex[commit.Index] = entry
		}
		for i, target := range targets {
			candidate, err := inspectFinanceGiftScopeHandoff(ctx, tx, target, verified[i])
			if err != nil {
				return err
			}
			if candidate.updated > p.Targets[i].RowsToUpdate {
				return errors.New("receiver scope grew after approval")
			}
			if candidate.updated == 0 {
				result.MatchedTargets++
			}
			if entry, ok := byIndex[i]; ok {
				if candidate.updated != 0 || candidate.current.State.ContentHash != entry.ContentHash {
					return errors.New("committed facts changed")
				}
				result.CommittedTargets++
				result.RowsUpdated += entry.RowsUpdated
			}
		}
		return nil
	})
	if err != nil {
		return financeGiftAuthorizedProgress{TaskID: id, Status: "unverifiable"}, err
	}
	if result.MatchedTargets == len(targets) {
		result.Status = "complete"
	} else if result.MatchedTargets > 0 {
		result.Status = "partial"
	}
	return result, nil
}
