//go:build unix

package monitor

import (
	"context"
	"errors"
	"time"
)

// Explicit superadmin tasks only, never startup or automatic scheduling.
// Testing live settings on copies must not require an external DSN or network.
func (m *Monitor) validateGiftLiveExecution() error {
	if !m.cfg.FinanceGiftHandoffLiveExecutionEnabled || m.shuttingDown.Load() ||
		m.cfg.LocalSnapshotOnly || m.cfg.FinanceGiftHandoffLocalExecutionEnabled || !m.cfg.FinanceGiftHandoffApprovalEnabled ||
		validateFinanceGiftPreviewSettings(m.cfg) != nil || m.storeDB == nil || m.usageFactsStore() == nil ||
		m.storeDB == m.usageFactsStore() || sameStorePath(m.cfg.StorePath, m.cfg.UsageFactsStorePath) {
		return errors.New("live handoff requires explicit approval and isolated Monitor stores")
	}
	if reader := m.financeFactsReadStore(); reader == nil || reader == m.usageFactsStore() {
		return errors.New("live handoff requires an independent facts reader")
	}
	return nil
}

func (m *Monitor) runFinanceGiftAuthorizedLive(parent context.Context, id string, wait func(context.Context, time.Duration) error) (financeGiftAuthorizedProgress, error) {
	if err := m.validateGiftLiveExecution(); err != nil {
		return financeGiftAuthorizedProgress{TaskID: id, Status: "rejected"}, err
	}
	return m.runFinanceGiftAuthorized(parent, id, wait, true)
}
