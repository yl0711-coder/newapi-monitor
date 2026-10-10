package monitor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CloudWatchNginxEvidenceCheckpoint lives in the evidence database, not the
// main database. Event counts cannot prove coverage: a fully queried window may
// legitimately contain no request evidence. The random store identity also
// prevents a restored main database from trusting another evidence volume.
type CloudWatchNginxEvidenceCheckpoint struct {
	ID               uint   `gorm:"primaryKey;autoIncrement:false"`
	StoreID          string `gorm:"size:64"`
	CoverageFromTs   int64
	ThroughTs        int64
	PendingFromTs    int64
	PendingToTs      int64
	RestoreThroughTs int64
}

func loadCloudWatchNginxEvidenceCheckpoint(db *gorm.DB, from int64) (CloudWatchNginxEvidenceCheckpoint, error) {
	var proof CloudWatchNginxEvidenceCheckpoint
	err := db.First(&proof, "id = ?", cloudWatchNginxCursorID).Error
	if err == nil || !errors.Is(err, gorm.ErrRecordNotFound) {
		return proof, err
	}
	var identity [32]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return proof, err
	}
	proof = CloudWatchNginxEvidenceCheckpoint{ID: cloudWatchNginxCursorID,
		StoreID: hex.EncodeToString(identity[:]), CoverageFromTs: from, ThroughTs: from}
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&proof).Error; err != nil {
		return proof, err
	}
	err = db.First(&proof, "id = ?", cloudWatchNginxCursorID).Error
	return proof, err
}

func cloudWatchNginxEvidenceProofMatches(state CloudWatchNginxCursor, proof CloudWatchNginxEvidenceCheckpoint, retentionFrom int64) bool {
	return state.EvidenceStoreID != "" && proof.StoreID == state.EvidenceStoreID &&
		proof.PendingToTs == 0 && proof.CoverageFromTs <= max(state.EvidenceCoverageFromTs, retentionFrom) &&
		proof.ThroughTs >= max(state.EvidenceThroughTs, retentionFrom) && state.EvidenceNextTs == state.EvidenceThroughTs
}

func (m *Monitor) reconcileCloudWatchNginxEvidence(state *CloudWatchNginxCursor, from, target int64, now time.Time) error {
	if nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" || m.nginxEvidenceDB == nil {
		state.EvidenceStatus = "disabled"
		return nil
	}
	proof, err := loadCloudWatchNginxEvidenceCheckpoint(m.nginxEvidenceDB, from)
	if err != nil {
		return err
	}
	if !cloudWatchNginxEvidenceProofMatches(*state, proof, from) || state.EvidenceThroughTs > target {
		// Old versions have no in-store commit proof. Replay them once rather
		// than inferring continuity from a nonempty evidence table.
		if state.EvidenceThroughTs > from || state.EvidenceNextTs > from {
			state.EvidenceGapFromTs = from
			state.EvidenceGapToTs = target
			state.EvidenceGapDetectedAt = now.Unix()
			state.EvidenceGapReason = "evidence_coverage_unverified"
			if state.EvidenceStoreID != "" && state.EvidenceStoreID != proof.StoreID {
				state.EvidenceGapReason = "evidence_store_replaced"
			}
		}
		proof.CoverageFromTs, proof.ThroughTs = from, from
		proof.PendingFromTs, proof.PendingToTs, proof.RestoreThroughTs = 0, 0, 0
		if err := m.nginxEvidenceDB.Save(&proof).Error; err != nil {
			return err
		}
		state.EvidenceCoverageFromTs, state.EvidenceNextTs, state.EvidenceThroughTs = from, from, from
		state.EvidenceLastSuccessAt = 0
	}
	state.EvidenceStoreID = proof.StoreID
	state.EvidenceCoverageFromTs = max(state.EvidenceCoverageFromTs, from)
	state.EvidenceNextTs = max(state.EvidenceNextTs, from)
	state.EvidenceThroughTs = max(state.EvidenceThroughTs, from)
	state.EvidenceStatus = "running"
	if state.EvidenceNextTs >= target {
		state.EvidenceStatus = "caught_up"
	}
	if state.EvidenceGapToTs > state.EvidenceThroughTs {
		state.EvidenceStatus = "recovering"
	}
	return nil
}

func (m *Monitor) refreshCloudWatchNginxEvidenceCursor(ctx context.Context, state *CloudWatchNginxCursor, now time.Time) error {
	next := *state
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	if err := m.reconcileCloudWatchNginxEvidence(&next, from, target, now); err != nil {
		return err
	}
	if next != *state {
		next.UpdatedAt = now.Unix()
		if err := m.storeDB.WithContext(ctx).Save(&next).Error; err != nil {
			return err
		}
		*state = next
	}
	m.cloudWatchNginxEvidenceFrom.Store(state.EvidenceCoverageFromTs)
	m.cloudWatchNginxEvidenceThrough.Store(state.EvidenceThroughTs)
	m.cloudWatchNginxEvidenceLastSuccess.Store(state.EvidenceLastSuccessAt)
	m.cloudWatchNginxEvidenceLastFailure.Store(state.EvidenceLastFailureAt)
	m.refreshCloudWatchNginxEvidenceRecovery(now)
	return nil
}

// Invalidate proof in the same transaction as the first destructive replace.
// Inserts use bounded transactions; a crash in the middle must not leave the
// old complete watermark looking valid on restart or in /ready.
func beginCloudWatchNginxEvidenceReplace(tx *gorm.DB, from, to int64) error {
	proof, err := loadCloudWatchNginxEvidenceCheckpoint(tx, from)
	if err != nil {
		return err
	}
	if proof.PendingToTs != 0 && (proof.PendingFromTs != from || proof.PendingToTs != to) {
		// An interrupted different window has not been repaired. Discard its
		// restore frontier; later windows cannot heal that missing interval.
		proof.RestoreThroughTs = 0
	} else if proof.PendingToTs == 0 {
		proof.RestoreThroughTs = proof.ThroughTs
	}
	proof.PendingFromTs, proof.PendingToTs = from, to
	if from < proof.ThroughTs && to > proof.CoverageFromTs {
		proof.ThroughTs = max(from, proof.CoverageFromTs)
	}
	return tx.Save(&proof).Error
}

func completeCloudWatchNginxEvidenceReplace(tx *gorm.DB, from, to int64) error {
	var proof CloudWatchNginxEvidenceCheckpoint
	if err := tx.First(&proof, "id = ?", cloudWatchNginxCursorID).Error; err != nil {
		return err
	}
	if proof.PendingFromTs != from || proof.PendingToTs != to {
		return fmt.Errorf("cloudwatch nginx evidence replacement proof mismatch")
	}
	if from <= proof.ThroughTs && to >= proof.CoverageFromTs {
		proof.ThroughTs = max(proof.ThroughTs, to, proof.RestoreThroughTs)
	}
	proof.PendingFromTs, proof.PendingToTs, proof.RestoreThroughTs = 0, 0, 0
	return tx.Save(&proof).Error
}

type cloudWatchNginxEvidenceRecoveryStatus struct {
	Verified   bool   `json:"-"`
	ThroughTs  int64  `json:"-"`
	Incomplete bool   `json:"incomplete"`
	Reason     string `json:"reason,omitempty"`
	FromTs     int64  `json:"from_ts"`
	ToTs       int64  `json:"to_ts"`
	DetectedAt int64  `json:"detected_at"`
}

func (m *Monitor) cloudWatchNginxEvidenceRecovery(now time.Time) cloudWatchNginxEvidenceRecoveryStatus {
	if !m.cfg.CloudWatchNginxEnabled || nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" {
		return cloudWatchNginxEvidenceRecoveryStatus{}
	}
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	if cached := m.cloudWatchNginxEvidenceRecoveryCache.Load(); cached != nil {
		result := *cached
		if result.Verified && result.ThroughTs < from {
			// The retained whole-minute boundary can advance before the worker
			// publishes its next proof. Expire that proof, not the already
			// detected recovery history: callers still need the original gap,
			// reason and detection time to explain why evidence is missing.
			result.Verified, result.ThroughTs, result.Incomplete = false, 0, true
			if result.DetectedAt == 0 {
				result.Reason, result.FromTs, result.ToTs = "evidence_coverage_unverified", from, target
			}
			return result
		}
		if result.Verified && result.ToTs <= from {
			result.Incomplete = false
		}
		return result
	}
	return cloudWatchNginxEvidenceRecoveryStatus{Incomplete: true, Reason: "evidence_coverage_unverified", FromTs: from, ToTs: target}
}

// Called only from evidence-store lifecycle / collection work. HTTP readiness
// and troubleshooting read the immutable atomic snapshot and cannot queue on a
// busy SQLite writer. Until the first successful check they remain fail-closed.
func (m *Monitor) refreshCloudWatchNginxEvidenceRecovery(now time.Time) {
	result := m.probeCloudWatchNginxEvidenceRecovery(now)
	m.cloudWatchNginxEvidenceRecoveryCache.Store(&result)
}

func (m *Monitor) probeCloudWatchNginxEvidenceRecovery(now time.Time) cloudWatchNginxEvidenceRecoveryStatus {
	var result cloudWatchNginxEvidenceRecoveryStatus
	if !m.cfg.CloudWatchNginxEnabled || nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" {
		return result
	}
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	result = cloudWatchNginxEvidenceRecoveryStatus{Incomplete: true, Reason: "evidence_coverage_unverified", FromTs: from, ToTs: target}
	if m.storeDB == nil || m.nginxEvidenceDB == nil {
		return result
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var state CloudWatchNginxCursor
	var proof CloudWatchNginxEvidenceCheckpoint
	if m.storeDB.WithContext(ctx).First(&state, "id = ?", cloudWatchNginxCursorID).Error != nil ||
		m.nginxEvidenceDB.WithContext(ctx).First(&proof, "id = ?", cloudWatchNginxCursorID).Error != nil {
		return result
	}
	if !cloudWatchNginxEvidenceProofMatches(state, proof, from) {
		if state.EvidenceStoreID != "" && state.EvidenceStoreID != proof.StoreID {
			result.Reason = "evidence_store_replaced"
		}
		return result
	}
	result = cloudWatchNginxEvidenceRecoveryStatus{Verified: true, ThroughTs: state.EvidenceThroughTs,
		Reason: state.EvidenceGapReason, FromTs: state.EvidenceGapFromTs,
		ToTs: state.EvidenceGapToTs, DetectedAt: state.EvidenceGapDetectedAt}
	result.Incomplete = state.EvidenceGapToTs > max(state.EvidenceThroughTs, from)
	return result
}
