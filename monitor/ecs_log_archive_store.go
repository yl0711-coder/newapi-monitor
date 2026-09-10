package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"gorm.io/gorm"
)

const ecsArchiveReceiptLimit = 100000

// Bookkeeping only. Original per-protocol transaction receipts remain the
// authority when a crash occurs between a fact commit and this projection.
type ECSLogArchiveReceipt struct {
	ObjectKey   string `gorm:"primaryKey;size:512" json:"-"`
	ETag        string `gorm:"size:128" json:"-"`
	Node        string `gorm:"index:idx_ecs_archive_dependency,priority:1;size:64" json:"node"`
	Lane        string `gorm:"index:idx_ecs_archive_dependency,priority:2;size:16" json:"lane"`
	BodyHash    string `gorm:"index:idx_ecs_archive_dependency,priority:3;size:64" json:"-"`
	ArchiveAt   int64  `gorm:"index" json:"archive_at"`
	Status      int    `gorm:"index:idx_ecs_archive_retry,priority:1" json:"status"`
	LastAttempt int64  `gorm:"index:idx_ecs_archive_retry,priority:2" json:"last_attempt"`
	// Only learned after signature, source and lease validation, never from a
	// client-supplied hash alone. Old rows remain eligible for revalidation.
	PreviousHash          string `gorm:"size:64" json:"-"`
	PreviousVerified      bool   `json:"-"`
	Kind                  string `gorm:"size:32;index" json:"kind,omitempty"`
	FinalBoundaryVerified bool   `json:"final_boundary_verified"`
}
type ECSLogArchiveScan struct {
	ID           int    `gorm:"primaryKey" json:"-"`
	Binding      string `json:"-"`
	NextToken    string `json:"-"`
	PageJSON     string `json:"-"`
	PageIndex    int    `json:"-"`
	LastProgress int64  `json:"last_progress"`
	LastSuccess  int64  `json:"last_success"`
	LastFailure  int64  `json:"last_failure"`
	LastError    string `json:"last_error,omitempty"`
}

func (r *ecsArchiveReplayer) record(ctx context.Context, o ecsarchive.Object, result ecsArchiveReplayResult) error {
	_, node, lane, hash, err := ecsarchive.ParseKey(r.prefix, o.Key)
	if err != nil {
		sum := sha256.Sum256([]byte(o.Key))
		o.Key = "invalid:" + hex.EncodeToString(sum[:])
		node, lane, hash = "", "", ""
	}
	return r.m.infraAssetWrite(ctx, 3*time.Second, func(tx *gorm.DB) error {
		var previous ECSLogArchiveReceipt
		err := tx.First(&previous, "object_key = ?", o.Key).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			var count int64
			if err := tx.Model(&ECSLogArchiveReceipt{}).Count(&count).Error; err != nil {
				return err
			}
			if count >= ecsArchiveReceiptLimit {
				return errors.New("archive recovery ledger capacity reached")
			}
		} else if err != nil {
			return err
		} else if previous.ETag != o.ETag || previous.ArchiveAt != o.Modified.Unix() {
			return errors.New("archive object immutability violated")
		}
		// A later conflict cannot erase an earlier committed delivery proof.
		if previous.Status == 200 {
			return nil
		}
		if result.PreviousHash != nil {
			previous.PreviousHash, previous.PreviousVerified = *result.PreviousHash, true
		}
		if result.Kind != "" {
			previous.Kind = result.Kind
		}
		return tx.Save(&ECSLogArchiveReceipt{ObjectKey: o.Key, ETag: o.ETag, Node: node, Lane: lane, BodyHash: hash, ArchiveAt: o.Modified.Unix(), Status: result.Status, LastAttempt: time.Now().Unix(), PreviousHash: previous.PreviousHash, PreviousVerified: previous.PreviousVerified, Kind: previous.Kind, FinalBoundaryVerified: result.FinalBoundaryVerified}).Error
	})
}

func (r *ecsArchiveReplayer) dependency(ctx context.Context, node, lane, hash string) int {
	if hash == "" {
		return 200
	}
	var receipt ECSLogArchiveReceipt
	err := r.m.storeDB.WithContext(ctx).Where("node = ? AND lane = ? AND body_hash = ? AND status = 200", node, lane, hash).First(&receipt).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 425
	}
	if err != nil {
		return 503
	}
	return 200
}
