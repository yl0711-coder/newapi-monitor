//go:build unix

package monitor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const financeGiftAuthorizationTTL = 30 * time.Minute
const financeGiftAuthorizationDBTimeout = 2 * time.Second

var errGiftAuthorizationConflict = errors.New("handoff authorization conflicts with existing request")

type financeGiftAuthorizationRequest struct {
	Confirmation string `json:"confirmation"`
	RequestID    string `json:"request_id"`
	MaxHours     int    `json:"max_hours"`
}

type financeGiftAuthorizationPayload struct {
	Version         int                              `json:"version"`
	Actor           string                           `json:"actor"`
	SourcePlan      string                           `json:"source_plan_sha256"`
	SourceSnapshot  string                           `json:"source_snapshot_sha256"`
	ReceiverBinding string                           `json:"receiver_binding"`
	SourceEpoch     string                           `json:"source_epoch"`
	MaxHours        int                              `json:"max_hours"`
	ApprovedAt      int64                            `json:"approved_at"`
	ExpiresAt       int64                            `json:"expires_at"`
	Targets         []FinanceGiftHandoffPreviewEntry `json:"targets"`
}

func financeGiftAuthorizationID(requestID string) (string, error) {
	b, err := hex.DecodeString(requestID)
	if err != nil || len(b) != 16 || strings.ToLower(requestID) != requestID {
		return "", errors.New("request_id must be 32 lowercase hexadecimal characters")
	}
	return giftLocalDigest([]byte("gift-handoff-authorization-v1:" + requestID)), nil
}

func financeGiftReceiverBinding(cfg Settings) string {
	// This is a configuration identity, NOT proof that current facts are
	// unchanged. Execution must revalidate evidence against a live transaction.
	b, _ := json.Marshal([]string{filepath.Clean(cfg.UsageFactsStorePath), cfg.UsageFactsHistorySourceEpoch})
	return giftLocalDigest(b)
}

func decodeFinanceGiftAuthorization(row financeGiftHandoffAuthorization) (financeGiftAuthorizationPayload, error) {
	var p financeGiftAuthorizationPayload
	if !giftSeriesDigestValid(row.ID) || row.PayloadHash != giftLocalDigest([]byte(row.Payload)) || giftLocalDecodeJSON([]byte(row.Payload), &p) != nil ||
		p.Version != 1 || p.Actor == "" || len(p.Actor) > 256 || !giftSeriesDigestValid(p.SourcePlan) || !giftSeriesDigestValid(p.SourceSnapshot) || !giftSeriesDigestValid(p.ReceiverBinding) ||
		p.SourceEpoch == "" || p.ApprovedAt <= 0 || p.ExpiresAt-p.ApprovedAt != int64(financeGiftAuthorizationTTL/time.Second) || validateFinanceGiftHandoffLimit(p.MaxHours) != nil ||
		len(p.Targets) == 0 || len(p.Targets) > p.MaxHours || row.RevokedAt < 0 || (row.RevokedAt == 0) != (row.RevokedBy == "") {
		return p, errors.New("invalid authorization record")
	}
	targets := make([]financeGiftScopeTarget, len(p.Targets))
	remainingRows := financeGiftLocalMaxRows
	for i, target := range p.Targets {
		if target.SourceEpoch != p.SourceEpoch || target.Status != "ready" || target.RowsToUpdate <= 0 || target.RowsToUpdate > target.EvidenceRows || target.EvidenceRows > remainingRows || target.Reason != "" {
			return p, errors.New("invalid authorization target")
		}
		remainingRows -= target.EvidenceRows
		targets[i] = target.financeGiftScopeTarget
	}
	if err := validateFinanceGiftScopeBatch(targets, p.ApprovedAt); err != nil {
		return p, err
	}
	return p, nil
}

func readFinanceGiftAuthorization(ctx context.Context, db *gorm.DB, id string) (financeGiftHandoffAuthorization, financeGiftAuthorizationPayload, error) {
	var row financeGiftHandoffAuthorization
	ctx, cancel := context.WithTimeout(ctx, financeGiftAuthorizationDBTimeout)
	defer cancel()
	err := db.WithContext(ctx).Where("id = ?", id).First(&row).Error
	if err != nil {
		return row, financeGiftAuthorizationPayload{}, err
	}
	p, err := decodeFinanceGiftAuthorization(row)
	return row, p, err
}

func saveFinanceGiftAuthorization(ctx context.Context, db *gorm.DB, id string, p financeGiftAuthorizationPayload) (financeGiftHandoffAuthorization, financeGiftAuthorizationPayload, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return financeGiftHandoffAuthorization{}, p, err
	}
	row := financeGiftHandoffAuthorization{ID: id, Payload: string(b), PayloadHash: giftLocalDigest(b)}
	if _, err := decodeFinanceGiftAuthorization(row); err != nil {
		return row, p, err
	}
	ctx, cancel := context.WithTimeout(ctx, financeGiftAuthorizationDBTimeout)
	defer cancel()
	// Immutable authorization: retries cannot overwrite scope, actor or expiry.
	if err := db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
		return row, p, err
	}
	saved, actual, err := readFinanceGiftAuthorization(ctx, db, id)
	if err == nil && saved.Payload != row.Payload {
		err = errGiftAuthorizationConflict
	}
	return saved, actual, err
}
