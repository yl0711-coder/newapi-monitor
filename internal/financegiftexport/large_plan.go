package financegiftexport

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// A separate opt-in budget for ONE oversized hour, not a new batch default.
const LargeHourMaxRows = 5000
const largeHourPageRows = 500

// LargeHourPlan enumerates the existing verified ledger, not arbitrary source
// history. Its digest binds every monetary identity and already known group.
// SourceEpoch remains provenance; the caller must verify source identity.
type LargeHourPlan struct {
	Version          int               `json:"version"`
	SourceEpoch      string            `json:"source_epoch"`
	UserID           int64             `json:"user_id"`
	HourTs           int64             `json:"hour_ts"`
	LocalContentHash string            `json:"local_content_hash"`
	Rows             []LargeHourRecord `json:"rows"`
}

type LargeHourRecord struct {
	ID        int64   `json:"id"`
	CreatedAt int64   `json:"created_at"`
	Type      int     `json:"type"`
	Quota     int64   `json:"quota"`
	Group     *string `json:"group"` // nil means unknown, not an empty group.
}

func LargeHourConfirmation(plan LargeHourPlan) (string, error) {
	hash, err := hex.DecodeString(plan.LocalContentHash)
	if plan.Version != 1 || plan.SourceEpoch == "" || strings.TrimSpace(plan.SourceEpoch) != plan.SourceEpoch || len(plan.SourceEpoch) > 64 ||
		plan.UserID <= 0 || plan.HourTs <= 0 || plan.HourTs%3600 != 0 || plan.HourTs >= time.Now().Unix()-3600 ||
		len(plan.Rows) <= giftScopeExplicitExportLimit || len(plan.Rows) > LargeHourMaxRows || err != nil || len(hash) != sha256.Size {
		return "", errors.New("large-hour plan requires one closed hour, an epoch/hash and 3001 to 5000 verified records")
	}
	var previous int64
	groupBytes := 0
	for _, row := range plan.Rows {
		if row.ID <= previous || row.CreatedAt < plan.HourTs || row.CreatedAt >= plan.HourTs+3600 || (row.Type != 2 && row.Type != 6) || row.Quota < 0 {
			return "", errors.New("invalid or unordered large-hour monetary identity")
		}
		previous = row.ID
		if row.Group != nil {
			groupBytes += len(*row.Group)
		}
		if groupBytes > giftScopeExportBytes {
			return "", errors.New("large-hour plan exceeds byte budget")
		}
	}
	data, err := json.Marshal(plan)
	if err != nil || len(data) > giftScopeExportBytes {
		return "", errors.New("large-hour plan exceeds encoded byte budget")
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func cloneLargeHourPlan(plan LargeHourPlan) LargeHourPlan {
	plan.Rows = append([]LargeHourRecord(nil), plan.Rows...)
	for i := range plan.Rows {
		if plan.Rows[i].Group != nil {
			group := *plan.Rows[i].Group
			plan.Rows[i].Group = &group
		}
	}
	return plan
}
