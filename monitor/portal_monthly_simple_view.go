package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

// loadSimpleBillExtras marks a bill issued by the simple generator and
// attaches its subscription part, verified against the SHA-256 recorded in
// the published evidence. Bills from other generators are left unchanged.
func (m *Monitor) loadSimpleBillExtras(ctx context.Context, record PortalMonthlyBill, bill *monthlyCustomerBill) error {
	if bill.retained != nil {
		return nil
	}
	var meta struct {
		Reviewer sql.NullString
		Ref      sql.NullString
	}
	err := m.storeDB.WithContext(ctx).Raw("SELECT json_extract(evidence, '$.reviewed_by') AS reviewer, json_extract(evidence, '$.source_reference') AS ref FROM portal_monthly_bills WHERE group_id = ? AND month = ? AND publication_id = ?",
		record.GroupID, record.Month, record.PublicationID).Scan(&meta).Error
	if err != nil {
		return err
	}
	if meta.Reviewer.String != simpleBillReviewer {
		return nil
	}
	bill.simple = true
	i := strings.Index(meta.Ref.String, simpleSubscriptionRefPrefix)
	if i < 0 {
		return nil
	}
	digest := meta.Ref.String[i+len(simpleSubscriptionRefPrefix):]
	if j := strings.IndexByte(digest, ';'); j >= 0 {
		digest = digest[:j]
	}
	var row PortalMonthlySimpleSubscription
	if err := m.storeDB.WithContext(ctx).First(&row, "group_id = ? AND month = ?", record.GroupID, record.Month).Error; err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(row.Data))
	if len(digest) != 64 || row.Digest != digest || hex.EncodeToString(sum[:]) != digest {
		return errors.New("subscription part does not match the published bill")
	}
	var sub simpleSubscriptionBill
	if err := json.Unmarshal([]byte(row.Data), &sub); err != nil || sub.Version != 1 {
		return errors.New("invalid subscription part")
	}
	bill.sub = &sub
	return nil
}
