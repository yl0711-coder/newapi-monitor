package monitor

// A bounded operator-only recovery path. Withdrawal archives the full immutable
// publication before removing its customer-visible projection, in one transaction.
// Reissue still goes through the original strict evidence/member checks.
import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// PortalMonthlyBillWithdrawal is append-only audit evidence; no customer route
// exposes it, and the recovery tool never deletes or updates an archived record.
type PortalMonthlyBillWithdrawal struct {
	ID          int64  `gorm:"primaryKey"`
	GroupID     int64  `gorm:"index:idx_monthly_withdrawal_scope,priority:1"`
	Month       string `gorm:"index:idx_monthly_withdrawal_scope,priority:2"`
	Publication string `gorm:"uniqueIndex"`
	Record      string
	RecordHash  string
	Actor       string
	Reason      string
	WithdrawnAt int64
}

func (PortalMonthlyBillWithdrawal) BeforeUpdate(*gorm.DB) error {
	return errors.New("portal_monthly_bill_withdrawals is append-only")
}

func (PortalMonthlyBillWithdrawal) BeforeDelete(*gorm.DB) error {
	return errors.New("portal_monthly_bill_withdrawals is append-only")
}

var errMonthlyPublicationChanged = errors.New("账单版本已变化或已撤回，请重新核验")

func validMonthlyPublication(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// Only public metadata is needed to compare issuance versions; source evidence
// is not fetched on the final HTTP recheck or included in the version token.
func monthlyBillVersion(r PortalMonthlyBill) string {
	b, _ := json.Marshal(struct {
		GroupID                                              int64
		Month, MemberFingerprint, PayloadHash, PublicationID string
		PublishedAt                                          int64
		MemberManifest                                       string `json:",omitempty"`
	}{r.GroupID, r.Month, r.MemberFingerprint, r.PayloadHash, r.PublicationID, r.PublishedAt, r.MemberManifest})
	return monthlyDigest(b)
}

func monthlyBillTarget(gid int64, month string, now time.Time) error {
	if gid <= 0 || month == "" {
		return errors.New("必须明确指定客户组 ID 和月份")
	}
	_, _, err := monthlyWindow(month, now)
	return err
}

// MonthlyBillPublicationVersion inspects one local publication without changing
// it. It also works for an old publication whose member scope is no longer active.
func MonthlyBillPublicationVersion(ctx context.Context, db *gorm.DB, gid int64, month string, now time.Time) (string, error) {
	if err := monthlyBillTarget(gid, month, now); err != nil {
		return "", err
	}
	var r PortalMonthlyBill
	if err := db.WithContext(ctx).First(&r, "group_id = ? AND month = ?", gid, month).Error; err != nil {
		return "", err
	}
	return monthlyBillVersion(r), nil
}

// WithdrawMonthlyBill validates by default; apply must be explicitly requested.
// expected is the inspected publication version, not the source evidence hash.
// Replaying an old command can never withdraw a newly issued bill for that month.
func WithdrawMonthlyBill(ctx context.Context, db *gorm.DB, gid int64, month, expected, actor, reason string, apply bool, now time.Time) error {
	if err := monthlyBillTarget(gid, month, now); err != nil {
		return err
	}
	actor, reason = strings.TrimSpace(actor), strings.TrimSpace(reason)
	if !validMonthlyPublication(expected) || actor == "" || reason == "" || len(actor) > 128 || len(reason) > 500 {
		return errors.New("撤回必须提供准确的账单版本、操作人和原因")
	}
	operation := func(tx *gorm.DB) error {
		var old PortalMonthlyBill
		err := tx.First(&old, "group_id = ? AND month = ?", gid, month).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Idempotent only when this exact version was already archived and
			// no newer active bill exists. Do not treat an unknown month as success.
			var prior PortalMonthlyBillWithdrawal
			if err := tx.First(&prior, "group_id = ? AND month = ? AND publication = ?", gid, month, expected).Error; err != nil {
				return err
			}
			var record PortalMonthlyBill
			if monthlyDigest([]byte(prior.Record)) != prior.RecordHash || json.Unmarshal([]byte(prior.Record), &record) != nil || record.GroupID != gid || record.Month != month || monthlyBillVersion(record) != expected {
				return errors.New("撤回审计记录完整性校验失败")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if monthlyBillVersion(old) != expected {
			return errMonthlyPublicationChanged
		}
		if !apply {
			return nil
		}
		raw, err := json.Marshal(old)
		if err != nil {
			return err
		}
		audit := PortalMonthlyBillWithdrawal{GroupID: gid, Month: month, Publication: expected, Record: string(raw), RecordHash: monthlyDigest(raw), Actor: actor, Reason: reason, WithdrawnAt: now.Unix()}
		if err := tx.Create(&audit).Error; err != nil {
			return err
		}
		result := tx.Where("group_id = ? AND month = ? AND payload_hash = ? AND COALESCE(publication_id, '') = ?", gid, month, old.PayloadHash, old.PublicationID).Delete(&PortalMonthlyBill{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errMonthlyPublicationChanged
		}
		return nil
	}
	if apply {
		return db.WithContext(ctx).Transaction(operation)
	}
	return operation(db.WithContext(ctx))
}

func checkMonthlyPublicationCurrent(ctx context.Context, db *gorm.DB, original PortalMonthlyBill) error {
	var current PortalMonthlyBill
	err := db.WithContext(ctx).Select("group_id, month, payload_hash, member_fingerprint, member_manifest, published_at, publication_id").First(&current, "group_id = ? AND month = ?", original.GroupID, original.Month).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errMonthlyPublicationChanged
	}
	if err != nil {
		return err
	}
	if monthlyBillVersion(current) != monthlyBillVersion(original) {
		return errMonthlyPublicationChanged
	}
	return nil
}

func (m *Monitor) portalMonthlyPublicationStillCurrent(ctx context.Context, c *gin.Context, original PortalMonthlyBill) bool {
	err := checkMonthlyPublicationCurrent(ctx, m.storeDB, original)
	if errors.Is(err, errMonthlyPublicationChanged) {
		c.JSON(409, gin.H{"error": "账单已变更，请刷新后重试"})
		return false
	}
	if err != nil {
		c.JSON(503, gin.H{"error": "账单版本暂时无法核验，请稍后重试"})
		return false
	}
	return true
}
