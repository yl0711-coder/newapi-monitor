package monitor

// Trusted background-service bridge only. None of these types is accepted by a
// customer HTTP endpoint. Original financial evidence must already be audited.
import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

type MonthlyDisplaySeriesScope = monthlyDisplaySeriesScope
type MonthlyDisplayPublicationSource = monthlyDisplayPublicationSource

// Opaque and owned: callers cannot substitute cent amounts or a different plan.
type PreparedMonthlyDisplayPublication struct {
	value *monthlyPreparedDisplayPublication
}

func PrepareMonthlyDisplayPublication(ctx context.Context, scope MonthlyDisplaySeriesScope, input []MonthlyDisplayPublicationSource, now time.Time) (*PreparedMonthlyDisplayPublication, error) {
	p, err := monthlyPrepareDisplayPublication(ctx, scope, input, now)
	if err != nil {
		return nil, err
	}
	return &PreparedMonthlyDisplayPublication{value: p}, nil
}

// Private recovery journal identity. Range extension does not change a selected
// month's identity, but a new origin, evidence, or cent wallet does. This is not
// a source attestation or a customer-facing publication/download identifier.
func (p *PreparedMonthlyDisplayPublication) BindingSHA256(month string) (string, error) {
	if p == nil || p.value == nil {
		return "", errMonthlyDisplayChainInvalid
	}
	if p.value.cent != nil {
		for _, link := range p.value.cent.Months {
			if link.Month == month {
				return monthlyPublicationBindingHash(monthlyCentBillFormat, p.value.scope.SourceIdentity, monthlyCentSeriesAnchor(*p.value.cent), p.value.evidence[month], link.Digest), nil
			}
		}
		return "", errMonthlyDisplaySeriesIncomplete
	}
	for _, link := range p.value.plan.Months {
		if link.Month != month {
			continue
		}
		return monthlyDisplayBindingHash(p.value.scope.SourceIdentity, p.value.plan.AnchorSHA256, p.value.evidence[month], link.Digest), nil
	}
	return "", errMonthlyDisplaySeriesIncomplete
}

func monthlyDisplayBindingHash(origin, anchor, evidence, link string) string {
	return monthlyPublicationBindingHash(monthlyDisplaySeriesPolicy, origin, anchor, evidence, link)
}

func monthlyPublicationBindingHash(policy, origin, anchor, evidence, link string) string {
	raw, _ := json.Marshal(struct{ Policy, Origin, Anchor, Evidence, Link string }{policy, origin, anchor, evidence, link})
	return monthlyDigest(raw)
}

// MonthlyBillSavedGenerationBinding re-derives the existing recovery journal
// identity from ONE saved fixed-origin publication, without preparing a new
// series, writing a cache or changing money. Legacy publications are not
// silently treated as fixed-origin records by the new maintenance operation.
func MonthlyBillSavedGenerationBinding(record PortalMonthlyBill, origin string, now time.Time) (string, error) {
	if err := ValidateMonthlyBillPublicationRecord(record, now); err != nil {
		return "", err
	}
	if strings.HasPrefix(record.PublicationID, monthlyCentBillPublicationPrefix) {
		parts := strings.Split(strings.TrimPrefix(record.PublicationID, monthlyCentBillPublicationPrefix), ":")
		if len(parts) != 4 || parts[1] != origin || !validMonthlyPublication(origin) || !validMonthlyPublication(parts[2]) || parts[3] == "" {
			return "", errMonthlyDisplayChainInvalid
		}
		from, ok := monthlyDisplayChainMonth(parts[0])
		month, mok := monthlyDisplayChainMonth(record.Month)
		if !ok || !mok || from.After(month) {
			return "", errMonthlyDisplayChainInvalid
		}
		retained, err := monthlyReadCentBill(record)
		if err != nil {
			return "", err
		}
		display := monthlyPublicationBindingHash(monthlyCentBillFormat, origin, parts[2], record.EvidenceHash, retained.Plan.Digest)
		raw, _ := json.Marshal(struct{ Mode, EvidenceSHA256, DisplaySHA256 string }{"fixed-origin-publication-v1", record.EvidenceHash, display})
		return monthlyDigest(raw), nil
	}
	if !strings.HasPrefix(record.PublicationID, monthlySeriesPublicationPrefix) {
		return "", errMonthlyDisplayChainInvalid
	}
	parts := strings.Split(strings.TrimPrefix(record.PublicationID, monthlySeriesPublicationPrefix), ":")
	if len(parts) != 4 || parts[1] != origin || !validMonthlyPublication(origin) || !validMonthlyPublication(parts[2]) || parts[3] == "" {
		return "", errMonthlyDisplayChainInvalid
	}
	from, ok := monthlyDisplayChainMonth(parts[0])
	month, mok := monthlyDisplayChainMonth(record.Month)
	if !ok || !mok || from.After(month) {
		return "", errMonthlyDisplayChainInvalid
	}
	p, err := readPublishedMonthlyBill(record)
	if err != nil {
		return "", err
	}
	link, err := monthlyDisplayLinkFromBill(record.GroupID, p)
	if err != nil {
		return "", err
	}
	display := monthlyDisplayBindingHash(origin, parts[2], record.EvidenceHash, link.Digest)
	raw, _ := json.Marshal(struct{ Mode, EvidenceSHA256, DisplaySHA256 string }{"fixed-origin-publication-v1", record.EvidenceHash, display})
	return monthlyDigest(raw), nil
}

func ImportMonthlyBillWithDisplaySeries(ctx context.Context, db *gorm.DB, raw []byte, expected MonthlyBillExpectedAuthorization, prepared *PreparedMonthlyDisplayPublication, now time.Time) (string, error) {
	if prepared == nil || prepared.value == nil || expected.GroupID <= 0 || expected.AuthVersion < 0 || expected.MemberManifest == "" || expected.MemberFingerprint == "" {
		return "", errors.New("自动账单缺少明确的授权或连续月份准备资料")
	}
	return importMonthlyBillPrepared(ctx, db, raw, true, now, &expected, prepared.value)
}

func (m *Monitor) PublishMonthlyBillWithDisplaySeries(ctx context.Context, raw []byte, expected MonthlyBillExpectedAuthorization, prepared *PreparedMonthlyDisplayPublication) (string, error) {
	if ctx == nil || m == nil || m.storeDB == nil || !m.cfg.PortalMonthlyStatementEnabled {
		return "", errors.New("月度账单保存未启用或资源不可用")
	}
	return ImportMonthlyBillWithDisplaySeries(ctx, m.storeDB, raw, expected, prepared, time.Now())
}
