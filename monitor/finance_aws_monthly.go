package monitor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

// One bounded, self-contained file; never resolve paths from evidence or make
// an AWS call. Monthly CE never becomes CUR day buckets. Monthly management
// accounting requires the explicit, user-confirmed accounting_basis below.
type financeAWSEvidenceBundle struct {
	AccountingBasis string                 `json:"accounting_basis,omitempty"`
	SchemaVersion   int                    `json:"schema_version"`
	Kind            string                 `json:"kind"`
	CUR             financecur.SourceSet   `json:"cur"`
	HistoricalCE    financeCEEvidenceInput `json:"historical_ce"`
}
type financeCEEvidenceInput struct {
	FromMonth        string          `json:"from_month"`
	ToMonthExclusive string          `json:"to_month_exclusive"`
	Services         json.RawMessage `json:"services"`
	RecordTypes      json.RawMessage `json:"record_types"`
	Daily            json.RawMessage `json:"daily"`
}
type financeAWSMonthlyBillView struct {
	Period        string                     `json:"period"`
	FromUTC       string                     `json:"from_utc"`
	ToUTC         string                     `json:"to_utc_exclusive"`
	NetUSD        string                     `json:"net_usd"`
	Status        string                     `json:"status"`
	RangeRelation string                     `json:"range_relation"`
	VerifiedDays  int                        `json:"verified_days"`
	Services      []financecur.CEMonthAmount `json:"services"`
	RecordTypes   []financecur.CEMonthAmount `json:"record_types"`
}

func readFinanceAWSEvidenceBundle(data []byte) (*financeCURSource, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var bundle financeAWSEvidenceBundle
	if err := decoder.Decode(&bundle); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("AWS evidence has trailing data")
	}
	if bundle.Kind != "aws_cost_evidence_bundle" || bundle.SchemaVersion != 1 {
		return nil, fmt.Errorf("unsupported AWS evidence bundle")
	}
	if bundle.AccountingBasis != "" && bundle.AccountingBasis != financeAWSBillingMonthBasis {
		return nil, fmt.Errorf("unsupported AWS accounting basis")
	}
	review, err := financecur.ReviewSourceSet(bundle.CUR)
	if err != nil {
		return nil, err
	}
	if review.TimeZone != "Asia/Shanghai" {
		return nil, fmt.Errorf("AWS CUR 时区不是 Asia/Shanghai")
	}
	h := bundle.HistoricalCE
	monthly, err := financecur.ReconcileCEMonths(bytes.NewReader(h.Services), bytes.NewReader(h.RecordTypes), bytes.NewReader(h.Daily), h.FromMonth, h.ToMonthExclusive)
	if err != nil {
		return nil, fmt.Errorf("历史 AWS 月账核验失败: %w", err)
	}
	// No silent preference or sum of two sources for the same billing period.
	// A later CUR replacement requires explicitly removing that CE month.
	for _, month := range monthly.Months {
		from, _ := time.Parse("2006-01-02", month.FromUTC)
		to, _ := time.Parse("2006-01-02", month.ToUTC)
		for _, s := range review.Statements {
			if from.Unix() < s.BillingPeriodEnd && to.Unix() > s.BillingPeriodStart {
				return nil, fmt.Errorf("AWS CE 与 CUR 账期重叠: %s", month.Period)
			}
		}
	}
	p, err := financecur.NewSourceProjector(bundle.CUR)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	return &financeCURSource{projector: p, review: review, monthly: &monthly, billingMonth: bundle.AccountingBasis == financeAWSBillingMonthBasis, bundleVersion: "aws-bundle-v1:" + hex.EncodeToString(digest[:])}, nil
}

func (s *financeCURSource) monthlyBills(from, to int64) []financeAWSMonthlyBillView {
	if s == nil || s.monthly == nil || from >= to {
		return nil
	}
	var rows []financeAWSMonthlyBillView
	for _, m := range s.monthly.Months {
		start, _ := time.Parse("2006-01-02", m.FromUTC)
		end, _ := time.Parse("2006-01-02", m.ToUTC)
		if start.Unix() >= to || end.Unix() <= from {
			continue
		}
		relation := "partial_overlap"
		if from <= start.Unix() && to >= end.Unix() {
			relation = "whole_utc_month"
		}
		status := "reconciled"
		if m.Estimated {
			status = "estimated"
		}
		rows = append(rows, financeAWSMonthlyBillView{Period: m.Period, FromUTC: m.FromUTC, ToUTC: m.ToUTC, NetUSD: m.NetUSD, Status: status, RangeRelation: relation, VerifiedDays: m.VerifiedDays, Services: append([]financecur.CEMonthAmount(nil), m.Services...), RecordTypes: append([]financecur.CEMonthAmount(nil), m.RecordTypes...)})
	}
	return rows
}
