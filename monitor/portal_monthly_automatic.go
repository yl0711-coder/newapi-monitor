package monitor

// Automatic issuance is a trusted background-service boundary, NOT a customer
// evidence endpoint. Callers must retain and audit the source captures before
// constructing a candidate. Hashes bind that audit to bytes; they do not turn
// arbitrary JSON, a successful SQL query or a customer assertion into proof.
import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"time"

	"gorm.io/gorm"
)

const monthlyAutomaticPolicy = "automatic-wallet-v1"

// Conservative, per-account AND per-month limits, both must pass: at most ¥5
// and 0.01% of logged net consumption. No negative correction is automated.
const monthlyCorrectionQuotaLimit int64 = 5 * 500000

var ErrMonthlyObservationUnstable = errors.New("monthly wallet observation not yet stable")

// A closed month's closing and used-counter projections, independently rebuilt
// at two different capture boundaries. Live balance itself need NOT stop moving.
// ScopeSHA256 binds this exact account/month/opening/events/consumption/refund;
// it must be recomputed if an event, opening or log total changes.
type MonthlyBillCorrectionObservation struct {
	UserID       int64  `json:"user_id"`
	Month        string `json:"month"`
	CapturedAt   int64  `json:"captured_at"`
	SourceSHA256 string `json:"source_sha256"`
	ScopeSHA256  string `json:"scope_sha256"`
	ClosingQuota int64  `json:"closing_quota"`
	NetUsedQuota int64  `json:"net_used_quota"`
}

type MonthlyBillBalanceCorrection struct {
	Version      int                                `json:"version"`
	Quota        int64                              `json:"quota"`
	Observations []MonthlyBillCorrectionObservation `json:"observations"`
}

func monthlyCorrectionJSONShape(raw []byte, required func(map[string]json.RawMessage, ...string) error) error {
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shape); err != nil {
		return err
	}
	if err := required(shape, "version", "quota", "observations"); err != nil {
		return err
	}
	var observations []map[string]json.RawMessage
	if err := json.Unmarshal(shape["observations"], &observations); err != nil {
		return err
	}
	for _, observation := range observations {
		if err := required(observation, "user_id", "month", "captured_at", "source_sha256", "scope_sha256", "closing_quota", "net_used_quota"); err != nil {
			return err
		}
	}
	return nil
}

func monthlyCorrectionMoney(s string) (int64, bool) {
	if s == "" {
		return 0, true // only the additive, absent legacy field means no correction
	}
	if len(s) > 30 {
		return 0, false
	}
	q, ok := newMonthlyMoney(s)
	return q, ok && q >= 0
}

// MonthlyCorrectionScopeSHA256 contains no editable reviewer or correction.
// Carrying last month's published closing as this month's opening makes the
// recognized correction part of the opening, NOT a new correction each month.
func MonthlyCorrectionScopeSHA256(u MonthlyBillUserEvidence, month string) string {
	raw, _ := json.Marshal(struct {
		UserID  int64                       `json:"user_id"`
		Month   string                      `json:"month"`
		Opening int64                       `json:"opening"`
		Consume int64                       `json:"consume"`
		Refund  int64                       `json:"refund"`
		Events  []MonthlyBillCreditEvidence `json:"events"`
	}{u.UserID, month, u.OpeningQuota, u.ConsumeQuota, u.RefundQuota, u.Events})
	return monthlyDigest(raw)
}

func validateMonthlyBalanceCorrection(u MonthlyBillUserEvidence, month string, reviewedAt int64, now time.Time) (int64, error) {
	c := u.BalanceCorrection
	if c == nil {
		return 0, nil
	}
	// Historical recognition is a separate, explicit decision. It must not
	// manufacture the two month-specific observations required by wallet-v1.
	if u.HistoricalBalance != nil {
		if err := validateMonthlyHistoricalBalance(u, u.HistoricalBalance.Recognition.GroupID, month, reviewedAt, now); err != nil {
			return 0, err
		}
		return c.Quota, nil
	}
	if c.Version != 1 || c.Quota <= 0 || c.Quota > monthlyCorrectionQuotaLimit || len(c.Observations) < 2 || len(c.Observations) > 4 || u.Accounting == nil ||
		u.ConsumeQuota != u.Accounting.LogConsumeQuota || u.RefundQuota != u.Accounting.LogRefundQuota {
		return 0, errors.New("余额校正范围或日志依据无效；不能修改消费、退款或充值来平账")
	}
	logNet := new(big.Int).Sub(big.NewInt(u.ConsumeQuota), big.NewInt(u.RefundQuota))
	if new(big.Int).Mul(big.NewInt(c.Quota), big.NewInt(10000)).Cmp(logNet) > 0 {
		return 0, errors.New("余额差额超过自动校正比例，须内部处理")
	}
	expectedUsed := new(big.Int).Sub(logNet, big.NewInt(c.Quota))
	_, end, err := monthlyWindow(month, now)
	if err != nil || !expectedUsed.IsInt64() {
		return 0, errors.New("余额校正月份或用量无效")
	}
	scope := MonthlyCorrectionScopeSHA256(u, month)
	var previous int64
	sources := map[string]bool{}
	for _, observation := range c.Observations {
		if observation.UserID != u.UserID || observation.Month != month || observation.ScopeSHA256 != scope || !validMonthlyPublication(observation.SourceSHA256) ||
			sources[observation.SourceSHA256] || observation.CapturedAt < end.Unix() || observation.CapturedAt > reviewedAt || observation.CapturedAt > now.Unix() || (previous != 0 && observation.CapturedAt <= previous) {
			return 0, errors.New("余额校正采集范围、来源或时间无效")
		}
		if observation.ClosingQuota != u.ClosingQuota || observation.NetUsedQuota != expectedUsed.Int64() {
			return 0, ErrMonthlyObservationUnstable
		}
		previous = observation.CapturedAt
		sources[observation.SourceSHA256] = true
	}
	if c.Observations[len(c.Observations)-1].CapturedAt-c.Observations[0].CapturedAt < 300 {
		return 0, ErrMonthlyObservationUnstable
	}
	return c.Quota, nil
}

// PrepareAutomaticMonthlyBill produces a deterministic machine-policy record,
// never a fabricated human approval. It preserves every raw funding event and
// gross consumption/refund. Repeating the same audited candidate is idempotent.
// No database, source repair, network, publication or customer response here.
func PrepareAutomaticMonthlyBill(candidate MonthlyBillCandidate, verifiedAt int64, now time.Time) ([]byte, error) {
	if len(candidate.Users) == 0 || len(candidate.Users) > maxTrackedUsers {
		return nil, errors.New("自动账单成员范围无效")
	}
	for _, u := range candidate.Users {
		if u.Accounting == nil || u.ConsumeQuota != u.Accounting.LogConsumeQuota || u.RefundQuota != u.Accounting.LogRefundQuota || len(u.Reversals) != 0 {
			return nil, errors.New("自动账单必须保留原始消费、退款及资金变动；不猜测冲正")
		}
	}
	raw, err := json.Marshal(candidate)
	if err != nil || len(raw) > 2<<20 || len(candidate.SourceReference) > 800 || candidate.SourceReference == "" {
		return nil, errors.New("自动账单来源或大小无效")
	}
	policy := monthlyAutomaticPolicy
	for _, user := range candidate.Users {
		if user.HistoricalBalance != nil {
			policy = monthlyHistoricalBalancePolicy
		}
	}
	e := MonthlyBillEvidence{Version: monthlyEvidenceVersion(candidate.Users), GroupID: candidate.GroupID, Month: candidate.Month,
		ReviewedBy: "policy:" + policy, ReviewedAt: verifiedAt,
		SourceReference: "policy:" + policy + "/candidate:" + monthlyDigest(raw) + "/" + candidate.SourceReference, Users: candidate.Users}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		return nil, err
	}
	if !monthlyStatementAmountsComplete(p) {
		return nil, errors.New("自动账单逐账号金额校验失败")
	}
	return json.Marshal(e)
}

// IssueMonthlyBillAutomatically is called only by a trusted background service
// after source auditing, not by GET/read or with customer-provided evidence.
// The existing transaction rechecks live authorization, adjacent-month/member
// balances and immutability. It writes ONLY a local bill publication, never a
// NewAPI wallet, a recharge/consumption log, or an already-published bill.
func IssueMonthlyBillAutomatically(ctx context.Context, db *gorm.DB, candidate MonthlyBillCandidate, verifiedAt int64, now time.Time) (string, error) {
	raw, err := PrepareAutomaticMonthlyBill(candidate, verifiedAt, now)
	if err != nil {
		return "", err
	}
	return ImportMonthlyBill(ctx, db, raw, true, now)
}
