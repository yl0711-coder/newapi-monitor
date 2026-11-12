package monitor

// Customer bill readers use immutable local publications only. Evidence is
// imported through trusted internal adapters, never from customer request JSON.
import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PortalMonthlyBill struct {
	GroupID           int64  `gorm:"primaryKey;autoIncrement:false"`
	Month             string `gorm:"primaryKey;size:7"`
	MemberFingerprint string
	MemberManifest    string // authorization revisions at issuance; independent of live usage facts
	Payload           string
	PayloadHash       string
	Evidence          string // internal evidence, never returned to a customer
	EvidenceHash      string
	PublishedAt       int64
	PublicationID     string // unique per issuance, including identical reissues
}

// ValidateMonthlyBillPublicationRecord uses the customer reader's validation
// without publishing, withdrawing, recalculating or returning private evidence.
// Internal maintenance must still match the record to its original task.
func ValidateMonthlyBillPublicationRecord(record PortalMonthlyBill, now time.Time) error {
	if record.GroupID <= 0 || record.PublicationID == "" || record.PublishedAt <= 0 || record.PublishedAt > now.Unix() {
		return errors.New("invalid saved publication identity")
	}
	p, err := readPublishedMonthlyBill(record)
	if err != nil {
		return err
	}
	e, err := readStoredMonthlyEvidence(record)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	if err != nil || !bytes.Equal(raw, []byte(record.Evidence)) || monthlyDigest(raw) != record.EvidenceHash || e.GroupID != record.GroupID || e.Month != record.Month {
		return errors.New("invalid saved publication evidence")
	}
	expected, _, err := buildPublishedMonthlyBill(e, time.Unix(record.PublishedAt, 0))
	if err != nil {
		return err
	}
	// Presentation names and a separately validated frozen-cent wallet are not
	// raw financial evidence. Every other published field must match the same
	// builder used at issuance, including each account and token's amounts.
	p.Publication, p.Account, p.DisplayBalances = "", "", nil
	for i := range p.Members {
		p.Members[i].Username = ""
	}
	for i := range p.Credits.Rows {
		p.Credits.Rows[i].Username = ""
	}
	actualRaw, err := json.Marshal(p)
	expectedRaw, expectedErr := json.Marshal(expected)
	if err != nil || expectedErr != nil || !bytes.Equal(actualRaw, expectedRaw) {
		return errors.New("saved publication differs from its original evidence")
	}
	return nil
}

// Compression is CPU/memory work even for a bounded bill. Do not let parallel
// downloads consume the shared Monitor process without a separate small bound.
var monthlyWorkbookSlots = make(chan struct{}, 2)

// Quota integers are authoritative (500000 = one displayed RMB balance unit).
// Complete/WalletOnly are explicit source-review attestations, not inferred
// from a successful SELECT, missing subscription metadata, or balanced totals.
type MonthlyBillEvidence struct {
	Version         int                       `json:"version"`
	GroupID         int64                     `json:"group_id"`
	Month           string                    `json:"month"`
	ReviewedBy      string                    `json:"reviewed_by"`
	ReviewedAt      int64                     `json:"reviewed_at"`
	SourceReference string                    `json:"source_reference"`
	Users           []MonthlyBillUserEvidence `json:"users"`
}
type MonthlyBillUserEvidence struct {
	UserID               int64 `json:"user_id"`
	Complete             bool  `json:"complete"`
	WalletOnly           bool  `json:"wallet_only"`
	Through              int64 `json:"through"`
	ExpectedCreditEvents int   `json:"expected_credit_events"`
	OpeningQuota         int64 `json:"opening_quota"`
	ClosingQuota         int64 `json:"closing_quota"`
	// Independently reconstructed from a retained balance and its source bridge;
	// the accepted historical forward mode can use an earlier quiet observation.
	ReverseClosingQuota int64                          `json:"reverse_closing_quota"`
	ConsumeQuota        int64                          `json:"consume_quota"` // original logged consumption; accepted balance corrections remain separate
	RefundQuota         int64                          `json:"refund_quota"`  // actual consumption refunds, not a second funding event
	Accounting          *MonthlyBillAccountingEvidence `json:"accounting,omitempty"`
	BalanceCorrection   *MonthlyBillBalanceCorrection  `json:"balance_correction,omitempty"`
	HistoricalBalance   *MonthlyBillHistoricalBalance  `json:"historical_balance,omitempty"`
	Requests            int64                          `json:"requests"`
	Tokens              int64                          `json:"tokens"`
	Statistics          *MonthlyBillStatisticsCoverage `json:"statistics,omitempty"`
	TokenUsage          *MonthlyBillTokenUsage         `json:"token_usage,omitempty"`
	Events              []MonthlyBillCreditEvidence    `json:"events"`
	Reversals           []MonthlyBillReversal          `json:"reversals"`
}
type MonthlyBillCreditEvidence struct {
	ID         int64  `json:"id"`
	At         int64  `json:"at"`
	DeltaQuota int64  `json:"delta_quota"`
	Kind       string `json:"kind"`   // recharge, gift, adjustment
	Method     string `json:"method"` // fixed allowlist; no raw log text
}
type MonthlyBillReversal struct {
	CreditID int64  `json:"credit_id"`
	DebitID  int64  `json:"debit_id"`
	Reason   string `json:"reason"`
}

func decodeMonthlyBillEvidence(raw []byte) (MonthlyBillEvidence, error) {
	var e MonthlyBillEvidence
	if len(raw) > 2<<20 {
		return e, errors.New("账单证据超过2MiB")
	}
	if err := monthlyUniqueJSONKeys(raw); err != nil {
		return e, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return e, err
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return e, errors.New("证据包含多余内容")
	}
	// Missing numeric fields are not zero. Require an explicit value for every
	// accounting control, including legitimate zero opening/refund balances.
	var shape map[string]json.RawMessage
	_ = json.Unmarshal(raw, &shape)
	required := func(obj map[string]json.RawMessage, keys ...string) error {
		for _, key := range keys {
			if v, ok := obj[key]; !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
				return errors.New("证据字段缺失或为空: " + key)
			}
		}
		return nil
	}
	if err := required(shape, "version", "group_id", "month", "reviewed_by", "reviewed_at", "source_reference", "users"); err != nil {
		return e, err
	}
	var users []map[string]json.RawMessage
	_ = json.Unmarshal(shape["users"], &users)
	for _, u := range users {
		for key := range u {
			if strings.EqualFold(key, "historical_balance") && key != "historical_balance" {
				return e, errors.New("历史余额校正字段不得使用大小写别名")
			}
		}
		if err := required(u, "user_id", "complete", "wallet_only", "through", "expected_credit_events", "opening_quota", "closing_quota", "reverse_closing_quota", "consume_quota", "refund_quota", "requests", "tokens", "events", "reversals"); err != nil {
			return e, err
		}
		if e.Version >= 2 && e.Version <= 5 {
			if err := required(u, "accounting"); err != nil {
				return e, err
			}
			var accounting map[string]json.RawMessage
			if err := json.Unmarshal(u["accounting"], &accounting); err != nil {
				return e, err
			}
			if err := required(accounting, "basis", "opening_source", "closing_source", "movement_source", "refund_source", "reconciliation_source", "log_consume_quota", "log_refund_quota"); err != nil {
				return e, err
			}
		}
		if value, ok := u["statistics"]; ok {
			if e.Version != 3 && e.Version != 4 && e.Version != 5 {
				return e, errors.New("缺失辅助统计须使用版本 3；不能降级为零")
			}
			var stats map[string]json.RawMessage
			if err := json.Unmarshal(value, &stats); err != nil {
				return e, err
			}
			if err := required(stats, "requests_known", "tokens_known"); err != nil {
				return e, err
			}
		}
		if value, ok := u["balance_correction"]; ok {
			if (e.Version != 4 && e.Version != 5) || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return e, errors.New("余额校正须使用版本 4 的完整证据")
			}
			if err := monthlyCorrectionJSONShape(value, required); err != nil {
				return e, err
			}
		}
		if value, ok := u["historical_balance"]; ok {
			if (e.Version != 4 && e.Version != 5) || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return e, errors.New("历史余额校正须使用完整证据版本，不得降级")
			}
			if err := monthlyHistoricalBalanceJSONShape(value, required); err != nil {
				return e, err
			}
		}
		if value, ok := u["token_usage"]; ok {
			if e.Version != 5 {
				return e, errors.New("令牌消费明细须使用版本 5")
			}
			if err := monthlyTokenJSONShape(value, required); err != nil {
				return e, err
			}
		} else if e.Version == 5 {
			return e, errors.New("令牌消费明细缺少成员")
		}
		var events []map[string]json.RawMessage
		_ = json.Unmarshal(u["events"], &events)
		for _, r := range events {
			if err := required(r, "id", "at", "delta_quota", "kind", "method"); err != nil {
				return e, err
			}
		}
		var reversals []map[string]json.RawMessage
		_ = json.Unmarshal(u["reversals"], &reversals)
		for _, r := range reversals {
			if err := required(r, "credit_id", "debit_id", "reason"); err != nil {
				return e, err
			}
		}
	}
	return e, nil
}

func monthlyUniqueJSONKeys(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 16 {
			return errors.New("证据 JSON 嵌套过深")
		}
		t, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return errors.New("证据 JSON 含重复字段")
				}
				// encoding/json matches struct fields case-insensitively, while
				// the required-field maps below do not. Only the exact lowercase
				// ASCII schema spelling is allowed, including at nested levels.
				// Otherwise USERS can shadow users and turn missing amounts into 0.
				if s == "" || len(s) > 64 {
					return errors.New("证据 JSON 字段名不符合模板")
				}
				for _, ch := range s {
					if ch != '_' && (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') {
						return errors.New("证据 JSON 字段名必须使用模板中的小写名称")
					}
				}
				seen[s] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return errors.New("证据 JSON 结构无效")
		}
		_, err = d.Token()
		return err
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("证据 JSON 包含多余内容")
	}
	return nil
}

func buildPublishedMonthlyBill(e MonthlyBillEvidence, now time.Time) (monthlyStatement, []int64, error) {
	p := monthlyStatement{}
	fail := func(reason string) (monthlyStatement, []int64, error) { return p, nil, errors.New(reason) }
	start, end, err := monthlyWindow(e.Month, now)
	if err != nil {
		return p, nil, err
	}
	if (e.Version < 2 || e.Version > 5) || e.Version != monthlyEvidenceVersion(e.Users) {
		return fail("新账单须使用版本 2 的完整统计或版本 3 的缺失统计实际扣费证据")
	}
	if e.GroupID <= 0 || e.Month == "" || len(e.Users) == 0 || len(e.Users) > maxTrackedUsers || strings.TrimSpace(e.ReviewedBy) == "" || len(e.ReviewedBy) > 128 || strings.TrimSpace(e.SourceReference) == "" || len(e.SourceReference) > 1024 || e.ReviewedAt < end.Unix() || e.ReviewedAt > now.Unix() {
		return fail("缺少有效的账单版本、范围或人工复核记录")
	}
	p = monthlyStatement{Month: e.Month, From: start.Format("2006-01-02"), To: end.AddDate(0, 0, -1).Format("2006-01-02"), AsOf: e.ReviewedAt, Published: true, ConsumptionThrough: end.Unix(), MemberDetailsVersion: 1}
	if monthlyHasUnknownStatistics(e.Users) {
		p.MemberDetailsVersion = 2
	}
	if e.Version == 5 {
		p.TokenDetailsVersion = 1
	}
	hasCorrection := monthlyHasBalanceCorrection(e.Users)
	cr := monthlyCredits{Available: true, Rows: []monthlyCredit{}}
	ids := []int64{}
	users := map[int64]bool{}
	eventIDs := map[int64]bool{}
	var recharge, gift, adjust, consume, refund, opening, closing, correction int64
	count := 0
	methods := map[string]string{"在线充值": "recharge", "后台充值": "recharge", "兑换码充值": "recharge", "注册赠送": "gift", "邀请赠送": "gift", "签到赠送": "gift", "活动赠送": "gift", "余额扣减": "adjustment"}
	for _, u := range e.Users {
		if u.UserID <= 0 || users[u.UserID] || !u.Complete || !u.WalletOnly || u.Through != end.Unix() || u.ExpectedCreditEvents != len(u.Events) || len(u.Events) > monthlyCreditLimit || u.ConsumeQuota < 0 || u.RefundQuota < 0 || u.Requests < 0 || u.Tokens < 0 || u.Requests > 9007199254740991 || u.Tokens > 9007199254740991 {
			return fail("成员数据不完整、重复、含订阅或统计值无效")
		}
		if err := validateMonthlyAccountingEvidence(u.Accounting); err != nil {
			return p, nil, err
		}
		if err := validateMonthlyHistoricalBalance(u, e.GroupID, e.Month, e.ReviewedAt, now); err != nil {
			return p, nil, err
		}
		memberConsumption := monthlyUserConsumption(u)
		if !monthlyStatisticsValid(memberConsumption) || (u.Statistics != nil && u.Statistics.RequestsKnown && u.Statistics.TokensKnown) {
			return fail("辅助统计缺失状态与数值不一致")
		}
		users[u.UserID] = true
		ids = append(ids, u.UserID)
		local := map[int64]MonthlyBillCreditEvidence{}
		balance := u.OpeningQuota
		for _, r := range u.Events {
			if r.ID <= 0 || eventIDs[r.ID] || r.At < start.Unix() || r.At >= end.Unix() || r.DeltaQuota == 0 || methods[r.Method] != r.Kind || r.Kind == "" || ((r.Kind == "adjustment") != (r.DeltaQuota < 0)) {
				return fail("到账事件重复、越界、金额或方式无效")
			}
			eventIDs[r.ID] = true
			local[r.ID] = r
			if err := addEconomicsInt64(&balance, r.DeltaQuota); err != nil {
				return p, nil, err
			}
		}
		canceled := map[int64]bool{}
		for _, pair := range u.Reversals {
			a, aok := local[pair.CreditID]
			b, bok := local[pair.DebitID]
			if !aok || !bok || a.Kind != "recharge" || b.Kind != "adjustment" || a.DeltaQuota <= 0 || b.DeltaQuota != -a.DeltaQuota || a.At > b.At || canceled[a.ID] || canceled[b.ID] || strings.TrimSpace(pair.Reason) == "" || len(pair.Reason) > 500 {
				return fail("冲正必须逐笔确认同一成员、同月、等额的加减记录，不允许自动猜测")
			}
			canceled[a.ID] = true
			canceled[b.ID] = true
		}
		if err := addEconomicsInt64(&balance, -u.ConsumeQuota); err != nil {
			return p, nil, err
		}
		if err := addEconomicsInt64(&balance, u.RefundQuota); err != nil {
			return p, nil, err
		}
		correctionQuota, err := validateMonthlyBalanceCorrection(u, e.Month, e.ReviewedAt, now)
		if err != nil {
			return p, nil, err
		}
		if err := addEconomicsInt64(&balance, correctionQuota); err != nil {
			return p, nil, err
		}
		if balance != u.ClosingQuota || balance != u.ReverseClosingQuota {
			return fail("月末余额正向、反向核对不一致")
		}
		var memberCredit, memberAdjustment int64
		for _, r := range u.Events {
			if canceled[r.ID] {
				continue
			}
			dst := &recharge
			switch r.Kind {
			case "gift":
				dst = &gift
			case "adjustment":
				dst = &adjust
			default:
				count++
			}
			if err := addEconomicsInt64(dst, r.DeltaQuota); err != nil {
				return p, nil, err
			}
			memberTarget := &memberCredit
			if r.Kind == "adjustment" {
				memberTarget = &memberAdjustment
			}
			if err := addEconomicsInt64(memberTarget, r.DeltaQuota); err != nil {
				return p, nil, err
			}
			// Preserve both sides of ordinary quota changes. Intra-organization
			// transfers are source events, not inferred corrections to hide.
			cr.Rows = append(cr.Rows, monthlyCredit{UserID: u.UserID, At: r.At, Amount: monthlyQuotaMoney(r.DeltaQuota), Kind: r.Kind, Method: r.Method})
		}
		member := monthlyMemberStatement{
			UserID: u.UserID, Opening: monthlyQuotaMoney(u.OpeningQuota), Closing: monthlyQuotaMoney(u.ClosingQuota),
			CreditTotal: monthlyQuotaMoney(memberCredit), Adjustment: monthlyQuotaMoney(memberAdjustment),
			Consumption: memberConsumption,
		}
		if hasCorrection {
			member.BalanceCorrection = monthlyQuotaMoney(correctionQuota)
		}
		if e.Version == 5 {
			member.TokenConsumption, err = monthlyBuildTokenConsumption(u, e.Month)
			if err != nil {
				return p, nil, err
			}
		}
		p.Members = append(p.Members, member)
		for _, x := range []struct {
			dst *int64
			v   int64
		}{{&consume, u.ConsumeQuota}, {&refund, u.RefundQuota}, {&opening, u.OpeningQuota}, {&closing, u.ClosingQuota}, {&correction, correctionQuota}} {
			if err := addEconomicsInt64(x.dst, x.v); err != nil {
				return p, nil, err
			}
		}
	}
	if len(cr.Rows) > monthlyCreditLimit {
		return fail("账单超过展示上限")
	}
	total := recharge
	if err := addEconomicsInt64(&total, gift); err != nil {
		return p, nil, err
	}
	net := consume
	if err := addEconomicsInt64(&net, -refund); err != nil {
		return p, nil, err
	}
	rechargeText, giftText, closingText := monthlyQuotaMoney(recharge), monthlyQuotaMoney(gift), monthlyQuotaMoney(closing)
	cr.RechargeCount = &count
	cr.RechargeTotal = &rechargeText
	cr.GiftTotal = &giftText
	cr.KnownTotal = monthlyQuotaMoney(total)
	cr.Adjustment = monthlyQuotaMoney(adjust)
	if hasCorrection {
		cr.BalanceCorrection = monthlyQuotaMoney(correction)
	}
	sort.SliceStable(cr.Rows, func(i, j int) bool { return cr.Rows[i].At < cr.Rows[j].At })
	p.Credits = cr
	totals, err := monthlyAggregateStatistics(p.Members)
	if err != nil {
		return p, nil, err
	}
	totals.Amount, totals.Refund, totals.Net = monthlyQuotaMoney(consume), monthlyQuotaMoney(refund), monthlyQuotaMoney(net)
	p.Consumption = &totals
	openingText := monthlyQuotaMoney(opening)
	p.MonthOpeningBalance = &openingText
	p.MonthEndBalance = &closingText
	if !monthlyTokensComplete(p) {
		return fail("令牌消费与账号消费不一致或归属重复")
	}
	sort.Slice(p.Members, func(i, j int) bool { return p.Members[i].UserID < p.Members[j].UserID })
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return p, ids, nil
}

func monthlyDigest(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }

// ImportMonthlyBill validates only local evidence and the local customer scope.
// apply=false is a read-only preflight. No NewAPI DB, credentials, or network.
// Changed evidence cannot overwrite an already issued bill.
func ImportMonthlyBill(ctx context.Context, db *gorm.DB, raw []byte, apply bool, now time.Time) (string, error) {
	return importMonthlyBill(ctx, db, raw, apply, now, nil)
}

// Trusted automatic workers must bind the live authorization in the SAME
// publication transaction, not only in an earlier source-read precheck.
type MonthlyBillExpectedAuthorization struct {
	GroupID           int64
	AuthVersion       int64
	MemberFingerprint string
	MemberManifest    string
}

func ImportMonthlyBillForAuthorization(ctx context.Context, db *gorm.DB, raw []byte, expected MonthlyBillExpectedAuthorization, now time.Time) (string, error) {
	if expected.GroupID <= 0 || expected.AuthVersion < 0 || expected.MemberManifest == "" || expected.MemberFingerprint == "" {
		return "", errors.New("自动账单缺少明确的当前授权范围")
	}
	return importMonthlyBill(ctx, db, raw, true, now, &expected)
}

// PublishMonthlyBillForAuthorization is an internal worker adapter, not an HTTP
// endpoint. It reuses the Monitor-owned store without transferring its lifetime
// to the generation worker. Authorization is checked in the import transaction.
func (m *Monitor) PublishMonthlyBillForAuthorization(ctx context.Context, raw []byte, expected MonthlyBillExpectedAuthorization) (string, error) {
	if ctx == nil || m == nil || m.storeDB == nil || !m.cfg.PortalMonthlyStatementEnabled {
		return "", errors.New("月度账单保存未启用或资源不可用")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return ImportMonthlyBillForAuthorization(ctx, m.storeDB, raw, expected, time.Now())
}

func importMonthlyBill(ctx context.Context, db *gorm.DB, raw []byte, apply bool, now time.Time, expected *MonthlyBillExpectedAuthorization) (string, error) {
	return importMonthlyBillPrepared(ctx, db, raw, apply, now, expected, nil)
}

func importMonthlyBillPrepared(ctx context.Context, db *gorm.DB, raw []byte, apply bool, now time.Time, expected *MonthlyBillExpectedAuthorization, series *monthlyPreparedDisplayPublication) (string, error) {
	if ctx == nil || db == nil {
		return "", errors.New("账单保存缺少上下文或数据库")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	e, err := decodeMonthlyBillEvidence(raw)
	if err != nil {
		return "", err
	}
	p, ids, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		return "", err
	}
	canonical, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	digest := monthlyDigest(canonical)
	operation := func(tx *gorm.DB) error {
		scope, err := loadMonthlyBillAuthorization(ctx, tx, e.GroupID)
		if err != nil {
			return err
		}
		if expected != nil && (e.GroupID != expected.GroupID || scope.Group.PortalAuthVer != expected.AuthVersion || scope.Manifest != expected.MemberManifest || scope.Fingerprint != expected.MemberFingerprint || strings.TrimSpace(scope.Group.PortalEmail) == "") {
			return errors.New("自动账单生成期间组织授权或成员范围已变更")
		}
		tracked := scope.Members
		fp := portalMemberFingerprintFromIDs(ids)
		if len(tracked) != len(ids) || portalMemberFingerprint(tracked) != fp {
			return errors.New("证据成员与当前客户授权成员不一致")
		}
		p.Account = scope.Group.Name
		names := make(map[int64]string, len(tracked))
		for _, member := range tracked {
			names[member.UserID] = member.Username
		}
		for i := range p.Credits.Rows {
			p.Credits.Rows[i].Username = names[p.Credits.Rows[i].UserID]
		}
		for i := range p.Members {
			p.Members[i].Username = names[p.Members[i].UserID]
		}
		if series != nil {
			if err := monthlyRetainPreparedDisplayNames(series, e.GroupID, digest, &p); err != nil {
				return err
			}
		}
		var old PortalMonthlyBill
		lookup := tx.First(&old, "group_id = ? AND month = ?", e.GroupID, e.Month).Error
		if lookup == nil {
			if old.EvidenceHash != digest || old.MemberFingerprint != fp || old.MemberManifest != scope.Manifest {
				return errors.New("该月已有不同账单，拒绝覆盖已发布数据")
			}
			if _, err := readStoredMonthlyEvidence(old); err != nil {
				return err
			}
			if series != nil {
				if err := monthlyApplyPreparedDisplayPublication(ctx, tx, e.GroupID, digest, &p, series); err != nil {
					return err
				}
			}
			if err := validateMonthlyBillContinuity(tx, e, now); err != nil {
				return err
			}
			_, err := readPublishedMonthlyBill(old)
			return err
		}
		if !errors.Is(lookup, gorm.ErrRecordNotFound) {
			return lookup
		}
		if err := validateMonthlyBillContinuity(tx, e, now); err != nil {
			return err
		}
		var payload []byte
		var displayErr error
		if series != nil && series.cent != nil {
			var cent monthlyBillCentMonth
			cent, displayErr = monthlyApplyPreparedCentPublication(ctx, tx, e.GroupID, digest, &p, series)
			if displayErr == nil {
				payload, displayErr = monthlyEncodeCentBill(p, cent)
			}
		} else if series != nil {
			displayErr = monthlyApplyPreparedDisplayPublication(ctx, tx, e.GroupID, digest, &p, series)
		} else {
			displayErr = monthlyFreezeDisplayBalances(tx, e.GroupID, &p)
		}
		if displayErr != nil {
			return displayErr
		}
		if payload == nil {
			var err error
			payload, err = json.Marshal(p)
			if err != nil {
				return err
			}
		}
		if len(payload) > 2<<20 {
			return errors.New("账单展示数据超过2MiB，拒绝发布而非截断")
		}
		if !apply {
			return nil
		}
		publicationID := monthlyFrozenPublicationPrefix + rand.Text()
		if series != nil {
			publicationID = monthlySeriesPublicationID(series)
		}
		record := PortalMonthlyBill{GroupID: e.GroupID, Month: e.Month, MemberFingerprint: fp, MemberManifest: scope.Manifest, Payload: string(payload), PayloadHash: monthlyDigest(payload), Evidence: string(canonical), EvidenceHash: digest, PublishedAt: now.Unix(), PublicationID: publicationID}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&record)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			var old PortalMonthlyBill
			if err := tx.First(&old, "group_id = ? AND month = ?", e.GroupID, e.Month).Error; err != nil {
				return err
			}
			if old.EvidenceHash != digest || old.MemberFingerprint != fp || old.MemberManifest != scope.Manifest {
				return errors.New("该月已有不同账单，拒绝覆盖已发布数据")
			}
			if _, err := readStoredMonthlyEvidence(old); err != nil {
				return err
			}
			if series != nil {
				if err := monthlyApplyPreparedDisplayPublication(ctx, tx, e.GroupID, digest, &p, series); err != nil {
					return err
				}
			}
			_, err := readPublishedMonthlyBill(old)
			return err
		}
		return nil
	}
	if apply {
		err = db.WithContext(ctx).Transaction(operation)
	} else {
		err = operation(db.WithContext(ctx))
	}
	return digest, err
}

func (m *Monitor) portalPublishedMonthlyStatement(c *gin.Context) {
	c.Header("Cache-Control", "private, no-store")
	c.Header("X-Content-Type-Options", "nosniff")
	start, _, err := monthlyWindow(c.Query("month"), time.Now())
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	scope, ok := monthlyAuthorizedScope(c)
	if !ok {
		return
	}
	var record PortalMonthlyBill
	// A verified publication belongs to its organization, not its current roster.
	// Keep pre-verification prototype rows invisible; never rewrite the frozen
	// member snapshot merely to make an old statement readable.
	err = m.storeDB.WithContext(ctx).Select("group_id, month, payload, payload_hash, member_fingerprint, member_manifest, published_at, publication_id").Where("group_id = ? AND month = ? AND member_fingerprint <> '' AND member_manifest <> ''", scope.Group.ID, start.Format("2006-01")).First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(404, gin.H{"error": "该月账单暂不可用，可能尚未生成或已撤回"})
		return
	}
	if err != nil {
		c.JSON(503, gin.H{"error": "账单暂不可用"})
		return
	}
	bill, err := monthlyReadCustomerBill(record)
	if err == nil {
		err = m.loadSimpleBillExtras(ctx, record, &bill)
	}
	if err != nil {
		c.JSON(503, gin.H{"error": "账单完整性校验失败"})
		return
	}
	p := bill.statement
	if !monthlyClientSupportsStatistics(c, bill.source) {
		return
	}
	if c.Request.URL.Path == "/api/monthly-statement/download" {
		version := c.Query("publication")
		if !validMonthlyPublication(version) {
			c.JSON(400, gin.H{"error": "请先打开账单详情，再下载对应版本"})
			return
		}
		if version != p.Publication {
			c.JSON(409, gin.H{"error": "账单版本已变更，请重新打开详情后下载"})
			return
		}
		select {
		case monthlyWorkbookSlots <- struct{}{}:
			defer func() { <-monthlyWorkbookSlots }()
		default:
			c.Header("Retry-After", "2")
			c.JSON(429, gin.H{"error": "账单下载繁忙，请稍后重试"})
			return
		}
		payload, err := bill.workbook()
		if err != nil {
			c.JSON(422, gin.H{"error": "账单下载失败"})
			return
		}
		if !m.portalMonthlyPublicationStillCurrent(ctx, c, record) {
			return
		}
		filename := monthlyBillFilename(p.Account, p.Month)
		if filename == "" {
			c.JSON(503, gin.H{"error": "账单文件信息无效"})
			return
		}
		c.Header("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
		c.Data(http.StatusOK, monthlyXLSXMIME, payload)
		return
	}
	if !m.portalMonthlyPublicationStillCurrent(ctx, c, record) {
		return
	}
	if bill.sub != nil {
		c.JSON(200, gin.H{"ok": true, "data": p, "subscription": bill.sub})
		return
	}
	c.JSON(200, gin.H{"ok": true, "data": p})
}
func readPublishedMonthlyBill(record PortalMonthlyBill) (monthlyStatement, error) {
	if strings.HasPrefix(record.PublicationID, monthlyCentBillPublicationPrefix) {
		retained, err := monthlyReadCentBill(record)
		if err != nil {
			return monthlyStatement{}, err
		}
		p := retained.Source
		p.Publication = monthlyBillVersion(record)
		return p, nil
	}
	return readLegacyMonthlyBill(record)
}

func readLegacyMonthlyBill(record PortalMonthlyBill) (monthlyStatement, error) {
	var p monthlyStatement
	if record.MemberManifest != "" && !validMonthlyPublication(record.MemberManifest) {
		return p, errors.New("invalid bill member manifest")
	}
	if len(record.Payload) > 2<<20 || monthlyDigest([]byte(record.Payload)) != record.PayloadHash {
		return p, errors.New("bill checksum mismatch")
	}
	err := json.Unmarshal([]byte(record.Payload), &p)
	if err == nil && strings.HasPrefix(record.PublicationID, monthlyFrozenPublicationPrefix) && p.DisplayBalances == nil {
		err = errors.New("missing frozen display balances")
	}
	if err == nil && (!p.Published || p.Month != record.Month || p.CurrentMonth || !monthlySummary(p).Downloadable || p.MonthEndBalance == nil) {
		err = errors.New("invalid publication")
	}
	if err == nil && (p.MemberDetailsVersion == 1 || p.MemberDetailsVersion == 2) && record.MemberFingerprint != "" {
		ids := make([]int64, 0, len(p.Members))
		for _, member := range p.Members {
			ids = append(ids, member.UserID)
		}
		if portalMemberFingerprintFromIDs(ids) != record.MemberFingerprint {
			err = errors.New("bill member scope mismatch")
		}
	}
	// Computed by the server, not accepted from evidence or a stored DTO.
	p.Publication = monthlyBillVersion(record)
	return p, err
}
func (m *Monitor) portalPublishedMonthlyStatementList(c *gin.Context) {
	c.Header("Cache-Control", "private, no-store")
	now := time.Now()
	month, before := c.Query("month"), c.Query("before")
	if month != "" && before != "" {
		c.JSON(400, gin.H{"error": "月份筛选和翻页参数不能同时使用"})
		return
	}
	value := month
	if value == "" {
		value = before
	}
	start, _, err := monthlyWindow(value, now)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	scope, ok := monthlyAuthorizedScope(c)
	if !ok {
		return
	}
	query := m.storeDB.WithContext(ctx).Where("group_id = ? AND member_fingerprint <> '' AND member_manifest <> ''", scope.Group.ID)
	if month != "" {
		query = query.Where("month = ?", month)
	} else {
		query = query.Where("month <= ?", start.Format("2006-01"))
	}
	var records []PortalMonthlyBill
	if err := query.Select("group_id, month, payload, payload_hash, member_fingerprint, member_manifest, published_at, publication_id").Order("month DESC").Limit(monthlyStatementPageSize + 1).Find(&records).Error; err != nil {
		c.JSON(503, gin.H{"error": "账单列表暂不可用"})
		return
	}
	next := ""
	if len(records) > monthlyStatementPageSize {
		next = records[monthlyStatementPageSize].Month
		records = records[:monthlyStatementPageSize]
	}
	rows := []monthlyStatementRow{}
	unavailable := []string{}
	for _, record := range records {
		bill, err := monthlyReadCustomerBill(record)
		if err != nil {
			unavailable = append(unavailable, record.Month)
			continue // never return damaged amounts or hide other valid months
		}
		if !monthlyClientSupportsStatistics(c, bill.source) {
			return
		}
		row, err := bill.summary()
		if err != nil {
			unavailable = append(unavailable, record.Month)
			continue
		}
		rows = append(rows, row)
	}
	for _, record := range records {
		if !m.portalMonthlyPublicationStillCurrent(ctx, c, record) {
			return
		}
	}
	c.Header("Cache-Control", "private, no-store")
	c.JSON(200, gin.H{"ok": true, "data": gin.H{"rows": rows, "unavailable_months": unavailable, "next": next, "as_of": now.Unix()}})
}
