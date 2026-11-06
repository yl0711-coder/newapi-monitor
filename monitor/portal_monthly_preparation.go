package monitor

// Offline preparation, not customer publication. This reconstructs monthly
// ledger boundaries from retained source intervals and independent snapshots.
// It neither changes a wallet nor claims that matching endpoints prove every
// historical transaction was atomic. A prepared draft still needs review.
import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"time"
)

const MonthlyPreparationInputLimit = 8 << 20

type MonthlyPreparationSource struct {
	ID     string `json:"id"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type MonthlyPreparationInput struct {
	Version  int                         `json:"version"`
	Sources  []MonthlyPreparationSource  `json:"sources"`
	Accounts []MonthlyPreparationAccount `json:"accounts"`
}

type MonthlyPreparationBalance struct {
	UserID       int64  `json:"user_id"`
	At           int64  `json:"at"` // Interpretation depends on TimeBasis; collection time is not a settled log cutoff.
	BalanceQuota int64  `json:"balance_quota"`
	UsedQuota    int64  `json:"used_quota"`
	Source       string `json:"source"`
	// Empty preserves legacy draft calculations, not an attestation of timing.
	// New wallet/history recovery requires an independently reviewed cutoff source.
	TimeBasis    string `json:"time_basis,omitempty"`
	CutoffSource string `json:"cutoff_source,omitempty"`
}

const monthlyCollectionObservation = "collection_observation"
const monthlySourceLogCutoff = "source_log_cutoff"

func (b MonthlyPreparationBalance) hasReviewedCutoff() bool {
	return b.TimeBasis == monthlySourceLogCutoff && b.CutoffSource != ""
}

type MonthlyPreparationSpan struct {
	UserID       int64  `json:"user_id"`
	From         int64  `json:"from"`
	To           int64  `json:"to"`
	ConsumeQuota int64  `json:"consume_quota"`
	RefundQuota  int64  `json:"refund_quota"`
	Requests     *int64 `json:"requests"` // explicit null means not retained, never zero
	Tokens       *int64 `json:"tokens"`
	Source       string `json:"source"`
}

type MonthlyPreparationMovement struct {
	UserID     int64  `json:"user_id"`
	ID         int64  `json:"id"`
	At         int64  `json:"at"`
	DeltaQuota int64  `json:"delta_quota"`
	Source     string `json:"source"`
}

type MonthlyPreparationCoverage struct {
	UserID int64  `json:"user_id"`
	From   int64  `json:"from"`
	To     int64  `json:"to"`
	Source string `json:"source"`
}

type MonthlyPreparationAccount struct {
	UserID           int64                        `json:"user_id"`
	GroupID          int64                        `json:"group_id"`
	Username         string                       `json:"username"`
	Months           []string                     `json:"months"`
	CounterBasis     string                       `json:"counter_basis"`
	BasisSource      string                       `json:"basis_source"`
	Baseline         MonthlyPreparationBalance    `json:"baseline"`
	Spans            []MonthlyPreparationSpan     `json:"spans"`
	Movements        []MonthlyPreparationMovement `json:"movements"`
	MovementCoverage []MonthlyPreparationCoverage `json:"movement_coverage"`
	Snapshots        []MonthlyPreparationBalance  `json:"snapshots"`
	// Optional exact-month wallet evidence. Omitted inputs retain the original
	// log-reconciled path and byte-identical candidate output.
	WalletMonths []MonthlyPreparationWalletMonth `json:"wallet_months,omitempty"`
	TokenMonths  []MonthlyBillTokenUsage         `json:"token_months,omitempty"`
	// Independently retained whole-month counts, never money or partial-window
	// estimates. Keeps known statistics when exact money-tail counts are absent.
	MonthStatistics []MonthlyPreparationStatistics `json:"month_statistics,omitempty"`
}

type MonthlyPreparationAmounts struct {
	Opening  string `json:"opening"`
	Increase string `json:"increase"`
	Decrease string `json:"decrease"` // signed, never netted into another member's credits
	Consume  string `json:"consume"`
	Refund   string `json:"refund"`
	Closing  string `json:"closing"`
}

type MonthlyPreparationAnchorCheck struct {
	At              int64  `json:"at"`
	Source          string `json:"source"`
	Status          string `json:"status"`
	BalanceGapQuota string `json:"balance_gap_quota,omitempty"`
	CounterGapQuota string `json:"counter_gap_quota,omitempty"`
	Reason          string `json:"reason,omitempty"`
	// Diagnostics only: a covered collection observation is still not a
	// reviewed source cutoff, and a missing interval never implies zero money.
	TimeBasis     string                           `json:"time_basis,omitempty"`
	CoverageIssue *MonthlyPreparationCoverageIssue `json:"coverage_issue,omitempty"`
}

type MonthlyPreparationRow struct {
	Month         string                     `json:"month"`
	Status        string                     `json:"status"`
	Reason        string                     `json:"reason,omitempty"`
	LogConsume    string                     `json:"log_consume,omitempty"`
	LogRefund     string                     `json:"log_refund,omitempty"`
	Requests      *int64                     `json:"requests"`
	Tokens        *int64                     `json:"tokens"`
	Amounts       *MonthlyPreparationAmounts `json:"amounts,omitempty"`
	AnchorAt      int64                      `json:"anchor_at,omitempty"`
	AnchorSource  string                     `json:"anchor_source,omitempty"`
	Sources       []string                   `json:"sources"`
	Basis         string                     `json:"basis,omitempty"`
	OpeningSource string                     `json:"opening_source,omitempty"`
	RefundSource  string                     `json:"refund_source,omitempty"`
}

type MonthlyPreparationAccountReport struct {
	UserID   int64                           `json:"user_id"`
	GroupID  int64                           `json:"group_id"`
	Username string                          `json:"username"`
	Error    string                          `json:"error,omitempty"`
	Anchors  []MonthlyPreparationAnchorCheck `json:"anchors"`
	Months   []MonthlyPreparationRow         `json:"months"`
}

type MonthlyPreparationReport struct {
	Mode            string                            `json:"mode"`
	SourceIntegrity string                            `json:"source_integrity"`
	InputSHA256     string                            `json:"input_sha256"`
	Sources         []MonthlyPreparationSource        `json:"sources"`
	Accounts        []MonthlyPreparationAccountReport `json:"accounts"`
}

// Explicit zero is required. In particular, an omitted snapshot balance or
// refund total must not silently turn into a genuine zero amount.
func DecodeMonthlyPreparationInput(raw []byte) (MonthlyPreparationInput, error) {
	var input MonthlyPreparationInput
	if len(raw) > MonthlyPreparationInputLimit {
		return input, errors.New("核对输入超过 8MiB")
	}
	if err := monthlyUniqueJSONKeys(raw); err != nil {
		return input, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&input); err != nil {
		return input, err
	}
	if err := monthlyPreparationShape(raw); err != nil {
		return input, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return input, errors.New("核对输入包含多余内容")
	}
	if input.Version != 1 || len(input.Accounts) == 0 || len(input.Accounts) > 500 || len(input.Sources) == 0 || len(input.Sources) > 512 {
		return input, errors.New("核对输入版本或范围无效")
	}
	seen := map[int64]bool{}
	for _, a := range input.Accounts {
		if a.UserID <= 0 || seen[a.UserID] {
			return input, errors.New("核对账号无效或重复")
		}
		seen[a.UserID] = true
	}
	sources := map[string]bool{}
	for _, s := range input.Sources {
		if s.ID == "" || len(s.ID) > 128 || strings.ContainsAny(s.ID, "# \t\r\n") || sources[s.ID] || !validMonthlyPublication(s.SHA256) || s.Path == "" || len(s.Path) > 4096 {
			return input, errors.New("来源标识、路径或 SHA-256 无效")
		}
		sources[s.ID] = true
	}
	for _, a := range input.Accounts {
		if err := monthlyPreparationStatisticsValid(a, sources); err != nil {
			return input, err
		}
		if len(a.TokenMonths) > len(a.Months) {
			return input, errors.New("令牌月份超出账号账单范围")
		}
		seenMonths := map[string]bool{}
		for _, month := range a.TokenMonths {
			matched := false
			for _, selected := range a.Months {
				matched = matched || selected == month.Month
			}
			if month.UserID != a.UserID || seenMonths[month.Month] || !matched || !monthlyBundleRef(month.Source, sources) || month.Rows == nil || len(month.Rows) > monthlyTokenPerUserLimit {
				return input, errors.New("令牌消费月份、账号或留存来源无效")
			}
			seenMonths[month.Month] = true
		}
	}
	return input, nil
}

func monthlyPreparationShape(raw []byte) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return err
	}
	require := func(obj map[string]json.RawMessage, names string) error {
		for _, name := range strings.Fields(names) {
			v, ok := obj[name]
			if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
				return fmt.Errorf("核对字段缺失或为空: %s", name)
			}
		}
		return nil
	}
	if err := require(root, "version sources accounts"); err != nil {
		return err
	}
	var accounts []map[string]json.RawMessage
	_ = json.Unmarshal(root["accounts"], &accounts)
	for _, a := range accounts {
		if err := require(a, "user_id group_id username months counter_basis basis_source baseline spans movements movement_coverage snapshots"); err != nil {
			return err
		}
		for key := range a {
			if strings.EqualFold(key, "wallet_months") && key != "wallet_months" {
				return errors.New("wallet_months 字段大小写无效")
			}
			if strings.EqualFold(key, "month_statistics") && key != "month_statistics" {
				return errors.New("month_statistics 字段大小写无效")
			}
		}
		if value, ok := a["month_statistics"]; ok && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("month_statistics 未提供时应省略，不得为 null")
		}
		if value, ok := a["wallet_months"]; ok && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("wallet_months 不得为空值；未提供时应省略")
		}
		if value, ok := a["token_months"]; ok {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return errors.New("token_months 未提供时应省略，不得为 null")
			}
			var months []json.RawMessage
			if err := json.Unmarshal(value, &months); err != nil {
				return err
			}
			for _, month := range months {
				if err := monthlyTokenJSONShape(month, func(obj map[string]json.RawMessage, keys ...string) error {
					return require(obj, strings.Join(keys, " "))
				}); err != nil {
					return err
				}
			}
		}
		for key, names := range map[string]string{
			"baseline":          "user_id at balance_quota used_quota source",
			"snapshots":         "user_id at balance_quota used_quota source",
			"spans":             "user_id from to consume_quota refund_quota source",
			"movements":         "user_id id at delta_quota source",
			"movement_coverage": "user_id from to source",
			"wallet_months":     "user_id month refund_quota source",
			"month_statistics":  "user_id month requests tokens source",
		} {
			var records []map[string]json.RawMessage
			if key == "baseline" {
				var b map[string]json.RawMessage
				_ = json.Unmarshal(a[key], &b)
				records = append(records, b)
			} else {
				_ = json.Unmarshal(a[key], &records)
			}
			for _, record := range records {
				if err := require(record, names); err != nil {
					return err
				}
				if key == "spans" {
					for _, name := range []string{"requests", "tokens"} {
						if _, ok := record[name]; !ok {
							return fmt.Errorf("核对统计字段缺失（未知须显式 null）: %s", name)
						}
					}
				}
			}
		}
	}
	return nil
}

type monthlyPreparationTotals struct {
	logsComplete                                          bool
	consume, refund, requests, tokens, increase, decrease int64
	requestsKnown, tokensKnown                            bool
	sources                                               map[string]bool
}

func (v monthlyPreparationTotals) balance(opening int64) (int64, error) {
	for _, delta := range []int64{v.increase, v.decrease, -v.consume, v.refund} {
		if err := addEconomicsInt64(&opening, delta); err != nil {
			return 0, err
		}
	}
	return opening, nil
}

// Ranges are half-open. Nonzero aggregated intervals cannot be split or
// interpolated: request a smaller retained source window instead.
func monthlyPreparationRange(a MonthlyPreparationAccount, from, to int64) (monthlyPreparationTotals, error) {
	v := monthlyPreparationTotals{sources: map[string]bool{}, requestsKnown: true, tokensKnown: true}
	if from == to {
		return v, nil
	}
	if from > to {
		return v, errors.New("核对时间范围倒置")
	}
	cursor := from
	for _, span := range a.Spans {
		if span.To <= from || span.From >= to {
			continue
		}
		start, end := max(span.From, from), min(span.To, to)
		if start != cursor {
			return v, monthlyCoverageError("usage", cursor, start, end)
		}
		if (start != span.From || end != span.To) && (span.ConsumeQuota != 0 || span.RefundQuota != 0 || span.Requests == nil || *span.Requests != 0 || span.Tokens == nil || *span.Tokens != 0) {
			boundary := start
			if start == span.From {
				boundary = end
			}
			return v, &monthlyPreparationCoverageError{
				issue:   MonthlyPreparationCoverageIssue{Kind: "usage_window_requires_split", From: start, To: end, WindowFrom: span.From, WindowTo: span.To, Source: span.Source},
				message: fmt.Sprintf("非空消费窗口跨过核对边界 %d，需补取细分窗口", boundary),
			}
		}
		for _, x := range []struct {
			target *int64
			value  int64
		}{{&v.consume, span.ConsumeQuota}, {&v.refund, span.RefundQuota}} {
			if err := addEconomicsInt64(x.target, x.value); err != nil {
				return v, err
			}
		}
		if span.Requests == nil {
			v.requestsKnown = false
		} else if err := addEconomicsInt64(&v.requests, *span.Requests); err != nil {
			return v, err
		}
		if span.Tokens == nil {
			v.tokensKnown = false
		} else if err := addEconomicsInt64(&v.tokens, *span.Tokens); err != nil {
			return v, err
		}
		v.sources[span.Source] = true
		cursor = end
	}
	if cursor != to {
		return v, &monthlyPreparationCoverageError{issue: MonthlyPreparationCoverageIssue{Kind: "usage_gap", From: cursor, To: to},
			message: fmt.Sprintf("消费覆盖未到达 %d（目前 %d）", to, cursor)}
	}
	if v.requests > 9007199254740991 || v.tokens > 9007199254740991 {
		return v, errors.New("条数或 Token 超出安全展示范围")
	}
	v.logsComplete = true
	cursor = from
	for _, span := range a.MovementCoverage {
		if span.To <= from || span.From >= to {
			continue
		}
		start, end := max(span.From, from), min(span.To, to)
		if start != cursor {
			return v, monthlyCoverageError("funding", cursor, start, end)
		}
		cursor = end
		v.sources[span.Source] = true
	}
	if cursor != to {
		return v, &monthlyPreparationCoverageError{issue: MonthlyPreparationCoverageIssue{Kind: "funding_gap", From: cursor, To: to},
			message: fmt.Sprintf("额度变动覆盖未到达 %d（目前 %d）", to, cursor)}
	}
	for _, movement := range a.Movements {
		if movement.At < from || movement.At >= to {
			continue
		}
		dst := &v.increase
		if movement.DeltaQuota < 0 {
			dst = &v.decrease
		}
		if err := addEconomicsInt64(dst, movement.DeltaQuota); err != nil {
			return v, err
		}
		v.sources[movement.Source] = true
	}
	return v, nil
}

func monthlyPreparationAccountValid(a MonthlyPreparationAccount, sources map[string]bool, now time.Time) error {
	ref := func(s string) bool {
		id, _, _ := strings.Cut(s, "#")
		return len(s) <= 1024 && sources[id]
	}
	validBalance := func(b MonthlyPreparationBalance) bool {
		validTiming := (b.TimeBasis == "" || b.TimeBasis == monthlyCollectionObservation) && b.CutoffSource == "" ||
			b.hasReviewedCutoff() && ref(b.CutoffSource)
		return b.UserID == a.UserID && b.At > 0 && b.At <= now.Unix() && b.UsedQuota >= 0 && ref(b.Source) && validTiming
	}
	if a.GroupID <= 0 || len(a.Username) > 256 || len(a.Months) == 0 || len(a.Months) > 120 || a.CounterBasis != "net_consumption" || !ref(a.BasisSource) || !validBalance(a.Baseline) || len(a.Spans) > 25000 || len(a.Movements) > 25000 || len(a.MovementCoverage) > 25000 || len(a.Snapshots) > 1000 {
		return errors.New("账号、范围、计数语义或基准来源无效")
	}
	for _, span := range a.Spans {
		if span.UserID != a.UserID || span.From < a.Baseline.At || span.To <= span.From || span.To > now.Unix() || span.ConsumeQuota < 0 || span.RefundQuota < 0 || (span.Requests != nil && *span.Requests < 0) || (span.Tokens != nil && *span.Tokens < 0) || !ref(span.Source) {
			return errors.New("消费区间无效或混入其他账号")
		}
	}
	for i, span := range a.MovementCoverage {
		if span.UserID != a.UserID || span.From < a.Baseline.At || span.To <= span.From || span.To > now.Unix() || !ref(span.Source) {
			return errors.New("额度变动覆盖范围无效")
		}
		if i > 0 && a.MovementCoverage[i-1].To > span.From {
			return errors.New("额度变动覆盖范围重叠")
		}
	}
	ids := map[int64]bool{}
	for _, movement := range a.Movements {
		if movement.UserID != a.UserID || movement.ID <= 0 || ids[movement.ID] || movement.At < a.Baseline.At || movement.At > 9007199254740991 || movement.DeltaQuota == 0 || !ref(movement.Source) {
			return errors.New("额度变动重复、无效或混入其他账号")
		}
		ids[movement.ID] = true
		i := sort.Search(len(a.MovementCoverage), func(i int) bool { return a.MovementCoverage[i].To > movement.At })
		if i == len(a.MovementCoverage) || a.MovementCoverage[i].From > movement.At {
			return errors.New("额度变动不在已取证范围内")
		}
	}
	at := map[int64]bool{}
	for _, b := range a.Snapshots {
		if !validBalance(b) || b.At <= a.Baseline.At || at[b.At] {
			return errors.New("余额快照重复、越界或混入其他账号")
		}
		at[b.At] = true
	}
	months := map[string]bool{}
	for _, month := range a.Months {
		if months[month] {
			return errors.New("月份重复")
		}
		months[month] = true
	}
	walletMonths := map[string]bool{}
	if len(a.WalletMonths) > 120 {
		return errors.New("实际钱包月份数量超限")
	}
	for _, wallet := range a.WalletMonths {
		if wallet.UserID != a.UserID || !months[wallet.Month] || walletMonths[wallet.Month] || wallet.RefundQuota < 0 || !ref(wallet.Source) {
			return errors.New("实际钱包退款依据重复、无效或混入其他账号月份")
		}
		walletMonths[wallet.Month] = true
	}
	return nil
}

func monthlyPreparationDifference(actual, expected int64) string {
	return new(big.Int).Sub(big.NewInt(actual), big.NewInt(expected)).String()
}

func monthlyPreparationRefs(refs map[string]bool) []string {
	result := make([]string, 0, len(refs))
	for ref := range refs {
		result = append(result, ref)
	}
	sort.Strings(result)
	return result
}

// PrepareMonthlyReconciliation reports every account independently. A later
// mismatching anchor cannot poison a month supported by an earlier matching
// anchor. It deliberately returns no importable publication or apply switch.
func PrepareMonthlyReconciliation(raw []byte, now time.Time) (MonthlyPreparationReport, error) {
	input, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return MonthlyPreparationReport{}, err
	}
	report := MonthlyPreparationReport{Mode: "local_reconciliation_draft_not_publication", SourceIntegrity: "not_checked_by_pure_calculator", InputSHA256: monthlyDigest(raw), Sources: input.Sources, Accounts: []MonthlyPreparationAccountReport{}}
	sources := map[string]bool{}
	for _, s := range input.Sources {
		sources[s.ID] = true
	}
	for _, a := range input.Accounts {
		r := MonthlyPreparationAccountReport{UserID: a.UserID, GroupID: a.GroupID, Username: a.Username, Anchors: []MonthlyPreparationAnchorCheck{}, Months: []MonthlyPreparationRow{}}
		sort.Slice(a.MovementCoverage, func(i, j int) bool { return a.MovementCoverage[i].From < a.MovementCoverage[j].From })
		if err := monthlyPreparationAccountValid(a, sources, now); err != nil {
			r.Error = err.Error()
			report.Accounts = append(report.Accounts, r)
			continue
		}
		sort.Slice(a.Spans, func(i, j int) bool { return a.Spans[i].From < a.Spans[j].From })
		sort.Slice(a.Snapshots, func(i, j int) bool { return a.Snapshots[i].At < a.Snapshots[j].At })
		sort.Strings(a.Months)
		for _, snapshot := range a.Snapshots {
			check := MonthlyPreparationAnchorCheck{At: snapshot.At, Source: snapshot.Source, Status: "incomplete_sources"}
			check.TimeBasis = snapshot.TimeBasis
			if a.Baseline.TimeBasis == monthlyCollectionObservation {
				check.TimeBasis = monthlyCollectionObservation
			}
			v, err := monthlyPreparationRange(a, a.Baseline.At, snapshot.At)
			var balance, used int64
			if err == nil {
				balance, err = v.balance(a.Baseline.BalanceQuota)
			}
			if err == nil {
				used = a.Baseline.UsedQuota
				err = addEconomicsInt64(&used, v.consume)
			}
			if err == nil {
				err = addEconomicsInt64(&used, -v.refund)
			}
			if err != nil {
				check.Reason = err.Error()
				var coverageErr *monthlyPreparationCoverageError
				if errors.As(err, &coverageErr) {
					issue := coverageErr.issue
					check.CoverageIssue = &issue
				}
			} else {
				// Big integers only for diagnostic subtraction: two individually
				// valid int64 balances can have a difference outside int64.
				check.BalanceGapQuota = monthlyPreparationDifference(snapshot.BalanceQuota, balance)
				check.CounterGapQuota = monthlyPreparationDifference(snapshot.UsedQuota, used)
				check.Status = "mismatch"
				if check.BalanceGapQuota == "0" && check.CounterGapQuota == "0" {
					check.Status = "matched"
				}
				if snapshot.TimeBasis == monthlyCollectionObservation || a.Baseline.TimeBasis == monthlyCollectionObservation {
					check.Status = "observation_only"
					check.Reason = "采集时间不是已结算日志截点；此差额仅供内部诊断，不作为月界或历史调账依据"
				}
			}
			r.Anchors = append(r.Anchors, check)
		}
		for _, month := range a.Months {
			row := MonthlyPreparationRow{Month: month, Status: "incomplete_sources", Sources: []string{}}
			start, end, err := monthlyWindow(month, now)
			if len(month) != 7 || start.Format("2006-01") != month || err != nil || start.Unix() < a.Baseline.At {
				row.Status, row.Reason = "invalid_month", "月份未结束、格式错误或早于基准"
				r.Months = append(r.Months, row)
				continue
			}
			v, err := monthlyPreparationRange(a, start.Unix(), end.Unix())
			if err == nil {
				v, err = monthlyResolveMonthStatistics(a, month, v)
			}
			row.Sources = monthlyPreparationRefs(v.sources)
			if v.logsComplete {
				row.LogConsume, row.LogRefund = monthlyQuotaMoney(v.consume), monthlyQuotaMoney(v.refund)
				if v.requestsKnown {
					row.Requests = &v.requests
				}
				if v.tokensKnown {
					row.Tokens = &v.tokens
				}
			}
			if err != nil {
				row.Reason = err.Error()
				r.Months = append(r.Months, row)
				continue
			}
			if monthlyPreparationWalletRow(a, v, start, end, &row) {
				r.Months = append(r.Months, row)
				continue
			}
			row.Status, row.Reason = "no_matching_balance_anchor", "本月日志已取得，尚无覆盖月末且与基准、流水相符的余额依据；未分摊差额"
			for i, check := range r.Anchors {
				if check.Status != "matched" || check.At < end.Unix() {
					continue
				}
				before, err := monthlyPreparationRange(a, a.Baseline.At, start.Unix())
				var opening, closing int64
				if err == nil {
					opening, err = before.balance(a.Baseline.BalanceQuota)
				}
				if err == nil {
					closing, err = v.balance(opening)
				}
				after, afterErr := monthlyPreparationRange(a, end.Unix(), check.At)
				if err == nil {
					err = afterErr
				}
				if err == nil {
					var reconstructed int64
					reconstructed, err = after.balance(closing)
					if err == nil && reconstructed != a.Snapshots[i].BalanceQuota {
						err = errors.New("逐月及基准余额核对不一致")
					}
				}
				if err != nil {
					row.Status, row.Reason = "incomplete_sources", err.Error()
					continue
				}
				row.Status, row.Reason, row.AnchorAt, row.AnchorSource = "reconciled_draft", "", check.At, check.Source
				row.Amounts = &MonthlyPreparationAmounts{Opening: monthlyQuotaMoney(opening), Increase: monthlyQuotaMoney(v.increase), Decrease: monthlyQuotaMoney(v.decrease), Consume: monthlyQuotaMoney(v.consume), Refund: monthlyQuotaMoney(v.refund), Closing: monthlyQuotaMoney(closing)}
				refs := map[string]bool{a.Baseline.Source: true, a.BasisSource: true, check.Source: true}
				for _, b := range []MonthlyPreparationBalance{a.Baseline, a.Snapshots[i]} {
					if b.CutoffSource != "" {
						refs[b.CutoffSource] = true
					}
				}
				for _, totals := range []monthlyPreparationTotals{before, v, after} {
					for ref := range totals.sources {
						refs[ref] = true
					}
				}
				row.Sources = monthlyPreparationRefs(refs)
				break
			}
			r.Months = append(r.Months, row)
		}
		report.Accounts = append(report.Accounts, r)
	}
	return report, nil
}
