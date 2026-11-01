package monitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type MonthlySameViewFunding struct {
	ID         int64  `json:"id"`
	At         int64  `json:"at"`
	DeltaQuota int64  `json:"delta_quota"`
	Source     string `json:"source"`
}

type MonthlySameViewSnapshot struct {
	Version          int                      `json:"version"`
	Boundary         int64                    `json:"boundary"`
	CompletedAtMS    int64                    `json:"completed_at_ms"`
	OriginalSHA256   [2]string                `json:"original_sha256"`
	Funding          []MonthlySameViewFunding `json:"funding"`
	TailConsumeQuota *int64                   `json:"tail_consume_quota,omitempty"`
	TailRefundQuota  *int64                   `json:"tail_refund_quota,omitempty"`
}

// The server-owned verifier MUST independently establish the original view,
// account scope and exact input/context, honoring its own cancellation/I/O
// budgets. It is executable trusted code, never an HTTP/JSON approval flag.
// The ordinary JSON preparation API rejects same-view metadata. Neither API
// publishes a bill. Copies prevent the verifier mutating calculation inputs.
func PrepareMonthlyCandidateBundleWithSameView(inputRaw, contextRaw []byte, now time.Time, verify func([]byte, []byte) error) (MonthlyPreparationBundle, error) {
	if verify == nil || len(inputRaw) > MonthlyPreparationInputLimit || len(contextRaw) > MonthlyPreparationInputLimit {
		return MonthlyPreparationBundle{}, errors.New("同视图原件核验器缺失或输入过大")
	}
	input, scope := bytes.Clone(inputRaw), bytes.Clone(contextRaw)
	if err := verify(bytes.Clone(input), bytes.Clone(scope)); err != nil {
		return MonthlyPreparationBundle{}, err
	}
	return prepareMonthlyCandidateBundle(input, scope, now, true)
}

func monthlySameViewShape(row map[string]json.RawMessage) error {
	for key, value := range row {
		if !strings.EqualFold(key, "same_view") {
			continue
		}
		var fields map[string]json.RawMessage
		if key != "same_view" || json.Unmarshal(value, &fields) != nil || (len(fields) != 5 && len(fields) != 7) {
			return errors.New("同视图结构或字段名称无效")
		}
		if len(fields) == 7 {
			for _, name := range []string{"tail_consume_quota", "tail_refund_quota"} {
				v, ok := fields[name]
				if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
					return errors.New("同视图尾部消费或退款缺失")
				}
			}
		}
		for _, name := range []string{"version", "boundary", "completed_at_ms", "original_sha256", "funding"} {
			v, ok := fields[name]
			if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
				return errors.New("同视图字段缺失或为空")
			}
		}
		var hashes []string
		if json.Unmarshal(fields["original_sha256"], &hashes) != nil || len(hashes) != 2 {
			return errors.New("同视图必须绑定两份原件")
		}
		var funds []map[string]json.RawMessage
		if json.Unmarshal(fields["funding"], &funds) != nil {
			return errors.New("同视图资金格式无效")
		}
		for _, fund := range funds {
			if len(fund) != 4 {
				return errors.New("同视图资金字段无效")
			}
			for _, name := range []string{"id", "at", "delta_quota", "source"} {
				v, ok := fund[name]
				if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
					return errors.New("同视图资金字段缺失或为空")
				}
			}
		}
	}
	return nil
}

func monthlySameViewValid(r MonthlySnapshotReconstruction, monthEnd int64, sources map[string]bool, now time.Time) error {
	v := r.SameView
	if v == nil {
		return nil
	}
	at := time.UnixMilli(v.CompletedAtMS).In(time.FixedZone("Usage-UTC+8", 8*3600))
	boundary := time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, at.Location()).Unix()
	if v.Version != 1 || r.BridgeDirection != "" || v.CompletedAtMS <= 0 || v.CompletedAtMS > now.UnixMilli() ||
		v.CompletedAtMS/1000 != r.ObservedAt || v.Boundary != boundary || v.Boundary < monthEnd || v.Boundary > r.ObservedAt ||
		!validMonthlyPublication(v.OriginalSHA256[0]) || !validMonthlyPublication(v.OriginalSHA256[1]) || v.OriginalSHA256[0] == v.OriginalSHA256[1] ||
		!strings.HasSuffix(r.SnapshotSource, "#wallet-tail:"+v.OriginalSHA256[1]) || v.Funding == nil || len(v.Funding) > 2000 {
		return errors.New("同视图原件、时间或衔接范围无效")
	}
	if r.HistoricalBalance != nil {
		if v.TailConsumeQuota == nil || v.TailRefundQuota == nil || *v.TailConsumeQuota < 0 || *v.TailRefundQuota < 0 {
			return errors.New("历史差异处理缺少原件尾部消费和退款")
		}
	} else if v.TailConsumeQuota != nil || v.TailRefundQuota != nil {
		return errors.New("未选择历史处理规则不能附加尾部处理字段")
	}
	seen := map[int64]bool{}
	for _, m := range v.Funding {
		if m.ID <= 0 || seen[m.ID] || m.At < v.Boundary || m.At > r.ObservedAt || m.DeltaQuota == 0 || !monthlyBundleRef(m.Source, sources) {
			return errors.New("同视图资金重复、越界或缺少来源")
		}
		seen[m.ID] = true
	}
	return nil
}
