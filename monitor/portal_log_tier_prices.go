package monitor

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// A restricted grammar parser, NOT a general expression evaluator. Accept a complete
// chain of simple linear tiers. Never search a complex expression for a few
// plausible coefficients: omitted multipliers would misrepresent the price.
const portalTierNumber = `(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?`

var (
	portalTierCondition = regexp.MustCompile(`^len\s*(<=|>=|<|>)\s*(` + portalTierNumber + `)\s*\?\s*`)
	portalTierBody      = regexp.MustCompile(`^tier\s*\(\s*"([A-Za-z0-9_-]{1,64})"\s*,\s*([^()]*)\)\s*`)
	portalTierTerm      = regexp.MustCompile(`^\s*(p|c|cr|cc|cc1h|img|img_o|ai|ao)\s*\*\s*(` + portalTierNumber + `)\s*(\+|$)`)
)

type portalTierPrices struct {
	Index    int
	Prices   map[string]portalEvidenceField
	Branches []portalTierBranch
	Used     map[string]bool // union across ALL branches, as in NewAPI UsedVars
}

type portalTierBranch struct {
	Op        string // empty means the final default branch
	Threshold portalEvidenceField
	Order     []string
	Prices    map[string]portalEvidenceField
}

func portalRecordedTierPrices(e portalLogEvidence) (portalTierPrices, bool) {
	if e.State != portalEvidenceValid || e.Mode != "tiered_expr" || e.Expression.State != portalEvidenceValid || e.Strings["matched_tier"].State != portalEvidenceValid {
		return portalTierPrices{}, false
	}
	if len(e.Expression.Text) > 8192 {
		return portalTierPrices{}, false
	}
	s := strings.TrimSpace(e.Expression.Text)
	s = strings.TrimSpace(strings.TrimPrefix(s, "v1:"))
	labels := map[string]bool{}
	var selected portalTierPrices
	used := map[string]bool{}
	var branches []portalTierBranch
	for index := 1; index <= 16; index++ {
		conditional := false
		branch := portalTierBranch{}
		if m := portalTierCondition.FindStringSubmatch(s); m != nil {
			branch.Op, branch.Threshold = m[1], portalEvidenceNumber(json.RawMessage(m[2]), false, false)
			if branch.Threshold.State != portalEvidenceValid {
				return portalTierPrices{}, false
			}
			s = s[len(m[0]):]
			conditional = true
		}
		m := portalTierBody.FindStringSubmatch(s)
		if m == nil || labels[m[1]] {
			return portalTierPrices{}, false
		}
		labels[m[1]] = true
		prices := map[string]portalEvidenceField{}
		body := m[2]
		for {
			term := portalTierTerm.FindStringSubmatch(body)
			if term == nil {
				return portalTierPrices{}, false
			}
			if _, exists := prices[term[1]]; exists {
				return portalTierPrices{}, false
			}
			price := portalEvidenceNumber(json.RawMessage(term[2]), false, false)
			if price.State != portalEvidenceValid {
				return portalTierPrices{}, false
			}
			prices[term[1]] = price
			branch.Order = append(branch.Order, term[1])
			used[term[1]] = true
			body = body[len(term[0]):]
			if term[3] == "" {
				break
			}
		}
		if m[1] == e.Strings["matched_tier"].Text {
			selected.Index, selected.Prices = index, prices
		}
		branch.Prices = prices
		branches = append(branches, branch)
		s = strings.TrimSpace(s[len(m[0]):])
		if !conditional {
			selected.Branches, selected.Used = branches, used
			return selected, s == "" && selected.Index != 0
		}
		if !strings.HasPrefix(s, ":") {
			return portalTierPrices{}, false
		}
		s = strings.TrimSpace(s[1:])
	}
	return portalTierPrices{}, false
}

func portalTierPriceItems(e portalLogEvidence) []portalLogDetailItem {
	p, ok := portalRecordedTierPrices(e)
	if !ok {
		return nil
	}
	items := []portalLogDetailItem{{Label: "阶梯档位（按日志匹配）", Value: "第 " + strconv.Itoa(p.Index) + " 档", State: portalEvidenceValid}}
	for _, item := range [][2]string{{"p", "输入"}, {"c", "输出"}, {"cr", "缓存读取"}, {"cc", "缓存创建"}, {"cc1h", "1 小时缓存创建"}, {"img", "图片输入"}, {"img_o", "图片输出"}, {"ai", "音频输入"}, {"ao", "音频输出"}} {
		if price, exists := p.Prices[item[0]]; exists {
			items = append(items, portalLogDetailItem{Label: "阶梯" + item[1] + "基础单价（$/百万 Tokens，未乘分组倍率）", Value: price.Text, State: price.State})
		}
	}
	return items
}
