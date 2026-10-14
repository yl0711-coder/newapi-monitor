package monitor

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
)

// Presentation only. Terms come from the already validated accounting path;
// never use this text (or rounded display values) to verify or change quota.
type portalBillingTerm struct {
	label   string
	count   *big.Rat
	price   *big.Rat // USD per million tokens, or USD per call
	perCall bool
}

func portalBillingDisplayNumber(v *big.Rat) string {
	const places = 8
	s := v.FloatString(places)
	rounded, _ := new(big.Rat).SetString(s)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if rounded.Cmp(v) != 0 {
		if rounded.Sign() == 0 && v.Sign() > 0 {
			return "<0.00000001"
		}
		return "≈" + s
	}
	return s
}

func portalReadableBilling(terms []portalBillingTerm, group *big.Rat, quota int64, minimum bool) string {
	var lines []string
	var formula []string
	subtotal := new(big.Rat)
	// Copy before sorting: displaying an RC4-style explanation must not change
	// the validated expression order or any caller-owned terms.
	ordered := append([]portalBillingTerm(nil), terms...)
	rank := map[string]int{"普通输入": 0, "缓存读取": 1, "缓存创建": 2, "5 分钟缓存创建": 3, "1 小时缓存创建": 4, "输出": 5}
	sort.SliceStable(ordered, func(i, j int) bool { return rank[ordered[i].label] < rank[ordered[j].label] })
	prices := make(map[string]string)
	for _, term := range ordered {
		label := term.label
		switch label {
		case "普通输入":
			label = "输入"
		case "5 分钟缓存创建":
			label = "5m缓存创建"
		case "1 小时缓存创建":
			label = "1h缓存创建"
		}
		if term.count.Sign() == 0 && label != "输入" && label != "输出" {
			continue
		}
		amount := new(big.Rat).Mul(term.count, term.price)
		price := portalBillingDisplayNumber(term.price)
		if term.perCall {
			lines = append(lines, "模型价格：$"+price+" / 次")
			formula = append(formula, "$"+price+" / 次 × "+term.count.Num().String()+" 次")
		} else {
			amount.Quo(amount, big.NewRat(1000000, 1))
			prices[label] = "$" + price + " / 1M tokens"
			if term.count.Sign() != 0 {
				formula = append(formula, fmt.Sprintf("%s %s tokens / 1M tokens × $%s", label, term.count.Num(), price))
			}
		}
		subtotal.Add(subtotal, amount)
	}
	for _, label := range []string{"输入", "输出", "缓存读取", "缓存创建", "5m缓存创建", "1h缓存创建"} {
		if price, ok := prices[label]; ok {
			lines = append(lines, label+"价格："+price)
		}
	}
	actualUSD := new(big.Rat).SetFrac(big.NewInt(quota), big.NewInt(500000))
	actual := actualUSD.FloatString(6)
	calculated := new(big.Rat).Mul(subtotal, group)
	operator := "="
	groupText := portalBillingDisplayNumber(group)
	if calculated.Cmp(actualUSD) != 0 || strings.Contains(strings.Join(formula, "")+groupText, "≈") || strings.Contains(strings.Join(formula, "")+groupText, "<") {
		operator = "≈"
	}
	if len(formula) == 0 {
		formula = []string{"$0"}
	}
	lines = append(lines, fmt.Sprintf("（%s）× 分组倍率 %s %s $%s", strings.Join(formula, " + "), groupText, operator, actual))
	note := ""
	if minimum {
		note = "（已按平台最低计费单位处理）"
	}
	lines = append(lines, "实际扣费：$"+actual+note)
	lines = append(lines, "仅供参考，以实际扣费为准。")
	return strings.Join(lines, "\n")
}
