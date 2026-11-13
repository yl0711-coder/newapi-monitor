package metering

import "math"

// EvaluateFloat uses the same bounded, content-free AST as Evaluate, but
// follows the float64 arithmetic used by NewAPI rc.48 tiered settlement.
// The exact rational evaluator remains available for decimal billing paths.
func (e *PriceExpression) EvaluateFloat(vars map[string]float64) (float64, string, error) {
	if e == nil || e.root == nil {
		return 0, "", errPriceExpression
	}
	v, err := priceEvalFloat(e.root, vars)
	if err != nil || v.boolean || v.n < 0 || math.IsNaN(v.n) || math.IsInf(v.n, 0) {
		return 0, "", errPriceExpression
	}
	return v.n, v.tier, nil
}

type priceFloatValue struct {
	n              float64
	boolean, truth bool
	tier           string
}

func priceEvalFloat(n *priceNode, vars map[string]float64) (priceFloatValue, error) {
	fail := func() (priceFloatValue, error) { return priceFloatValue{}, errPriceExpression }
	if n.op == "number" {
		v, err := priceEval(n, nil)
		if err != nil {
			return fail()
		}
		f, _ := v.n.Float64()
		if math.IsInf(f, 0) {
			return fail()
		}
		return priceFloatValue{n: f}, nil
	}
	if n.op == "variable" {
		f, ok := vars[n.text]
		if !ok || f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
			return fail()
		}
		return priceFloatValue{n: f}, nil
	}
	a, err := priceEvalFloat(n.children[0], vars)
	if err != nil {
		return fail()
	}
	if n.op == "?" {
		if !a.boolean {
			return fail()
		}
		i := 2
		if a.truth {
			i = 1
		}
		return priceEvalFloat(n.children[i], vars)
	}
	if a.boolean {
		return fail()
	}
	switch n.op {
	case "tier":
		if a.tier != "" {
			return fail()
		}
		a.tier = n.text
		return a, nil
	case "u+":
		return a, nil
	case "u-":
		a.n = -a.n
		return a, nil
	}
	b, err := priceEvalFloat(n.children[1], vars)
	if err != nil || b.boolean || a.tier != "" && b.tier != "" {
		return fail()
	}
	v := priceFloatValue{tier: a.tier}
	if v.tier == "" {
		v.tier = b.tier
	}
	switch n.op {
	case "+":
		v.n = a.n + b.n
	case "-":
		v.n = a.n - b.n
	case "*":
		v.n = a.n * b.n
	case "/":
		if b.n == 0 {
			return fail()
		}
		v.n = a.n / b.n
	case "<":
		v.boolean = true
		v.truth = a.n < b.n
	case "<=":
		v.boolean = true
		v.truth = a.n <= b.n
	case ">":
		v.boolean = true
		v.truth = a.n > b.n
	case ">=":
		v.boolean = true
		v.truth = a.n >= b.n
	case "==":
		v.boolean = true
		v.truth = a.n == b.n
	case "!=":
		v.boolean = true
		v.truth = a.n != b.n
	default:
		return fail()
	}
	if v.boolean && v.tier != "" || math.IsInf(v.n, 0) || math.IsNaN(v.n) {
		return fail()
	}
	return v, nil
}
