package metering

import (
	"errors"
	"math/big"
	"strconv"
	"strings"
	"text/scanner"
)

// PriceExpression is a bounded numeric subset of NewAPI's v1 tier expressions.
// It has no access to request content, time, functions, files or networks.
type PriceExpression struct {
	root      *priceNode
	Used      map[string]bool
	Canonical string
}
type priceNode struct {
	op, text string
	children []*priceNode
}
type priceParser struct {
	s     scanner.Scanner
	tok   rune
	text  string
	nodes int
	used  map[string]bool
}

var errPriceExpression = errors.New("unsupported_numeric_expression")

// ParsePriceExpression accepts numeric arithmetic, comparisons, ternaries and
// tier labels only. Canonical is rebuilt from validated nodes, never raw text.
func ParsePriceExpression(source string) (*PriceExpression, error) {
	if len(source) > 512 || strings.Contains(source, "/*") || strings.Contains(source, "//") || strings.Contains(source, "#") {
		return nil, errPriceExpression
	}
	source = strings.TrimSpace(source)
	source = strings.TrimPrefix(source, "v1:")
	p := &priceParser{used: map[string]bool{}}
	p.s.Init(strings.NewReader(source))
	p.s.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanFloats | scanner.ScanStrings
	p.s.Error = func(_ *scanner.Scanner, _ string) {}
	p.next()
	n, err := p.expr(0)
	if err != nil || p.tok != scanner.EOF || p.s.ErrorCount != 0 {
		return nil, errPriceExpression
	}
	return &PriceExpression{root: n, Used: p.used, Canonical: priceFormat(n)}, nil
}
func (p *priceParser) next() { p.tok = p.s.Scan(); p.text = p.s.TokenText() }
func (p *priceParser) node(op, text string, cs ...*priceNode) (*priceNode, error) {
	p.nodes++
	if p.nodes > 128 {
		return nil, errPriceExpression
	}
	return &priceNode{op, text, cs}, nil
}
func (p *priceParser) expr(depth int) (*priceNode, error) {
	if depth > 24 {
		return nil, errPriceExpression
	}
	a, e := p.binary(0, depth+1)
	if e != nil {
		return nil, e
	}
	if p.tok != '?' {
		return a, nil
	}
	p.next()
	b, e := p.expr(depth + 1)
	if e != nil || p.tok != ':' {
		return nil, errPriceExpression
	}
	p.next()
	c, e := p.expr(depth + 1)
	if e != nil {
		return nil, e
	}
	return p.node("?", "", a, b, c)
}
func (p *priceParser) binary(level, depth int) (*priceNode, error) {
	if depth > 24 {
		return nil, errPriceExpression
	}
	if level == 3 {
		return p.atom(depth + 1)
	}
	a, e := p.binary(level+1, depth)
	if e != nil {
		return nil, e
	}
	for {
		op := p.text
		accept := (level == 0 && (op == "<" || op == ">" || op == "=" || op == "!")) || (level == 1 && (op == "+" || op == "-")) || (level == 2 && (op == "*" || op == "/"))
		if !accept {
			return a, nil
		}
		p.next()
		if level == 0 {
			if p.tok == '=' {
				op += "="
				p.next()
			} else if op == "=" || op == "!" {
				return nil, errPriceExpression
			}
		}
		b, e := p.binary(level+1, depth)
		if e != nil {
			return nil, e
		}
		a, e = p.node(op, "", a, b)
		if e != nil {
			return nil, e
		}
	}
}
func (p *priceParser) atom(depth int) (*priceNode, error) {
	if depth > 24 {
		return nil, errPriceExpression
	}
	v := p.text
	switch p.tok {
	case '(':
		p.next()
		n, e := p.expr(depth + 1)
		if e != nil || p.tok != ')' {
			return nil, errPriceExpression
		}
		p.next()
		return n, nil
	case '+', '-':
		p.next()
		n, e := p.atom(depth + 1)
		if e != nil {
			return nil, e
		}
		return p.node("u"+v, "", n)
	case scanner.Int, scanner.Float:
		f := Number([]byte(v), false, true)
		if f.State != "valid" {
			return nil, errPriceExpression
		}
		p.next()
		return p.node("number", f.Value)
	case scanner.Ident:
		p.next()
		if v == "tier" {
			if p.tok != '(' {
				return nil, errPriceExpression
			}
			p.next()
			if p.tok != scanner.String {
				return nil, errPriceExpression
			}
			label, e := strconv.Unquote(p.text)
			if e != nil || len(label) == 0 || len(label) > 32 {
				return nil, errPriceExpression
			}
			for _, c := range label {
				if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
					return nil, errPriceExpression
				}
			}
			p.next()
			if p.tok != ',' {
				return nil, errPriceExpression
			}
			p.next()
			n, e := p.expr(depth + 1)
			if e != nil || p.tok != ')' {
				return nil, errPriceExpression
			}
			p.next()
			return p.node("tier", label, n)
		}
		switch v {
		case "p", "c", "len", "cr", "cc", "cc1h", "cc12h":
			p.used[v] = true
			return p.node("variable", v)
		}
	}
	return nil, errPriceExpression
}
func priceFormat(n *priceNode) string {
	switch n.op {
	case "number":
		if strings.Contains(n.text, "/") {
			return "(" + n.text + ")"
		}
		return n.text
	case "variable":
		return n.text
	case "tier":
		return "tier(" + strconv.Quote(n.text) + "," + priceFormat(n.children[0]) + ")"
	case "?":
		return "(" + priceFormat(n.children[0]) + "?" + priceFormat(n.children[1]) + ":" + priceFormat(n.children[2]) + ")"
	case "u+", "u-":
		return "(" + n.op[1:] + priceFormat(n.children[0]) + ")"
	default:
		return "(" + priceFormat(n.children[0]) + n.op + priceFormat(n.children[1]) + ")"
	}
}

type priceValue struct {
	n       *big.Rat
	boolean bool
	truth   bool
	tier    string
}

// Evaluate returns an exact rational and the selected tier. Resource bounds
// also apply to intermediate products, including adversarial divisors.
func (e *PriceExpression) Evaluate(vars map[string]*big.Rat) (*big.Rat, string, error) {
	if e == nil || e.root == nil {
		return nil, "", errPriceExpression
	}
	v, err := priceEval(e.root, vars)
	if err != nil || v.boolean || v.n == nil || v.n.Sign() < 0 {
		return nil, "", errPriceExpression
	}
	return v.n, v.tier, nil
}
func priceEval(n *priceNode, vars map[string]*big.Rat) (priceValue, error) {
	fail := func() (priceValue, error) { return priceValue{}, errPriceExpression }
	if n.op == "number" {
		r, ok := new(big.Rat).SetString(n.text)
		if !ok {
			return fail()
		}
		return priceValue{n: r}, nil
	}
	if n.op == "variable" {
		v := vars[n.text]
		if v == nil || v.Sign() < 0 || v.Num().BitLen() > 4096 || v.Denom().BitLen() > 4096 {
			return fail()
		}
		return priceValue{n: new(big.Rat).Set(v)}, nil
	}
	a, err := priceEval(n.children[0], vars)
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
		return priceEval(n.children[i], vars)
	}
	if a.boolean {
		return fail()
	}
	if n.op == "tier" {
		if a.tier != "" {
			return fail()
		}
		a.tier = n.text
		return a, nil
	}
	if n.op == "u+" {
		return a, nil
	}
	if n.op == "u-" {
		a.n.Neg(a.n)
		return a, nil
	}
	b, err := priceEval(n.children[1], vars)
	if err != nil || b.boolean || a.tier != "" && b.tier != "" {
		return fail()
	}
	tier := a.tier
	if tier == "" {
		tier = b.tier
	}
	r := new(big.Rat)
	switch n.op {
	case "+":
		r.Add(a.n, b.n)
	case "-":
		r.Sub(a.n, b.n)
	case "*":
		r.Mul(a.n, b.n)
	case "/":
		if b.n.Sign() == 0 {
			return fail()
		}
		r.Quo(a.n, b.n)
	default:
		// A tier is a recorded label on the numeric result. A single tier
		// may be scaled outside tier(...), as rc.48 request rules do. Do not
		// infer evaluation ordering for multiple tiers or tiered comparisons.
		if tier != "" {
			return fail()
		}
		c := a.n.Cmp(b.n)
		truth := false
		switch n.op {
		case "<":
			truth = c < 0
		case "<=":
			truth = c <= 0
		case ">":
			truth = c > 0
		case ">=":
			truth = c >= 0
		case "==":
			truth = c == 0
		case "!=":
			truth = c != 0
		default:
			return fail()
		}
		return priceValue{boolean: true, truth: truth}, nil
	}
	if r.Num().BitLen() > 4096 || r.Denom().BitLen() > 4096 {
		return fail()
	}
	return priceValue{n: r, tier: tier}, nil
}
