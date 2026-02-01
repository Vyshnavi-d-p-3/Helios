package promql

import (
	"fmt"
	"strconv"
)

// Functions Helios supports. Aggregations are checked separately.
var rangeFunctions = map[string]bool{
	"rate":             true,
	"irate":            true,
	"increase":         true,
	"avg_over_time":    true,
	"sum_over_time":    true,
	"count_over_time":  true,
	"min_over_time":    true,
	"max_over_time":    true,
	"stddev_over_time": true,
}

var aggregations = map[string]bool{
	"sum":   true,
	"count": true,
	"avg":   true,
	"min":   true,
	"max":   true,
}

// Parse parses a single PromQL expression into an AST.
func Parse(input string) (Expr, error) {
	p := &parser{lex: newLexer(input)}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if t := p.lex.next(); t.kind != tkEOF {
		return nil, &ParseError{Msg: fmt.Sprintf("unexpected token %s after expression", t), Pos: t.pos}
	}
	return expr, nil
}

type parser struct {
	lex *lexer
}

// parseExpr is the top of the grammar:
//   expr = funcOrAgg | vectorSelector | number
func (p *parser) parseExpr() (Expr, error) {
	t := p.lex.peekTok()

	switch t.kind {
	case tkNumber:
		p.lex.next()
		v, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			return nil, &ParseError{Msg: fmt.Sprintf("invalid number %q", t.val), Pos: t.pos}
		}
		return &NumberLiteral{Value: v}, nil

	case tkMinus:
		// Unary minus; only meaningful for numbers in this subset.
		p.lex.next()
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		num, ok := inner.(*NumberLiteral)
		if !ok {
			return nil, &ParseError{Msg: "unary minus only supported on numbers", Pos: t.pos}
		}
		num.Value = -num.Value
		return num, nil

	case tkIdent:
		// Could be: aggregation, function, or metric name.
		name := t.val
		if aggregations[name] {
			return p.parseAggregation(name)
		}
		if rangeFunctions[name] {
			return p.parseFunction(name)
		}
		return p.parseVectorSelector()

	case tkLBrace:
		// Selector with no metric: {a="1"}
		return p.parseVectorSelector()

	default:
		return nil, &ParseError{Msg: fmt.Sprintf("unexpected %s", t), Pos: t.pos}
	}
}

func (p *parser) parseAggregation(op string) (Expr, error) {
	p.lex.next() // consume the agg name
	// Optional `by (l1, l2)` BEFORE the args, e.g., `sum by (status) (rate(...))`.
	var by []string
	if t := p.lex.peekTok(); t.kind == tkBy {
		p.lex.next()
		labels, err := p.parseLabelList()
		if err != nil {
			return nil, err
		}
		by = labels
	}
	if err := p.expect(tkLParen, "("); err != nil {
		return nil, err
	}
	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tkRParen, ")"); err != nil {
		return nil, err
	}
	// Optional `by (l1, l2)` AFTER the args, e.g., `sum(rate(...)) by (status)`.
	if t := p.lex.peekTok(); t.kind == tkBy {
		p.lex.next()
		labels, err := p.parseLabelList()
		if err != nil {
			return nil, err
		}
		by = labels
	}
	if t := p.lex.peekTok(); t.kind == tkWithout {
		return nil, &ParseError{Msg: "without is not supported in this subset; use by", Pos: t.pos}
	}
	return &AggregateExpr{Op: op, Inner: inner, By: by}, nil
}

func (p *parser) parseFunction(name string) (Expr, error) {
	p.lex.next() // consume the function name
	if err := p.expect(tkLParen, "("); err != nil {
		return nil, err
	}
	arg, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	// Range functions require a range vector argument. Check at parse time
	// for friendlier errors.
	if rangeFunctions[name] {
		vs, ok := arg.(*VectorSelector)
		if !ok || vs.Range == 0 {
			return nil, &ParseError{
				Msg: fmt.Sprintf("%s requires a range vector argument like metric[5m]", name),
			}
		}
	}
	if err := p.expect(tkRParen, ")"); err != nil {
		return nil, err
	}
	return &FunctionCall{Name: name, Arg: arg}, nil
}

func (p *parser) parseVectorSelector() (Expr, error) {
	sel := &VectorSelector{}

	// Optional metric name
	if t := p.lex.peekTok(); t.kind == tkIdent {
		p.lex.next()
		sel.Metric = t.val
	}

	// Optional matchers
	if t := p.lex.peekTok(); t.kind == tkLBrace {
		p.lex.next()
		for {
			if p.lex.peekTok().kind == tkRBrace {
				p.lex.next()
				break
			}
			m, err := p.parseMatcher()
			if err != nil {
				return nil, err
			}
			sel.Matchers = append(sel.Matchers, m)

			t := p.lex.next()
			if t.kind == tkRBrace {
				break
			}
			if t.kind != tkComma {
				return nil, &ParseError{
					Msg: fmt.Sprintf("expected , or } got %s", t), Pos: t.pos,
				}
			}
		}
	}

	if sel.Metric == "" && len(sel.Matchers) == 0 {
		return nil, &ParseError{Msg: "empty selector"}
	}

	// Optional range: [5m]
	if t := p.lex.peekTok(); t.kind == tkLBracket {
		p.lex.next()
		dur, _, err := p.lex.scanDuration()
		if err != nil {
			return nil, &ParseError{Msg: err.Error(), Pos: t.pos}
		}
		d, err := ParseDuration(dur)
		if err != nil {
			return nil, &ParseError{Msg: err.Error(), Pos: t.pos}
		}
		sel.Range = d.Milliseconds()
		if err := p.expect(tkRBracket, "]"); err != nil {
			return nil, err
		}
	}

	return sel, nil
}

func (p *parser) parseMatcher() (Matcher, error) {
	nameTok := p.lex.next()
	if nameTok.kind != tkIdent {
		return Matcher{}, &ParseError{
			Msg: fmt.Sprintf("expected label name, got %s", nameTok), Pos: nameTok.pos,
		}
	}
	opTok := p.lex.next()
	var op MatcherOp
	switch opTok.kind {
	case tkEqual:
		op = MatchEqual
	case tkNotEqual:
		op = MatchNotEqual
	case tkRegex:
		op = MatchRegex
	case tkNotRegex:
		op = MatchNotRegex
	default:
		return Matcher{}, &ParseError{
			Msg: fmt.Sprintf("expected matcher operator, got %s", opTok), Pos: opTok.pos,
		}
	}
	valTok := p.lex.next()
	if valTok.kind != tkString {
		return Matcher{}, &ParseError{
			Msg: fmt.Sprintf("expected quoted string, got %s", valTok), Pos: valTok.pos,
		}
	}
	return Matcher{Name: nameTok.val, Op: op, Value: valTok.val}, nil
}

func (p *parser) parseLabelList() ([]string, error) {
	if err := p.expect(tkLParen, "("); err != nil {
		return nil, err
	}
	var out []string
	for {
		if p.lex.peekTok().kind == tkRParen {
			p.lex.next()
			break
		}
		t := p.lex.next()
		if t.kind != tkIdent {
			return nil, &ParseError{
				Msg: fmt.Sprintf("expected label name in by(), got %s", t), Pos: t.pos,
			}
		}
		out = append(out, t.val)
		t2 := p.lex.next()
		if t2.kind == tkRParen {
			break
		}
		if t2.kind != tkComma {
			return nil, &ParseError{
				Msg: fmt.Sprintf("expected , or ) got %s", t2), Pos: t2.pos,
			}
		}
	}
	return out, nil
}

func (p *parser) expect(k tokenKind, label string) error {
	t := p.lex.next()
	if t.kind != k {
		return &ParseError{
			Msg: fmt.Sprintf("expected %s, got %s", label, t), Pos: t.pos,
		}
	}
	return nil
}
