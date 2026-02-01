// Package promql implements the PromQL subset Helios supports.
//
// Supported in v1:
//   - Vector selectors with label matchers: foo{a="1", b!="2", c=~"3.*", d!~"x"}
//   - Range vector selectors:               foo{...}[5m]
//   - Aggregations:                         sum, count, avg, min, max [by (labels)]
//   - Range-vector functions:               rate, irate, increase, avg_over_time,
//                                           sum_over_time, count_over_time,
//                                           min_over_time, max_over_time
//   - Numeric literals
//
// Not supported in v1 (intentionally):
//   - Binary operators between vectors (a + b, a / b)
//   - Subqueries: foo[5m:30s]
//   - Histogram-specific functions (histogram_quantile, etc.)
//   - String literals as expressions (only inside matchers)
//   - @ modifier and offset (deferred)
//
// The grammar is small enough to hand-roll a recursive-descent parser; this is
// also faster to debug than ANTLR and matches what Prometheus itself does.
package promql

import (
	"fmt"
	"time"
)

// MatcherOp is the operator of a label matcher.
type MatcherOp int

const (
	MatchEqual MatcherOp = iota
	MatchNotEqual
	MatchRegex
	MatchNotRegex
)

func (op MatcherOp) String() string {
	switch op {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	case MatchRegex:
		return "=~"
	case MatchNotRegex:
		return "!~"
	}
	return "?"
}

// Matcher is one label matcher inside a vector selector.
type Matcher struct {
	Name  string
	Op    MatcherOp
	Value string
}

// Expr is implemented by every node in the AST. The tagging method exists so
// the type switch in the evaluator is exhaustive at the type-assertion layer.
type Expr interface {
	exprNode()
}

// VectorSelector references a metric and label matchers. When Range != 0 it
// is a range vector selector. Range is in milliseconds.
type VectorSelector struct {
	Metric   string // empty if matchers alone identify the series
	Matchers []Matcher
	Range    int64 // ms; 0 = instant vector
}

func (*VectorSelector) exprNode() {}

// FunctionCall is a function applied to one expression. Helios' supported
// functions are unary (single argument) so the AST node is simple.
type FunctionCall struct {
	Name string
	Arg  Expr
}

func (*FunctionCall) exprNode() {}

// AggregateExpr aggregates over an inner expression with optional grouping.
type AggregateExpr struct {
	Op    string   // "sum", "count", "avg", "min", "max"
	Inner Expr
	By    []string // empty means aggregate everything into one series
}

func (*AggregateExpr) exprNode() {}

// NumberLiteral is a numeric scalar.
type NumberLiteral struct {
	Value float64
}

func (*NumberLiteral) exprNode() {}

// ParseError carries position information.
type ParseError struct {
	Msg string
	Pos int
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("promql: %s (at byte %d)", e.Msg, e.Pos)
}

// ParseDuration accepts Prometheus-style durations: 5m, 1h, 30s, 1d, 1w, 90ms.
// We use a separate function from time.ParseDuration because Prometheus accepts
// "d" and "w" which time.ParseDuration rejects.
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	// Try the stdlib first (handles 30s, 5m, 1h, 250ms, etc.)
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	// Fall back: parse N + unit where unit is d or w.
	last := s[len(s)-1]
	if last == 'd' || last == 'w' {
		num := s[:len(s)-1]
		var n int64
		for i := 0; i < len(num); i++ {
			c := num[i]
			if c < '0' || c > '9' {
				return 0, fmt.Errorf("invalid duration %q", s)
			}
			n = n*10 + int64(c-'0')
		}
		switch last {
		case 'd':
			return time.Duration(n) * 24 * time.Hour, nil
		case 'w':
			return time.Duration(n) * 7 * 24 * time.Hour, nil
		}
	}
	return 0, fmt.Errorf("invalid duration %q", s)
}
