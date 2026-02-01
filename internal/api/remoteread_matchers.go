package api

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/prometheus/prometheus/prompb"
)

// remoteReadMatcherPlan uses equality matchers for index narrowing (intersection
// in postings) and optional predicates for NEQ/RE/NRE. __name__ must be an EQ
// matcher (remote read / Prometheus convention).
type remoteReadMatcherPlan struct {
	metric   string
	eqNarrow map[string]string
	pred     func(map[string]string) bool
}

func planRemoteReadMatchers(matchers []*prompb.LabelMatcher) (*remoteReadMatcherPlan, error) {
	if len(matchers) == 0 {
		return nil, errors.New("remote read: at least one label matcher is required")
	}
	var metric string
	eqNarrow := make(map[string]string)
	var preds []func(map[string]string) bool

	for _, m := range matchers {
		if m == nil {
			continue
		}
		name, val := m.GetName(), m.GetValue()
		if name == "" {
			return nil, errors.New("remote read: empty matcher name")
		}
		typ := m.GetType()
		if name == "__name__" {
			if typ != prompb.LabelMatcher_EQ {
				return nil, errors.New("remote read: __name__ must use equality (EQ) matcher")
			}
			metric = val
			continue
		}
		switch typ {
		case prompb.LabelMatcher_EQ:
			if prev, ok := eqNarrow[name]; ok && prev != val {
				return nil, fmt.Errorf("remote read: conflicting equality matchers for label %q", name)
			}
			eqNarrow[name] = val
		case prompb.LabelMatcher_NEQ:
			v := val
			preds = append(preds, neqLabelPred(name, v))
		case prompb.LabelMatcher_RE:
			re, err := fullValueRegex(val)
			if err != nil {
				return nil, fmt.Errorf("remote read: bad regexp for label %q: %w", name, err)
			}
			preds = append(preds, reLabelPred(name, re))
		case prompb.LabelMatcher_NRE:
			re, err := fullValueRegex(val)
			if err != nil {
				return nil, fmt.Errorf("remote read: bad negative regexp for label %q: %w", name, err)
			}
			preds = append(preds, nreLabelPred(name, re))
		default:
			return nil, fmt.Errorf("remote read: unknown matcher type %v", typ)
		}
	}
	if strings.TrimSpace(metric) == "" {
		return nil, errors.New("remote read: __name__ = \"...\" matcher is required")
	}
	var narrow map[string]string
	if len(eqNarrow) > 0 {
		narrow = eqNarrow
	}
	plan := &remoteReadMatcherPlan{metric: metric, eqNarrow: narrow}
	if len(preds) > 0 {
		plan.pred = andLabelPreds(preds)
	}
	return plan, nil
}

// fullValueRegex compiles a Prometheus-style regex: the entire label value must match.
func fullValueRegex(s string) (*regexp.Regexp, error) {
	// Multiline not split across label values; anchor like Prom's labels.NewFastRegexMatcher.
	return regexp.Compile("^(?:" + s + ")$")
}

// neqLabelPred: Prom semantics — if label is missing, the point matches != v.
func neqLabelPred(name, notVal string) func(map[string]string) bool {
	return func(lab map[string]string) bool {
		v, ok := lab[name]
		if !ok {
			return true
		}
		return v != notVal
	}
}

// reLabelPred: label must be present and match the (full-value) regex.
func reLabelPred(name string, re *regexp.Regexp) func(map[string]string) bool {
	return func(lab map[string]string) bool {
		v, ok := lab[name]
		if !ok {
			return false
		}
		return re.MatchString(v)
	}
}

// nreLabelPred: if label missing, match; if present, must not match regex.
func nreLabelPred(name string, re *regexp.Regexp) func(map[string]string) bool {
	return func(lab map[string]string) bool {
		v, ok := lab[name]
		if !ok {
			return true
		}
		return !re.MatchString(v)
	}
}

func andLabelPreds(preds []func(map[string]string) bool) func(map[string]string) bool {
	return func(lab map[string]string) bool {
		for _, p := range preds {
			if !p(lab) {
				return false
			}
		}
		return true
	}
}
