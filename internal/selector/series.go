// Package selector implements a small subset of Prometheus instant vector
// selectors used for HTTP match[] parameters.
package selector

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// ParseSeriesSelector parses "metric" or `metric{label="value", ...}`. Label
// names must match [a-zA-Z_][a-zA-Z0-9_]*. Values are double-quoted; backslash
// escapes the next character inside a value.
func ParseSeriesSelector(s string) (metric string, labels map[string]string, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil, errors.New("empty selector")
	}
	i := strings.IndexByte(s, '{')
	if i < 0 {
		if !isValidMetricName(s) {
			return "", nil, fmt.Errorf("invalid metric name %q", s)
		}
		return s, nil, nil
	}
	metric = strings.TrimSpace(s[:i])
	if metric == "" || !isValidMetricName(metric) {
		return "", nil, fmt.Errorf("invalid metric name %q", metric)
	}
	if s[len(s)-1] != '}' {
		return "", nil, errors.New("unclosed { in selector")
	}
	inner := strings.TrimSpace(s[i+1 : len(s)-1])
	if inner == "" {
		return metric, nil, nil
	}
	labels, err = parseBraceContent(inner)
	if err != nil {
		return "", nil, err
	}
	return metric, labels, nil
}

func isValidMetricName(m string) bool {
	if m == "" {
		return false
	}
	for i, r := range m {
		if i == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return false
			}
			continue
		}
		if r != '_' && r != ':' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func parseBraceContent(inner string) (map[string]string, error) {
	out := make(map[string]string)
	for pos := 0; pos < len(inner); {
		for pos < len(inner) && isSpace(inner[pos]) {
			pos++
		}
		if pos >= len(inner) {
			break
		}
		start := pos
		if !isLabelFirstByte(inner[pos]) {
			return nil, fmt.Errorf("expected label name at %q", inner[pos:])
		}
		pos++
		for pos < len(inner) && isLabelContByte(inner[pos]) {
			pos++
		}
		if pos == start {
			return nil, fmt.Errorf("expected label name at %q", inner[start:])
		}
		key := inner[start:pos]
		for pos < len(inner) && isSpace(inner[pos]) {
			pos++
		}
		if pos >= len(inner) || inner[pos] != '=' {
			return nil, fmt.Errorf("expected = after label %q", key)
		}
		pos++
		for pos < len(inner) && isSpace(inner[pos]) {
			pos++
		}
		if pos >= len(inner) || inner[pos] != '"' {
			return nil, fmt.Errorf("expected quoted value for label %q", key)
		}
		pos++
		var val strings.Builder
		closed := false
		for pos < len(inner) {
			c := inner[pos]
			if c == '\\' {
				pos++
				if pos >= len(inner) {
					return nil, errors.New("unterminated escape in label value")
				}
				val.WriteByte(inner[pos])
				pos++
				continue
			}
			if c == '"' {
				pos++
				closed = true
				break
			}
			val.WriteByte(c)
			pos++
		}
		if !closed {
			return nil, errors.New("unclosed string in label value")
		}
		out[key] = val.String()
		for pos < len(inner) && isSpace(inner[pos]) {
			pos++
		}
		if pos < len(inner) {
			if inner[pos] != ',' {
				return nil, fmt.Errorf("expected comma, got %q", inner[pos:])
			}
			pos++
		}
	}
	return out, nil
}

func isSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }

func isLabelFirstByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isLabelContByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
