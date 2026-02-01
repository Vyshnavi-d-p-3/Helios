package memtable

import (
	"sort"
	"strings"
)

// splitPostingKey splits a postings map key of the form "name=value" on the
// first '='. Label names are assumed not to contain '='.
func splitPostingKey(s string) (name, value string) {
	i := strings.IndexByte(s, '=')
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i+1:]
}

// LabelNames returns sorted distinct label names present in the in-memory
// posting index (from __name__=… and k=v keys).
func (m *Memtable) LabelNames() []string {
	if m == nil {
		return nil
	}
	seen := make(map[string]struct{})
	for k := range m.posting {
		n, _ := splitPostingKey(k)
		if n != "" {
			seen[n] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// LabelValuesForName returns sorted distinct values for a label name in the
// memtable posting index.
func (m *Memtable) LabelValuesForName(name string) []string {
	if m == nil || name == "" {
		return nil
	}
	seen := make(map[string]struct{})
	for k := range m.posting {
		n, v := splitPostingKey(k)
		if n == name && v != "" {
			seen[v] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
