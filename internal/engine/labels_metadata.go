package engine

import "sort"

// AllLabelNames returns sorted distinct label names from the memtable posting
// index and all v2 SSTable label-value sections (v1 SSTs contribute nothing).
func (e *Engine) AllLabelNames() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	seen := make(map[string]struct{})
	for _, n := range e.mem.LabelNames() {
		seen[n] = struct{}{}
	}
	for _, fm := range e.frozen {
		for _, n := range fm.LabelNames() {
			seen[n] = struct{}{}
		}
	}
	for _, t := range e.sst {
		for _, n := range t.AllLabelNames() {
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

// AllLabelValues returns sorted distinct values for a label (e.g. __name__ or
// instance) merged from the memtable and v2 SSTs.
func (e *Engine) AllLabelValues(name string) []string {
	if name == "" {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	seen := make(map[string]struct{})
	for _, v := range e.mem.LabelValuesForName(name) {
		seen[v] = struct{}{}
	}
	for _, fm := range e.frozen {
		for _, v := range fm.LabelValuesForName(name) {
			seen[v] = struct{}{}
		}
	}
	for _, t := range e.sst {
		vs := t.LabelValues(name)
		for i := range vs {
			seen[vs[i]] = struct{}{}
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
