package storage

import (
	"reflect"
	"testing"
)

func TestSeriesKey_roundTripParse(t *testing.T) {
	cases := []struct {
		metric string
		lab    map[string]string
	}{
		{"m", nil},
		{"http_requests", map[string]string{"status": "500", "method": "GET"}},
		{"a", map[string]string{"x": "y"}},
	}
	for _, c := range cases {
		k := SeriesKey(c.metric, c.lab)
		m, l := ParseSeriesKeyString(k)
		if m != c.metric {
			t.Fatalf("metric %q got %q from %q", c.metric, m, k)
		}
		if len(c.lab) == 0 {
			if l != nil && len(l) > 0 {
				t.Fatalf("want nil/empty labels, got %v", l)
			}
			continue
		}
		if !reflect.DeepEqual(c.lab, l) {
			t.Fatalf("labels want %#v got %#v key=%q", c.lab, l, k)
		}
	}
}

func TestSeriesKeyMatchesFilter(t *testing.T) {
	k := SeriesKey("m", map[string]string{"a": "1", "b": "2"})
	if !SeriesKeyMatchesFilter(k, "m", map[string]string{"a": "1"}) {
		t.Fatal("subset should match")
	}
	if !SeriesKeyMatchesFilter(k, "m", nil) {
		t.Fatal("metric-only filter")
	}
	if SeriesKeyMatchesFilter(k, "m", map[string]string{"a": "2"}) {
		t.Fatal("wrong value")
	}
}
