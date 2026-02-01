package selector

import (
	"reflect"
	"testing"
)

func TestParseSeriesSelector_bare(t *testing.T) {
	m, lab, err := ParseSeriesSelector("  http_requests  ")
	if err != nil {
		t.Fatal(err)
	}
	if m != "http_requests" || lab != nil {
		t.Fatalf("got %q %#v", m, lab)
	}
}

func TestParseSeriesSelector_labeled(t *testing.T) {
	m, lab, err := ParseSeriesSelector(`m{ status = "200" , j = "ab" }`)
	if err != nil {
		t.Fatal(err)
	}
	if m != "m" {
		t.Fatalf("metric %q", m)
	}
	w := map[string]string{"status": "200", "j": "ab"}
	if !reflect.DeepEqual(lab, w) {
		t.Fatalf("got %#v", lab)
	}
}

func TestParseSeriesSelector_err(t *testing.T) {
	if _, _, err := ParseSeriesSelector(""); err == nil {
		t.Fatal("expected err")
	}
	if _, _, err := ParseSeriesSelector(`m{1bad="1"}`); err == nil {
		t.Fatal("expected err")
	}
}
