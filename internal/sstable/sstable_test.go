package sstable

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestWriteFromMemtable_queryRoundTrip(t *testing.T) {
	m := memtable.New(0)
	m.Put(storage.Sample{Metric: "a", Timestamp: 10, Value: 1})
	m.Put(storage.Sample{Metric: "a", Timestamp: 20, Value: 2})
	p := filepath.Join(t.TempDir(), "1.sst")
	if err := WriteFromMemtable(p, m); err != nil {
		t.Fatal(err)
	}
	tab, err := OpenTable(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tab.Close() }()
	sk := storage.SeriesKey("a", nil)
	samples := tab.QueryRange(sk, 0, 30)
	if len(samples) != 2 || samples[0].Value != 1 || samples[1].Value != 2 {
		t.Fatalf("got %+v", samples)
	}
}

func TestBuildPostingsRoundTrip(t *testing.T) {
	m := memtable.New(0)
	m.Put(storage.Sample{Metric: "m", Labels: map[string]string{"env": "a"}, Timestamp: 1, Value: 1})
	m.Put(storage.Sample{Metric: "m", Labels: map[string]string{"env": "b"}, Timestamp: 2, Value: 2})
	groups, _, _, err := buildGroups(m)
	if err != nil {
		t.Fatal(err)
	}
	pm, _ := buildPostingsAndLabelValues(groups)
	var buf bytes.Buffer
	if err := writeV2Postings(&buf, pm); err != nil {
		t.Fatal(err)
	}
	if _, err := readV2Postings(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
}

func TestPostingsAndLabelV2CodecRoundTrip(t *testing.T) {
	pm := map[string][]uint64{
		"__name__=m": {0, 1},
		"env=a":      {0},
		"env=b":      {1},
	}
	var buf bytes.Buffer
	if err := writeV2Postings(&buf, pm); err != nil {
		t.Fatal(err)
	}
	got, err := readV2Postings(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d keys", len(got))
	}
	lv := map[string][]string{
		"__name__": {"m"},
		"env":      {"a", "b"},
	}
	var lbuf bytes.Buffer
	if err := writeV2LabelValues(&lbuf, lv); err != nil {
		t.Fatal(err)
	}
	glv, err := readV2LabelValues(bytes.NewReader(lbuf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(glv) != 2 {
		t.Fatalf("label name count %d", len(glv))
	}
}

func TestV2_postingsAndLabelValues(t *testing.T) {
	m := memtable.New(0)
	m.Put(storage.Sample{Metric: "m", Labels: map[string]string{"env": "a"}, Timestamp: 1, Value: 1})
	m.Put(storage.Sample{Metric: "m", Labels: map[string]string{"env": "b"}, Timestamp: 2, Value: 2})
	p := filepath.Join(t.TempDir(), "v2.sst")
	if err := WriteFromMemtable(p, m); err != nil {
		t.Fatal(err)
	}
	tab, err := OpenTable(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tab.Close() }()
	if tab.Version() != 2 {
		t.Fatalf("version %d", tab.Version())
	}
	if got := tab.PostingList("env=a"); len(got) != 1 || got[0] != 0 {
		t.Fatalf("env=a: %v", got)
	}
	if got := tab.PostingList("env=b"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("env=b: %v", got)
	}
	ids := tab.PostingList("__name__=m")
	if len(ids) != 2 {
		t.Fatalf("__name__=m: %v", ids)
	}
	if ids[0] != 0 || ids[1] != 1 {
		t.Fatalf("__name__ order: %v", ids)
	}
	lv := tab.LabelValues("env")
	if len(lv) != 2 || lv[0] != "a" || lv[1] != "b" {
		t.Fatalf("label values: %v", lv)
	}
}

func TestOpenTable_v1NoTrailer(t *testing.T) {
	m := memtable.New(0)
	m.Put(storage.Sample{Metric: "x", Timestamp: 5, Value: 3.14})
	p := filepath.Join(t.TempDir(), "v1.sst")
	writeV1SSTableForTest(t, p, m)
	tab, err := OpenTable(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tab.Close() }()
	if tab.Version() != 1 {
		t.Fatalf("version %d", tab.Version())
	}
	if tab.PostingList("x=y") != nil {
		t.Fatal("v1 should have no postings")
	}
	sk := storage.SeriesKey("x", nil)
	if got := tab.QueryRange(sk, 0, 10); len(got) != 1 || got[0].Value != 3.14 {
		t.Fatalf("got %+v", got)
	}
}

// writeV1SSTableForTest writes the legacy on-disk format (no postings / trailer) for read compatibility tests.
func TestCompactTo_mergesTables(t *testing.T) {
	dir := t.TempDir()
	m1 := memtable.New(0)
	m1.Put(storage.Sample{Metric: "a", Timestamp: 10, Value: 1})
	p1 := filepath.Join(dir, "1.sst")
	if err := WriteFromMemtable(p1, m1); err != nil {
		t.Fatal(err)
	}
	m2 := memtable.New(0)
	m2.Put(storage.Sample{Metric: "a", Timestamp: 10, Value: 2})
	m2.Put(storage.Sample{Metric: "b", Timestamp: 20, Value: 3})
	p2 := filepath.Join(dir, "2.sst")
	if err := WriteFromMemtable(p2, m2); err != nil {
		t.Fatal(err)
	}
	t1, err := OpenTable(p1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = t1.Close() }()
	t2, err := OpenTable(p2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = t2.Close() }()
	pOut := filepath.Join(dir, "out.sst")
	if err := CompactTo(pOut, []*Table{t1, t2}); err != nil {
		t.Fatal(err)
	}
	out, err := OpenTable(pOut)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	skA := storage.SeriesKey("a", nil)
	qa := out.QueryRange(skA, 0, 30)
	if len(qa) != 1 || qa[0].Value != 2 {
		t.Fatalf("duplicate ts: should take later table, got %+v", qa)
	}
	skB := storage.SeriesKey("b", nil)
	if qb := out.QueryRange(skB, 0, 30); len(qb) != 1 || qb[0].Value != 3 {
		t.Fatalf("series b: %+v", qb)
	}
}

func writeV1SSTableForTest(t *testing.T, path string, m *memtable.Memtable) {
	t.Helper()
	groups, minT, maxT, err := buildGroups(m)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(fileMagicV1); err != nil {
		t.Fatal(err)
	}
	if err := writeI64(f, minT); err != nil {
		t.Fatal(err)
	}
	if err := writeI64(f, maxT); err != nil {
		t.Fatal(err)
	}
	if err := writeU32(f, uint32(len(groups))); err != nil {
		t.Fatal(err)
	}
	for _, g := range groups {
		if err := writeSeriesBlock(f, g); err != nil {
			t.Fatal(err)
		}
	}
}
