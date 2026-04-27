package cluster

import (
	"io"

	"github.com/hashicorp/raft"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
)

// HeliosFSM applies replicated sample batches into the engine.
type HeliosFSM struct {
	eng *engine.Engine
}

func NewFSM(eng *engine.Engine) *HeliosFSM {
	return &HeliosFSM{eng: eng}
}

func (f *HeliosFSM) Apply(log *raft.Log) interface{} {
	batch, err := decodeBatch(log.Data)
	if err != nil {
		return fsmError{err: err}
	}
	if err := f.eng.WriteDirect(batch); err != nil {
		return fsmError{err: err}
	}
	return fsmResult{count: len(batch)}
}

// ApplyBatch gives Raft an optimized path for batched log apply.
func (f *HeliosFSM) ApplyBatch(logs []*raft.Log) []interface{} {
	out := make([]interface{}, len(logs))
	for i, l := range logs {
		out[i] = f.Apply(l)
	}
	return out
}

func (f *HeliosFSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.eng.NewSnapshot()
}

func (f *HeliosFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.eng.RestoreFromSnapshot(rc)
}

type fsmResult struct{ count int }
type fsmError struct{ err error }

func (e fsmError) Error() string { return e.err.Error() }
