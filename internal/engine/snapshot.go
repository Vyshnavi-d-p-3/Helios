package engine

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

// NewSnapshot returns an FSM snapshot for Raft.
// Phase-13 scaffold: full hardlink snapshot implementation follows in next step.
func (e *Engine) NewSnapshot() (raft.FSMSnapshot, error) {
	return nil, fmt.Errorf("engine snapshot not implemented yet")
}

// RestoreFromSnapshot restores engine state from a Raft snapshot stream.
// Phase-13 scaffold: full restore implementation follows in next step.
func (e *Engine) RestoreFromSnapshot(rc io.ReadCloser) error {
	_ = rc.Close()
	return fmt.Errorf("engine snapshot restore not implemented yet")
}
