package cluster

// Snapshot plumbing is implemented in engine snapshot methods that the FSM
// delegates to:
//   - (*engine.Engine).NewSnapshot()
//   - (*engine.Engine).RestoreFromSnapshot(io.ReadCloser)
//
// This file intentionally holds the cluster package marker for phase-13
// scaffolding so snapshot logic can evolve without crowding node/fsm code.
