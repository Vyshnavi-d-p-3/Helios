package cluster

import (
	"time"

	"github.com/hashicorp/raft"
)

const (
	defaultBatchSize = 256
	defaultBatchWait = 2 * time.Millisecond
)

type batchWriter struct {
	r       *raft.Raft
	timeout time.Duration
	size    int
	wait    time.Duration
	ch      chan writeReq
}

type writeReq struct {
	payload []byte
	resp    chan error
}

func newBatchWriter(r *raft.Raft, timeout time.Duration, size int, wait time.Duration) *batchWriter {
	if size <= 0 {
		size = defaultBatchSize
	}
	if wait <= 0 {
		wait = defaultBatchWait
	}
	bw := &batchWriter{
		r:       r,
		timeout: timeout,
		size:    size,
		wait:    wait,
		ch:      make(chan writeReq, 1024),
	}
	go bw.loop()
	return bw
}

func (bw *batchWriter) Submit(payload []byte) error {
	resp := make(chan error, 1)
	bw.ch <- writeReq{payload: payload, resp: resp}
	return <-resp
}

// v1 strategy: each submitted payload is already a batch; apply it directly.
func (bw *batchWriter) loop() {
	for req := range bw.ch {
		err := bw.r.Apply(req.payload, bw.timeout).Error()
		req.resp <- err
	}
}
