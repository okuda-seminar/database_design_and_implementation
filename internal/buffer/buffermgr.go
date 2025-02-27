package buffer

import (
	"errors"
	"sync"
	"time"

	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
)

const maxWaitTime = 5 * time.Millisecond

// BufferMgr manages the pinning and unpinning of buffers to blocks.
type BufferMgr struct {
	bufferPool   []*Buffer
	numAvailable int
	mutex        sync.Mutex
}

// NewBufferMgr creates a new buffer manager with the specified number of buffers.
func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numBuffers int) *BufferMgr {
	bufferPool := make([]*Buffer, numBuffers)
	for i := 0; i < numBuffers; i++ {
		bufferPool[i] = NewBuffer(fm, lm)
	}

	return &BufferMgr{
		bufferPool:   bufferPool,
		numAvailable: numBuffers,
	}
}

// Available returns the number of available (unpinned) buffers.
func (bm *BufferMgr) Available() int {
	return bm.numAvailable
}

// FlushAll flushes the dirty buffers modified by the specified transaction.
func (bm *BufferMgr) FlushAll(txNum int) {
	bm.mutex.Lock()
	for _, buff := range bm.bufferPool {
		if buff.ModifyingTx() == txNum {
			buff.Flush()
		}
	}
	bm.mutex.Unlock()
}

// Unpin unpins the specified buffer. If its pin count goes to zero, it notifies waiting threads.
func (bm *BufferMgr) Unpin(buff *Buffer) {
	bm.mutex.Lock()
	buff.Unpin()
	bm.mutex.Unlock()
	if !buff.IsPinned() {
		bm.numAvailable = min(len(bm.bufferPool), bm.numAvailable+1)
	}
}

// Pin pins a buffer to the specified block, waiting until one becomes available if necessary.
// If no buffer becomes available within a fixed time period, an error is returned.
func (bm *BufferMgr) Pin(blk *file.BlockId) (*Buffer, error) {
	startTime := time.Now()

	for {
		bm.mutex.Lock()
		buff := bm.tryToPin(blk)
		bm.mutex.Unlock()

		if buff != nil {
			return buff, nil
		}

		if time.Since(startTime) >= maxWaitTime {
			return nil, errors.New("buffer allocation timeout")
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// tryToPin tries to pin a buffer to the specified block.
func (bm *BufferMgr) tryToPin(blk *file.BlockId) *Buffer {
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		buff.AssignToBlock(blk)
	}

	if !buff.IsPinned() {
		bm.numAvailable = max(0, bm.numAvailable-1)
	}
	buff.Pin()
	return buff
}

// findExistingBuffer searches for a buffer assigned to the given block.
func (bm *BufferMgr) findExistingBuffer(blk *file.BlockId) *Buffer {
	for _, buff := range bm.bufferPool {
		if buff.Block() != nil && *buff.Block() == *blk {
			return buff
		}
	}
	return nil
}

// chooseUnpinnedBuffer selects an available unpinned buffer.
func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bm.bufferPool {
		if !buff.IsPinned() {
			return buff
		}
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
