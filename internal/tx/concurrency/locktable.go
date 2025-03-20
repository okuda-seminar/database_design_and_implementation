package concurrency

import (
	"errors"
	"sync"
	"time"

	"database_design_and_implementation/internal/file"
)

var ErrLockAbort = errors.New("lock aborted due to timeout")

const MaxTime = 10 * time.Second

type LockTable struct {
	locks   map[file.BlockId]int
	lockMu  sync.Mutex
	maxTime time.Duration
}

// NewLockTable creates a new LockTable instance.
func NewLockTable(maxTime time.Duration) *LockTable {
	return &LockTable{
		locks:   make(map[file.BlockId]int),
		maxTime: maxTime,
	}
}

// SLock acquires a shared lock on the given block with busy-wait + timeout.
func (lt *LockTable) SLock(blk file.BlockId) error {
	start := time.Now()
	for {
		lt.lockMu.Lock()
		if !lt.hasXLockLocked(blk) {
			lt.locks[blk]++
			lt.lockMu.Unlock()
			return nil
		}
		lt.lockMu.Unlock()

		if time.Since(start) >= lt.maxTime {
			return ErrLockAbort
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// XLock acquires an exclusive lock on the given block with busy-wait + timeout.
func (lt *LockTable) XLock(blk file.BlockId) error {
	start := time.Now()
	for {
		lt.lockMu.Lock()
		if !lt.hasOtherSLocksLocked(blk) {
			lt.locks[blk] = -1
			lt.lockMu.Unlock()
			return nil
		}
		lt.lockMu.Unlock()

		if time.Since(start) >= lt.maxTime {
			return ErrLockAbort
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Unlock releases the lock on the given block.
func (lt *LockTable) Unlock(blk file.BlockId) {
	lt.lockMu.Lock()
	defer lt.lockMu.Unlock()

	val, ok := lt.locks[blk]
	if !ok {
		return
	}

	if val > 1 {
		lt.locks[blk]--
	} else {
		delete(lt.locks, blk)
	}
}

// hasXLockLocked: call this when lockMu is already held
func (lt *LockTable) hasXLockLocked(blk file.BlockId) bool {
	val, exists := lt.locks[blk]
	return exists && val < 0
}

// hasOtherSLocksLocked: call this when lockMu is already held
func (lt *LockTable) hasOtherSLocksLocked(blk file.BlockId) bool {
	val, exists := lt.locks[blk]
	return exists && val > 1
}
