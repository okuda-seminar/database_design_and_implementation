package concurrency

import (
	"sync"

	"database_design_and_implementation/internal/file"
)

// ConcurrencyMgr manages the locks for a single transaction (or context).
var locktbl = NewLockTable(MaxTime)

// ConcurrencyMgr manages the locks for a single transaction (or context).
type ConcurrencyMgr struct {
	locks map[file.BlockId]string
	mu    sync.Mutex
}

// NewConcurrencyMgr returns a new instance of ConcurrencyMgr.
func NewConcurrencyMgr() *ConcurrencyMgr {
	return &ConcurrencyMgr{
		locks: make(map[file.BlockId]string),
	}
}

// SLock acquires a shared lock on the given block.
func (cm *ConcurrencyMgr) SLock(blk file.BlockId) error {

	// Check if we already have an XLock on the block.
	if cm.hasXLock(blk) {
		return nil
	}

	cm.mu.Lock()
	_, alreadyLocked := cm.locks[blk]
	cm.mu.Unlock()

	if alreadyLocked {
		return nil
	}

	if err := locktbl.SLock(blk); err != nil {
		return err
	}

	cm.mu.Lock()
	cm.locks[blk] = "S"
	cm.mu.Unlock()

	return nil
}

// XLock acquires an exclusive lock on the given block.
func (cm *ConcurrencyMgr) XLock(blk file.BlockId) error {
	cm.mu.Lock()
	lockType, exists := cm.locks[blk]
	cm.mu.Unlock()

	if exists && lockType == "X" {
		return nil
	}

	if err := cm.SLock(blk); err != nil {
		return err
	}

	if err := locktbl.XLock(blk); err != nil {
		locktbl.Unlock(blk)
		// delete the lock from the locks map
		cm.Unlock(blk)
		return err
	}

	cm.mu.Lock()
	cm.locks[blk] = "X"
	cm.mu.Unlock()

	return nil
}

// Release releases all locks that this ConcurrencyMgr holds.
func (cm *ConcurrencyMgr) Release() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for blk := range cm.locks {
		locktbl.Unlock(blk)
	}
	cm.locks = make(map[file.BlockId]string)
}

// hasXLock is a helper method to check if we hold an XLock on the block.
func (cm *ConcurrencyMgr) hasXLock(blk file.BlockId) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	lockType, ok := cm.locks[blk]
	return ok && lockType == "X"
}

// Unlock releases the lock on the given block.
func (cm *ConcurrencyMgr) Unlock(blk file.BlockId) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	val, exists := cm.locks[blk]
	if !exists {
		return
	}

	// If it is a shared lock with multiple holders, decrement the count
	if val == "S" {
		delete(cm.locks, blk)
	} else if val == "X" {
		delete(cm.locks, blk)
	}
}
