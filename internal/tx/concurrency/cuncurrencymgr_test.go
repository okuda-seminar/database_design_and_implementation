package concurrency

import (
	"sync"
	"testing"
	"time"

	"database_design_and_implementation/internal/file"
)

// TestSingleMgrSLockThenXLock
func TestSingleMgrSLockThenXLock(t *testing.T) {
	cm := NewConcurrencyMgr()
	blk := file.BlockId{Filename: "testfile", Blknum: 1}

	if err := cm.SLock(blk); err != nil {
		t.Fatalf("SLock failed: %v", err)
	}
	if cm.hasXLock(blk) {
		t.Fatalf("Should not have XLock immediately after SLock")
	}

	if err := cm.XLock(blk); err != nil {
		t.Fatalf("XLock failed: %v", err)
	}
	if !cm.hasXLock(blk) {
		t.Fatalf("Expected XLock but not found")
	}

	cm.Release()
}

// TestTwoMgrConflict
func TestTwoMgrConflict(t *testing.T) {
	cm1 := NewConcurrencyMgr()
	cm2 := NewConcurrencyMgr()
	blk := file.BlockId{Filename: "testfile", Blknum: 2}

	if err := cm1.XLock(blk); err != nil {
		t.Fatalf("cm1 XLock failed: %v", err)
	}

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- cm2.SLock(blk)
	}()

	time.Sleep(500 * time.Millisecond)
	cm1.Release()

	wg.Wait()
	close(errCh)
	err := <-errCh
	if err != nil {
		t.Fatalf("cm2 SLock should eventually succeed after cm1 release, but got error: %v", err)
	}

	cm2.Release()
}

// TestReleaseClearsAll
func TestReleaseClearsAll(t *testing.T) {
	cm := NewConcurrencyMgr()
	blk1 := file.BlockId{Filename: "testfile", Blknum: 10}
	blk2 := file.BlockId{Filename: "testfile", Blknum: 20}

	if err := cm.SLock(blk1); err != nil {
		t.Fatalf("SLock(blk1) failed: %v", err)
	}
	if err := cm.XLock(blk2); err != nil {
		t.Fatalf("XLock(blk2) failed: %v", err)
	}

	cm.Release()

	if cm.hasXLock(blk1) || cm.hasXLock(blk2) {
		t.Fatal("locks map was not cleared after release")
	}
}
