package concurrency

import (
	"sync"
	"testing"
	"time"

	"database_design_and_implementation/internal/file"
)

// Set timeout for lock test operations.
const MaxLockTime = 2 * time.Second

// TestSLockAcquiresLock tests the SLock method.
func TestSLockAllowsMultipleReaders(t *testing.T) {
	lt := NewLockTable(MaxTime)
	blk := file.BlockId{Filename: "testfile", Blknum: 1}

	var wg sync.WaitGroup
	numReaders := 5
	errCh := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := lt.SLock(blk); err != nil {
				errCh <- err
				return
			}
			time.Sleep(100 * time.Millisecond)
			lt.Unlock(blk)
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("SLock should not fail but got error: %v", err)
		}
	}
}

// TestXLockAcquiresLock tests the XLock method.
func TestXLockBlocksReaders(t *testing.T) {
	lt := NewLockTable(MaxTime)
	blk := file.BlockId{Filename: "testfile", Blknum: 2}

	if err := lt.XLock(blk); err != nil {
		t.Fatalf("failed to get XLock: %v", err)
	}

	var wg sync.WaitGroup
	numReaders := 3
	errCh := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errCh <- lt.SLock(blk)
		}(i)
	}

	time.Sleep(500 * time.Millisecond)
	lt.Unlock(blk)

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("SLock failed even after XLock released: %v", err)
		}
	}
}

// TestSLockBlocksXLock tests that SLock blocks XLock.
func TestSLockBlocksXLock(t *testing.T) {
	lt := NewLockTable(MaxTime)
	blk := file.BlockId{Filename: "testfile", Blknum: 3}

	numReaders := 2
	for i := 0; i < numReaders; i++ {
		if err := lt.SLock(blk); err != nil {
			t.Fatalf("failed to get SLock: %v", err)
		}
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- lt.XLock(blk)
	}()

	time.Sleep(300 * time.Millisecond)
	for i := 0; i < numReaders; i++ {
		lt.Unlock(blk)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("XLock should succeed after SLocks are released, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("XLock did not succeed in time")
	}
}

// TestLockTimeout tests that locks timeout after a certain duration.
func TestLockTimeout(t *testing.T) {
	lt := NewLockTable(MaxLockTime)
	blk := file.BlockId{Filename: "testfile", Blknum: 4}

	if err := lt.XLock(blk); err != nil {
		t.Fatalf("failed to get XLock: %v", err)
	}

	errCh := make(chan error, 1)

	go func() {
		errCh <- lt.SLock(blk)
	}()

	time.Sleep(3 * time.Second)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected timeout error but got nil")
		} else if err != ErrLockAbort {
			t.Fatalf("expected ErrLockAbort but got: %v", err)
		}
	default:
		t.Fatal("expected an error but none received")
	}

	lt.Unlock(blk)
}
