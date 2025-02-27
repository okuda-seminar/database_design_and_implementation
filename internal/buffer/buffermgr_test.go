package buffer

import (
	"fmt"
	"testing"
	"time"

	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
)

// setupBufferMgrTest sets up a Buffer Manager test environment with a specified number of buffers.
func setupBufferMgrTest(numBuffers int) (*BufferMgr, *file.FileMgr, *log.LogMgr, error) {
	blockSize := 1024
	fm, err := file.NewFileMgr("../../temp", blockSize)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create FileMgr: %w", err)
	}

	lm := log.NewLogMgr(fm, "logfile-buffermgr")
	bm := NewBufferMgr(fm, lm, numBuffers)

	return bm, fm, lm, nil
}

// TestBufferMgr tests the buffer manager functionality.
func TestBufferMgr(t *testing.T) {
	t.Run("Initialize Buffer Manager", func(t *testing.T) {
		bm, _, _, err := setupBufferMgrTest(3)
		if err != nil {
			t.Fatalf("Failed to set up buffer manager: %v", err)
		}

		if bm.Available() != 3 {
			t.Fatalf("Expected 3 available buffers, got %d", bm.Available())
		}
	})

	t.Run("Pin and Unpin Buffers", func(t *testing.T) {
		bm, _, _, err := setupBufferMgrTest(3)
		if err != nil {
			t.Fatalf("Failed to set up buffer manager: %v", err)
		}

		blk1 := file.NewBlockId("logfile-buffermgr", 1)
		blk2 := file.NewBlockId("logfile-buffermgr", 2)

		buff1, err := bm.Pin(&blk1)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		buff2, err := bm.Pin(&blk2)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		if bm.Available() != 1 {
			t.Fatalf("Expected 1 available buffer, got %d", bm.Available())
		}

		bm.Unpin(buff1)
		if bm.Available() != 2 {
			t.Fatalf("Expected 2 available buffers after unpin, got %d", bm.Available())
		}
		bm.Unpin(buff2)
		if bm.Available() != 3 {
			t.Fatalf("Expected 3 available buffers after unpin, got %d", bm.Available())
		}
	})

	t.Run("Buffer Pin Timeout", func(t *testing.T) {
		bm, _, _, err := setupBufferMgrTest(1)
		if err != nil {
			t.Fatalf("Failed to set up buffer manager: %v", err)
		}

		blk1 := file.NewBlockId("logfile-buffermgr", 1)
		blk2 := file.NewBlockId("logfile-buffermgr", 2)

		_, err = bm.Pin(&blk1)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		startTime := time.Now()
		_, err = bm.Pin(&blk2)
		elapsedTime := time.Since(startTime)

		wantErr := "buffer allocation timeout"
		if err == nil {
			t.Fatalf("Expected error: %v, but got none", wantErr)
		}

		if err.Error() != wantErr {
			t.Fatalf("Expected error: %v, but got: %v", wantErr, err)
		}

		if elapsedTime < maxWaitTime {
			t.Fatalf("Expected pin timeout around %v, but it returned early in %v", maxWaitTime, elapsedTime)
		}
	})

	t.Run("Flush Buffers", func(t *testing.T) {
		bm, _, _, err := setupBufferMgrTest(2)
		if err != nil {
			t.Fatalf("Failed to set up buffer manager: %v", err)
		}

		blk1 := file.NewBlockId("logfile-buffermgr", 1)
		blk2 := file.NewBlockId("logfile-buffermgr", 2)

		buff1, err := bm.Pin(&blk1)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}
		buff2, err := bm.Pin(&blk2)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		buff1.SetModified(1, 100)
		buff2.SetModified(1, 200)

		bm.FlushAll(1)

		if buff1.ModifyingTx() != -1 || buff2.ModifyingTx() != -1 {
			t.Fatalf("Expected buffers to be flushed, but modifications remain")
		}
	})

}
