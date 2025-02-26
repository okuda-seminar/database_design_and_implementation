package buffer

import (
	"testing"

	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
)

// setupTestBuffer sets up a buffer for testing.
func setupTestBuffer(t *testing.T) (*Buffer, *file.FileMgr, *file.BlockId, []byte) {
	blockSize := 1024
	fm, err := file.NewFileMgr("../../temp", blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}

	lm := log.NewLogMgr(fm, "logfile-buffer")
	buffer := NewBuffer(fm, lm)
	blk := file.NewBlockId("logfile-buffer", 1)

	testData := []byte("test_record")

	return buffer, fm, &blk, testData
}

// TestBuffer tests the buffer functionality.
func TestBuffer(t *testing.T) {
	t.Run("Test Block Assignment", func(t *testing.T) {
		buffer, _, blk, _ := setupTestBuffer(t)

		buffer.AssignToBlock(blk)
		if buffer.Block() == nil || *buffer.Block() != *blk {
			t.Fatalf("Buffer block assignment failed. Expected: %+v, Got: %+v", *blk, buffer.Block())
		}
	})

	t.Run("Test Data Write and Flush", func(t *testing.T) {
		buffer, fm, blk, testData := setupTestBuffer(t)

		buffer.AssignToBlock(blk)

		err := buffer.Contents().SetBytes(100, testData)
		if err != nil {
			t.Fatalf("Failed to write data to page: %v", err)
		}
		buffer.SetModified(1, 100)

		buffer.Flush()

		page := file.NewPage(fm.BlockSize())
		err = fm.Read(*blk, page.Contents())
		if err != nil {
			t.Fatalf("Failed to read from disk: %v", err)
		}

		readData, err := page.GetBytes(100)
		if err != nil {
			t.Fatalf("Failed to read data from page: %v", err)
		}

		if string(readData) != string(testData) {
			t.Fatalf("Data mismatch after flush. Expected: %s, Got: %s", string(testData), string(readData))
		}
	})

	t.Run("Test Pin and Unpin", func(t *testing.T) {
		buffer, _, _, _ := setupTestBuffer(t)

		buffer.Pin()
		if !buffer.IsPinned() {
			t.Fatalf("Expected buffer to be pinned, but it is not.")
		}

		buffer.Unpin()
		if buffer.IsPinned() {
			t.Fatalf("Expected buffer to be unpinned, but it is still pinned.")
		}
	})

	t.Run("Test Modifying Transaction", func(t *testing.T) {
		buffer, _, _, _ := setupTestBuffer(t)

		buffer.SetModified(5, 200)
		if buffer.ModifyingTx() != 5 {
			t.Fatalf("Expected modifying transaction ID to be 5, but got %d", buffer.ModifyingTx())
		}
	})

	t.Log("TestBuffer passed successfully.")
}
