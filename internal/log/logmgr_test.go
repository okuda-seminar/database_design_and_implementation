package log

import (
	"bytes"
	"testing"

	"database_design_and_implementation/internal/file"
)

// TestLogMgr tests the log manager functionality.
func TestLogMgr(t *testing.T) {
	blockSize := 1024
	fm, err := file.NewFileMgr("../../temp", blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}

	logMgr := NewLogMgr(fm, "logfile-logmgr")

	logData := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	lsns := make([]int, len(logData))
	for i, data := range logData {
		lsns[i] = logMgr.Append(data)
		t.Logf("Appended log record: %s (LSN: %d)", data, lsns[i])
	}

	logMgr.Flush(lsns[len(lsns)-1])
	t.Log("Flushed logs up to latest LSN.")

	iter := logMgr.Iterator()
	t.Log("Created LogIterator.")

	for i := len(logData) - 1; i >= 0; i-- {
		if !iter.HasNext() {
			t.Fatalf("Expected more records, but iterator has no next element at index %d", i)
		}

		t.Logf("Reading log record at position: %d", iter.currentPos)

		rec, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to read log record at position %d: %v", iter.currentPos, err)
		}

		t.Logf("Retrieved record: %s (expected: %s)", rec, logData[i])

		if !bytes.Equal(rec, logData[i]) {
			t.Fatalf("Mismatch: expected %s, but got %s", logData[i], rec)
		}
	}

	t.Log("TestLogMgr completed successfully.")
}
