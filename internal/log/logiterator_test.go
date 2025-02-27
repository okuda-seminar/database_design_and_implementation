package log

import (
	"bytes"
	"testing"

	"database_design_and_implementation/internal/file"
)

// TestLogIterator tests the log iterator functionality.
func TestLogIterator(t *testing.T) {
	blockSize := 1024
	fm, err := file.NewFileMgr("../../temp", blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}

	blk := file.NewBlockId("logfile", 1)
	page := file.NewPage(blockSize)

	logData := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	boundary := blockSize
	for i := len(logData) - 1; i >= 0; i-- {
		boundary -= file.IntSize + len(logData[i])
		writeLogRecord(page, boundary, logData[i])
	}

	err = page.SetInt(0, int32(boundary))
	if err != nil {
		t.Fatalf("Failed to write boundary to page: %v", err)
	}

	retrievedBoundary, err := page.GetInt(0)
	if err != nil || int(retrievedBoundary) != boundary {
		t.Fatalf("Boundary mismatch: expected %d, got %d", boundary, retrievedBoundary)
	}

	t.Logf("Final boundary position: %d", boundary)
	t.Logf("Page Contents: %v", page.Contents())

	bytesData, err := page.GetBytes(boundary)
	if err != nil {
		t.Fatalf("Failed to retrieve written bytes: %v", err)
	}
	t.Logf("Retrieved log record: %s", string(bytesData))

	fm.Write(blk, page.Contents())

	iter := NewLogIterator(fm, &blk)
	t.Log("Created LogIterator.")

	for i := 0; i < len(logData); i++ {
		t.Logf("Before HasNext: currentPos = %d", iter.currentPos)
		if !iter.HasNext() {
			t.Fatalf("Expected more records, but iterator has no next element at index %d", i)
		}

		t.Logf("Reading log record at position: %d", iter.currentPos)

		rec, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to read log record: %v", err)
		}

		t.Logf("Retrieved record: %s (expected: %s)", rec, logData[i])

		if !bytes.Equal(rec, logData[i]) {
			t.Fatalf("Mismatch: expected %s, but got %s", logData[i], rec)
		}
		t.Logf("After Next: currentPos = %d", iter.currentPos)
	}

	t.Logf("Final HasNext check: currentPos = %d", iter.currentPos)
	if iter.HasNext() {
		t.Fatalf("Expected no more records, but iterator has next element")
	}

	t.Log("TestLogIterator completed successfully.")
}

// writeLogRecord writes the given data to the specified position in the page.
func writeLogRecord(p *file.Page, pos int, data []byte) {
	err := p.SetBytes(pos, data)
	if err != nil {
		panic("Failed to write bytes to page")
	}
}
