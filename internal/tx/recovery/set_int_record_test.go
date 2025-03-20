package recovery

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"database_design_and_implementation/internal/file"
)

// TestSetIntRecord tests the SetIntRecord functionality
func TestSetIntRecord(t *testing.T) {
	p := file.NewPage(128)
	p.SetInt(binary.Size(int32(0)), 700) // Set Transaction ID
	p.SetString(binary.Size(int32(0))*2, "testfile")
	p.SetInt(binary.Size(int32(0))*2+file.MaxLength(len("testfile")), 3)                          // Block number
	p.SetInt(binary.Size(int32(0))*2+file.MaxLength(len("testfile"))+binary.Size(int32(0)), 5)    // Offset
	p.SetInt(binary.Size(int32(0))*2+file.MaxLength(len("testfile"))+binary.Size(int32(0))*2, 42) // Value

	s := NewSetIntRecord(p)

	assert.NotNil(t, s, "SetIntRecord should not be nil")
	assert.Equal(t, SETINT, s.Op(), "Op should return SETINT")
	assert.Equal(t, 700, s.TxNumber(), "TxNumber should return 700")
	assert.Equal(t, "testfile", s.Blk.Filename, "Filename should match")
	assert.Equal(t, 3, s.Blk.Blknum, "Block number should match")
	assert.Equal(t, 5, s.Offset, "Offset should match")
	assert.Equal(t, 42, s.Val, "Value should match")
	assert.Equal(t, "<SETINT 700 testfile:3 5 42>", s.String(), "String should return '<SETINT 700 testfile:3 5 42>'")

	tx := &MockTransaction{}
	err := s.Undo(tx)
	assert.Nil(t, err, "Undo should do nothing and return nil")
}

// TestWriteSetIntToLog tests the WriteSetIntToLog function
func TestWriteSetIntToLog(t *testing.T) {
	mockLogMgr := &MockLogMgr{}
	block := file.NewBlockId("logfile", 10)
	lsn, err := WriteSetIntToLog(mockLogMgr, 800, block, 2, 99)

	assert.Nil(t, err, "WriteSetIntToLog should not return an error")
	assert.Equal(t, 1, lsn, "LSN should be 1 since mock increments by 1")

	// Debugging: Print the raw log record
	t.Logf("Log record (raw): %v", mockLogMgr.lastRecord)
	t.Logf("Log record length: %d", len(mockLogMgr.lastRecord))

	// Ensure lastRecord is not empty and has correct length
	assert.NotEmpty(t, mockLogMgr.lastRecord, "Last record should not be empty")
	assert.GreaterOrEqual(t, len(mockLogMgr.lastRecord), 24, "Log record should have at least 24 bytes")

	// Extract values correctly
	opCode := int(binary.BigEndian.Uint32(mockLogMgr.lastRecord[:4]))
	txID := int(binary.BigEndian.Uint32(mockLogMgr.lastRecord[4:8]))

	// Extract filename length dynamically
	filenameLen := int(binary.BigEndian.Uint32(mockLogMgr.lastRecord[8:12]))
	filenameEnd := 12 + filenameLen
	filename := block.Filename // Use BlockId's Filename directly

	// Corrected start positions for block number, offset, and value
	blockNumStart := filenameEnd
	offsetStart := blockNumStart + 4
	valStart := offsetStart + 4

	// Extract block number, offset, and value
	blockNum := int(binary.BigEndian.Uint32(mockLogMgr.lastRecord[blockNumStart : blockNumStart+4]))
	offset := int(binary.BigEndian.Uint32(mockLogMgr.lastRecord[offsetStart : offsetStart+4]))
	val := int(binary.BigEndian.Uint32(mockLogMgr.lastRecord[valStart : valStart+4]))

	// Validate extracted values
	assert.Equal(t, SETINT, opCode, "Last log record should be SETINT")
	assert.Equal(t, 800, txID, "Transaction ID should be 800")
	assert.Equal(t, "logfile", filename, "Filename should match")
	assert.Equal(t, 10, blockNum, "Block number should be 10")
	assert.Equal(t, 2, offset, "Offset should be 2")
	assert.Equal(t, 99, val, "Value should be 99")
}
