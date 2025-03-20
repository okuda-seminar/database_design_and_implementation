package recovery

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"database_design_and_implementation/internal/file"
)

// TestStartRecord tests the StartRecord functionality
func TestStartRecord(t *testing.T) {
	p := file.NewPage(64)
	p.SetInt(binary.Size(int32(0)), 100)

	s := NewStartRecord(p)

	assert.NotNil(t, s, "StartRecord should not be nil")
	assert.Equal(t, START, s.Op(), "Op should return START")
	assert.Equal(t, 100, s.TxNumber(), "TxNumber should return 100")
	assert.Equal(t, "<START 100>", s.String(), "String should return '<START 100>'")

	tx := new(MockTransaction)
	err := s.Undo(tx)
	assert.Nil(t, err, "Undo should do nothing and return nil")
}

// TestWriteStartToLog tests the WriteStartToLog function
func TestWriteStartToLog(t *testing.T) {
	mockLogMgr := &MockLogMgr{}
	lsn, err := WriteStartToLog(mockLogMgr, 200)

	assert.Nil(t, err, "WriteStartToLog should not return an error")
	assert.Equal(t, 1, lsn, "LSN should be 1 since mock increments by 1")

	// Debugging: Print the raw log record
	t.Logf("Log record (raw): %v", mockLogMgr.lastRecord)
	t.Logf("Log record length: %d", len(mockLogMgr.lastRecord))

	// Ensure lastRecord is not empty and has correct length
	assert.NotEmpty(t, mockLogMgr.lastRecord, "Last record should not be empty")
	assert.GreaterOrEqual(t, len(mockLogMgr.lastRecord), 8, "Log record should have at least 8 bytes")

	// Extract values correctly
	opCode := int(binary.BigEndian.Uint32(mockLogMgr.lastRecord[:4]))
	txID := int(binary.BigEndian.Uint32(mockLogMgr.lastRecord[4:]))

	assert.Equal(t, START, opCode, "Last log record should be START")
	assert.Equal(t, 200, txID, "Transaction ID should be 200")
}
