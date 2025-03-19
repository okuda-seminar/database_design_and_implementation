package recovery

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"database_design_and_implementation/internal/file"
)

// TestCommitRecord tests the CommitRecord functionality
func TestCommitRecord(t *testing.T) {
	p := file.NewPage(64)
	p.SetInt(binary.Size(int32(0)), 300) // Set Transaction ID

	c := NewCommitRecord(p)

	assert.NotNil(t, c, "CommitRecord should not be nil")
	assert.Equal(t, COMMIT, c.Op(), "Op should return COMMIT")
	assert.Equal(t, 300, c.TxNumber(), "TxNumber should return 300")
	assert.Equal(t, "<COMMIT 300>", c.String(), "String should return '<COMMIT 300>'")

	tx := new(MockTransaction)
	err := c.Undo(tx)
	assert.Nil(t, err, "Undo should do nothing and return nil")
}

// TestWriteCommitToLog tests the WriteCommitToLog function
func TestWriteCommitToLog(t *testing.T) {
	mockLogMgr := &MockLogMgr{}
	lsn, err := WriteCommitToLog(mockLogMgr, 400)

	assert.Nil(t, err, "WriteCommitToLog should not return an error")
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

	assert.Equal(t, COMMIT, opCode, "Last log record should be COMMIT")
	assert.Equal(t, 400, txID, "Transaction ID should be 400")
}
