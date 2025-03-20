package recovery

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"database_design_and_implementation/internal/file"
)

// TestRollbackRecord tests the RollbackRecord functionality
func TestRollbackRecord(t *testing.T) {
	p := file.NewPage(64)
	p.SetInt(binary.Size(int32(0)), 500) // Set Transaction ID

	r := NewRollbackRecord(p)

	assert.NotNil(t, r, "RollbackRecord should not be nil")
	assert.Equal(t, ROLLBACK, r.Op(), "Op should return ROLLBACK")
	assert.Equal(t, 500, r.TxNumber(), "TxNumber should return 500")
	assert.Equal(t, "<ROLLBACK 500>", r.String(), "String should return '<ROLLBACK 500>'")

	tx := &MockTransaction{}
	err := r.Undo(tx)
	assert.Nil(t, err, "Undo should do nothing and return nil")
}

// TestWriteRollbackToLog tests the WriteRollbackToLog function
func TestWriteRollbackToLog(t *testing.T) {
	mockLogMgr := &MockLogMgr{}
	lsn, err := WriteRollbackToLog(mockLogMgr, 600)

	assert.Nil(t, err, "WriteRollbackToLog should not return an error")
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

	assert.Equal(t, ROLLBACK, opCode, "Last log record should be ROLLBACK")
	assert.Equal(t, 600, txID, "Transaction ID should be 600")
}
