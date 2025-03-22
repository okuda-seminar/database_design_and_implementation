// checkpoint_record.go
package recovery

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCheckpointRecord tests the CheckpointRecord functionality
func TestCheckpointRecord(t *testing.T) {
	c := NewCheckpointRecord()

	assert.Equal(t, CHECKPOINT, c.Op(), "Op should return CHECKPOINT")
	assert.Equal(t, -1, c.TxNumber(), "TxNumber should return -1")
	assert.Equal(t, "<CHECKPOINT>", c.String(), "String should return '<CHECKPOINT>'")

	tx := new(MockTransaction)
	err := c.Undo(tx)
	assert.Nil(t, err, "Undo should do nothing and return nil")
}

// TestWriteCheckpointToLog tests the WriteCheckpointToLog function
func TestWriteCheckpointToLog(t *testing.T) {
	mockLogMgr := &MockLogMgr{}
	lsn, err := WriteCheckpointToLog(mockLogMgr)

	assert.Nil(t, err, "WriteCheckpointToLog should not return an error")
	assert.Equal(t, 1, lsn, "LSN should be 1 since mock increments by 1")
	assert.Equal(t, CHECKPOINT, int(binary.LittleEndian.Uint32(mockLogMgr.lastRecord)), "Last log record should be CHECKPOINT")
}

// Ensure MockLogMgr implements LogManager
var _ LogManager = (*MockLogMgr)(nil)
