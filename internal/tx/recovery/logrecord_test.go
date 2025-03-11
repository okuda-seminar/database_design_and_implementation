package recovery

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCreateLogRecord tests the CreateLogRecord function
func TestCreateLogRecord(t *testing.T) {
	rec := make([]byte, 4)
	binary.LittleEndian.PutUint32(rec, uint32(CHECKPOINT))

	logRecord, err := CreateLogRecord(rec)

	assert.Nil(t, err, "CreateLogRecord should not return an error")
	assert.NotNil(t, logRecord, "CreateLogRecord should return a valid LogRecord")
	assert.Equal(t, CHECKPOINT, logRecord.Op(), "Created LogRecord should have Op() == CHECKPOINT")
}