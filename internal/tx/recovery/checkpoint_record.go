// checkpoint_record.go
package recovery

import (
	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
)

// CheckpointRecord struct
type CheckpointRecord struct{}

// LogManager is an interface to abstract the log manager
type LogManager interface {
	Append([]byte) int
}

// Ensure LogMgr implements LogManager
var _ LogManager = (*log.LogMgr)(nil)

// NewCheckpointRecord creates a new CheckpointRecord
func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

// Op returns the CHECKPOINT constant
func (c *CheckpointRecord) Op() int {
	return CHECKPOINT
}

// TxNumber returns -1 as CHECKPOINT has no associated transaction
func (c *CheckpointRecord) TxNumber() int {
	return -1
}

// Undo does nothing as CHECKPOINT doesn't require undo
func (c *CheckpointRecord) Undo(tx Transaction) error {
	return nil
}

// String representation of CheckpointRecord
func (c *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

// WriteCheckpointToLog writes a CHECKPOINT record to the log
func WriteCheckpointToLog(lm LogManager) (int, error) {
	rec := make([]byte, 4) // int32 size
	page := file.NewPage(len(rec))
	page.SetInt(0, CHECKPOINT)
	return lm.Append(page.Contents()), nil
}
