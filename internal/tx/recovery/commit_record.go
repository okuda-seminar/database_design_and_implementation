package recovery

import (
	"encoding/binary"
	"fmt"

	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
)

// CommitRecord represents a COMMIT log record
type CommitRecord struct {
	TxNum int
}

// LogRecord is an interface for log record operations
type CommitLogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx Transaction) error
	String() string
}

// LogManager interface to abstract the log manager
type CommitLogManager interface {
	Append([]byte) int
}

// Ensure LogMgr implements LogManager
var _ LogManager = (*log.LogMgr)(nil)

// NewCommitRecord creates a CommitRecord from a Page
func NewCommitRecord(p *file.Page) *CommitRecord {
	txNum, err := p.GetInt(binary.Size(int32(0))) // Read transaction ID from page
	if err != nil {
		return nil
	}
	return &CommitRecord{TxNum: int(txNum)}
}

// Op returns the COMMIT operation code
func (c *CommitRecord) Op() int {
	return COMMIT
}

// TxNumber returns the transaction ID
func (c *CommitRecord) TxNumber() int {
	return c.TxNum
}

// Undo does nothing as COMMIT doesn't require undo
func (c *CommitRecord) Undo(tx Transaction) error {
	return nil
}

// String returns a string representation of the CommitRecord
func (c *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", c.TxNum)
}

// WriteCommitToLog writes a COMMIT record to the log
func WriteCommitToLog(lm CommitLogManager, txNum int) (int, error) {
	// Create a buffer to store log record
	rec := make([]byte, 8) // int32 size * 2
	page := file.NewPage(len(rec))
	page.SetInt(0, COMMIT)
	page.SetInt(4, int32(txNum))
	return lm.Append(page.Contents()), nil
}
