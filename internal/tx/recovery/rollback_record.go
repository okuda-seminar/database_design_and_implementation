package recovery

import (
	"encoding/binary"
	"fmt"

	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
)

// RollbackRecord represents a ROLLBACK log record
type RollbackRecord struct {
	TxNum int
}

// LogRecord is an interface for log record operations
type RollbackLogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx Transaction) error
	String() string
}

// LogManager interface to abstract the log manager
type RollbackLogManager interface {
	Append([]byte) (int, error)
}

// Ensure LogMgr implements LogManager
var _ LogManager = (*log.LogMgr)(nil)

// NewRollbackRecord creates a RollbackRecord from a Page
func NewRollbackRecord(p *file.Page) *RollbackRecord {
	txNum, err := p.GetInt(binary.Size(int32(0))) // Read transaction ID from page
	if err != nil {
		return nil
	}
	return &RollbackRecord{TxNum: int(txNum)}
}

// Op returns the ROLLBACK operation code
func (r *RollbackRecord) Op() int {
	return ROLLBACK
}

// TxNumber returns the transaction ID
func (r *RollbackRecord) TxNumber() int {
	return r.TxNum
}

// Undo does nothing as ROLLBACK doesn't require undo
func (r *RollbackRecord) Undo(tx Transaction) error {
	return nil
}

// String returns a string representation of the RollbackRecord
func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.TxNum)
}

// WriteRollbackToLog writes a ROLLBACK record to the log
func WriteRollbackToLog(lm LogManager, txNum int) (int, error) {
	// Create a buffer to store log record
	rec := make([]byte, 8) // int32 size * 2
	page := file.NewPage(len(rec))
	page.SetInt(0, ROLLBACK)
	page.SetInt(4, int32(txNum))
	return lm.Append(page.Contents()), nil
}
