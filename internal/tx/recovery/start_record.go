package recovery

import (
	"encoding/binary"
	"fmt"

	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
)

// Define the int32 size
const intSize = 8

// StartRecord represents a START log record
type StartRecord struct {
	TxNum int
}

// LogRecord is an interface for log record operations
type StartLogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx Transaction) error
	String() string
}

// LogManager interface to abstract the log manager
type StartLogManager interface {
	Append([]byte) (int, error)
}

// Ensure LogMgr implements LogManager
var _ LogManager = (*log.LogMgr)(nil)

// NewStartRecord creates a StartRecord from a Page
func NewStartRecord(p *file.Page) *StartRecord {
	txNum, err := p.GetInt(binary.Size(int32(0))) // Read transaction ID from page
	if err != nil {
		return nil
	}
	return &StartRecord{TxNum: int(txNum)}
}

// Op returns the START operation code
func (s *StartRecord) Op() int {
	return START
}

// TxNumber returns the transaction ID
func (s *StartRecord) TxNumber() int {
	return s.TxNum
}

// Undo does nothing as START doesn't require undo
func (s *StartRecord) Undo(tx Transaction) error {
	return nil
}

// String returns a string representation of the StartRecord
func (s *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", s.TxNum)
}

// WriteStartToLog writes a START record to the log
func WriteStartToLog(lm LogManager, txNum int) (int, error) {
	// Create a buffer to store log record
	rec := make([]byte, intSize) // int32 size
	page := file.NewPage(len(rec))
	page.SetInt(0, START)
	page.SetInt(4, int32(txNum))
	return lm.Append(page.Contents()), nil
}
