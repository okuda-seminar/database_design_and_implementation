package recovery

import (
	"encoding/binary"
	"fmt"

	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
	"database_design_and_implementation/internal/tx"
)

// SetIntRecord represents a SETINT log record
type SetIntRecord struct {
	TxNum  int
	Blk    file.BlockId
	Offset int
	Val    int
}

// LogRecord is an interface for log record operations
type SetIntLogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx Transaction) error
	String() string
}

// LogManager interface to abstract the log manager
type SetIntLogManager interface {
	Append([]byte) (int, error)
}

// Ensure LogMgr implements LogManager
var _ LogManager = (*log.LogMgr)(nil)

// NewSetIntRecord creates a SetIntRecord from a Page
func NewSetIntRecord(p *file.Page) *SetIntRecord {
	txNum, err := p.GetInt(binary.Size(int32(0))) // Read transaction ID
	if err != nil {
		return nil
	}

	fileName, err := p.GetString(binary.Size(int32(0)) + binary.Size(int32(0)))
	if err != nil {
		return nil
	}

	blkNumPos := binary.Size(int32(0))*2 + file.MaxLength(len(fileName))
	blkNum, err := p.GetInt(blkNumPos)
	if err != nil {
		return nil
	}

	offsetPos := blkNumPos + binary.Size(int32(0))
	offset, err := p.GetInt(offsetPos)
	if err != nil {
		return nil
	}

	valPos := offsetPos + binary.Size(int32(0))
	val, err := p.GetInt(valPos)
	if err != nil {
		return nil
	}

	return &SetIntRecord{
		TxNum:  int(txNum),
		Blk:    file.NewBlockId(fileName, int(blkNum)),
		Offset: int(offset),
		Val:    int(val),
	}
}

// Op returns the SETINT operation code
func (s *SetIntRecord) Op() int {
	return SETINT
}

// TxNumber returns the transaction ID
func (s *SetIntRecord) TxNumber() int {
	return s.TxNum
}

// Undo restores the previous integer value
func (s *SetIntRecord) Undo(tx tx.Transaction) error {
	tx.Pin(s.Blk)
	tx.SetInt(s.Blk, s.Offset, s.Val, false)
	tx.Unpin(s.Blk)
	return nil
}

// String returns a string representation of the SetIntRecord
func (s *SetIntRecord) String() string {
	return fmt.Sprintf("<SETINT %d %s:%d %d %d>", s.TxNum, s.Blk.Filename, s.Blk.Blknum, s.Offset, s.Val)
}

// WriteSetIntToLog writes a SETINT record to the log
func WriteSetIntToLog(lm LogManager, txNum int, blk file.BlockId, offset int, val int) (int, error) {
	tPos := binary.Size(int32(0))
	fPos := tPos + binary.Size(int32(0))
	bPos := fPos + file.MaxLength(len(blk.String()))
	oPos := bPos + binary.Size(int32(0))
	vPos := oPos + binary.Size(int32(0))
	// Create a buffer to store log record
	rec := make([]byte, vPos+binary.Size(int32(0)))
	page := file.NewPage(len(rec))

	// Write values to the log
	page.SetInt(0, SETINT)
	page.SetInt(tPos, int32(txNum))
	page.SetString(fPos, blk.String())
	page.SetInt(bPos, int32(blk.Blknum))
	page.SetInt(oPos, int32(offset))
	page.SetInt(vPos, int32(val))

	return lm.Append(page.Contents()), nil
}
