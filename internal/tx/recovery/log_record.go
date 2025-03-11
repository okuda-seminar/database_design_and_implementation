package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// The operation types
const (
	CHECKPOINT = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

// LogRecord interface
type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx Transaction) error
}

// Transaction interface
type Transaction interface {
	UndoSetInt(txID, offset, oldValue int) error
	UndoSetString(txID, offset int, oldValue string) error
}

// CreateLogRecord creates a new LogRecord instance from the given data.
func CreateLogRecord(data []byte) (LogRecord, error) {
	if len(data) < 4 {
		return nil, errors.New("invalid log record data")
	}

	buf := bytes.NewReader(data)
	var opType int32
	if err := binary.Read(buf, binary.LittleEndian, &opType); err != nil {
		return nil, err
	}

	switch opType {
	case CHECKPOINT:
		return NewCheckpointRecord(), nil
	}
	return nil, errors.New("unknown log record type")
}
