package recovery

import (
	"database_design_and_implementation/internal/file"
)

// MockTransaction is a mock implementation of Transaction interface
type MockTransaction struct{}

func (m *MockTransaction) UndoSetInt(txID, offset, oldValue int) error {
	return nil
}

func (m *MockTransaction) UndoSetString(txID, offset int, oldValue string) error {
	return nil
}

func (m *MockTransaction) Commit() error                                              { return nil }
func (m *MockTransaction) Rollback() error                                            { return nil }
func (m *MockTransaction) Pin(blk file.BlockId)                                       {}
func (m *MockTransaction) Unpin(blk file.BlockId)                                     {}
func (m *MockTransaction) SetInt(blk file.BlockId, offset int, val int, logging bool) {}

// MockLogMgr is a mock implementation of LogMgr for testing purposes
type MockLogMgr struct {
	lastRecord []byte
	nextLSN    int
}

func (m *MockLogMgr) Append(logrec []byte) int {
	m.lastRecord = make([]byte, len(logrec))
	copy(m.lastRecord, logrec)
	m.nextLSN++
	return m.nextLSN
}
