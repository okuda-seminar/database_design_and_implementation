package recovery

// MockTransaction is a mock implementation of Transaction interface
type MockTransaction struct{}

func (m *MockTransaction) UndoSetInt(txID, offset, oldValue int) error {
	return nil
}

func (m *MockTransaction) UndoSetString(txID, offset int, oldValue string) error {
	return nil
}

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
