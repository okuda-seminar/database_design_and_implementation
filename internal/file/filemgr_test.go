package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileMgr(t *testing.T) {
	// Setup test database directory
	testDir := "testdb"
	defer os.RemoveAll(testDir) // Cleanup after test
	blockSize := 512

	// Initialize FileMgr
	fm, err := NewFileMgr(testDir, blockSize)
	require.NoError(t, err, "Failed to create FileMgr")

	require.True(t, fm.IsNew(), "Expected new database, got isNew = false")

	// Test Append
	t.Run("Append block", func(t *testing.T) {
		block, err := fm.Append("testfile")
		require.NoError(t, err, "Append failed")
		require.Equal(t, 0, block.Blknum, "Expected block number 0, got %d", block.Blknum)

		// Append another block and verify it's block #1
		block2, err := fm.Append("testfile")
		require.NoError(t, err, "Append second block failed")
		require.Equal(t, 1, block2.Blknum, "Expected block number 1, got %d", block2.Blknum)
	})

	// Test Write
	t.Run("Write block", func(t *testing.T) {
		data := make([]byte, blockSize)
		copy(data, "hello world")

		block := BlockId{Filename: "testfile", Blknum: 0}
		err := fm.Write(block, data)
		require.NoError(t, err, "Write failed")
	})

	// Test Read
	t.Run("Read block", func(t *testing.T) {
		readBuffer := make([]byte, blockSize)
		block := BlockId{Filename: "testfile", Blknum: 0}
		err := fm.Read(block, readBuffer)
		require.NoError(t, err, "Read failed")

		// Check if data is correctly written and read
		require.Equal(t, "hello world", string(readBuffer[:11]), "Data mismatch after read")
	})

	// Test Length
	t.Run("Check file length", func(t *testing.T) {
		length, err := fm.Length("testfile")
		require.NoError(t, err, "Length failed")
		require.Equal(t, 2, length, "Expected length 2, got %d", length)
	})

	// Test Read and Write Counts
	t.Run("Check read/write counts", func(t *testing.T) {
		require.Equal(t, 1, fm.GetWriteCount(), "Expected write count 1, got %d", fm.GetWriteCount())
		require.Equal(t, 1, fm.GetReadCount(), "Expected read count 1, got %d", fm.GetReadCount())

		// Perform additional operations
		block := BlockId{Filename: "testfile", Blknum: 1}
		data := make([]byte, blockSize)
		copy(data, "another block")
		_ = fm.Write(block, data)
		_ = fm.Read(block, make([]byte, blockSize))

		// Verify updated counts
		require.Equal(t, 2, fm.GetWriteCount(), "Write count mismatch")
		require.Equal(t, 2, fm.GetReadCount(), "Read count mismatch")
	})

	// Test Persistence (Ensure file exists)
	t.Run("Check file existence", func(t *testing.T) {
		filePath := filepath.Join(testDir, "testfile")
		_, err := os.Stat(filePath)
		require.NoError(t, err, "Expected file testfile to exist")
	})
}
