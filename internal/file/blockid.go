package file

import "fmt"

// BlockId represents a block identifier in a file.
type BlockId struct {
	Filename string
	Blknum   int
}

// NewBlockId creates a new BlockId instance.
func NewBlockId(filename string, blknum int) BlockId {
	return BlockId{Filename: filename, Blknum: blknum}
}

// String returns a string representation of the BlockId.
func (b BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.Blknum)
}

// Equals compares two BlockId instances for equality.
func (b BlockId) Equals(other BlockId) bool {
	return b.Filename == other.Filename && b.Blknum == other.Blknum
}

// HashCode returns a hash code based on the string representation.
func (b BlockId) HashCode() int {
	return int(hash(b.String()))
}

// hash is a simple hash function (not cryptographic).
func hash(s string) uint32 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h = 31*h + uint32(s[i])
	}
	return h
}
