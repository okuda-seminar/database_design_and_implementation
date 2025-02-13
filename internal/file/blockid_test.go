package file

import (
	"testing"
)

func TestBlockId(t *testing.T) {
	tests := []struct {
		name     string
		block1   BlockId
		block2   BlockId
		expected bool
	}{
		{"Same File and Block", NewBlockId("test.db", 1), NewBlockId("test.db", 1), true},
		{"Different Block Number", NewBlockId("test.db", 1), NewBlockId("test.db", 2), false},
		{"Different File Name", NewBlockId("test.db", 1), NewBlockId("another.db", 1), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.block1.Equals(tc.block2); got != tc.expected {
				t.Errorf("Equals() = %v; want %v", got, tc.expected)
			}
		})
	}

	t.Run("String Representation", func(t *testing.T) {
		b := NewBlockId("test.db", 1)
		expected := "[file test.db, block 1]"
		if b.String() != expected {
			t.Errorf("String() = %s; want %s", b.String(), expected)
		}
	})

	t.Run("HashCode Consistency", func(t *testing.T) {
		b1 := NewBlockId("test.db", 1)
		b2 := NewBlockId("test.db", 1)
		b3 := NewBlockId("test.db", 2)

		hash1 := b1.HashCode()
		hash2 := b2.HashCode()
		hash3 := b3.HashCode()

		if hash1 != hash2 {
			t.Errorf("HashCode() failed: hash1 (%d) and hash2 (%d) should be the same", hash1, hash2)
		}
		if hash1 == hash3 {
			t.Errorf("HashCode() failed: hash1 (%d) and hash3 (%d) should be different", hash1, hash3)
		}
	})
}
