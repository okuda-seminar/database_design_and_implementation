package log

import (
	"errors"
	"fmt"

	"database_design_and_implementation/internal/file"
)

// LogIterator provides a way to iterate over log records in reverse order.
type LogIterator struct {
	fm         *file.FileMgr
	blk        *file.BlockId
	p          *file.Page
	currentPos int
	boundary   int
}

// NewLogIterator creates a new LogIterator for the given file manager and block ID.
func NewLogIterator(fm *file.FileMgr, blk *file.BlockId) *LogIterator {
	blockSize := fm.BlockSize()
	p := file.NewPage(blockSize)

	iterator := &LogIterator{
		fm:  fm,
		blk: blk,
		p:   p,
	}

	iterator.moveToBlock(blk)
	return iterator
}

// HasNext returns true if there are more log records to read.
func (it *LogIterator) HasNext() bool {
	return it.currentPos >= 0 && it.currentPos < it.fm.BlockSize()
}

// Next reads the next log record from the log file.
func (it *LogIterator) Next() ([]byte, error) {
	if !it.HasNext() {
		return nil, errors.New("no more records")
	}

	rec, err := it.p.GetBytes(it.currentPos)
	if err != nil {
		return nil, err
	}

	nextPos := it.currentPos + file.IntSize + len(rec)

	if nextPos >= it.fm.BlockSize() {
		it.currentPos = -1
	} else {
		it.currentPos = nextPos
	}

	return rec, nil
}

// moveToBlock reads the specified block and sets the iterator's boundary and current position.
func (it *LogIterator) moveToBlock(blk *file.BlockId) {
	it.fm.Read(*blk, it.p.Contents())

	val, err := it.p.GetInt(0)
	if err != nil {
		panic(err)
	}
	it.boundary = int(val)

	fmt.Printf("moveToBlock: boundary = %d, block = %s\n", it.boundary, blk.String())

	if it.boundary == 0 {
		it.currentPos = 0
	} else {
		it.currentPos = it.boundary
	}
}
