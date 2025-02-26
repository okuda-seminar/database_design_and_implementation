package buffer

import (
	"database_design_and_implementation/internal/file"
	"database_design_and_implementation/internal/log"
)

type Buffer struct {
	fm       *file.FileMgr
	lm       *log.LogMgr
	contents *file.Page
	blk      *file.BlockId
	pins     int
	txnum    int
	lsn      int
}

func NewBuffer(fm *file.FileMgr, lm *log.LogMgr) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPage(fm.BlockSize()),
		blk:      nil,
		txnum:    -1,
		lsn:      -1,
	}
}

// Contents returns the page contained in the buffer.
func (b *Buffer) Contents() *file.Page {
	return b.contents
}

// Block returns the block assigned to the buffer.
func (b *Buffer) Block() *file.BlockId {
	return b.blk
}

// SetModified marks the buffer as modified by a transaction.
func (b *Buffer) SetModified(txnum, lsn int) {
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// IsPinned returns true if the buffer is pinned.
func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

// ModifyingTx returns the transaction number that modified the buffer.
func (b *Buffer) ModifyingTx() int {
	return b.txnum
}

// AssignToBlock assigns the buffer to a block and reads its contents.
func (b *Buffer) AssignToBlock(blk *file.BlockId) {
	b.Flush()
	b.blk = blk
	b.fm.Read(*blk, b.contents.Contents())
	b.pins = 0
}

func (b *Buffer) Flush() {
	if b.txnum >= 0 && b.blk != nil {
		b.lm.Flush(b.lsn)
		err := b.fm.Write(*b.blk, b.contents.Contents())
		if err != nil {
			panic("Flush failed: " + err.Error())
		}
		b.txnum = -1
	}
}

// Pin increases the pin count of the buffer.
func (b *Buffer) Pin() {
	b.pins++
}

// Unpin decreases the pin count of the buffer.
func (b *Buffer) Unpin() {
	b.pins--
}
