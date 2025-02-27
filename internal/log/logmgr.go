package log

import (
	"database_design_and_implementation/internal/file"
)

// LogMgr manages the writing and retrieval of log records.
type LogMgr struct {
	fm           *file.FileMgr
	logfile      string
	logpage      *file.Page
	currentblk   *file.BlockId
	latestLSN    int
	lastSavedLSN int
}

// NewLogMgr initializes the log manager.
func NewLogMgr(fm *file.FileMgr, logfile string) *LogMgr {
	blockSize := fm.BlockSize()
	logpage := file.NewPage(blockSize)

	logsize, err := fm.Length(logfile)
	if err != nil {
		panic(err)
	}

	var currentblk *file.BlockId
	if logsize == 0 {
		currentblk = appendNewBlock(fm, logfile, logpage)
	} else {
		blk := file.NewBlockId(logfile, logsize-1)
		currentblk = &blk
		fm.Read(*currentblk, logpage.Contents())
	}

	return &LogMgr{
		fm:         fm,
		logfile:    logfile,
		logpage:    logpage,
		currentblk: currentblk,
	}
}

// Flush ensures that the log record corresponding to the given LSN is written to disk.
func (lm *LogMgr) Flush(lsn int) {
	if lsn > lm.lastSavedLSN {
		lm.flush()
	}
}

// Iterator returns an iterator for reading the log in reverse order.
func (lm *LogMgr) Iterator() *LogIterator {
	lm.flush()
	return NewLogIterator(lm.fm, lm.currentblk)
}

// Append writes a log record to the log buffer.
func (lm *LogMgr) Append(logrec []byte) int {
	boundaryInt, _ := lm.logpage.GetInt(0)
	boundary := int(boundaryInt)
	recsize := len(logrec)
	bytesneeded := recsize + file.IntSize

	if boundary-bytesneeded < 0 {
		lm.flush()
		lm.currentblk = appendNewBlock(lm.fm, lm.logfile, lm.logpage)
		boundaryInt, _ = lm.logpage.GetInt(0)
		boundary = int(boundaryInt)
	}

	recpos := boundary - bytesneeded
	lm.logpage.SetBytes(recpos, logrec)
	lm.logpage.SetInt(0, int32(recpos))
	lm.latestLSN++
	return lm.latestLSN
}

// appendNewBlock initializes a new block for log storage.
func appendNewBlock(fm *file.FileMgr, logfile string, logpage *file.Page) *file.BlockId {
	blk, err := fm.Append(logfile)
	if err != nil {
		panic(err)
	}

	logpage.SetInt(0, int32(fm.BlockSize()))
	fm.Write(blk, logpage.Contents())
	return &blk
}

// flush writes the current log buffer to disk.
func (lm *LogMgr) flush() {
	lm.fm.Write(*lm.currentblk, lm.logpage.Contents())
	lm.lastSavedLSN = lm.latestLSN
}
