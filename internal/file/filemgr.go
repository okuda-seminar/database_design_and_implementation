package file

import (
	"os"
	"path/filepath"
	"sync"
)

type FileMgr struct {
	dbDirectory string
	blockSize   int
	isNew       bool
	openFiles   map[string]*os.File
	writeCount  int
	readCount   int
	mutex       sync.Mutex
}

func NewFileMgr(dbDirectory string, blockSize int) (*FileMgr, error) {
	isNew := false
	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		isNew = true
		if err := os.MkdirAll(dbDirectory, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Remove temporary files
	files, err := os.ReadDir(dbDirectory)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if filepath.HasPrefix(file.Name(), "temp") {
			_ = os.Remove(filepath.Join(dbDirectory, file.Name()))
		}
	}

	return &FileMgr{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		isNew:       isNew,
		openFiles:   make(map[string]*os.File),
	}, nil
}

func (fm *FileMgr) Read(blk BlockId, p []byte) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	file, err := fm.getFile(blk.Filename)
	if err != nil {
		return err
	}
	_, err = file.ReadAt(p, int64(blk.Blknum*fm.blockSize))
	if err != nil {
		return err
	}
	fm.readCount++
	return nil
}

func (fm *FileMgr) Write(blk BlockId, p []byte) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	file, err := fm.getFile(blk.Filename)
	if err != nil {
		return err
	}
	_, err = file.WriteAt(p, int64(blk.Blknum*fm.blockSize))
	if err != nil {
		return err
	}
	fm.writeCount++
	return nil
}

func (fm *FileMgr) Append(filename string) (BlockId, error) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	newBlkNum, err := fm.Length(filename)
	if err != nil {
		return BlockId{}, err
	}

	blk := NewBlockId(filename, newBlkNum)
	buffer := make([]byte, fm.blockSize)

	file, err := fm.getFile(filename)
	if err != nil {
		return BlockId{}, err
	}
	_, err = file.WriteAt(buffer, int64(blk.Blknum*fm.blockSize))
	if err != nil {
		return BlockId{}, err
	}

	return blk, nil
}

func (fm *FileMgr) Length(filename string) (int, error) {
	file, err := fm.getFile(filename)
	if err != nil {
		return 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(info.Size()) / fm.blockSize, nil
}

func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	if file, exists := fm.openFiles[filename]; exists {
		return file, nil
	}

	path := filepath.Join(fm.dbDirectory, filename)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	fm.openFiles[filename] = file
	return file, nil
}

func (fm *FileMgr) GetWriteCount() int {
	return fm.writeCount
}

func (fm *FileMgr) GetReadCount() int {
	return fm.readCount
}

func (fm *FileMgr) IsNew() bool {
	return fm.isNew
}

func (fm *FileMgr) BlockSize() int {
	return fm.blockSize
}
