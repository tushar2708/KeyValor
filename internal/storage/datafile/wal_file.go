package datafile

import (
	"KeyValor/internal/utils/strictchecks"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type DataFileMode int8

const (
	DF_MODE_READ_WRITE DataFileMode = iota
	DF_MODE_WRITE_ONLY
	DF_MODE_READ_ONLY
)

type OffsetUtility interface {
	GetCurrentReadOffset() int64
	GetCurrentWriteOffset() int64
	Size() (int64, error)
}

var _ OffsetUtility = &ReadWriteDataFile{}

type ReaderAtWritetCloser interface {
	io.ReadWriteCloser
	io.ReaderAt
}

var _ ReaderAtWritetCloser = &ReadWriteDataFile{}

// ReadWriteDataFile represents an append-only file,
// used to store the records on the disk.
type ReadWriteDataFile struct {
	sync.RWMutex
	writer      *os.File
	reader      *os.File
	id          int
	writeOffset int64
	readOffset  int64
	mode        DataFileMode
}

func ListDataFiles(directory string) (files []string, ids []int, err error) {
	files, err = filepath.Glob(fmt.Sprintf("%s/.db", directory))
	if err != nil {
		return nil, nil, err
	}

	ids = make([]int, len(files))

	// store_file_<int>.db
	for i, file := range files {
		fileNumber := strings.TrimPrefix(strings.TrimSuffix(filepath.Base(file), ".db"), WAL_FILENAME_PREFIX)
		id, err := strconv.ParseInt(fileNumber, 10, 32)
		if err != nil {
			return nil, nil, err
		}

		// fileNameIDMap[file] = int(id)
		ids[i] = int(id)
	}

	return files, ids, nil
}

func NewDataFile(dir string, fileID int, mode DataFileMode) (*ReadWriteDataFile, error) {
	filePath := filepath.Join(dir, fmt.Sprintf(WAL_FILE_NAME_FORMAT, fileID))
	return newDataFile(filePath, fileID, mode)
}

func NewDataFileWithPath(filePath string, fileID int, mode DataFileMode) (*ReadWriteDataFile, error) {
	return newDataFile(filePath, fileID, mode)
}

func newDataFile(filePath string, fileID int, mode DataFileMode) (*ReadWriteDataFile, error) {

	wal := &ReadWriteDataFile{
		writer:      nil,
		reader:      nil,
		id:          fileID,
		writeOffset: 0,
		readOffset:  0,
		mode:        mode,
	}

	var err error

	if mode == DF_MODE_WRITE_ONLY || mode == DF_MODE_READ_WRITE {
		wal.writer, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("error opening file for writing db: %w", err)
		}

		stat, err := wal.writer.Stat()
		if err != nil {
			return nil, fmt.Errorf("error fetching file stats: %v", err)
		}

		wal.writeOffset = stat.Size()

	}

	if mode == DF_MODE_READ_ONLY || mode == DF_MODE_READ_WRITE {
		wal.reader, err = os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("error opening file for reading db: %w", err)
		}
	}

	return wal, nil
}

func (wal *ReadWriteDataFile) ID() int {
	return wal.id
}

func (wal *ReadWriteDataFile) Size() (int64, error) {

	if wal.writer != nil {
		stat, err := wal.writer.Stat()
		if err != nil {
			return -1, fmt.Errorf("error fetching file size: %v", err)
		}
		return stat.Size(), nil
	}

	if wal.reader != nil {
		stat, err := wal.reader.Stat()
		if err != nil {
			return -1, fmt.Errorf("error fetching file size: %v", err)
		}
		return stat.Size(), nil
	}

	return -1, fmt.Errorf("both reader and writer are nil, can't get size")
}

func (wal *ReadWriteDataFile) Sync() error {
	return wal.writer.Sync()
}

func (wal *ReadWriteDataFile) Write(p []byte) (int, error) {

	strictchecks.MustBeTrueOrPanic(wal.mode != DF_MODE_READ_ONLY, "cannot write in read-only mode (BUG)")

	wal.Lock()
	defer wal.Unlock()

	n, err := wal.writer.Write(p)
	wal.writeOffset += int64(n)

	return n, err
}

func (wal *ReadWriteDataFile) GetCurrentWriteOffset() int64 {
	return wal.writeOffset
}

func (wal *ReadWriteDataFile) GetCurrentReadOffset() int64 {
	return wal.readOffset
}

func (wal *ReadWriteDataFile) Read(record []byte) (int, error) {

	strictchecks.MustBeTrueOrPanic(wal.mode != DF_MODE_WRITE_ONLY, "cannot read in write-only mode (BUG)")

	wal.RLock()
	defer wal.RUnlock()

	size := len(record)

	n, err := wal.reader.ReadAt(record, wal.readOffset)
	if err != nil {
		return n, fmt.Errorf("error reading from file: %w", err)
	}
	if n != size {
		return n, fmt.Errorf("error reading record from file: %d", n)
	}

	wal.readOffset += int64(n)

	return n, nil
}

func (wal *ReadWriteDataFile) ReadAt(record []byte, pos int64) (int, error) {

	strictchecks.MustBeTrueOrPanic(wal.mode != DF_MODE_WRITE_ONLY, "cannot read in write-only mode (BUG)")

	wal.RLock()
	defer wal.RUnlock()

	size := len(record)

	n, err := wal.reader.ReadAt(record, pos)
	if err != nil {
		return n, fmt.Errorf("error reading from file: %w", err)
	}
	if n != size {
		return n, fmt.Errorf("error reading record from file: %d", n)
	}

	wal.readOffset = pos + int64(n)

	return n, nil
}

func (wal *ReadWriteDataFile) Close() error {
	wal.Lock()
	defer wal.Unlock()

	if wal.writer != nil {
		wal.writer.Close()
	}
	if wal.reader != nil {
		wal.reader.Close()
	}

	return nil
}
