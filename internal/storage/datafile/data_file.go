package datafile

import (
	"KeyValor/internal/utils/strictchecks"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type DataFileMode int8

const (
	DF_MODE_READ_WRITE DataFileMode = iota
	DF_MODE_WRITE_ONLY
	DF_MODE_READ_ONLY
)

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

func newDataFile(dir string, fileNameFormat string, fileID int, mode DataFileMode) (*ReadWriteDataFile, error) {
	filePath := filepath.Join(dir, fmt.Sprintf(fileNameFormat, fileID))
	return newDataFileCommon(filePath, fileID, mode)
}

func newDataFileWithPath(filePath string, fileID int, mode DataFileMode) (*ReadWriteDataFile, error) {
	return newDataFileCommon(filePath, fileID, mode)
}

func newDataFileCommon(filePath string, fileID int, mode DataFileMode) (*ReadWriteDataFile, error) {

	rwdf := &ReadWriteDataFile{
		writer:      nil,
		reader:      nil,
		id:          fileID,
		writeOffset: 0,
		readOffset:  0,
		mode:        mode,
	}

	var err error

	if mode == DF_MODE_WRITE_ONLY || mode == DF_MODE_READ_WRITE {
		rwdf.writer, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("error opening file for writing db: %w", err)
		}

		stat, err := rwdf.writer.Stat()
		if err != nil {
			return nil, fmt.Errorf("error fetching file stats: %v", err)
		}

		rwdf.writeOffset = stat.Size()

	}

	if mode == DF_MODE_READ_ONLY || mode == DF_MODE_READ_WRITE {
		rwdf.reader, err = os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("error opening file for reading db: %w", err)
		}
	}

	return rwdf, nil
}

func (rwdf *ReadWriteDataFile) ID() int {
	return rwdf.id
}

func (rwdf *ReadWriteDataFile) Size() (int64, error) {

	if rwdf.writer != nil {
		stat, err := rwdf.writer.Stat()
		if err != nil {
			return -1, fmt.Errorf("error fetching file size: %v", err)
		}
		return stat.Size(), nil
	}

	if rwdf.reader != nil {
		stat, err := rwdf.reader.Stat()
		if err != nil {
			return -1, fmt.Errorf("error fetching file size: %v", err)
		}
		return stat.Size(), nil
	}

	return -1, fmt.Errorf("both reader and writer are nil, can't get size")
}

func (rwdf *ReadWriteDataFile) Sync() error {
	return rwdf.writer.Sync()
}

func (rwdf *ReadWriteDataFile) Write(p []byte) (int, error) {

	strictchecks.MustBeTrueOrPanic(rwdf.mode != DF_MODE_READ_ONLY, "cannot write in read-only mode (BUG)")

	rwdf.Lock()
	defer rwdf.Unlock()

	n, err := rwdf.writer.Write(p)
	rwdf.writeOffset += int64(n)

	return n, err
}

func (rwdf *ReadWriteDataFile) GetCurrentWriteOffset() int64 {
	return rwdf.writeOffset
}

func (rwdf *ReadWriteDataFile) GetCurrentReadOffset() int64 {
	return rwdf.readOffset
}

func (rwdf *ReadWriteDataFile) Read(record []byte) (int, error) {

	strictchecks.MustBeTrueOrPanic(rwdf.mode != DF_MODE_WRITE_ONLY, "cannot read in write-only mode (BUG)")

	rwdf.RLock()
	defer rwdf.RUnlock()

	size := len(record)

	n, err := rwdf.reader.ReadAt(record, rwdf.readOffset)
	if err != nil {
		return n, fmt.Errorf("error reading from file: %w", err)
	}
	if n != size {
		return n, fmt.Errorf("error reading record from file: %d", n)
	}

	rwdf.readOffset += int64(n)

	return n, nil
}

func (rwdf *ReadWriteDataFile) ReadAt(record []byte, pos int64) (int, error) {

	strictchecks.MustBeTrueOrPanic(rwdf.mode != DF_MODE_WRITE_ONLY, "cannot read in write-only mode (BUG)")

	rwdf.RLock()
	defer rwdf.RUnlock()

	size := len(record)

	n, err := rwdf.reader.ReadAt(record, pos)
	if err != nil {
		return n, fmt.Errorf("error reading from file: %w", err)
	}
	if n != size {
		return n, fmt.Errorf("error reading record from file: %d", n)
	}

	rwdf.readOffset = pos + int64(n)

	return n, nil
}

func (rwdf *ReadWriteDataFile) Seek(offset int64, whence int) (int64, error) {
	rwdf.Lock()
	defer rwdf.Unlock()

	strictchecks.MustBeTrueOrPanic(rwdf.mode != DF_MODE_WRITE_ONLY,
		"seek not allowed in write-only mode (BUG)")

	newOffset, err := rwdf.reader.Seek(offset, whence)
	if err != nil {
		return newOffset, fmt.Errorf("error seeking in file: %w", err)
	}
	rwdf.readOffset = newOffset

	return newOffset, nil
}

func (rwdf *ReadWriteDataFile) Close() error {
	rwdf.Lock()
	defer rwdf.Unlock()

	if rwdf.writer != nil {
		rwdf.writer.Close()
	}
	if rwdf.reader != nil {
		rwdf.reader.Close()
	}

	return nil
}
