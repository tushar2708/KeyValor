package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// WriteAheadLogFile represents an append-only file,
// used to store the records on the disk.
type WriteAheadLogFile struct {
	sync.RWMutex
	writer *os.File
	reader *os.File
	id     int
	offset int
}

const (
	STORAGE_FILENAME_PREFIX     = "wal_file_"
	WAL_FILE_NAME_FORMAT        = "wal_file_%d.db"
	MERGED_WAL_FILE_NAME_FORMAT = "wal_file.merged.wip"
	WAL_FILE_EXTENSION          = ".db"
	LOCKFILE                    = "store.lock"
	INDEX_FILENAME              = "index"
)

func ListWALFiles(directory string) (files []string, ids []int, err error) {
	files, err = filepath.Glob(fmt.Sprintf("%s/.db", directory))
	if err != nil {
		return nil, nil, err
	}

	ids = make([]int, len(files))

	// store_file_<int>.db
	for i, file := range files {
		fileNumber := strings.TrimPrefix(strings.TrimSuffix(filepath.Base(file), ".db"), STORAGE_FILENAME_PREFIX)
		id, err := strconv.ParseInt(fileNumber, 10, 32)
		if err != nil {
			return nil, nil, err
		}

		// fileNameIDMap[file] = int(id)
		ids[i] = int(id)
	}

	return files, ids, nil
}

func NewWALFile(dir string, fileID int) (*WriteAheadLogFile, error) {
	filePath := filepath.Join(dir, fmt.Sprintf(WAL_FILE_NAME_FORMAT, fileID))
	return newWALFile(filePath, fileID)
}

func NewWALFileWithPath(filePath string, fileID int) (*WriteAheadLogFile, error) {
	return newWALFile(filePath, fileID)
}

func newWALFile(filePath string, fileID int) (*WriteAheadLogFile, error) {
	fWriter, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file for writing db: %w", err)
	}

	fReader, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file for reading db: %w", err)
	}

	stat, err := fWriter.Stat()
	if err != nil {
		return nil, fmt.Errorf("error fetching file stats: %v", err)
	}

	currentOffset := stat.Size()

	return &WriteAheadLogFile{
		writer: fWriter,
		reader: fReader,
		id:     fileID,
		offset: int(currentOffset),
	}, nil
}

func (df *WriteAheadLogFile) ID() int {
	return df.id
}

func (df *WriteAheadLogFile) Size() (int64, error) {
	stat, err := df.writer.Stat()
	if err != nil {
		return -1, fmt.Errorf("error fetching file size: %v", err)
	}
	return stat.Size(), nil
}

func (df *WriteAheadLogFile) Sync() error {
	return df.writer.Sync()
}

func (df *WriteAheadLogFile) Write(p []byte) (int, error) {
	df.Lock()
	defer df.Unlock()

	n, err := df.writer.Write(p)
	df.offset += n

	return n, err
}

func (df *WriteAheadLogFile) GetCurrentOffset() int {
	return df.offset
}

func (df *WriteAheadLogFile) Read(pos int, size int) ([]byte, error) {
	df.RLock()
	defer df.RUnlock()

	start := int64(pos - size)

	record := make([]byte, size)

	n, err := df.reader.ReadAt(record, start)
	if err != nil {
		return nil, fmt.Errorf("error reading from file: %w", err)
	}
	if n != size {
		return nil, fmt.Errorf("error reading record from file: %d", n)
	}

	return record, nil
}

func (df *WriteAheadLogFile) Close() error {
	df.Lock()
	defer df.Unlock()

	return df.writer.Close()
}
