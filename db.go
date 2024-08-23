package KeyValor

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"KeyValor/internal/index"
	"KeyValor/internal/storage"
)

type DBCfgOpts struct {
	directory             string
	syncWriteInterval     time.Duration
	compactInterval       time.Duration
	checkFileSizeInterval time.Duration
	maxActiveFileSize     int64
}

type KeyValorDatabase struct {
	sync.RWMutex

	cfg        *DBCfgOpts
	bufferPool sync.Pool // crate an object pool to reuse buffers

	keyLocationIndex index.DatabaseIndex
	activeWALFile    *storage.WriteAheadLogFile
	oldWALFilesMap   map[int]*storage.WriteAheadLogFile
	lockFile         *os.File
}

func NewKeyValorDB(options ...Option) (*KeyValorDatabase, error) {
	opts := DefaultOpts()
	for _, option := range options {
		option(opts)
	}

	var (
		lockFile    *os.File
		oldWalFiles = make(map[int]*storage.WriteAheadLogFile)
	)

	_, ids, err := storage.ListWALFiles(opts.directory)
	if err != nil {
		return nil, err
	}

	sort.Ints(ids)

	for _, id := range ids {
		walFile, err := storage.NewWALFile(opts.directory, id)
		if err != nil {
			return nil, err
		}
		oldWalFiles[id] = walFile
	}

	lockFilePath := filepath.Join(opts.directory, storage.LOCKFILE)
	lockFile, err = acquireLockFile(lockFilePath)
	if err != nil {
		return nil, fmt.Errorf("error creating lockfile: %w", err)
	}

	nextIndex := len(ids) + 1
	activeWalFile, err := storage.NewWALFile(opts.directory, nextIndex)
	if err != nil {
		return nil, err
	}

	keyLocationHash := index.NewLogStructuredHashTableIndex()

	indexFilePath := filepath.Join(opts.directory, storage.INDEX_FILENAME)
	if fileExists(indexFilePath) {
		if err := keyLocationHash.LoadFromFile(indexFilePath); err != nil {
			return nil, fmt.Errorf("error decoding index file: %w", err)
		}
	}

	kvDB := &KeyValorDatabase{
		cfg: opts,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer([]byte{})
			},
		},
		keyLocationIndex: keyLocationHash,
		activeWALFile:    activeWalFile,
		oldWALFilesMap:   oldWalFiles,
		lockFile:         lockFile,
	}

	// run periodic compaction
	go kvDB.CompactionLoop(kvDB.cfg.compactInterval)

	// run periodic sync
	go kvDB.FileRotationLoop(kvDB.cfg.checkFileSizeInterval)

	return kvDB, nil
}

// createLockFile creates a lock file at the specified path and acquires an exclusive lock on it.
// The function returns the created file and any encountered errors.
//
// Parameters:
// - lockFilePath (string): The path where the lock file will be created.
//
// Returns:
// - *os.File: The created lock file.
// - error: Any error encountered during the creation or locking of the lock file.
func acquireLockFile(lockFilePath string) (*os.File, error) {
	lockFile, err := os.Create(lockFilePath)
	if err != nil {
		return nil, fmt.Errorf("error creating lockfile (%s) error: %w", lockFilePath, err)
	}

	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("error acquiring lock on file (%s) error: %w", lockFilePath, err)
	}
	return lockFile, nil
}

func freeLockFile(lockFile *os.File) error {
	// unlock the lock file
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("error unlocking lock file, error: %w", err)
	}

	// close te file descriptor
	if err := lockFile.Close(); err != nil {
		return fmt.Errorf("error closing lock file, error: %w", err)
	}

	// delete the lock file
	if err := os.Remove(lockFile.Name()); err != nil {
		return fmt.Errorf("error deleting lock file, error: %w", err)
	}
	return nil
}

func (db *KeyValorDatabase) Shutdown() error {

	// persist the index to the disk
	if err := db.persistIndexFile(); err != nil {
		return fmt.Errorf("error persisting index file: %w", err)
	}

	// close the active file
	if err := db.activeWALFile.Close(); err != nil {
		return fmt.Errorf("error closing active WAL file: %w", err)
	}

	// close old files
	for _, file := range db.oldWALFilesMap {
		if err := file.Close(); err != nil {
			return fmt.Errorf("error closing old WAL file: %w", err)
		}
	}

	// free the lock file
	if err := freeLockFile(db.lockFile); err != nil {
		return fmt.Errorf("error freeing lock file: %w", err)
	}
	return nil
}
