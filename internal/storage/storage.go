package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"golang.org/x/sys/unix"

	"KeyValor/config"
	"KeyValor/dbops"
	"KeyValor/internal/index"
	"KeyValor/internal/utils/fileutils"
)

type DiskStorage interface {
	Init() error
	Close() error
	dbops.DatabaseOperations
}

type LshtStorage struct {
	sync.RWMutex
	cfg              *config.DBCfgOpts
	bufferPool       sync.Pool // crate an object pool to reuse buffers
	keyLocationIndex index.DatabaseIndex
	activeWALFile    *WriteAheadLogFile
	oldWALFilesMap   map[int]*WriteAheadLogFile
	lockFile         *os.File
}

func NewLshtStorage(cfg *config.DBCfgOpts) (*LshtStorage, error) {

	var (
		lockFile    *os.File
		oldWalFiles = make(map[int]*WriteAheadLogFile)
	)

	_, ids, err := ListWALFiles(cfg.Directory)
	if err != nil {
		return nil, err
	}

	sort.Ints(ids)

	for _, id := range ids {
		walFile, err := NewWALFile(cfg.Directory, id)
		if err != nil {
			return nil, err
		}
		oldWalFiles[id] = walFile
	}

	lockFilePath := filepath.Join(cfg.Directory, LOCKFILE)
	lockFile, err = acquireLockFile(lockFilePath)
	if err != nil {
		return nil, fmt.Errorf("error creating lockfile: %w", err)
	}

	nextIndex := len(ids) + 1
	activeWalFile, err := NewWALFile(cfg.Directory, nextIndex)
	if err != nil {
		return nil, err
	}

	keyLocationHash := index.NewLogStructuredHashTableIndex()

	indexFilePath := filepath.Join(cfg.Directory, INDEX_FILENAME)
	if fileutils.FileExists(indexFilePath) {
		if err := keyLocationHash.LoadFromFile(indexFilePath); err != nil {
			return nil, fmt.Errorf("error decoding index file: %w", err)
		}
	}

	return &LshtStorage{
		cfg: cfg,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer([]byte{})
			},
		},
		keyLocationIndex: keyLocationHash,
		activeWALFile:    activeWalFile,
		oldWALFilesMap:   oldWalFiles,
		lockFile:         lockFile,
	}, nil
}

func (ls *LshtStorage) Init() error {
	// run periodic compaction
	go ls.CompactionLoop(ls.cfg.CompactInterval)

	// run periodic sync
	go ls.FileRotationLoop(ls.cfg.CheckFileSizeInterval)

	return nil
}

func (ls *LshtStorage) Close() error {
	// persist the index to the disk
	if err := ls.persistIndexFile(); err != nil {
		return fmt.Errorf("error persisting index file: %w", err)
	}

	// close the active file
	if err := ls.activeWALFile.Close(); err != nil {
		return fmt.Errorf("error closing active WAL file: %w", err)
	}

	// close old files
	for _, file := range ls.oldWALFilesMap {
		if err := file.Close(); err != nil {
			return fmt.Errorf("error closing old WAL file: %w", err)
		}
	}

	// free the lock file
	if err := freeLockFile(ls.lockFile); err != nil {
		return fmt.Errorf("error freeing lock file: %w", err)
	}
	return nil
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
