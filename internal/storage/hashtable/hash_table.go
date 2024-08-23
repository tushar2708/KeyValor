package hashtable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"KeyValor/config"
	"KeyValor/internal/storage/storagecommon"
	"KeyValor/internal/utils/fileutils"
)

type HashTableStorage struct {
	sync.RWMutex
	cfg              *config.DBCfgOpts
	bufferPool       sync.Pool // crate an object pool to reuse buffers
	keyLocationIndex storagecommon.DatabaseIndex
	activeWALFile    *storagecommon.WriteAheadLogFile
	oldWALFilesMap   map[int]*storagecommon.WriteAheadLogFile
	lockFile         *os.File
}

func NewHashTableStorage(cfg *config.DBCfgOpts) (*HashTableStorage, error) {

	var (
		lockFile    *os.File
		oldWalFiles = make(map[int]*storagecommon.WriteAheadLogFile)
	)

	_, ids, err := storagecommon.ListWALFiles(cfg.Directory)
	if err != nil {
		return nil, err
	}

	sort.Ints(ids)

	for _, id := range ids {
		walFile, err := storagecommon.NewWALFile(cfg.Directory, id)
		if err != nil {
			return nil, err
		}
		oldWalFiles[id] = walFile
	}

	lockFilePath := filepath.Join(cfg.Directory, storagecommon.LOCKFILE)
	lockFile, err = storagecommon.AcquireLockFile(lockFilePath)
	if err != nil {
		return nil, fmt.Errorf("error creating lockfile: %w", err)
	}

	nextIndex := len(ids) + 1
	activeWalFile, err := storagecommon.NewWALFile(cfg.Directory, nextIndex)
	if err != nil {
		return nil, err
	}

	keyLocationHash := NewLogStructuredHashTableIndex()

	indexFilePath := filepath.Join(cfg.Directory, storagecommon.INDEX_FILENAME)
	if fileutils.FileExists(indexFilePath) {
		if err := keyLocationHash.LoadFromFile(indexFilePath); err != nil {
			return nil, fmt.Errorf("error decoding index file: %w", err)
		}
	}

	return &HashTableStorage{
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

func (hts *HashTableStorage) Init() error {
	// run periodic compaction
	go hts.CompactionLoop(hts.cfg.CompactInterval)

	// run periodic sync
	go hts.FileRotationLoop(hts.cfg.CheckFileSizeInterval)

	return nil
}

func (hts *HashTableStorage) Close() error {
	// persist the index to the disk
	if err := hts.persistIndexFile(); err != nil {
		return fmt.Errorf("error persisting index file: %w", err)
	}

	// close the active file
	if err := hts.activeWALFile.Close(); err != nil {
		return fmt.Errorf("error closing active WAL file: %w", err)
	}

	// close old files
	for _, file := range hts.oldWALFilesMap {
		if err := file.Close(); err != nil {
			return fmt.Errorf("error closing old WAL file: %w", err)
		}
	}

	// free the lock file
	if err := storagecommon.FreeLockFile(hts.lockFile); err != nil {
		return fmt.Errorf("error freeing lock file: %w", err)
	}
	return nil
}
