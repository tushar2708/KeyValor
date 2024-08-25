package hashtable

import (
	"fmt"
	"path/filepath"
	"sort"

	"KeyValor/config"
	"KeyValor/internal/storage/storagecommon"
	"KeyValor/internal/storage/wal"
	"KeyValor/internal/utils/fileutils"
)

type HashTableStorage struct {
	*storagecommon.CommonStorage
	keyLocationIndex storagecommon.DatabaseIndex
	oldWALFilesMap   map[int]*wal.WriteAheadLogRWFile
}

func NewHashTableStorage(cfg *config.DBCfgOpts) (*HashTableStorage, error) {

	var (
		oldWalFiles = make(map[int]*wal.WriteAheadLogRWFile)
	)

	_, ids, err := wal.ListWALFiles(cfg.Directory)
	if err != nil {
		return nil, err
	}

	sort.Ints(ids)

	for _, id := range ids {
		walFile, err := wal.NewWALFile(cfg.Directory, id, wal.WAL_MODE_READ_ONLY)
		if err != nil {
			return nil, err
		}
		oldWalFiles[id] = walFile
	}

	nextIndex := len(ids) + 1
	activeWalFile, err := wal.NewWALFile(cfg.Directory, nextIndex, wal.WAL_MODE_READ_WRITE)
	if err != nil {
		return nil, err
	}

	keyLocationHash := NewLogStructuredHashTableIndex()

	indexFilePath := filepath.Join(cfg.Directory, INDEX_FILENAME)
	if fileutils.FileExists(indexFilePath) {
		if err := keyLocationHash.LoadFromFile(indexFilePath); err != nil {
			return nil, fmt.Errorf("error decoding index file: %w", err)
		}
	}

	cs, err := storagecommon.NewCommonStorage(cfg, activeWalFile)
	if err != nil {
		return nil, fmt.Errorf("error creating common storage: %w", err)
	}

	return &HashTableStorage{
		CommonStorage:    cs,
		keyLocationIndex: keyLocationHash,
		oldWALFilesMap:   oldWalFiles,
	}, nil
}

func (hts *HashTableStorage) Init() error {
	// run periodic compaction
	go hts.CompactionLoop(hts.Cfg.CompactInterval)

	// run periodic sync
	go hts.FileRotationLoop(hts.Cfg.CheckFileSizeInterval)

	return nil
}

func (hts *HashTableStorage) Close() error {
	// persist the index to the disk
	if err := hts.persistIndexFile(); err != nil {
		return fmt.Errorf("error persisting index file: %w", err)
	}

	// close the active file
	if err := hts.ActiveWALFile.Close(); err != nil {
		return fmt.Errorf("error closing active WAL file: %w", err)
	}

	// close old files
	for _, file := range hts.oldWALFilesMap {
		if err := file.Close(); err != nil {
			return fmt.Errorf("error closing old WAL file: %w", err)
		}
	}

	// free the lock file
	if err := storagecommon.FreeLockFile(hts.LockFile); err != nil {
		return fmt.Errorf("error freeing lock file: %w", err)
	}
	return nil
}
