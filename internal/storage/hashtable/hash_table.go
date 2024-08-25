package hashtable

import (
	"fmt"
	"path/filepath"
	"sort"

	"KeyValor/config"
	"KeyValor/internal/storage/datafile"
	"KeyValor/internal/storage/storagecommon"
	"KeyValor/internal/utils/fileutils"
)

type HashTableStorage struct {
	*storagecommon.CommonStorage
	keyLocationIndex    storagecommon.DatabaseIndex
	olddatafileFilesMap map[int]*datafile.ReadWriteDataFile
}

func NewHashTableStorage(cfg *config.DBCfgOpts) (*HashTableStorage, error) {

	var (
		olddatafileFiles = make(map[int]*datafile.ReadWriteDataFile)
	)

	_, ids, err := datafile.ListDataFiles(cfg.Directory)
	if err != nil {
		return nil, err
	}

	sort.Ints(ids)

	for _, id := range ids {
		datafileFile, err := datafile.NewDataFile(cfg.Directory, id, datafile.DF_MODE_READ_ONLY)
		if err != nil {
			return nil, err
		}
		olddatafileFiles[id] = datafileFile
	}

	nextIndex := len(ids) + 1
	activedatafileFile, err := datafile.NewDataFile(cfg.Directory, nextIndex, datafile.DF_MODE_READ_WRITE)
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

	cs, err := storagecommon.NewCommonStorage(cfg, activedatafileFile)
	if err != nil {
		return nil, fmt.Errorf("error creating common storage: %w", err)
	}

	return &HashTableStorage{
		CommonStorage:       cs,
		keyLocationIndex:    keyLocationHash,
		olddatafileFilesMap: olddatafileFiles,
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
	if err := hts.ActiveDataFile.Close(); err != nil {
		return fmt.Errorf("error closing active datafile file: %w", err)
	}

	// close old files
	for _, file := range hts.olddatafileFilesMap {
		if err := file.Close(); err != nil {
			return fmt.Errorf("error closing old datafile file: %w", err)
		}
	}

	// free the lock file
	if err := storagecommon.FreeLockFile(hts.LockFile); err != nil {
		return fmt.Errorf("error freeing lock file: %w", err)
	}
	return nil
}
