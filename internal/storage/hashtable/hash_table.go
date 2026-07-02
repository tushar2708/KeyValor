package hashtable

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"KeyValor/config"
	"KeyValor/internal/storage/datafile"
	"KeyValor/internal/storage/storagecommon"
)

type HashTableStorage struct {
	*storagecommon.CommonStorage
	ActiveDataFile      datafile.AppendOnlyWithRandomReads
	keyLocationIndex    storagecommon.DatabaseIndex
	olddatafileFilesMap map[int]datafile.ReadOnlyWithRandomReads
}

func NewHashTableStorage(cfg *config.DBCfgOpts) (*HashTableStorage, error) {

	var (
		olddatafileFiles = make(map[int]datafile.ReadOnlyWithRandomReads)
	)

	_, ids, err := listHashTableDataFiles(cfg.Directory)
	if err != nil {
		return nil, err
	}

	sort.Ints(ids)

	for _, id := range ids {
		datafile, err := datafile.NewReadOnlyDataFileWithRandomReads(cfg.Directory, HASHTABLE_DATAFILE_NAME_FORMAT, id)
		if err != nil {
			return nil, err
		}
		olddatafileFiles[id] = datafile
	}

	nextIndex := len(ids) + 1
	activedatafile, err := datafile.NewAppendOnlyDataFileWithRandomReads(cfg.Directory, HASHTABLE_DATAFILE_NAME_FORMAT, nextIndex)
	if err != nil {
		return nil, err
	}

	indexFilePath := filepath.Join(cfg.Directory, INDEX_FILENAME)
	keyLocationIndex := NewCheckpointIndex(indexFilePath)
	if err := keyLocationIndex.Open(); err != nil {
		return nil, fmt.Errorf("error opening index: %w", err)
	}

	cs, err := storagecommon.NewCommonStorage(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating common storage: %w", err)
	}

	return &HashTableStorage{
		CommonStorage:       cs,
		ActiveDataFile:      activedatafile,
		keyLocationIndex:    keyLocationIndex,
		olddatafileFilesMap: olddatafileFiles,
	}, nil
}

func listHashTableDataFiles(directory string) (files []string, ids []int, err error) {
	files, err = filepath.Glob(fmt.Sprintf("%s/%s", directory, HASHTABLE_DATAFILE_EXTENSION))
	if err != nil {
		return nil, nil, err
	}

	ids = make([]int, len(files))

	// store_file_<int>.db
	for i, file := range files {
		fileNumber := strings.TrimPrefix(strings.TrimSuffix(filepath.Base(file), HASHTABLE_DATAFILE_EXTENSION), HASHTABLE_DATAFILE_NAME_PREFIX)
		id, err := strconv.ParseInt(fileNumber, 10, 32)
		if err != nil {
			return nil, nil, err
		}

		// fileNameIDMap[file] = int(id)
		ids[i] = int(id)
	}

	return files, ids, nil
}

func (hts *HashTableStorage) Init() error {
	go hts.CompactionLoop(hts.Cfg.CompactInterval)
	go hts.FileRotationLoop(hts.Cfg.CheckFileSizeInterval)
	go hts.IndexFlushLoop(hts.Cfg.SyncWriteInterval)
	return nil
}

func (hts *HashTableStorage) Close() error {
	hts.Lock()
	if err := hts.keyLocationIndex.Flush(); err != nil {
		hts.Unlock()
		return fmt.Errorf("error flushing index on close: %w", err)
	}
	if err := hts.keyLocationIndex.Close(); err != nil {
		hts.Unlock()
		return fmt.Errorf("error closing index: %w", err)
	}
	hts.Unlock()

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
