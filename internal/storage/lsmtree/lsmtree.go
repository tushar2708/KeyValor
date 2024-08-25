package lsmtree

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/emirpasic/gods/utils"

	"KeyValor/config"
	"KeyValor/internal/records"
	"KeyValor/internal/storage/datafile"
	"KeyValor/internal/storage/storagecommon"
	"KeyValor/internal/treemapgen"
)

type LSMTreeStorage struct {
	*storagecommon.CommonStorage
	bufferPool sync.Pool // crate an object pool to reuse buffers

	ActiveWALFile datafile.AppendOnlyFile

	activeMemTable        *treemapgen.SerializableTreeMap[string, *records.CommandRecord]
	prevMemTableImmutable *treemapgen.SerializableTreeMap[string, *records.CommandRecord]

	ssTables []*SSTable
}

func NewLSMTreeStorage(cfg *config.DBCfgOpts) (*LSMTreeStorage, error) {

	memTable := treemapgen.NewSerializableTreeMap[string, *records.CommandRecord](utils.StringComparator)

	// create cs with nil WAL file for now
	cs, err := storagecommon.NewCommonStorage(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating common storage: %w", err)
	}

	lsmTree := &LSMTreeStorage{
		CommonStorage: cs,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 1024)
			},
		},
		activeMemTable:        memTable,
		ssTables:              make([]*SSTable, 0),
		prevMemTableImmutable: nil,
	}

	// iterate over all the files in the directory
	files, err := os.ReadDir(cfg.Directory)
	if err != nil {
		fmt.Printf("Error reading directory: %v\n", err)
		return nil, err
	}

	if len(files) == 0 {
		// we have an empty directory
		currentWalFilePath := filepath.Join(cfg.Directory, CURRENT_WAL_FILE_NAME)
		lsmTree.ActiveWALFile, err = datafile.NewAppendOnlyDataFileWithPath(currentWalFilePath)
		if err != nil {
			return nil, err
		}
	} else {
		// load existing files from the directory
		lsmTree.processExistingFiles(files)
	}

	return &LSMTreeStorage{
		CommonStorage: cs,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 1024)
			},
		},
		activeMemTable:        memTable,
		prevMemTableImmutable: nil,
	}, nil
}

func (lsmt *LSMTreeStorage) processExistingFiles(files []fs.DirEntry) error {

	for _, dirEntry := range files {
		if dirEntry.IsDir() {
			fmt.Printf("found a directory, skipping: %s\n", dirEntry.Name())
			continue
		}

		fileName := dirEntry.Name()
		filePath := filepath.Join(lsmt.Cfg.Directory, fileName)

		// ssTableTreeMap := treemap.NewWithStringComparator()
		ssTableTreeMap := treemapgen.NewTreeMap[int64, *SSTable](utils.Int64Comparator)

		if fileName == TEMPORARY_WAL_FILE_NAME {
			// load the treemap from the WAL file
			err := lsmt.restoreMemtableFromWalFile(filePath)
			if err != nil {
				continue
			}
		} else if fileName == CURRENT_WAL_FILE_NAME {
			err := lsmt.restoreMemtableFromWalFile(filePath)
			if err != nil {
				continue
			}
			lsmt.ActiveWALFile, err = datafile.NewAppendOnlyDataFileWithPath(filePath)
			if err != nil {
				continue
			}
		} else if filepath.Ext(filePath) == SSTABLE_FILE_EXTENSION {
			//it's an SST file (SSTable)
			fileNumber := strings.TrimPrefix(strings.TrimSuffix(filepath.Base(filePath), SSTABLE_FILE_EXTENSION), SSTABLE_FILE_PREFIX)
			timeStamp, err := strconv.ParseInt(fileNumber, 10, 32)
			if err != nil {
				fmt.Printf("Error fetching timestamp from SST file, error: %w", err)
				continue
			}

			ssTable, err := NewSSTableLoadedFromFile(filePath)
			if err != nil {
				fmt.Printf("Error loading SSTable from sst file: %w", err)
				continue
			}
			ssTableTreeMap.Put(timeStamp, ssTable)
		}
	}
	return nil
}

func (lsmt *LSMTreeStorage) restoreMemtableFromWalFile(filePath string) error {

	walFile, err := datafile.NewReadOnlyDataFileWithRandomReadsWithPath(filePath)
	if err != nil {
		return err
	}
	defer walFile.Close()

	fileLen, err := walFile.Size()
	if err != nil {
		return err
	}

	var currentPos int64 = 0

	encoder := records.NewRecordEncoder[string, *records.CommandHeader, *records.CommandRecord]()

	for currentPos < fileLen {

		var cmdRecord records.CommandRecord

		err = encoder.DecodeF(&cmdRecord, walFile)
		if err != nil {
			return fmt.Errorf("unexpected error decoding command record: %w", err)
		}

		lsmt.activeMemTable.Put(cmdRecord.Key, &cmdRecord)

		currentPos = walFile.GetCurrentReadOffset()
	}
	return nil
}
