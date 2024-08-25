package hashtable

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"KeyValor/internal/storage/datafile"
	"KeyValor/internal/storage/storagecommon"
	"KeyValor/internal/utils/fileutils"
)

func (hts *HashTableStorage) persistIndexFile() error {
	indexFilePath := filepath.Join(hts.Cfg.Directory, INDEX_FILENAME)
	if err := hts.keyLocationIndex.DumpToFile(indexFilePath); err != nil {
		return fmt.Errorf("error encoding index file: %w", err)
	}
	return nil
}

func (hts *HashTableStorage) FileRotationLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if err := hts.maybeRotateActiveFile(); err != nil {
			return
		}
	}
}

func (hts *HashTableStorage) maybeRotateActiveFile() error {

	hts.Lock()
	defer hts.Unlock()

	size, err := hts.ActiveDataFile.Size()
	if err != nil {
		return err
	}

	if size < hts.Cfg.MaxActiveFileSize {
		return nil
	}

	currentFileID := hts.ActiveDataFile.ID()
	hts.olddatafileFilesMap[currentFileID] = hts.ActiveDataFile

	// Create a new datafile file.
	df, err := datafile.NewAppendOnlyDataFileWithRandomReads(hts.Cfg.Directory, HASHTABLE_DATAFILE_NAME_FORMAT, currentFileID+1)
	if err != nil {
		return err
	}

	hts.ActiveDataFile = df
	return nil
}

func (hts *HashTableStorage) CompactionLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)

	for range ticker.C {
		func() {
			hts.Lock()
			defer hts.Unlock()

			// delete the expired keys from the index, and persist the index
			if err := hts.deleteExpiredKeysFromIndex(); err != nil {
				return
			}

			// merge old files into a new temp file, and keep updating indexes
			if err := hts.garbageCollectOldFilesDBMuLocked(); err != nil {
				return
			}

		}()
	}
}

func (hts *HashTableStorage) deleteExpiredKeysFromIndex() error {

	hts.keyLocationIndex.Map(func(key string, metaData storagecommon.Meta) error {
		record, err := hts.get(key)
		if err != nil {
			return fmt.Errorf("error getting key(%s): %w", key, err)
		}

		if record.IsExpired() {
			err := hts.Delete(key)
			if err != nil {
				return fmt.Errorf("unable to delete expired record: %v", err)
			}
		}
		return nil
	})

	return nil
}

func (hts *HashTableStorage) garbageCollectOldFilesDBMuLocked() error {

	tempMergedFilePath := filepath.Join(hts.Cfg.Directory, MERGED_WAL_FILE_NAME_FORMAT)

	/// move all the live records to a new file
	// force sync merged datafile file
	err := hts.mergedatafileFiles(tempMergedFilePath)
	if err != nil {
		return err
	}

	// mergedatafileFiles updates the indexes as it merges the files
	// so we need to persist those changes
	if err := hts.persistIndexFile(); err != nil {
		return err
	}

	// close all the old datafile files
	// empty the old files map
	// delete old datafile files from disk
	err = hts.cleanupOldFiles()
	if err != nil {
		return err
	}

	newActiveFilePath := filepath.Join(hts.Cfg.Directory, fmt.Sprintf(HASHTABLE_DATAFILE_NAME_FORMAT, 0))

	// rename the temporary index file to the active index
	err = os.Rename(tempMergedFilePath, newActiveFilePath)
	if err != nil {
		return fmt.Errorf("error renaming temporary index file: %w", err)
	}

	// sync storage diretory to persist all the above changes
	// (especially file deletion and rename operations)
	err = fileutils.SyncFile(hts.Cfg.Directory)
	if err != nil {
		return fmt.Errorf("error syncing directory: %w", err)
	}

	hts.ActiveDataFile, err = datafile.NewAppendOnlyDataFileWithRandomReadsWithPath(newActiveFilePath)
	if err != nil {
		return fmt.Errorf("error creating new active file handler: %w", err)
	}
	return nil
}

func (hts *HashTableStorage) cleanupOldFiles() error {
	for _, datafileFile := range hts.olddatafileFilesMap {
		if err := datafileFile.Close(); err != nil {
			fmt.Printf("error closing datafile file: %v", err)
			continue
		}
	}

	hts.olddatafileFilesMap = make(map[int]datafile.ReadOnlyWithRandomReads)

	deleteExistingDBFiles := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Base(path) == storagecommon.LOCKFILE {
			return nil
		}

		if filepath.Ext(path) == HASHTABLE_DATAFILE_EXTENSION {
			err = os.Remove(path)
			if err != nil {
				return err
			}
		}

		return nil
	}

	err := filepath.Walk(hts.Cfg.Directory, deleteExistingDBFiles)
	if err != nil {
		return err
	}
	return nil
}

func (hts *HashTableStorage) mergedatafileFiles(tempMergedFilePath string) error {
	mergedatafilefile, err := datafile.NewAppendOnlyDataFileWithPath(tempMergedFilePath)
	if err != nil {
		return err
	}

	hts.keyLocationIndex.Map(func(key string, metaData storagecommon.Meta) error {
		record, err := hts.get(key)
		if err != nil {
			return fmt.Errorf("error getting key(%s): %v", key, err)
		}

		expiryTimeUnixNano := record.Header.GetExpiry()
		if expiryTimeUnixNano == 0 {
			hts.set(mergedatafilefile, key, record.Value, nil)
			return nil
		}

		var expiryTime = time.Unix(0, expiryTimeUnixNano)
		return hts.set(mergedatafilefile, key, record.Value, &expiryTime)
	})

	err = mergedatafilefile.Sync()
	if err != nil {
		return fmt.Errorf("error syncing temporary storage file: %w", err)
	}
	err = mergedatafilefile.Close()
	if err != nil {
		return fmt.Errorf("error closing temporary storage file: %w", err)
	}
	return nil
}
