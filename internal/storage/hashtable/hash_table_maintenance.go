package hashtable

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"KeyValor/internal/storage/storagecommon"
	"KeyValor/internal/storage/wal"
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

	size, err := hts.ActiveWALFile.Size()
	if err != nil {
		return err
	}

	if size < hts.Cfg.MaxActiveFileSize {
		return nil
	}

	currentFileID := hts.ActiveWALFile.ID()
	hts.oldWALFilesMap[currentFileID] = hts.ActiveWALFile

	// Create a new WAL file.
	df, err := wal.NewWALFile(hts.Cfg.Directory, currentFileID+1, wal.WAL_MODE_READ_WRITE)
	if err != nil {
		return err
	}

	hts.ActiveWALFile = df
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
	// force sync merged WAL file
	err := hts.mergeWalFiles(tempMergedFilePath)
	if err != nil {
		return err
	}

	// mergeWalFiles updates the indexes as it merges the files
	// so we need to persist those changes
	if err := hts.persistIndexFile(); err != nil {
		return err
	}

	// close all the old WAL files
	// empty the old files map
	// delete old WAL files from disk
	err = hts.cleanupOldFiles()
	if err != nil {
		return err
	}

	newActiveFilePath := filepath.Join(hts.Cfg.Directory, fmt.Sprintf(wal.WAL_FILE_NAME_FORMAT, 0))

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

	hts.ActiveWALFile, err = wal.NewWALFileWithPath(newActiveFilePath, 0, wal.WAL_MODE_READ_WRITE)
	if err != nil {
		return fmt.Errorf("error creating new active file handler: %w", err)
	}
	return nil
}

func (hts *HashTableStorage) cleanupOldFiles() error {
	for _, walFile := range hts.oldWALFilesMap {
		if err := walFile.Close(); err != nil {
			fmt.Printf("error closing wal file: %v", err)
			continue
		}
	}

	hts.oldWALFilesMap = make(map[int]*wal.WriteAheadLogRWFile)

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

		if filepath.Ext(path) == wal.WAL_FILE_EXTENSION {
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

func (hts *HashTableStorage) mergeWalFiles(tempMergedFilePath string) error {
	mergeWalfile, err := wal.NewWALFileWithPath(tempMergedFilePath, 0, wal.WAL_MODE_WRITE_ONLY)
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
			hts.set(mergeWalfile, key, record.Value, nil)
			return nil
		}

		var expiryTime = time.Unix(0, expiryTimeUnixNano)
		return hts.set(mergeWalfile, key, record.Value, &expiryTime)
	})

	err = mergeWalfile.Sync()
	if err != nil {
		return fmt.Errorf("error syncing temporary storage file: %w", err)
	}
	err = mergeWalfile.Close()
	if err != nil {
		return fmt.Errorf("error closing temporary storage file: %w", err)
	}
	return nil
}
