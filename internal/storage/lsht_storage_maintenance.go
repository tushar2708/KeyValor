package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"KeyValor/internal/index"
	"KeyValor/internal/utils/fileutils"
)

func (ls *LshtStorage) persistIndexFile() error {
	indexFilePath := filepath.Join(ls.cfg.Directory, INDEX_FILENAME)
	if err := ls.keyLocationIndex.DumpToFile(indexFilePath); err != nil {
		return fmt.Errorf("error encoding index file: %w", err)
	}
	return nil
}

func (ls *LshtStorage) FileRotationLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if err := ls.maybeRotateActiveFile(); err != nil {
			return
		}
	}
}

func (ls *LshtStorage) maybeRotateActiveFile() error {

	ls.Lock()
	defer ls.Unlock()

	size, err := ls.activeWALFile.Size()
	if err != nil {
		return err
	}

	if size < ls.cfg.MaxActiveFileSize {
		return nil
	}

	currentFileID := ls.activeWALFile.ID()
	ls.oldWALFilesMap[currentFileID] = ls.activeWALFile

	// Create a new WAL file.
	df, err := NewWALFile(ls.cfg.Directory, currentFileID+1)
	if err != nil {
		return err
	}

	ls.activeWALFile = df
	return nil
}

func (ls *LshtStorage) CompactionLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)

	for range ticker.C {
		func() {
			ls.Lock()
			defer ls.Unlock()

			// delete the expired keys from the index, and persist the index
			if err := ls.deleteExpiredKeysFromIndex(); err != nil {
				return
			}

			// merge old files into a new temp file, and keep updating indexes
			if err := ls.garbageCollectOldFilesDBMuLocked(); err != nil {
				return
			}

		}()
	}
}

func (ls *LshtStorage) deleteExpiredKeysFromIndex() error {

	ls.keyLocationIndex.Map(func(key string, metaData index.Meta) error {
		record, err := ls.get(key)
		if err != nil {
			return fmt.Errorf("error getting key(%s): %w", key, err)
		}

		if record.IsExpired() {
			err := ls.Delete(key)
			if err != nil {
				return fmt.Errorf("unable to delete expired record: %v", err)
			}
		}
		return nil
	})

	return nil
}

func (ls *LshtStorage) garbageCollectOldFilesDBMuLocked() error {

	tempMergedFilePath := filepath.Join(ls.cfg.Directory, MERGED_WAL_FILE_NAME_FORMAT)

	/// move all the live records to a new file
	// force sync merged WAL file
	err := ls.mergeWalFiles(tempMergedFilePath)
	if err != nil {
		return err
	}

	// mergeWalFiles updates the indexes as it merges the files
	// so we need to persist those changes
	if err := ls.persistIndexFile(); err != nil {
		return err
	}

	// close all the old WAL files
	// empty the old files map
	// delete old WAL files from disk
	err = ls.cleanupOldFiles()
	if err != nil {
		return err
	}

	newActiveFilePath := filepath.Join(ls.cfg.Directory, fmt.Sprintf(WAL_FILE_NAME_FORMAT, 0))

	// rename the temporary index file to the active index
	err = os.Rename(tempMergedFilePath, newActiveFilePath)
	if err != nil {
		return fmt.Errorf("error renaming temporary index file: %w", err)
	}

	// sync storage diretory to persist all the above changes
	// (especially file deletion and rename operations)
	err = fileutils.SyncFile(ls.cfg.Directory)
	if err != nil {
		return fmt.Errorf("error syncing directory: %w", err)
	}

	ls.activeWALFile, err = NewWALFileWithPath(newActiveFilePath, 0)
	if err != nil {
		return fmt.Errorf("error creating new active file handler: %w", err)
	}
	return nil
}

func (ls *LshtStorage) cleanupOldFiles() error {
	for _, walFile := range ls.oldWALFilesMap {
		if err := walFile.Close(); err != nil {
			fmt.Printf("error closing wal file: %v", err)
			continue
		}
	}

	ls.oldWALFilesMap = make(map[int]*WriteAheadLogFile)

	deleteExistingDBFiles := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Base(path) == LOCKFILE {
			return nil
		}

		if filepath.Ext(path) == WAL_FILE_EXTENSION {
			err = os.Remove(path)
			if err != nil {
				return err
			}
		}

		return nil
	}

	err := filepath.Walk(ls.cfg.Directory, deleteExistingDBFiles)
	if err != nil {
		return err
	}
	return nil
}

func (ls *LshtStorage) mergeWalFiles(tempMergedFilePath string) error {
	mergeWalfile, err := NewWALFileWithPath(tempMergedFilePath, 0)
	if err != nil {
		return err
	}

	ls.keyLocationIndex.Map(func(key string, metaData index.Meta) error {
		record, err := ls.get(key)
		if err != nil {
			return fmt.Errorf("error getting key(%s): %v", key, err)
		}

		expiryTimeUnixNano := record.Header.GetExpiry()
		if expiryTimeUnixNano == 0 {
			ls.set(mergeWalfile, key, record.Value, nil)
			return nil
		}

		var expiryTime = time.Unix(0, expiryTimeUnixNano)
		return ls.set(mergeWalfile, key, record.Value, &expiryTime)
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
