package KeyValor

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"KeyValor/internal/storage"
	"KeyValor/internal/utils/fileutils"
)

func (db *KeyValorDatabase) persistIndexFile() error {
	indexFilePath := filepath.Join(db.cfg.directory, storage.INDEX_FILENAME)
	if err := db.keyLocationIndex.DumpToFile(indexFilePath); err != nil {
		return fmt.Errorf("error encoding index file: %w", err)
	}
	return nil
}

func (db *KeyValorDatabase) FileRotationLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if err := db.maybeRotateActiveFile(); err != nil {
			return
		}
	}
}

func (db *KeyValorDatabase) maybeRotateActiveFile() error {

	db.Lock()
	defer db.Unlock()

	size, err := db.activeDataFile.Size()
	if err != nil {
		return err
	}

	if size < db.cfg.maxActiveFileSize {
		return nil
	}

	currentFileID := db.activeDataFile.ID()
	db.oldDatafilesMap[currentFileID] = db.activeDataFile

	// Create a new datafile.
	df, err := storage.NewDataFile(db.cfg.directory, currentFileID+1)
	if err != nil {
		return err
	}

	db.activeDataFile = df
	return nil
}

func (db *KeyValorDatabase) CompactionLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)

	for range ticker.C {
		func() {
			db.Lock()
			defer db.Unlock()

			// delete the expired keys from the index, and persist the index
			if err := db.deleteExpiredKeysFromIndex(); err != nil {
				return
			}

			// merge old files into a new temp file, and keep updating indexes
			if err := db.garbageCollectOldFilesDBMuLocked(); err != nil {
				return
			}

		}()
	}
}

func (db *KeyValorDatabase) deleteExpiredKeysFromIndex() error {
	for key := range db.keyLocationIndex {

		record, err := db.get(key)
		if err != nil {
			fmt.Printf("Error getting key(%s): %v", key, err)
			continue
		}

		if record.IsExpired() {
			err := db.Delete(key)
			if err != nil {
				fmt.Printf("unable to delete expired record: %v", err)
				continue
			}
		}
	}
	return nil
}

func (db *KeyValorDatabase) garbageCollectOldFilesDBMuLocked() error {

	tempMergedFilePath := filepath.Join(db.cfg.directory, storage.MERGED_DATA_FILE_NAME_FORMAT)

	/// move all the live records to a new file
	// force sync merged datafile
	err := db.mergeDataFiles(tempMergedFilePath)
	if err != nil {
		return err
	}

	// mergeDataFiles updates the indexes as it merges the files
	// so we need to persist those changes
	if err := db.persistIndexFile(); err != nil {
		return err
	}

	// close all the old datafiles
	// empty the old files map
	// delete old datafiles from disk
	err = db.cleanupOldFiles()
	if err != nil {
		return err
	}

	newActiveFilePath := filepath.Join(db.cfg.directory, fmt.Sprintf(storage.DATA_FILE_NAME_FORMAT, 0))

	// rename the temporary index file to the active index
	err = os.Rename(tempMergedFilePath, newActiveFilePath)
	if err != nil {
		return fmt.Errorf("error renaming temporary index file: %w", err)
	}

	// sync storage diretory to persist all the above changes
	// (especially file deletion and rename operations)
	err = fileutils.SyncFile(db.cfg.directory)
	if err != nil {
		return fmt.Errorf("error syncing directory: %w", err)
	}

	db.activeDataFile, err = storage.NewDataFileWithPath(newActiveFilePath, 0)
	if err != nil {
		return fmt.Errorf("error creating new active file handler: %w", err)
	}
	return nil
}

func (db *KeyValorDatabase) cleanupOldFiles() error {
	for _, dataFile := range db.oldDatafilesMap {
		if err := dataFile.Close(); err != nil {
			fmt.Printf("error closing datafile: %v", err)
			continue
		}
	}

	db.oldDatafilesMap = make(map[int]*storage.DataFile)

	deleteExistingDBFiles := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Base(path) == storage.LOCKFILE {
			return nil
		}

		if filepath.Ext(path) == storage.DATA_FILE_EXTENSION {
			err = os.Remove(path)
			if err != nil {
				return err
			}
		}

		return nil
	}

	err := filepath.Walk(db.cfg.directory, deleteExistingDBFiles)
	if err != nil {
		return err
	}
	return nil
}

func (db *KeyValorDatabase) mergeDataFiles(tempMergedFilePath string) error {
	mergeDatafile, err := storage.NewDataFileWithPath(tempMergedFilePath, 0)
	if err != nil {
		return err
	}

	for key := range db.keyLocationIndex {
		record, err := db.get(key)
		if err != nil {
			fmt.Printf("Error getting key(%s): %v", key, err)
			continue
		}

		expiryTimeUnixNano := record.Header.GetExpiry()
		if expiryTimeUnixNano == 0 {
			db.set(mergeDatafile, key, record.Value, nil)
			continue
		}

		var expiryTime = time.Unix(0, expiryTimeUnixNano)
		db.set(mergeDatafile, key, record.Value, &expiryTime)
	}

	err = mergeDatafile.Sync()
	if err != nil {
		return fmt.Errorf("error syncing temporary storage file: %w", err)
	}
	err = mergeDatafile.Close()
	if err != nil {
		return fmt.Errorf("error closing temporary storage file: %w", err)
	}
	return nil
}
