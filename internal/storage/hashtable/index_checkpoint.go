package hashtable

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"

	"KeyValor/constants"
	"KeyValor/internal/storage/storagecommon"
	"KeyValor/log"
)

// CheckpointIndex is a Strategy-1 index: periodic gob snapshots.
// It owns its file path; callers never pass paths.
type CheckpointIndex struct {
	hashMap       map[string]storagecommon.Meta
	indexFilePath string
}

func NewCheckpointIndex(indexFilePath string) *CheckpointIndex {
	return &CheckpointIndex{
		hashMap:       make(map[string]storagecommon.Meta),
		indexFilePath: indexFilePath,
	}
}

// Open loads the index from disk if the snapshot file exists.
// If the file does not exist, Open is a no-op and returns nil
// (the engine will rebuild the index from WAL files on next compaction,
// or start with an empty index on a fresh run).
func (ci *CheckpointIndex) Open() error {
	if _, err := os.Stat(ci.indexFilePath); os.IsNotExist(err) {
		return nil
	}

	file, err := os.Open(ci.indexFilePath)
	if err != nil {
		return fmt.Errorf("error opening index file: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&ci.hashMap); err != nil {
		return fmt.Errorf("error decoding index file: %w", err)
	}
	return nil
}

// Flush atomically writes the index to disk using a temp-file + rename.
// This guarantees that a crash during Flush does not corrupt the last
// good snapshot.
func (ci *CheckpointIndex) Flush() error {
	tmpPath := ci.indexFilePath + ".tmp"

	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("error creating temp index file: %w", err)
	}

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(ci.hashMap); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("error encoding index: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("error syncing temp index file: %w", err)
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("error closing temp index file: %w", err)
	}

	if err := os.Rename(tmpPath, ci.indexFilePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("error renaming temp index file: %w", err)
	}

	// Sync the directory so the rename is durable.
	dir := filepath.Dir(ci.indexFilePath)
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("error syncing index directory: %w", err)
	}

	return nil
}

// Close is a no-op for Strategy 1 (the periodic flush loop handles persistence).
// A final Flush is called by hash_table.go Close() before this.
func (ci *CheckpointIndex) Close() error {
	return nil
}

func (ci *CheckpointIndex) Get(key string) (storagecommon.Meta, error) {
	val, ok := ci.hashMap[key]
	if !ok {
		return storagecommon.Meta{}, constants.ErrKeyMissing
	}
	return val, nil
}

func (ci *CheckpointIndex) Put(key string, metaData storagecommon.Meta) error {
	ci.hashMap[key] = metaData
	return nil
}

func (ci *CheckpointIndex) Delete(key string) error {
	delete(ci.hashMap, key)
	return nil
}

func (ci *CheckpointIndex) Map(f func(key string, metaData storagecommon.Meta) error) {
	for key, value := range ci.hashMap {
		if err := f(key, value); err != nil {
			log.Errorf("error in Map, err: %v", err)
		}
	}
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
