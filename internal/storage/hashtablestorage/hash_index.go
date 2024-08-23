package hashtablestorage

import (
	"encoding/gob"
	"fmt"
	"os"

	"KeyValor/constants"
	"KeyValor/internal/storage/storagecommon"
)

// LogStructuredHashTableIndex implements a Log structured HashMap's index
type LogStructuredHashTableIndex struct {
	hashMap map[string]storagecommon.Meta
}

func NewLogStructuredHashTableIndex() *LogStructuredHashTableIndex {
	return &LogStructuredHashTableIndex{
		hashMap: make(map[string]storagecommon.Meta),
	}
}

func (lski *LogStructuredHashTableIndex) Get(key string) (storagecommon.Meta, error) {
	val, ok := lski.hashMap[key]
	if !ok {
		return storagecommon.Meta{}, constants.ErrKeyMissing
	}
	return val, nil
}

func (lski *LogStructuredHashTableIndex) Put(key string, metaData storagecommon.Meta) error {
	lski.hashMap[key] = metaData
	return nil
}

func (lski *LogStructuredHashTableIndex) Delete(key string) error {
	delete(lski.hashMap, key)
	return nil
}

// LoadFromFile reads the LogStructuredKeyIndex from a file specified by filePath.
// The file is expected to contain a serialized LogStructuredKeyIndex using the gob encoding format.
//
// filePath: The path to the file from which to load the LogStructuredKeyIndex.
//
// Returns:
// - An error if the file cannot be opened, or if there is an error decoding the file contents.
// - nil if the LogStructuredKeyIndex is successfully loaded from the file.
func (lsi *LogStructuredHashTableIndex) LoadFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	return decoder.Decode(&lsi.hashMap)
}

// DumpToFile serializes the LogStructuredKeyIndex to a file specified by filePath using the gob encoding format.
//
// filePath: The path to the file where the LogStructuredKeyIndex will be saved.
//
// Returns:
// - An error if the file cannot be created, or if there is an error encoding the LogStructuredKeyIndex into the file.
// - nil if the LogStructuredKeyIndex is successfully serialized and saved to the file.
func (lsi *LogStructuredHashTableIndex) DumpToFile(filePath string) error {
	file, err := os.Create(filePath)

	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	return encoder.Encode(lsi.hashMap)
}

func (lsi *LogStructuredHashTableIndex) Map(f func(key string, metaData storagecommon.Meta) error) {
	for key, value := range lsi.hashMap {
		if err := f(key, value); err != nil {
			fmt.Printf("error in Map, err: %v", err)
		}
	}
}
