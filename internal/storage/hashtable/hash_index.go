package hashtable

import (
	"encoding/gob"
	"fmt"
	"os"

	"KeyValor/constants"
	"KeyValor/internal/storage/storagecommon"
)

// HashTableIndex implements a Log structured HashMap's index
type HashTableIndex struct {
	hashMap map[string]storagecommon.Meta
}

func NewLogStructuredHashTableIndex() *HashTableIndex {
	return &HashTableIndex{
		hashMap: make(map[string]storagecommon.Meta),
	}
}

func (hti *HashTableIndex) Get(key string) (storagecommon.Meta, error) {
	val, ok := hti.hashMap[key]
	if !ok {
		return storagecommon.Meta{}, constants.ErrKeyMissing
	}
	return val, nil
}

func (hti *HashTableIndex) Put(key string, metaData storagecommon.Meta) error {
	hti.hashMap[key] = metaData
	return nil
}

func (hti *HashTableIndex) Delete(key string) error {
	delete(hti.hashMap, key)
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
func (lsi *HashTableIndex) LoadFromFile(filePath string) error {
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
func (lsi *HashTableIndex) DumpToFile(filePath string) error {
	file, err := os.Create(filePath)

	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	return encoder.Encode(lsi.hashMap)
}

func (lsi *HashTableIndex) Map(f func(key string, metaData storagecommon.Meta) error) {
	for key, value := range lsi.hashMap {
		if err := f(key, value); err != nil {
			fmt.Printf("error in Map, err: %v", err)
		}
	}
}
