package index

import (
	"encoding/gob"
	"os"

	"KeyValor/constants"
)

// LogStructuredKeyIndex implements a Log structured HashMap's index
type LogStructuredKeyIndex map[string]Meta

func (lski LogStructuredKeyIndex) Get(key string) (Meta, error) {
	val, ok := lski[key]
	if !ok {
		return Meta{}, constants.ErrKeyMissing
	}
	return val, nil
}

func (lski LogStructuredKeyIndex) Put(key string, metaData Meta) {
	lski[key] = metaData
}

type Meta struct {
	Timestamp int64

	// path to record
	FileID       int
	RecordOffset int
	RecordSize   int
}

// LoadFromFile reads the LogStructuredKeyIndex from a file specified by filePath.
// The file is expected to contain a serialized LogStructuredKeyIndex using the gob encoding format.
//
// filePath: The path to the file from which to load the LogStructuredKeyIndex.
//
// Returns:
// - An error if the file cannot be opened, or if there is an error decoding the file contents.
// - nil if the LogStructuredKeyIndex is successfully loaded from the file.
func (lsi *LogStructuredKeyIndex) LoadFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	return decoder.Decode(lsi)
}

// DumpToFile serializes the LogStructuredKeyIndex to a file specified by filePath using the gob encoding format.
//
// filePath: The path to the file where the LogStructuredKeyIndex will be saved.
//
// Returns:
// - An error if the file cannot be created, or if there is an error encoding the LogStructuredKeyIndex into the file.
// - nil if the LogStructuredKeyIndex is successfully serialized and saved to the file.
func (lsi *LogStructuredKeyIndex) DumpToFile(filePath string) error {
	file, err := os.Create(filePath)

	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	return encoder.Encode(lsi)
}
