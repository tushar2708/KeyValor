package KeyValor

import (
	"bytes"
	"fmt"
	"time"

	"KeyValor/constants"
	"KeyValor/internal/index"
	"KeyValor/internal/storage"
)

func (db *KeyValorDatabase) getAndValidateMuLocked(key string) ([]byte, error) {
	record, err := db.get(key)
	if err != nil {
		return nil, err
	}

	if record.IsExpired() {
		return nil, constants.ErrKeyIsExpired
	}

	if !record.IsChecksumValid() {
		return nil, constants.ErrChecksumIsInvalid
	}

	return record.Value, nil
}

func (db *KeyValorDatabase) get(key string) (storage.Record, error) {
	meta, ok := db.keyLocationIndex[key]
	if !ok {
		return storage.Record{}, constants.ErrKeyMissing
	}

	file, err := db.getAppropriateFile(meta)
	if err != nil {
		return storage.Record{}, err
	}

	data, err := file.Read(meta.RecordOffset, meta.RecordSize)
	if err != nil {
		return storage.Record{}, err
	}

	var header storage.Header
	if err := header.Decode(data); err != nil {
		return storage.Record{}, fmt.Errorf("error decoding record header: %w", err)
	}

	// structure of record :
	// <HEADER> | <VALUE>
	valueOffset := meta.RecordSize - int(header.GetValueSize())
	value := data[valueOffset:]

	record := storage.Record{
		Header: header,
		Key:    key,
		Value:  value,
	}
	return record, nil
}

func (db *KeyValorDatabase) getAppropriateFile(meta index.Meta) (*storage.DataFile, error) {
	if meta.FileID == db.activeDataFile.ID() {
		return db.activeDataFile, nil
	}
	file, ok := db.oldDatafilesMap[meta.FileID]
	if !ok {
		return nil, constants.ErrDataFileNotFound
	}

	return file, nil
}

func (db *KeyValorDatabase) set(
	file *storage.DataFile,
	key string,
	value []byte,
	expiryTime *time.Time,
) error {
	header := storage.NewHeader(key, value)

	if expiryTime != nil {
		header.SetExpiry(expiryTime.UnixNano())
	}

	record := storage.Record{
		Header: header,
		Key:    key,
		Value:  value,
	}

	buf := db.bufferPool.Get().(*bytes.Buffer)

	// return the buffer to the pool
	defer db.bufferPool.Put(buf)

	// reset the buffer before returning
	defer buf.Reset()

	if err := record.Encode(buf); err != nil {
		return err
	}

	// write (append) to the file
	_, err := file.Write(buf.Bytes())
	if err != nil {
		return err
	}

	db.keyLocationIndex[key] = index.Meta{
		Timestamp:    record.Header.GetTs(),
		FileID:       file.ID(),
		RecordOffset: file.GetCurrentOffset(),
		RecordSize:   len(buf.Bytes()),
	}
	return nil
}
