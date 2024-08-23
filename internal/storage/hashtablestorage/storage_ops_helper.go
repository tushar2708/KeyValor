package hashtablestorage

import (
	"bytes"
	"fmt"
	"time"

	"KeyValor/constants"
	"KeyValor/internal/storage/storagecommon"
)

func (ls *LshtStorage) getAndValidateMuLocked(key string) ([]byte, error) {
	record, err := ls.get(key)
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

func (ls *LshtStorage) get(key string) (storagecommon.Record, error) {
	meta, err := ls.keyLocationIndex.Get(key)
	if err != nil {
		return storagecommon.Record{}, err
	}

	file, err := ls.getAppropriateFile(meta)
	if err != nil {
		return storagecommon.Record{}, err
	}

	data, err := file.Read(meta.RecordOffset, meta.RecordSize)
	if err != nil {
		return storagecommon.Record{}, err
	}

	var header storagecommon.Header
	if err := header.Decode(data); err != nil {
		return storagecommon.Record{}, fmt.Errorf("error decoding record header: %w", err)
	}

	// structure of record :
	// <HEADER> | <VALUE>
	valueOffset := meta.RecordSize - int(header.GetValueSize())
	value := data[valueOffset:]

	record := storagecommon.Record{
		Header: header,
		Key:    key,
		Value:  value,
	}
	return record, nil
}

func (ls *LshtStorage) getAppropriateFile(meta storagecommon.Meta) (*storagecommon.WriteAheadLogFile, error) {
	if meta.FileID == ls.activeWALFile.ID() {
		return ls.activeWALFile, nil
	}
	file, ok := ls.oldWALFilesMap[meta.FileID]
	if !ok {
		return nil, constants.ErrWalFileNotFound
	}

	return file, nil
}

func (ls *LshtStorage) set(
	file *storagecommon.WriteAheadLogFile,
	key string,
	value []byte,
	expiryTime *time.Time,
) error {
	header := storagecommon.NewHeader(key, value)

	if expiryTime != nil {
		header.SetExpiry(expiryTime.UnixNano())
	}

	record := storagecommon.Record{
		Header: header,
		Key:    key,
		Value:  value,
	}

	buf := ls.bufferPool.Get().(*bytes.Buffer)

	// return the buffer to the pool
	defer ls.bufferPool.Put(buf)

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

	ls.keyLocationIndex.Put(key, storagecommon.Meta{
		Timestamp:    record.Header.GetTs(),
		FileID:       file.ID(),
		RecordOffset: file.GetCurrentOffset(),
		RecordSize:   len(buf.Bytes()),
	})
	return nil
}

func validateEntry(k string, val []byte) error {
	if len(k) == 0 {
		return constants.ErrKeyIsEmpty
	}

	if len(k) > constants.MaxKeySize {
		return constants.ErrKeyTooBig
	}

	if len(val) == 0 {
		return constants.ErrValueIsEmpty
	}

	if len(val) > constants.MaxValueSize {
		return constants.ErrValueTooBig
	}

	return nil
}
