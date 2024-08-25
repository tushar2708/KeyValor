package lsmtree

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"KeyValor/constants"
	"KeyValor/internal/records"
	"KeyValor/internal/storage/storagecommon"
	"KeyValor/internal/storage/wal"
	"KeyValor/internal/treemapgen"
	"KeyValor/internal/utils/fileutils"

	"github.com/emirpasic/gods/utils"
)

func (lts *LSMTreeStorage) getAndValidateMuLocked(key string) ([]byte, error) {
	record, err := lts.get(key)
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

func (lts *LSMTreeStorage) get(key string) (storagecommon.DataRecord, error) {
	/*
	   meta, err := lts.keyLocationIndex.Get(key)

	   	if err != nil {
	   		return storagecommon.Record{}, err
	   	}

	   file, err := lts.getAppropriateFile(meta)

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
	*/
	return storagecommon.DataRecord{}, nil
}

func (lts *LSMTreeStorage) getAppropriateFile(meta storagecommon.Meta) (*wal.WriteAheadLogRWFile, error) {
	/*
		if meta.FileID == lts.ActiveWALFile.ID() {
			return lts.ActiveWALFile, nil
		}
		file, ok := lts.oldWALFilesMap[meta.FileID]
		if !ok {
			return nil, constants.ErrWalFileNotFound
		}

		return file, nil
	*/
	return nil, nil
}

func (lts *LSMTreeStorage) set(
	wlFile *wal.WriteAheadLogRWFile,
	key string,
	value []byte,
	expiryTime *time.Time,
) error {

	// cmdHeader := NewCommandHeader(Set, key, value)
	cmdRecord := records.NewSetCommandRecord(key, value)

	if expiryTime != nil {
		cmdRecord.Header.SetExpiry(expiryTime.UnixNano())
	}

	buf := lts.BufferPool.Get().(*bytes.Buffer)

	// return the buffer to the pool
	defer lts.BufferPool.Put(buf)

	// reset the buffer before returning
	defer buf.Reset()

	if err := cmdRecord.Encode(buf); err != nil {
		return err
	}

	// write (append) to the file
	_, err := wlFile.Write(buf.Bytes())
	if err != nil {
		return err
	}

	lts.activeMemTable.Put(key, cmdRecord)

	if lts.activeMemTable.Size() >= int(lts.Cfg.MaxActiveFileSize) {
		lts.rotateMemTableIndex()
		lts.persistPreviousMemtableToSSTable()
	}
	return nil
}

func (lts *LSMTreeStorage) rotateMemTableIndex() error {
	lts.Lock()
	defer lts.Unlock()

	lts.prevMemTableImmutable = lts.activeMemTable
	lts.activeMemTable = treemapgen.NewSerializableTreeMap[string, *records.CommandRecord](utils.StringComparator)

	lts.ActiveWALFile.Close()

	currentWalFilePath := filepath.Join(lts.Cfg.Directory, CURRENT_WAL_FILE_NAME)
	tempWalFilePath := filepath.Join(lts.Cfg.Directory, TEMPORARY_WAL_FILE_NAME)

	if fileutils.FileExists(tempWalFilePath) {
		if err := os.Remove(tempWalFilePath); err != nil {
			return fmt.Errorf("error removing old temporary WAL file: %w", err)
		}
	}

	// rename the temporary index file to the active index
	err := os.Rename(currentWalFilePath, tempWalFilePath)
	if err != nil {
		return fmt.Errorf("error renaming current WAL file: %w", err)
	}

	lts.ActiveWALFile, err = wal.NewWALFileWithPath(currentWalFilePath, 0, wal.WAL_MODE_WRITE_ONLY)
	if err != nil {
		return fmt.Errorf("error creating new active WAL file: %w", err)
	}

	// sync storage diretory to persist all the above changes
	// (especially file deletion and rename operations)
	err = fileutils.SyncFile(lts.Cfg.Directory)
	if err != nil {
		return fmt.Errorf("error syncing directory: %w", err)
	}
	return nil
}

func SSTFileName(directory string) string {
	return filepath.Join(directory, fmt.Sprintf(SSTABLE_FILE_NAME_FORMAT, time.Now().UnixNano()))
}

func (lts *LSMTreeStorage) persistPreviousMemtableToSSTable() error {

	sstFilePath := SSTFileName(lts.Cfg.Directory)
	// sstWalFile, err := wal.NewWALFileWithPath(sstFilePath, 0, wal.WAL_MODE_WRITE_ONLY)
	// if err != nil {
	// 	return nil, err
	// }
	ssTable, err := NewSSTableFromIndex(sstFilePath, SSTABLE_BATCH_SIZE, lts.prevMemTableImmutable)
	if err != nil {
		return err
	}

	lts.ssTables = append(lts.ssTables, ssTable)
	lts.prevMemTableImmutable = nil
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
