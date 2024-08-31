package lsmtree

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"KeyValor/constants"
	"KeyValor/internal/records"
	"KeyValor/internal/storage/datafile"
	"KeyValor/internal/storage/storagecommon"
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

	lts.RLock()
	defer lts.RUnlock()

	// 1. first try finding the key in the active memTable
	command, found := lts.activeMemTable.Get(key)
	if found && command != nil {
		return handleFoundCommand(command)
	}

	// 2. first try finding the key in the previous (now immutable) memTable
	command, found = lts.prevMemTableImmutable.Get(key)
	if found && command != nil {
		return handleFoundCommand(command)
	}

	// 3. TODO: Check in a bloom filter to know if we have ever seen this key

	// 4. Check in all the SSTables
	var err error

	for _, ssTable := range lts.ssTables {
		command, err = ssTable.Query(key)
		if err == nil && command != nil {
			break
		}
	}

	if err == nil && command != nil {
		return handleFoundCommand(command)
	}

	return storagecommon.DataRecord{}, constants.ErrKeyMissing
}

func handleFoundCommand(command *records.CommandRecord) (storagecommon.DataRecord, error) {
	data, err := commandToDataRecord(command)
	if err == nil {
		return *data, nil
	}
	if err == constants.ErrKeyIsDeleted {
		return storagecommon.DataRecord{}, constants.ErrKeyMissing
	}

	return storagecommon.DataRecord{}, err
}

func commandToDataRecord(command *records.CommandRecord) (*storagecommon.DataRecord, error) {

	if command == nil {
		return nil, constants.ErrKeyMissing
	}

	if command.Header.CmdType == records.Del {
		return nil, constants.ErrKeyIsDeleted
	}

	if command.Header.CmdType == records.Set {
		return &storagecommon.DataRecord{
			Header: storagecommon.Header{
				Crc:     0,
				Ts:      0,
				Expiry:  command.Header.GetExpiry(),
				KeySize: command.Header.KeySize,
				ValSize: command.Header.ValSize,
			},
			Key:   command.Key,
			Value: command.Value,
		}, nil
	}
	panic(fmt.Sprintf("invalid command found, cmd:[%+v]", command))
}

func (lts *LSMTreeStorage) set(
	wlFile datafile.AppendOnlyFile,
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

	lts.ActiveWALFile, err = datafile.NewAppendOnlyDataFileWithPath(currentWalFilePath)
	if err != nil {
		return fmt.Errorf("error creating new active WAL file: %w", err)
	}

	err = lts.persistMemtableToSSTable(lts.prevMemTableImmutable)
	if err != nil {
		return fmt.Errorf("failed to persist immutable memtable to SSTable: %w", err)
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

func (lts *LSMTreeStorage) persistMemtableToSSTable(memTable *treemapgen.SerializableTreeMap[string, *records.CommandRecord]) error {

	sstFilePath := SSTFileName(lts.Cfg.Directory)
	// sstWalFile, err := wal.NewWALFileWithPath(sstFilePath, 0, wal.WAL_MODE_WRITE_ONLY)
	// if err != nil {
	// 	return nil, err
	// }
	ssTable, err := NewSSTableFromIndex(sstFilePath, SSTABLE_BATCH_SIZE, memTable)
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
