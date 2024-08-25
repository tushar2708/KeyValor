package lsmtree

import (
	"KeyValor/internal/records"
	"KeyValor/internal/storage/datafile"
	"KeyValor/internal/treemapgen"
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/emirpasic/gods/utils"
)

type SSTable struct {
	tableFilePath string
	tableWalFile  *datafile.ReadWriteDataFile                                      // underlying file in which sstable is stored
	metaData      *SSTableMetaData                                                 // metadata about SSTable regions
	sparseIndex   *treemapgen.SerializableTreeMap[string, *records.PositionRecord] // sparse index of the keys in SSTable [key (string) -> Disk (Position)]
	BufferPool    sync.Pool                                                        // crate an object pool to reuse buffers
}

func NewSSTable(filePath string, partSize int) (*SSTable, error) {

	metaData := &SSTableMetaData{
		Version:          0,
		BatchSize:        int64(partSize),
		DataStartOffset:  0,
		DataSize:         0,
		IndexStartOffset: 0,
		IndexSize:        0,
	}

	sst := &SSTable{
		tableFilePath: filePath,
		metaData:      metaData,
		BufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer([]byte{})
			},
		},
	}

	// sst.sparseIndex = *treemap.NewWithStringComparator()
	sst.sparseIndex = treemapgen.NewSerializableTreeMap[string, *records.PositionRecord](utils.StringComparator)

	var err error
	sst.tableWalFile, err = datafile.NewDataFileWithPath(filePath, int(time.Now().UnixMilli()), datafile.DF_MODE_WRITE_ONLY)
	if err != nil {
		return nil, err
	}

	return sst, nil
}

func NewSSTableFromIndex(filePath string, partSize int, memTable *treemapgen.SerializableTreeMap[string, *records.CommandRecord]) (*SSTable, error) {
	sst, err := NewSSTable(filePath, partSize)
	if err != nil {
		return nil, err
	}

	err = sst.populateFromIndex(memTable)
	if err != nil {
		return nil, fmt.Errorf("failed to populate SSTable from index %w", err)
	}

	return sst, err
}

func NewSSTableLoadedFromFile(filePath string) (*SSTable, error) {
	sst, err := NewSSTable(filePath, 0) // partSize not relevant for loading from disk
	if err != nil {
		return nil, err
	}

	sst.tableWalFile, err = datafile.NewDataFileWithPath(filePath, int(time.Now().UnixMilli()), datafile.DF_MODE_READ_ONLY)
	if err != nil {
		return nil, err
	}

	return sst, nil

}

func (sst *SSTable) populateFromIndex(memTable *treemapgen.SerializableTreeMap[string, *records.CommandRecord]) error {
	// Implement the logic to populate the sparseIndex from the memTable
	// ...
	// commandBatch := make(map[string]*CommandRecord)
	commandBatch := make([]*records.CommandRecord, sst.metaData.BatchSize)

	mtIter := memTable.Iterator()
	i := 0
	for mtIter.Next() {

		command := mtIter.Value()

		commandBatch[i] = command
		i += 1

		if len(commandBatch) >= int(sst.metaData.BatchSize) {
			err := sst.writeBatch(commandBatch)
			if err != nil {
				return fmt.Errorf("failed to write batch %w", err)
			}
			commandBatch = make([]*records.CommandRecord, sst.metaData.BatchSize)
		}
	}

	if len(commandBatch) > 0 {
		err := sst.writeBatch(commandBatch)
		if err != nil {
			return fmt.Errorf("failed to write batch %w", err)
		}
	}

	dataRegionEndCursor := sst.tableWalFile.GetCurrentWriteOffset()
	dataRegionLength := dataRegionEndCursor - sst.metaData.DataStartOffset
	sst.metaData.DataSize = dataRegionLength
	sst.metaData.IndexStartOffset = dataRegionEndCursor

	// sst.sparseIndex = sst.metaData.

	return nil
}

func (sst *SSTable) writeBatch(commandBatch records.CommandBatch) error {
	startOfBAtch, err := sst.tableWalFile.Size()
	if err != nil {
		return fmt.Errorf("failed to get current write position %w", err)
	}

	buf := sst.BufferPool.Get().(*bytes.Buffer)

	// return the buffer to the pool
	defer sst.BufferPool.Put(buf)

	// reset the buffer before returning
	defer buf.Reset()

	err = commandBatch.Encode(buf)
	if err != nil {
		return err
	}

	// write to file
	_, err = sst.tableWalFile.Write(buf.Bytes())
	if err != nil {
		return err
	}

	firstKey := commandBatch[0].Key

	position := &records.Position{
		Start: startOfBAtch,
		Size:  int64(buf.Len()),
	}

	posRecord, err := records.NewPositionRecord(firstKey, position)
	if err != nil {
		return err
	}

	sst.sparseIndex.Put(firstKey, posRecord)

	// (&commandBatch).Clear()
	commandBatch = make(records.CommandBatch, commandBatch.Len())

	return nil
}
