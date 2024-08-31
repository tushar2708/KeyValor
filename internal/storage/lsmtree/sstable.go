package lsmtree

import (
	"KeyValor/constants"
	"KeyValor/internal/records"
	"KeyValor/internal/storage/datafile"
	"KeyValor/internal/treemapgen"
	"KeyValor/log"
	"bytes"
	"fmt"
	"sync"

	"github.com/emirpasic/gods/utils"
)

type SSTable struct {
	tableFilePath   string
	activeSstFile   datafile.AppendOnlyWithRandomReads // underlying file in which sstable is stored
	readOnlySstFile datafile.ReadOnlyWithRandomReads   // underlying file in which sstable is stored
	metaData        *SSTableMetaData                   // metadata about SSTable regions

	sparseIndex *treemapgen.SerializableTreeMap[string, *records.PositionRecord] // sparse index of the keys in SSTable [key (string) -> Disk (Position)]
	BufferPool  sync.Pool                                                        // crate an object pool to reuse buffers
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
	sst.activeSstFile, err = datafile.NewAppendOnlyDataFileWithRandomReadsWithPath(filePath)
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

	sst.readOnlySstFile, err = datafile.NewReadOnlyDataFileWithRandomReadsWithPath(filePath)
	if err != nil {
		return nil, err
	}

	sst.metaData.ReadFromFile(sst.readOnlySstFile)

	log.Debugf("loaded SST metadada from file %s, %+v", filePath, sst.metaData)

	indexBytes := make([]byte, sst.metaData.IndexSize)
	n, err := sst.readOnlySstFile.ReadAt(indexBytes, sst.metaData.IndexStartOffset)
	if err != nil || n != int(sst.metaData.IndexSize) {
		return nil, fmt.Errorf("couldn't read sparse index from SST file, error: %v", err)
	}

	err = sst.sparseIndex.Decode(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("error decoding sparse index read from SST file: %v", err)
	}

	// We don't load the actual data into the SSSTable structure.
	// As we have the index loaded into the memory, we can always fetch
	// the desired records from the sst.readOnlySstFile

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
			err := sst.writeCommandBatch(commandBatch)
			if err != nil {
				return fmt.Errorf("failed to write batch %w", err)
			}
			commandBatch = make([]*records.CommandRecord, sst.metaData.BatchSize)
		}
	}

	if len(commandBatch) > 0 {
		err := sst.writeCommandBatch(commandBatch)
		if err != nil {
			return fmt.Errorf("failed to write batch %w", err)
		}
	}

	dataRegionEndCursor := sst.activeSstFile.GetCurrentWriteOffset()
	dataRegionLength := dataRegionEndCursor - sst.metaData.DataStartOffset
	sst.metaData.DataSize = dataRegionLength
	sst.metaData.IndexStartOffset = dataRegionEndCursor

	err := sst.writeSparseIndex(sst.sparseIndex)
	if err != nil {
		return err
	}

	err = sst.writeSSTableMetadata(sst.metaData)
	if err != nil {
		return err
	}

	return nil
}

func (sst *SSTable) writeCommandBatch(commandBatch records.CommandBatch) error {
	startOfBAtch, err := sst.activeSstFile.Size()
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
	_, err = sst.activeSstFile.Write(buf.Bytes())
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

func (sst *SSTable) writeSparseIndex(sparseIndex *treemapgen.SerializableTreeMap[string, *records.PositionRecord]) error {

	startOfIndex, err := sst.activeSstFile.Size()
	if err != nil {
		return fmt.Errorf("failed to get current write position %w", err)
	}

	buf := sst.BufferPool.Get().(*bytes.Buffer)

	// return the buffer to the pool
	defer sst.BufferPool.Put(buf)

	// reset the buffer before returning
	defer buf.Reset()

	err = sparseIndex.Encode(buf)
	if err != nil {
		return err
	}

	// write to file
	_, err = sst.activeSstFile.Write(buf.Bytes())
	if err != nil {
		return err
	}

	endOfIndex, err := sst.activeSstFile.Size()
	if err != nil {
		return fmt.Errorf("failed to get current write position %w", err)
	}

	indexRegionLen := endOfIndex - startOfIndex

	sst.metaData.IndexStartOffset = startOfIndex
	sst.metaData.IndexSize = indexRegionLen

	return nil
}

func (sst *SSTable) writeSSTableMetadata(metaData *SSTableMetaData) error {
	buf := sst.BufferPool.Get().(*bytes.Buffer)

	// return the buffer to the pool
	defer sst.BufferPool.Put(buf)

	// reset the buffer before returning
	defer buf.Reset()

	err := metaData.Encode(buf)
	if err != nil {
		return err
	}

	// write to file
	_, err = sst.activeSstFile.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (sst *SSTable) Query(key string) (*records.CommandRecord, error) {
	// TODO: implement SSTable query
	// find lower and upper bound of the key in the sparse index of the SSTable

	minKey, _ := sst.sparseIndex.Min()
	maxKey, _ := sst.sparseIndex.Max()

	if key < minKey || key > maxKey {
		return nil, constants.ErrKeyNotPresentInSSTable
	}

	_, lowerBoundPosRecord := sst.sparseIndex.Floor(key)
	_, upperBoundPosRecord := sst.sparseIndex.Ceiling(key)

	var lowerboundPosition = records.Position{}
	var upperboundPosition = records.Position{}
	err := lowerboundPosition.Decode(lowerBoundPosRecord.Value)
	if err != nil {
		return nil, err
	}
	err = upperboundPosition.Decode(upperBoundPosRecord.Value)
	if err != nil {
		return nil, err
	}

	// read records from lowerBoundPos to upperBoundPos
	var currentPos int64 = lowerboundPosition.Start
	endOfScanOffset := upperboundPosition.Start + upperboundPosition.Size

	encoder := records.NewRecordEncoder[string, *records.CommandHeader, *records.CommandRecord]()
	sst.readOnlySstFile.Seek(currentPos, 0)
	for currentPos < endOfScanOffset {

		var cmdRecord records.CommandRecord

		err = encoder.DecodeF(&cmdRecord, sst.readOnlySstFile)
		if err != nil {
			return nil, fmt.Errorf("unexpected error decoding command record: %w", err)
		}

		if cmdRecord.Key == key {
			return &cmdRecord, nil
		}

		currentPos = sst.readOnlySstFile.GetCurrentReadOffset()
	}

	// buff := make([]byte, lowerboundPosition.Size)
	// sst.readOnlySstFile.ReadAt(buff, lowerboundPosition.Start)

	return nil, constants.ErrKeyNotPresentInSSTable
}
