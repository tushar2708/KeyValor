package sstable

import (
	"KeyValor/internal/storage/datafile"
	"bytes"
	"encoding/binary"
)

type SSTableMetaData struct {
	Version   int64
	BatchSize int64 // size of batch after which memtable is persisted to disk.

	// Data region bounds
	DataStartOffset int64
	DataSize        int64

	// Index region bounds
	IndexStartOffset int64
	IndexSize        int64
}

func (smd *SSTableMetaData) Length() int {
	return 6 * 8
}

func (smd *SSTableMetaData) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, smd)
}

func (smd *SSTableMetaData) Decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, smd)
}

func (smd *SSTableMetaData) ReadFromFile(readOnlyFile datafile.ReadOnlyWithRandomReads) error {
	record := make([]byte, smd.Length())
	_, err := readOnlyFile.ReadAt(record, smd.DataStartOffset)
	if err != nil {
		return err
	}
	err = smd.Decode(record)
	if err != nil {
		return err
	}

	_, err = readOnlyFile.Seek(0, 0)
	return err
}
