package lsmtree

import (
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

func (smd *SSTableMetaData) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, smd)
}

func (smd *SSTableMetaData) Decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, smd)
}
