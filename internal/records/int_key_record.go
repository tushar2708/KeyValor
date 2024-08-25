package records

import (
	"bytes"
	"encoding/binary"
	"io"

	"KeyValor/internal/utils/dataconvutils"
)

type SomeValue struct {
	Start int64
	Size  int64
}

func (p *SomeValue) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, p)
}

func (p *SomeValue) Decode(data []byte) error {
	buff := bytes.NewReader(data)
	return binary.Read(buff, binary.LittleEndian, p)
}

// SomeValueHeader contains the metadata about a SomeValueRecord
type SomeValueHeader struct {
	KeySize int32
	ValSize int32
}

type SomeValueRecord struct {
	Header SomeValueHeader
	Key    int64
	Value  []byte
}

func NewSomeValueRecord(key int64, pos *SomeValue) (*SomeValueRecord, error) {

	buff := &bytes.Buffer{}

	err := pos.Encode(buff)
	if err != nil {
		return nil, err
	}

	return &SomeValueRecord{
		Header: SomeValueHeader{
			KeySize: 8,
			ValSize: int32(buff.Len()),
		},
		Key:   key,
		Value: buff.Bytes(),
	}, nil
}

// SomeValueHeaderSerializedLength is the length of SomeValueHeader's
// buffer serialized version. We need to know this, to be able to
// read it from a file (there's a test that will fail if it changes)
const SomeValueHeaderSerializedLength = 8

// Implement Header interface methods for SomeValueHeader
func (ch *SomeValueHeader) GetHeaderLen() int {
	return SomeValueHeaderSerializedLength // total bytes for the fields
}

func (ch *SomeValueHeader) GetKeySize() int {
	return int(ch.KeySize)
}

func (ch *SomeValueHeader) GetValueSize() int32 {
	return ch.ValSize
}

func (ch *SomeValueHeader) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, ch)
}

func (ch *SomeValueHeader) Decode(data []byte) error {
	buff := bytes.NewReader(data)
	return binary.Read(buff, binary.LittleEndian, ch)
}

// Implement Record interface methods for SomeValueRecord
func (cr *SomeValueRecord) GetHeader() Header {
	return &cr.Header
}

func (cr *SomeValueRecord) SetHeader(headerData []byte) {
	cr.Header.Decode(headerData)
}

func (cr *SomeValueRecord) GetKey() (int64, error) {
	return cr.Key, nil
}

func (cr *SomeValueRecord) GetValue() ([]byte, error) {
	return cr.Value, nil
}

func (cr *SomeValueRecord) DecodeKeyVal(keyAndValue []byte) error {
	keyLen := int(cr.Header.KeySize)

	if keyLen > 0 {
		cr.Key = dataconvutils.BytesToInt64(keyAndValue[:keyLen])
	}
	if cr.Header.ValSize > 0 {
		cr.Value = make([]byte, cr.Header.ValSize)
		copy(cr.Value, keyAndValue[keyLen:])
	}

	return nil
}

func (cr *SomeValueRecord) Encode(buff *bytes.Buffer) error {
	if err := cr.Header.Encode(buff); err != nil {
		return err
	}
	if _, err := buff.Write(dataconvutils.Int64ToBytes(cr.Key)); err != nil {
		return err
	}
	if _, err := buff.Write(cr.Value); err != nil {
		return err
	}
	return nil
}

func (cr *SomeValueRecord) Decode(reader io.Reader) error {
	// buff := bytes.NewReader(record)
	header := &SomeValueHeader{}
	headerLen := header.GetHeaderLen()
	headerBytes := make([]byte, headerLen)

	if _, err := io.ReadFull(reader, headerBytes); err != nil {
		return err
	}

	if err := header.Decode(headerBytes); err != nil {
		return err
	}

	cr.Header = *header

	keyLen := int(cr.Header.KeySize)
	valLen := int(cr.Header.ValSize)
	keyValLen := keyLen + valLen
	keyValBytes := make([]byte, keyValLen)
	if _, err := io.ReadFull(reader, keyValBytes); err != nil {
		return err
	}

	return cr.DecodeKeyVal(keyValBytes)
}
