package records

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Position struct {
	Start int64
	Size  int64
}

func (p *Position) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, p)
}

func (p *Position) Decode(data []byte) error {
	buff := bytes.NewReader(data)
	return binary.Read(buff, binary.LittleEndian, p)
}

// PositionHeader contains the metadata about a PositionRecord
type PositionHeader struct {
	KeySize int32
	ValSize int32
}

type PositionRecord struct {
	Header PositionHeader
	Key    string
	Value  []byte
}

func NewPositionRecord(key string, pos *Position) (*PositionRecord, error) {

	buff := &bytes.Buffer{}

	err := pos.Encode(buff)
	if err != nil {
		return nil, err
	}

	return &PositionRecord{
		Header: PositionHeader{
			KeySize: int32(len(key)),
			ValSize: int32(buff.Len()),
		},
		Key:   key,
		Value: buff.Bytes(),
	}, nil
}

// PositionHeaderSerializedLength is the length of PositionHeader's
// buffer serialized version. We need to know this, to be able to
// read it from a file (there's a test that will fail if it changes)
const PositionHeaderSerializedLength = 8

// Implement Header interface methods for PositionHeader
func (ch *PositionHeader) GetHeaderLen() int {
	return PositionHeaderSerializedLength // total bytes for the fields
}

func (ch *PositionHeader) GetKeySize() int {
	return int(ch.KeySize)
}

func (ch *PositionHeader) GetValueSize() int32 {
	return ch.ValSize
}

func (ch *PositionHeader) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, ch)
}

func (ch *PositionHeader) Decode(data []byte) error {
	buff := bytes.NewReader(data)
	return binary.Read(buff, binary.LittleEndian, ch)
}

// Implement Record interface methods for PositionRecord
func (cr *PositionRecord) GetHeader() Header {
	return &cr.Header
}

func (cr *PositionRecord) SetHeader(headerData []byte) {
	cr.Header.Decode(headerData)
}

func (cr *PositionRecord) GetKey() (string, error) {
	return cr.Key, nil
}

func (cr *PositionRecord) GetValue() ([]byte, error) {
	return cr.Value, nil
}

func (cr *PositionRecord) DecodeKeyVal(keyAndValue []byte) error {
	keyLen := int(cr.Header.KeySize)

	if keyLen > 0 {
		cr.Key = string(keyAndValue[:keyLen])
	}
	if cr.Header.ValSize > 0 {
		cr.Value = make([]byte, cr.Header.ValSize)
		copy(cr.Value, keyAndValue[keyLen:])
	}

	return nil
}

func (cr *PositionRecord) Encode(buff *bytes.Buffer) error {
	if err := cr.Header.Encode(buff); err != nil {
		return err
	}
	if _, err := buff.WriteString(cr.Key); err != nil {
		return err
	}
	if _, err := buff.Write(cr.Value); err != nil {
		return err
	}
	return nil
}

func (cr *PositionRecord) Decode(reader io.Reader) error {
	// buff := bytes.NewReader(record)
	header := &PositionHeader{}
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
