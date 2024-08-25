package records

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"
)

// CommandType represents the type of command.
type CommandType byte

const (
	Get CommandType = iota
	Set
	Del
)

// CommandHeader contains the metadata about a CommandRecord
type CommandHeader struct {
	CmdType CommandType
	Expiry  int64
	KeySize int32
	ValSize int32
}

type CommandRecord struct {
	Header CommandHeader
	Key    string
	Value  []byte
}

func NewCommandHeader(cmdType CommandType, key string, value []byte) *CommandHeader {
	return &CommandHeader{
		CmdType: cmdType,
		Expiry:  0,
		KeySize: int32(len(key)),
		ValSize: int32(len(value)),
	}
}

func NewGetCommandRecord(key string) *CommandRecord {
	header := NewCommandHeader(Get, key, nil)
	return &CommandRecord{Header: *header, Key: key, Value: nil}
}

func NewSetCommandRecord(key string, value []byte) *CommandRecord {
	header := NewCommandHeader(Set, key, value)
	return &CommandRecord{Header: *header, Key: key, Value: value}
}

func NewDelCommandRecord(key string) *CommandRecord {
	header := NewCommandHeader(Del, key, nil)
	return &CommandRecord{Header: *header, Key: key, Value: nil}
	// return &CommandRecord{Key: key, CmdType: Del}
}

// CommandHeaderSerializedLength is the length of CommandHeader's
// buffer serialized version. We need to know this, to be able to
// read it from a file (there's a test that will fail if it changes)
const CommandHeaderSerializedLength = 17

// Implement Header interface methods for CommandHeader
func (ch *CommandHeader) GetHeaderLen() int {
	return CommandHeaderSerializedLength // total bytes for the fields
}

func (ch *CommandHeader) GetKeySize() int {
	return int(ch.KeySize)
}

func (ch *CommandHeader) GetValueSize() int32 {
	return ch.ValSize
}

func (h *CommandHeader) GetExpiry() int64 {
	return h.Expiry
}

func (h *CommandHeader) SetExpiry(n int64) {
	h.Expiry = n
}

func (ch *CommandHeader) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, ch)
}

func (ch *CommandHeader) Decode(data []byte) error {
	buff := bytes.NewReader(data)
	return binary.Read(buff, binary.LittleEndian, ch)
}

// Implement Record interface methods for CommandRecord
func (cr *CommandRecord) GetHeader() Header {
	return &cr.Header
}

func (cr *CommandRecord) SetHeader(headerData []byte) {
	cr.Header.Decode(headerData)
}

func (cr *CommandRecord) GetKey() (string, error) {
	return cr.Key, nil
}

func (cr *CommandRecord) GetValue() ([]byte, error) {
	return cr.Value, nil
}

func (r *CommandRecord) IsExpired() bool {
	// 0 value means no expiry was set
	if r.Header.Expiry == 0 {
		return false
	}
	return time.Now().Unix() > int64(r.Header.Expiry)
}

func (cr *CommandRecord) DecodeKeyVal(keyAndValue []byte) error {
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

func (cr *CommandRecord) Encode(buff *bytes.Buffer) error {
	if err := cr.Header.Encode(buff); err != nil {
		return err
	}
	if _, err := buff.Write([]byte(cr.Key)); err != nil {
		return err
	}
	if _, err := buff.Write(cr.Value); err != nil {
		return err
	}
	return nil
}

func (cr *CommandRecord) Decode(reader io.Reader) error {
	// buff := bytes.NewReader(record)
	header := &CommandHeader{}
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

type CommandBatch []*CommandRecord

func (cb CommandBatch) Len() int {
	return len(cb)
}

func (cb *CommandBatch) Clear() {
	*cb = make([]*CommandRecord, cb.Len())
}

func (r *CommandBatch) Encode(buff *bytes.Buffer) error {
	for _, cr := range *r {
		if err := cr.Encode(buff); err != nil {
			return err
		}
	}
	return nil
}

func (r *CommandBatch) Decode(record []byte) error {
	for len(record) > 0 {
		var cr CommandRecord
		buff := bytes.NewReader(record)
		if err := cr.Decode(buff); err != nil {
			return err
		}
		*r = append(*r, &cr)
		record = record[cr.Header.GetHeaderLen():]
	}
	return nil
}
