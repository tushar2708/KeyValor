package storagecommon

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"

	"KeyValor/internal/utils/timeutils"
)

type DataRecord struct {
	Header Header
	Key    string
	Value  []byte
}

type Header struct {
	Crc     uint32
	Ts      int64
	Expiry  int64
	KeySize int32
	ValSize int32
}

func NewHeader(key string, value []byte) Header {
	return Header{
		Crc:     crc32.ChecksumIEEE(value),
		Ts:      timeutils.CurrentTimeNanos(),
		Expiry:  0,
		KeySize: int32(len(key)),
		ValSize: int32(len(value)),
	}
}

func (h *Header) GetTs() int64 {
	return h.Ts
}

func (h *Header) GetValueSize() int32 {
	return h.ValSize
}

func (h *Header) GetExpiry() int64 {
	return h.Expiry
}

func (h *Header) SetExpiry(n int64) {
	h.Expiry = n
}

func (h *Header) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, h)
}

func (h *Header) Decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, h)
}

func (r *DataRecord) IsExpired() bool {
	// 0 value means no expiry was set
	if r.Header.Expiry == 0 {
		return false
	}
	return time.Now().Unix() > int64(r.Header.Expiry)
}

func (r *DataRecord) IsChecksumValid() bool {
	return crc32.ChecksumIEEE(r.Value) == r.Header.Crc
}

func (r *DataRecord) Encode(buff *bytes.Buffer) error {
	// write header to the buffer
	if err := r.Header.Encode(buff); err != nil {
		return err
	}

	// write key, and then value to the buffer, because these
	// variable sized values can't be encoded using binary.Write
	// https://pkg.go.dev/encoding/binary#Write
	if _, err := buff.WriteString(r.Key); err != nil {
		return err
	}
	if _, err := buff.Write(r.Value); err != nil {
		return err
	}
	return nil
}
