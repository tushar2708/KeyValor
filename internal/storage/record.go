package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"

	"KeyValor/internal/timeutils"
)

type Record struct {
	Header Header
	Key    string
	Value  []byte
}

type Header struct {
	crc     uint32
	ts      int64
	expiry  int64
	keySize int32
	valSize int32
}

func NewHeader(key string, value []byte) Header {
	return Header{
		crc:     crc32.ChecksumIEEE(value),
		ts:      timeutils.CurrentTimeNanos(),
		expiry:  0,
		keySize: int32(len(key)),
		valSize: int32(len(value)),
	}
}

func (h *Header) GetTs() int64 {
	return h.ts
}

func (h *Header) GetValueSize() int32 {
	return h.valSize
}

func (h *Header) GetExpiry() int64 {
	return h.expiry
}

func (h *Header) SetExpiry(n int64) {
	h.expiry = n
}

func (h *Header) Encode(buff *bytes.Buffer) error {
	return binary.Write(buff, binary.LittleEndian, h)
}

func (h *Header) Decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, h)
}

func (r *Record) IsExpired() bool {
	// 0 value means no expiry was set
	if r.Header.expiry == 0 {
		return false
	}
	return time.Now().Unix() > int64(r.Header.expiry)
}

func (r *Record) IsChecksumValid() bool {
	return crc32.ChecksumIEEE(r.Value) == r.Header.crc
}

func (r *Record) Encode(buff *bytes.Buffer) error {
	// write header to the buffer
	if err := r.Header.Encode(buff); err != nil {
		return err
	}

	// write key, and then value to the buffer
	if _, err := buff.WriteString(r.Key); err != nil {
		return err
	}
	if _, err := buff.Write(r.Value); err != nil {
		return err
	}
	return nil
}
