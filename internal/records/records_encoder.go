package records

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
)

//go:generate go run ../../cmd/tools/serialization_checker/main.go
type Header interface {
	GetHeaderLen() int
	GetKeySize() int
	GetValueSize() int32
	Encode(buff *bytes.Buffer) error
	Decode(header []byte) error
}

type Record[K comparable] interface {
	GetHeader() Header
	SetHeader(headerData []byte)
	GetKey() (K, error)
	GetValue() ([]byte, error)
	Encode(buff *bytes.Buffer) error
	Decode(reader io.Reader) error
	DecodeKeyVal(keyAndValue []byte) error
}

// RecordEncoder is responsible for encoding and decoding records with a header and body
type RecordEncoder[K comparable, H Header, R Record[K]] struct{}

// checkPointer ensures that a given type is a pointer type at compile-time.
func checkPointer[T any]() {
	var zero T
	if reflect.TypeOf(zero).Kind() != reflect.Ptr {
		panic("generic type T must be a pointer")
	}
}

// checkPointer ensures that a given type is a pointer type at runtime.
func checkThatRecordIsPointer(record interface{}) error {
	v := reflect.ValueOf(record)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("expected a non-nil pointer, but got %T", record)
	}
	return nil
}

// NewRecordEncoder creates a new RecordEncoder and ensures H and R are pointers.
func NewRecordEncoder[K comparable, H Header, R Record[K]]() *RecordEncoder[K, H, R] {
	// checkPointer[H]()
	checkPointer[R]()
	return &RecordEncoder[K, H, R]{}
}

// Encode encodes a record to the provided buffer.
func (re *RecordEncoder[K, H, R]) Encode(record R, buff *bytes.Buffer) error {
	return re.EncodeF(record, buff)
}

// Decode decodes a record from the provided byte slice.
func (re *RecordEncoder[K, H, R]) Decode(record R, data []byte) error {

	if err := checkThatRecordIsPointer(record); err != nil {
		return err
	}

	reader := bytes.NewReader(data)
	return re.DecodeF(record, reader)
}

// EncodeF encodes a record to the provided writer.
func (re *RecordEncoder[K, H, R]) EncodeF(record R, writer io.Writer) error {
	// Use a buffer to encode the data first
	buff := &bytes.Buffer{}

	// Encode the header
	header := record.GetHeader()
	if err := header.Encode(buff); err != nil {
		return fmt.Errorf("failed to encode header: %w", err)
	}

	// Encode the key
	key, err := record.GetKey()
	if err != nil {
		return fmt.Errorf("failed to get key: %w", err)
	}

	// always write the key as a string (individual decoders will have to take care of this)
	if _, err := buff.Write([]byte(fmt.Sprintf("%v", key))); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	// Encode the value
	value, err := record.GetValue()
	if err != nil {
		return fmt.Errorf("failed to get value: %w", err)
	}
	if _, err := buff.Write(value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	// Write buffer to the writer
	_, err = writer.Write(buff.Bytes())
	return err
}

// DecodeF decodes a record from the provided reader.
func (re *RecordEncoder[K, H, R]) DecodeF(record R, reader io.Reader) error {

	if err := checkThatRecordIsPointer(record); err != nil {
		return err
	}

	// Decode the header
	header := record.GetHeader()
	headerLen := header.GetHeaderLen()
	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(reader, headerBytes); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	if err := header.Decode(headerBytes); err != nil {
		return fmt.Errorf("failed to decode header: %w", err)
	}
	record.SetHeader(headerBytes)

	// Decode the key and value together
	keySize := header.GetKeySize()
	valueSize := header.GetValueSize()
	keyValBytes := make([]byte, keySize+int(valueSize))
	if _, err := io.ReadFull(reader, keyValBytes); err != nil {
		return fmt.Errorf("failed to read key and value: %w", err)
	}

	if err := record.DecodeKeyVal(keyValBytes); err != nil {
		return fmt.Errorf("failed to decode key and value: %w", err)
	}

	return nil
}
