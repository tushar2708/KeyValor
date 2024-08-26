package records

import (
	"bytes"
	"testing"
)

type EncoderTestCase struct {
	name              string
	record            *CommandRecord // Always use pointer here to ensure proper behavior
	shouldEncode      bool
	shouldDecode      bool
	expectDecodeError bool
	expectEncodeError bool
}

// TestRecordEncoder tests various encoding and decoding scenarios using RecordEncoder
func TestRecordEncoder(t *testing.T) {

	tests := []EncoderTestCase{
		{
			name: "Valid Encoding and Decoding",
			record: &CommandRecord{
				Header: CommandHeader{CmdType: 1, Expiry: 1234567890, KeySize: 3, ValSize: 5},
				Key:    "key",
				Value:  []byte("value"),
			},
			shouldEncode:      true,
			shouldDecode:      true,
			expectDecodeError: false,
		},
		{
			name: "Empty Key and Value",
			record: &CommandRecord{
				Header: CommandHeader{CmdType: 2, Expiry: 1234567891, KeySize: 0, ValSize: 0},
				Key:    "",
				Value:  []byte(""),
			},
			shouldEncode:      true,
			shouldDecode:      true,
			expectDecodeError: false,
		},
		{
			name: "Partial Data Read",
			record: &CommandRecord{
				Header: CommandHeader{CmdType: 3, Expiry: 1234567892, KeySize: 3, ValSize: 5},
				Key:    "key",
				Value:  []byte("val"),
			},
			shouldEncode:      true,
			shouldDecode:      true,
			expectDecodeError: true,
		},
		{
			name: "Maximum Key Size",
			record: &CommandRecord{
				Header: CommandHeader{CmdType: 4, Expiry: 1234567893, KeySize: 255, ValSize: 5},
				Key:    string(make([]byte, 255)), // Max size key
				Value:  []byte("value"),
			},
			shouldEncode:      true,
			shouldDecode:      true,
			expectDecodeError: false,
		},
		{
			name: "Maximum Value Size",
			record: &CommandRecord{
				Header: CommandHeader{CmdType: 5, Expiry: 1234567894, KeySize: 3, ValSize: 255},
				Key:    "key",
				Value:  make([]byte, 255), // Max size value
			},
			shouldEncode:      true,
			shouldDecode:      true,
			expectDecodeError: false,
		},
		{
			name:              "Invalid Input - Non-Pointer",
			record:            nil, // Will test without passing a proper record pointer
			shouldEncode:      false,
			shouldDecode:      true,
			expectDecodeError: true,
		},
	}

	encoder := NewRecordEncoder[string, *CommandHeader, *CommandRecord]()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldEncode && tt.record != nil {
				// Test encoding & decoding from buffer
				runTestsUsingBuffer(t, tt, encoder)
				runTestsUsingReaderWriter(t, tt, encoder)
			}

			// Test invalid non-pointer usage for decoding
			if tt.shouldDecode && tt.record == nil {
				buff := &bytes.Buffer{}
				err := encoder.Decode(nil, buff.Bytes())
				if err == nil {
					t.Error("Expected error when decoding non-pointer, but got nil")
				}
			}

		})
	}
}

func runTestsUsingReaderWriter(t *testing.T, tt EncoderTestCase, encoder *RecordEncoder[string, *CommandHeader, *CommandRecord]) {
	buff := &bytes.Buffer{}
	err := encoder.EncodeF(tt.record, buff)
	if (err != nil) != tt.expectEncodeError {
		t.Errorf("EncodeF() error = %v, expectEncodeError %v", err, tt.expectEncodeError)
	}
	if err == nil && !tt.expectDecodeError {
		decodedRecord := &CommandRecord{Header: CommandHeader{}}
		err = encoder.DecodeF(decodedRecord, buff)
		if (err != nil) != tt.expectDecodeError {
			t.Errorf("DecodeF() error = %v, expectDecodeError %v", err, tt.expectDecodeError)
		}
		if err == nil && !tt.expectDecodeError {
			if decodedRecord.Key != tt.record.Key || string(decodedRecord.Value) != string(tt.record.Value) {
				t.Errorf("Decoded record from io.Reader does not match, got = %v, want = %v", decodedRecord, tt.record)
			}
		}
	}
}

func runTestsUsingBuffer(t *testing.T, tt EncoderTestCase, encoder *RecordEncoder[string, *CommandHeader, *CommandRecord]) {
	buff := &bytes.Buffer{}
	err := encoder.Encode(tt.record, buff)
	if (err != nil) != tt.expectEncodeError {
		t.Errorf("Encode() error = %v, expectEncodeError %v", err, tt.expectEncodeError)
	}
	if err == nil && !tt.expectDecodeError {

		decodedRecord := &CommandRecord{Header: CommandHeader{}}
		err = encoder.Decode(decodedRecord, buff.Bytes())
		if (err != nil) != tt.expectDecodeError {
			t.Errorf("Decode() error = %v, expectDecodeError %v", err, tt.expectDecodeError)
		}
		if err == nil && !tt.expectDecodeError {
			if decodedRecord.Key != tt.record.Key || string(decodedRecord.Value) != string(tt.record.Value) {
				t.Errorf("Decoded record does not match, got = %v, want = %v", decodedRecord, tt.record)
			}
		}
	}
}

// TestRecordEncoderDecodeOnly tests decoding scenarios where the header is read separately
func TestRecordEncoderDecodeOnly(t *testing.T) {
	encoder := NewRecordEncoder[string, *CommandHeader, *CommandRecord]()

	// Construct raw data buffer simulating reading from a stream or file
	header := CommandHeader{CmdType: 1, Expiry: 1234567890, KeySize: 3, ValSize: 5}
	headerBuff := &bytes.Buffer{}
	if err := header.Encode(headerBuff); err != nil {
		t.Fatalf("Failed to encode header: %v", err)
	}

	key := []byte("key")
	value := []byte("value")
	combinedData := append(headerBuff.Bytes(), key...)
	combinedData = append(combinedData, value...)

	// Now decode using only DecodeF to simulate the actual read process
	decodedRecord := &CommandRecord{Header: CommandHeader{}}
	reader := bytes.NewReader(combinedData)

	if err := encoder.DecodeF(decodedRecord, reader); err != nil {
		t.Errorf("DecodeF() error = %v", err)
	} else {

		// Verify decoded header
		if decodedRecord.Header.CmdType != header.CmdType || decodedRecord.Header.Expiry != header.Expiry || decodedRecord.Header.KeySize != header.KeySize || decodedRecord.Header.ValSize != header.ValSize {
			t.Errorf("Decoded header does not match, got CmdType = %v, Expiry = %v, KeySize = %v, ValSize = %v; want CmdType = %v, Expiry = %v, KeySize = %v, ValSize = %v", decodedRecord.Header.CmdType, decodedRecord.Header.Expiry, decodedRecord.Header.KeySize, decodedRecord.Header.ValSize, header.CmdType, header.Expiry, header.KeySize, header.ValSize)
		}

		// Verify decoded values
		if decodedRecord.Key != "key" || string(decodedRecord.Value) != "value" {
			t.Errorf("Decoded record does not match, got Key = %v, Value = %v; want Key = %v, Value = %v", decodedRecord.Key, string(decodedRecord.Value), "key", "value")
		}
	}

	// Test with insufficient data after header (to ensure proper error handling)
	partialData := append(headerBuff.Bytes(), key...)
	reader = bytes.NewReader(partialData)

	err := encoder.DecodeF(decodedRecord, reader)
	if err == nil {
		t.Error("Expected error when decoding with insufficient data after header, but got nil")
	}
}
