package records

import (
	"bytes"
	"testing"
)

func TestCommandHeaderEncode(t *testing.T) {
	header := CommandHeader{
		CmdType: Del,
		Expiry:  0,
		KeySize: 0,
		ValSize: 0,
	}

	var buff bytes.Buffer
	err := header.Encode(&buff)
	if err != nil {
		t.Fatalf("Error encoding header: %v", err)
	}

	expected := []byte{
		0x02,                                           // CmdType: Del
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Expiry: 0
		0x00, 0x00, 0x00, 0x00, // KeySize: 0
		0x00, 0x00, 0x00, 0x00, // ValSize: 0
	}

	expectedLen := CommandHeaderSerializedLength
	actualLen := buff.Len()
	if actualLen != expectedLen {
		t.Errorf("Expected serialized buffer length %d, but got %d", expectedLen, actualLen)
	}

	if header.GetHeaderLen() != CommandHeaderSerializedLength {
		t.Error("GetHeaderLen() returned incorrect value. Something has changed")
	}

	if !bytes.Equal(buff.Bytes(), expected) {
		t.Errorf("Expected encoded header %v, but got %v", expected, buff.Bytes())
	}
}

func TestCommandBatchEncode(t *testing.T) {
	batch := CommandBatch{
		NewSetCommandRecord("key1", []byte("value1")),
		NewDelCommandRecord("key2"),
		NewGetCommandRecord("key3"),
	}

	var buff bytes.Buffer
	err := batch.Encode(&buff)
	if err != nil {
		t.Fatalf("Error encoding batch: %v", err)
	}

	expected := []byte{
		// encoded CommandHeader for Set command
		0x01,                                           // CmdType: Set
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Expiry: 0
		0x04, 0x00, 0x00, 0x00, // KeySize: 4
		0x06, 0x00, 0x00, 0x00, // ValSize: 6
		// "key1"
		0x6b, 0x65, 0x79, 0x31,
		// "value1"
		0x76, 0x61, 0x6c, 0x75, 0x65, 0x31,
		// encoded CommandHeader for Del command
		0x02,                                           // CmdType: Del
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Expiry: 0
		0x04, 0x00, 0x00, 0x00, // KeySize: 5
		0x00, 0x00, 0x00, 0x00, // ValSize: 0
		// "key2"
		0x6b, 0x65, 0x79, 0x32,
		// encoded CommandHeader for Get command
		0x00,                                           // CmdType: Get
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Expiry: 0
		0x04, 0x00, 0x00, 0x00, // KeySize: 5
		0x00, 0x00, 0x00, 0x00, // ValSize: 0
		// "key3"
		0x6b, 0x65, 0x79, 0x33,
	}

	if !bytes.Equal(buff.Bytes(), expected) {
		t.Errorf("Expected encoded batch \n%v, but got \n%v", expected, buff.Bytes())
	}
}
