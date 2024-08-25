package dataconvutils

import (
	"encoding/binary"
	"fmt"
)

// Helper function to convert bytes to int
func BytesToInt(b []byte) (int, error) {
	var i int
	for _, v := range b {
		i = i*10 + int(v-'0')
	}
	return i, nil
}

func BytesToInt32(buff []byte) int32 {
	_ = buff[3]
	return int32(buff[0]) | int32(buff[1])<<8 | int32(buff[2])<<16 | int32(buff[3])<<24
}

func BytesToInt64(buff []byte) int64 {
	value := binary.LittleEndian.Uint64(buff)
	return int64(value)
}

func Int64ToBytes(i int64) []byte {
	byteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteArray, uint64(i))
	return byteArray
}

// Helper function to convert int to bytes
func IntToBytes(i int) []byte {
	return []byte(fmt.Sprintf("%d", i))
}
