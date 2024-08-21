package KeyValor

import (
	"fmt"
	"os"

	"KeyValor/constants"
)

func fileExists(filePath string) bool {
	if _, err := os.Stat(filePath); err != nil {
		return false
	}
	return true
}

func validateEntry(k string, val []byte) error {
	if len(k) == 0 {
		return constants.ErrKeyIsEmpty
	}

	if len(k) > constants.MaxKeySize {
		return constants.ErrKeyTooBig
	}

	if len(val) == 0 {
		return constants.ErrValueIsEmpty
	}

	if len(val) > constants.MaxValueSize {
		return constants.ErrValueTooBig
	}

	return nil
}

// Helper function to convert bytes to int
func bytesToInt(b []byte) (int, error) {
	var i int
	for _, v := range b {
		i = i*10 + int(v-'0')
	}
	return i, nil
}

// Helper function to convert int to bytes
func intToBytes(i int) []byte {
	return []byte(fmt.Sprintf("%d", i))
}
