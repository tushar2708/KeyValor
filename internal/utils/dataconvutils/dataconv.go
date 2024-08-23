package dataconvutils

import "fmt"

// Helper function to convert bytes to int
func BytesToInt(b []byte) (int, error) {
	var i int
	for _, v := range b {
		i = i*10 + int(v-'0')
	}
	return i, nil
}

// Helper function to convert int to bytes
func IntToBytes(i int) []byte {
	return []byte(fmt.Sprintf("%d", i))
}
