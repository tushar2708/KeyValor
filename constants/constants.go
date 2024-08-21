package constants

const (
	KB = 1 << 10 // 1024 bytes
	MB = 1 << 20 // 1024 KB
	GB = 1 << 30 // 1024 MB
	TB = 1 << 40 // 1024 GB

	// MaxKeySize is the maximum key size that is allowed
	MaxKeySize = 1 * GB
	// MaxValueSize is the maximum value size that is allowed
	MaxValueSize = 4 * GB
)
