package storagecommon

type Meta struct {
	Timestamp int64

	// path to record
	FileID       int
	RecordOffset int
	RecordSize   int
}