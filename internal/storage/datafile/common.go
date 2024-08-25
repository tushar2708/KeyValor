package datafile

type FileUtilities interface {
	Size() (int64, error)
	ID() int
}

type ReadFileUtilities interface {
	FileUtilities
	GetCurrentReadOffset() int64
}

type WriteFileUtiities interface {
	FileUtilities
	GetCurrentWriteOffset() int64
	Sync() error
}

var _ ReadFileUtilities = &ReadWriteDataFile{}
var _ WriteFileUtiities = &ReadWriteDataFile{}
