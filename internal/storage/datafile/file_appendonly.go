package datafile

import "io"

type AppendOnlyFile interface {
	WriteFileUtiities
	io.WriteCloser
}

func NewAppendOnlyDataFileWithPath(filePath string) (AppendOnlyFile, error) {
	return newDataFileWithPath(filePath, 0, DF_MODE_READ_WRITE)
}

func NewAppendOnlyDataFile(directory string, fileNameFormat string, fileID int) (AppendOnlyFile, error) {
	return newDataFile(directory, fileNameFormat, fileID, DF_MODE_READ_WRITE)
}
