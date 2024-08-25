package datafile

import "io"

type ReadOnlyWithRandomReads interface {
	ReadFileUtilities
	io.ReadCloser
	io.ReaderAt
}

func NewReadOnlyDataFileWithRandomReadsWithPath(filePath string) (ReadOnlyWithRandomReads, error) {
	return newDataFileWithPath(filePath, 0, DF_MODE_READ_WRITE)
}

func NewReadOnlyDataFileWithRandomReads(directory string, fileNameFormat string, fileID int) (ReadOnlyWithRandomReads, error) {
	return newDataFile(directory, fileNameFormat, fileID, DF_MODE_READ_WRITE)
}
