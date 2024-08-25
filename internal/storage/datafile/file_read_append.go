package datafile

import "io"

type AppendOnlyWithRandomReads interface {
	ReadFileUtilities
	WriteFileUtiities
	io.ReadWriteCloser
	io.ReaderAt
}

func NewAppendOnlyDataFileWithRandomReadsWithPath(filePath string) (AppendOnlyWithRandomReads, error) {
	return newDataFileWithPath(filePath, 0, DF_MODE_READ_WRITE)
}

func NewAppendOnlyDataFileWithRandomReads(directory string, fileNameFormat string, fileID int) (AppendOnlyWithRandomReads, error) {
	return newDataFile(directory, fileNameFormat, fileID, DF_MODE_READ_WRITE)
}
