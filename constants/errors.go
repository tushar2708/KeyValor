package constants

import "errors"

var (
	// ErrKeyIsEmpty is returned when a key is empty
	ErrKeyIsEmpty = errors.New("key is empty")
	// ErrValueIsEmpty is returned when a value is empty - Not allowed
	// (empty byte-array is a special value used internally)
	ErrValueIsEmpty = errors.New("empty value is not allowed (empty byte-array is a special value used internally)")
	// ErrKeyMissing is returned when a key is missing from the store
	ErrKeyMissing = errors.New("key is missing")
	// ErrKeyIsDeleted
	// (this is different from ErrKeyMissing, because it may tell us that we found
	// a definitive latest record of the key being deleted), and do not need to look any further
	ErrKeyIsDeleted = errors.New("key is deleted")
	// ErrKeyNotPresentInSSTable is returned when a key is not present in the SSTable
	ErrKeyNotPresentInSSTable = errors.New("key not present in SSTable")
	// ErrKeyTooBig is returned when a key is too big
	ErrKeyTooBig = errors.New("key is larger than 1 GB")
	// ErrValueTooBig is returned when a value is too big
	ErrValueTooBig = errors.New("value is larger than 4 GB")
	// ErrKeyIsExpired is returned when a key is expired
	ErrKeyIsExpired = errors.New("the key is expired")
	// ErrChecksumIsInvalid is returned when a record's checksum is invalid
	ErrChecksumIsInvalid = errors.New("the checksum of the record is invalid")

	// ErrWalFileNotFound is returned when a record's WAL file is not found
	ErrWalFileNotFound = errors.New("the WAL file is missing for the given File ID")
	// ErrErrorReadingRecordFromFile is returned when a record couldn't be read from the WAL file
	ErrErrorReadingRecordFromFile = errors.New("error reading record from WAL file")
)
