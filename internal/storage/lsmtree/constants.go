package lsmtree

// LSM-tree & SSTable related constants
const (
	SSTABLE_FILE_EXTENSION   = ".sst"
	SSTABLE_FILE_PREFIX      = "data_file_"
	SSTABLE_FILE_NAME_FORMAT = "data_file_%d.sst"
	TEMPORARY_WAL_FILE_NAME  = "temp_wal_file"
	CURRENT_WAL_FILE_NAME    = "current_wal_file"
)

const (
	MAX_ENTRIES_IN_MEMTABLE = 100
	SSTABLE_BATCH_SIZE      = 100
)
