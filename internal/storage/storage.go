package storage

import (
	"KeyValor/dbops"
)

type DiskStorage interface {
	Init() error
	Close() error
	dbops.DatabaseOperations
}
