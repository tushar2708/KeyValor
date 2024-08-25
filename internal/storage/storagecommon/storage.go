package storagecommon

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"KeyValor/config"
)

type CommonStorage struct {
	sync.RWMutex
	Cfg        *config.DBCfgOpts
	LockFile   *os.File
	BufferPool sync.Pool // crate an object pool to reuse buffers
}

func NewCommonStorage(
	cfg *config.DBCfgOpts,
) (*CommonStorage, error) {

	lockFilePath := filepath.Join(cfg.Directory, LOCKFILE)
	lockFile, err := AcquireLockFile(lockFilePath)
	if err != nil {
		return nil, fmt.Errorf("error creating lockfile: %w", err)
	}

	return &CommonStorage{
		Cfg:      cfg,
		LockFile: lockFile,
		BufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer([]byte{})
			},
		},
	}, nil
}
