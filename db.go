package KeyValor

import (
	"sync"
	"time"

	"KeyValor/config"
	"KeyValor/internal/storage"
	"KeyValor/internal/storage/hashtable"
)

type KeyValorDatabase struct {
	sync.RWMutex

	cfg     *config.DBCfgOpts
	storage storage.DiskStorage
}

func NewKeyValorDB(options ...Option) (*KeyValorDatabase, error) {
	opts := config.DefaultOpts()
	for _, option := range options {
		option(opts)
	}

	storage, err := hashtable.NewHashTableStorage(opts)
	if err != nil {
		return nil, err
	}

	if err := storage.Init(); err != nil {
		return nil, err
	}

	kvDB := &KeyValorDatabase{
		cfg:     opts,
		storage: storage,
	}

	return kvDB, nil
}

// Option is a function that configures a DBCfgOpts.
type Option func(*config.DBCfgOpts)

// WithDirectory sets the directory option.
func WithDirectory(directory string) Option {
	return func(cfg *config.DBCfgOpts) {
		cfg.Directory = directory
	}
}

// WithSyncWriteInterval sets the syncWriteInterval option.
func WithSyncWriteInterval(interval time.Duration) Option {
	return func(cfg *config.DBCfgOpts) {
		cfg.SyncWriteInterval = interval
	}
}

// WithCompactInterval sets the compactInterval option.
func WithCompactInterval(interval time.Duration) Option {
	return func(cfg *config.DBCfgOpts) {
		cfg.CompactInterval = interval
	}
}

// WithCheckFileSizeInterval sets the checkFileSizeInterval option.
func WithCheckFileSizeInterval(interval time.Duration) Option {
	return func(cfg *config.DBCfgOpts) {
		cfg.CheckFileSizeInterval = interval
	}
}

// WithMaxActiveFileSize sets the maxActiveFileSize option.
func WithMaxActiveFileSize(size int64) Option {
	return func(cfg *config.DBCfgOpts) {
		cfg.MaxActiveFileSize = size
	}
}

func (db *KeyValorDatabase) Shutdown() error {
	return db.storage.Close()
}
