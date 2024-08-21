package KeyValor

import (
	"KeyValor/constants"
	"time"
)

const (
	defaultSyncInterval      = time.Minute * 1
	defaultCompactInterval   = time.Hour * 2
	defaultFileSizeInterval  = time.Minute * 1
	defaultMaxActiveFileSize = 5 * constants.MB
)

func DefaultOpts() *DBCfgOpts {
	return &DBCfgOpts{
		directory:             ".",
		syncWriteInterval:     defaultSyncInterval,
		compactInterval:       defaultCompactInterval,
		checkFileSizeInterval: defaultFileSizeInterval,
		maxActiveFileSize:     defaultMaxActiveFileSize,
	}
}

// Option is a function that configures a DBCfgOpts.
type Option func(*DBCfgOpts)

// WithDirectory sets the directory option.
func WithDirectory(directory string) Option {
	return func(cfg *DBCfgOpts) {
		cfg.directory = directory
	}
}

// WithSyncWriteInterval sets the syncWriteInterval option.
func WithSyncWriteInterval(interval time.Duration) Option {
	return func(cfg *DBCfgOpts) {
		cfg.syncWriteInterval = interval
	}
}

// WithCompactInterval sets the compactInterval option.
func WithCompactInterval(interval time.Duration) Option {
	return func(cfg *DBCfgOpts) {
		cfg.compactInterval = interval
	}
}

// WithCheckFileSizeInterval sets the checkFileSizeInterval option.
func WithCheckFileSizeInterval(interval time.Duration) Option {
	return func(cfg *DBCfgOpts) {
		cfg.checkFileSizeInterval = interval
	}
}

// WithMaxActiveFileSize sets the maxActiveFileSize option.
func WithMaxActiveFileSize(size int64) Option {
	return func(cfg *DBCfgOpts) {
		cfg.maxActiveFileSize = size
	}
}
