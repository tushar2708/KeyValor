package config

import (
	"KeyValor/constants"
	"time"
)

type DBCfgOpts struct {
	Directory             string
	SyncWriteInterval     time.Duration
	CompactInterval       time.Duration
	CheckFileSizeInterval time.Duration
	MaxActiveFileSize     int64
}

const (
	defaultSyncInterval      = time.Minute * 1
	defaultCompactInterval   = time.Hour * 2
	defaultFileSizeInterval  = time.Minute * 1
	defaultMaxActiveFileSize = 5 * constants.MB
)

func DefaultOpts() *DBCfgOpts {
	return &DBCfgOpts{
		Directory:             ".",
		SyncWriteInterval:     defaultSyncInterval,
		CompactInterval:       defaultCompactInterval,
		CheckFileSizeInterval: defaultFileSizeInterval,
		MaxActiveFileSize:     defaultMaxActiveFileSize,
	}
}
