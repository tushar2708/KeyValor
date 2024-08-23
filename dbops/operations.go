package dbops

import "time"

type Value struct {
	Val []byte
	Err error
}

type DatabaseOperations interface {
	ReadOnlyOps
	WriteOps
}

type ReadOnlyOps interface {
	Get(key string) ([]byte, error)
	MGet(keys []string) ([]Value, error)
	Exists(key string) bool
	TTL(key string) (int64, error)
	AllKeys() ([]string, error)
	Keys(regex string) ([]string, error)
}

type WriteOps interface {
	Set(key string, value []byte) error
	Delete(key string) error
	SetEx(key string, value []byte, ttlSeconds int64) error
	Expire(key string, expireTime *time.Time) error
	Persist(key string) error
	Incr(key string) error
	Decr(key string) error
}
