package storagecommon

type DatabaseIndex interface {
	Get(key string) (Meta, error)
	Put(key string, metaData Meta) error
	Delete(key string) error
	Map(f func(key string, metaData Meta) error)
	Open() error
	Flush() error
	FlushSnapshot(snapshot map[string]Meta) error
	Close() error
}
