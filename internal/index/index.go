package index

type DatabaseIndex interface {
	Get(key string) (Meta, error)
	Put(key string, metaData Meta) error
	Delete(key string) error
	LoadFromFile(filePath string) error
	DumpToFile(filePath string) error
	Map(f func(key string, metaData Meta) error)
}
