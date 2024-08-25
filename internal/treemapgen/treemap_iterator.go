package treemapgen

import (
	"fmt"
	"reflect"

	"github.com/emirpasic/gods/maps/treemap"
)

// TreeMapIterator is a type-safe iterator for TreeMap
type TreeMapIterator[K any, V any] struct {
	iterator treemap.Iterator
}

// Iterator returns a new TreeMapIterator for the TreeMap
func (tm *SerializableTreeMap[K, V]) Iterator() *TreeMapIterator[K, V] {
	return &TreeMapIterator[K, V]{iterator: tm.InternalMap.Iterator()}
}

// Next moves the iterator to the next element
func (it *TreeMapIterator[K, V]) Next() bool {
	return it.iterator.Next()
}

// Key returns the current key of the iterator
func (it *TreeMapIterator[K, V]) Key() K {
	key := it.iterator.Key()
	typedKey, ok := key.(K)
	if !ok {
		panic(fmt.Sprintf("key type mismatch: expected %v, got %v", reflect.TypeOf((*K)(nil)).Elem(), reflect.TypeOf(key)))
	}
	return typedKey
}

// Value returns the current value of the iterator
func (it *TreeMapIterator[K, V]) Value() V {
	value := it.iterator.Value()
	typedValue, ok := value.(V)
	if !ok {
		panic(fmt.Sprintf("value type mismatch: expected %v, got %v", reflect.TypeOf((*V)(nil)).Elem(), reflect.TypeOf(value)))
	}
	return typedValue
}
