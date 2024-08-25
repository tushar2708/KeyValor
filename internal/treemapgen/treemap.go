package treemapgen

import (
	"fmt"
	"reflect"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

// TreeMap is a generic wrapper around treemap.Map to add type safety
type TreeMap[K comparable, V any] struct {
	InternalMap *treemap.Map
	keyType     reflect.Type
	valueType   reflect.Type
}

// NewTreeMap creates a new TreeMap with type safety
func NewTreeMap[K comparable, V any](comparator utils.Comparator) *TreeMap[K, V] {
	return &TreeMap[K, V]{
		InternalMap: treemap.NewWith(comparator),
		keyType:     reflect.TypeOf((*K)(nil)).Elem(),
		valueType:   reflect.TypeOf((*V)(nil)).Elem(),
	}
}

// Put adds a key-value pair to the map with type checking
func (tm *TreeMap[K, V]) Put(key K, value V) {
	if reflect.TypeOf(key) != tm.keyType {
		panic(fmt.Sprintf("key type mismatch: expected %v, got %v", tm.keyType, reflect.TypeOf(key)))
	}
	if reflect.TypeOf(value) != tm.valueType {
		panic(fmt.Sprintf("value type mismatch: expected %v, got %v", tm.valueType, reflect.TypeOf(value)))
	}
	tm.InternalMap.Put(key, value)
}

func (tm *TreeMap[K, V]) Size() int {
	return tm.InternalMap.Size()
}

// Get retrieves a value for the given key with type checking
func (tm *TreeMap[K, V]) Get(key K) (V, bool) {
	if reflect.TypeOf(key) != tm.keyType {
		panic(fmt.Sprintf("key type mismatch: expected %v, got %v", tm.keyType, reflect.TypeOf(key)))
	}
	value, found := tm.InternalMap.Get(key)
	if !found {
		var zero V
		return zero, false
	}

	// Handle conversion from float64 to integer types
	switch tm.valueType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if floatValue, ok := value.(float64); ok {
			return V(reflect.ValueOf(int(floatValue)).Convert(tm.valueType).Interface().(V)), true
		}
	}

	typedValue, ok := value.(V)
	if !ok {
		panic(fmt.Sprintf("value type mismatch: expected %v, got %v", tm.valueType, reflect.TypeOf(value)))
	}
	return typedValue, true
}
