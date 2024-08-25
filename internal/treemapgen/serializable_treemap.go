package treemapgen

import (
	"KeyValor/internal/records"
	"bytes"
	"fmt"
	"reflect"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

// SerializableTreeMap is a generic wrapper around treemap.Map to add type safety and serializability
type SerializableTreeMap[K comparable, V records.Record[K]] struct {
	InternalMap *treemap.Map
	keyType     reflect.Type
	valueType   reflect.Type
	encoder     *records.RecordEncoder[K, records.Header, V] // Use RecordEncoder for encoding/decoding records
}

// NewSerializableTreeMap creates a new TreeMap with type safety and serializability
func NewSerializableTreeMap[K comparable, V records.Record[K]](comparator utils.Comparator) *SerializableTreeMap[K, V] {
	return &SerializableTreeMap[K, V]{
		InternalMap: treemap.NewWith(comparator),
		keyType:     reflect.TypeOf((*K)(nil)).Elem(),
		valueType:   reflect.TypeOf((*V)(nil)).Elem(),
		encoder:     records.NewRecordEncoder[K, records.Header, V](),
	}
}

// Put adds a key-value pair to the map with type checking
func (tm *SerializableTreeMap[K, V]) Put(key K, value V) {
	if reflect.TypeOf(key) != tm.keyType {
		panic(fmt.Sprintf("key type mismatch: expected %v, got %v", tm.keyType, reflect.TypeOf(key)))
	}
	if reflect.TypeOf(value) != tm.valueType {
		panic(fmt.Sprintf("value type mismatch: expected %v, got %v", tm.valueType, reflect.TypeOf(value)))
	}
	tm.InternalMap.Put(key, value)
}

func (tm *SerializableTreeMap[K, V]) Size() int {
	return tm.InternalMap.Size()
}

// Get retrieves a value for the given key with type checking
func (tm *SerializableTreeMap[K, V]) Get(key K) (V, bool) {
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

// Encode serializes the TreeMap to a byte slice using RecordEncoder
func (tm *SerializableTreeMap[K, V]) Encode() ([]byte, error) {
	buff := &bytes.Buffer{}

	// Iterate over the TreeMap and encode each record
	it := tm.InternalMap.Iterator()
	for it.Next() {
		// key := it.Key().(K)
		value := it.Value().(V)

		// Encode the value using RecordEncoder
		if err := tm.encoder.EncodeF(value, buff); err != nil {
			return nil, fmt.Errorf("failed to encode value: %w", err)
		}
	}

	return buff.Bytes(), nil
}

// Decode deserializes the byte slice into the TreeMap using RecordEncoder
func (tm *SerializableTreeMap[K, V]) Decode(data []byte) error {
	reader := bytes.NewReader(data)

	// Clear the existing map
	tm.InternalMap.Clear()

	// Iterate over the buffer and decode each record
	for reader.Len() > 0 {
		// Decode the value (which contains the key inside) using RecordEncoder

		// V s a pointer, but we need an instance of the type that it is pointing to
		// The following 2 lines acieves that
		valueType := reflect.TypeOf((*V)(nil)).Elem().Elem()
		// Create a new instance of the underlying type and get its address
		record := reflect.New(valueType).Interface().(V)

		if err := tm.encoder.DecodeF(record, reader); err != nil {
			return fmt.Errorf("failed to decode value: %w", err)
		}

		// Retrieve the key from the decoded value
		key, err := record.GetKey()
		if err != nil {
			return fmt.Errorf("failed to get key from decoded value: %w", err)
		}

		// var key K
		// if keyType := reflect.TypeOf(key); keyType.Kind() == reflect.String {
		// 	key = reflect.ValueOf(keyStr).Convert(keyType).Interface().(K)
		// } else {
		// 	return fmt.Errorf("key type mismatch or unsupported key type")
		// }

		tm.Put(key, record)
	}

	return nil
}
