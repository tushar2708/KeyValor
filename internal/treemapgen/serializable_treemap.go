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
	internalMap *treemap.Map
	keyType     reflect.Type
	valueType   reflect.Type
	encoder     *records.RecordEncoder[K, records.Header, V] // Use RecordEncoder for encoding/decoding records
}

// NewSerializableTreeMap creates a new TreeMap with type safety and serializability
func NewSerializableTreeMap[K comparable, V records.Record[K]](comparator utils.Comparator) *SerializableTreeMap[K, V] {
	return &SerializableTreeMap[K, V]{
		internalMap: treemap.NewWith(comparator),
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
	tm.internalMap.Put(key, value)
}

func (tm *SerializableTreeMap[K, V]) Size() int {
	return tm.internalMap.Size()
}

// Get retrieves a value for the given key with type checking
func (tm *SerializableTreeMap[K, V]) Get(key K) (V, bool) {
	if reflect.TypeOf(key) != tm.keyType {
		panic(fmt.Sprintf("key type mismatch: expected %v, got %v", tm.keyType, reflect.TypeOf(key)))
	}
	value, found := tm.internalMap.Get(key)
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

func (tm *SerializableTreeMap[K, V]) Min() (K, V) {

	key, value := tm.internalMap.Min()

	return tm.typeCheckKeyValue(key, value)
}

func (tm *SerializableTreeMap[K, V]) Max() (K, V) {

	key, value := tm.internalMap.Max()

	return tm.typeCheckKeyValue(key, value)
}

func (tm *SerializableTreeMap[K, V]) Floor(key K) (foundKey K, foundValue V) {

	k, v := tm.internalMap.Floor(key)

	if k == nil || v == nil {
		return tm.zeroValueOfK(), tm.zeroValueOfV()
	}

	return tm.typeCheckKeyValue(k, v)
}

func (tm *SerializableTreeMap[K, V]) Ceiling(key K) (foundKey K, foundValue V) {
	k, v := tm.internalMap.Ceiling(key)

	if k == nil || v == nil {
		return tm.zeroValueOfK(), tm.zeroValueOfV()
	}

	return tm.typeCheckKeyValue(k, v)
}

func (tm *SerializableTreeMap[K, V]) typeCheckKeyValue(key interface{}, value interface{}) (K, V) {
	typedKey, ok := key.(K)
	if !ok {
		panic(fmt.Sprintf("key type mismatch: expected %v, got %v", tm.keyType, reflect.TypeOf(key)))
	}

	typedValue, ok := value.(V)
	if !ok {
		panic(fmt.Sprintf("value type mismatch: expected %v, got %v", tm.valueType, reflect.TypeOf(value)))
	}

	return typedKey, typedValue
}

// zeroValueK returns the zero value for type K.
func (tm *SerializableTreeMap[K, V]) zeroValueOfK() K {
	var zero K
	return zero
}

// zeroValueV returns the zero value for type V.
func (tm *SerializableTreeMap[K, V]) zeroValueOfV() V {
	var zero V
	return zero
}

// isZeroValue checks if the given value is the zero value for its type.
func (tm *SerializableTreeMap[K, V]) IsZeroValue(value interface{}) bool {
	// Get the reflect.Value representation of the input
	v := reflect.ValueOf(value)

	// Compare the input with its zero value using reflect.Zero
	return v.IsValid() && v.Interface() == reflect.Zero(v.Type()).Interface()
}

// Encode serializes the TreeMap to a byte slice using RecordEncoder
func (tm *SerializableTreeMap[K, V]) Encode(buff *bytes.Buffer) error {
	// buff := &bytes.Buffer{}

	// Iterate over the TreeMap and encode each record
	it := tm.internalMap.Iterator()
	for it.Next() {
		// key := it.Key().(K)
		value := it.Value().(V)

		// Encode the value using RecordEncoder
		if err := tm.encoder.EncodeF(value, buff); err != nil {
			return fmt.Errorf("failed to encode value: %w", err)
		}
	}

	return nil
}

// Decode deserializes the byte slice into the TreeMap using RecordEncoder
func (tm *SerializableTreeMap[K, V]) Decode(data []byte) error {
	reader := bytes.NewReader(data)

	// Clear the existing map
	tm.internalMap.Clear()

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
