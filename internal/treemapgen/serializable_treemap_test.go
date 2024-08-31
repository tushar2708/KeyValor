package treemapgen

import (
	"KeyValor/internal/records"
	"bytes"
	"testing"

	"github.com/emirpasic/gods/utils"
	"github.com/stretchr/testify/require"
)

func TestSerializableTreeMapEncodeAndDecodeComplexTypes(t *testing.T) {
	// t.Parallel()

	// Create a TreeMap with comples values
	tm := NewSerializableTreeMap[string, *records.CommandRecord](utils.StringComparator)
	tm.Put("one", records.NewSetCommandRecord("one", []byte("value1")))
	tm.Put("two", records.NewSetCommandRecord("two", []byte("value2")))
	tm.Put("three", records.NewSetCommandRecord("three", []byte("value3")))

	// Encode the TreeMap
	var encodedBytes *bytes.Buffer
	err := tm.Encode(encodedBytes)
	require.NoError(t, err)

	decodedMap := NewSerializableTreeMap[string, *records.CommandRecord](utils.StringComparator)

	// Decode the TreeMap
	err = decodedMap.Decode(encodedBytes.Bytes())
	require.NoError(t, err)

	require.Equal(t, 3, decodedMap.Size())

	one, _ := decodedMap.Get("one")
	require.Equal(t, "one", one.Key)

	two, _ := decodedMap.Get("two")
	require.Equal(t, "two", two.Key)

	three, _ := decodedMap.Get("three")
	require.Equal(t, "three", three.Key)
}
