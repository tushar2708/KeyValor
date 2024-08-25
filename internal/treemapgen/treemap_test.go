package treemapgen

import (
	"testing"

	"github.com/emirpasic/gods/utils"
	"github.com/stretchr/testify/require"
)

func TestTreeMapEncodeAndDecode(t *testing.T) {
	t.Parallel()

	// Create a TreeMap with simple key-value pairs
	tm1 := NewTreeMap[string, int](utils.StringComparator)
	tm1.Put("one", 1)
	tm1.Put("two", 2)
	tm1.Put("three", 3)

	require.Equal(t, 3, tm1.Size())
	one, _ := tm1.Get("one")
	require.Equal(t, 1, one)
	two, _ := tm1.Get("two")
	require.Equal(t, 2, two)
	three, _ := tm1.Get("three")
	require.Equal(t, 3, three)

	tm2 := NewTreeMap[int, string](utils.IntComparator)
	tm2.Put(1, "one")
	tm2.Put(2, "two")
	tm2.Put(3, "three")

	require.Equal(t, 3, tm2.Size())
	oneS, _ := tm2.Get(1)
	require.Equal(t, "one", oneS)
	twoS, _ := tm2.Get(2)
	require.Equal(t, "two", twoS)
	threeS, _ := tm2.Get(3)
	require.Equal(t, "three", threeS)
}
