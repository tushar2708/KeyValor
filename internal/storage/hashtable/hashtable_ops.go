package hashtable

import (
	"fmt"
	"regexp"
	"time"

	"KeyValor/dbops"
	"KeyValor/internal/storage/storagecommon"
	"KeyValor/internal/utils/dataconvutils"
	"KeyValor/internal/utils/timeutils"
)

// Get retrieves the value associated with the given key from the key-value store.
// It acquires a read lock on the database to ensure thread safety.
//
// Parameters:
// - key: The key for which the value needs to be retrieved.
//
// Returns:
// - A byte slice containing the value associated with the key.
// - An error if the key is missing, expired, or the checksum is invalid.
//
// Note: This function does not perform any validation on the key or value.
func (hts *HashTableStorage) Get(key string) ([]byte, error) {
	hts.RLock()
	defer hts.RUnlock()

	return hts.getAndValidateMuLocked(key)
}

// MGet retrieves the values associated with the given keys from the key-value store.
// It acquires a write lock on the database to ensure thread safety.
//
// Parameters:
// - keys: A slice of keys for which the values need to be retrieved.
//
// Returns:
//   - A slice of Value structs, where each struct contains the value associated with a key and an error.
//     If the key is missing, expired, or the checksum is invalid, the corresponding error will be non-nil.
//     If the value is successfully retrieved, both the value and error will be nil.
//
// Note: This function does not perform any validation on the keys or values.
func (hts *HashTableStorage) MGet(keys []string) ([]dbops.Value, error) {
	hts.Lock()
	defer hts.Unlock()

	values := make([]dbops.Value, len(keys))

	for i, key := range keys {
		if val, err := hts.getAndValidateMuLocked(key); err != nil {
			values[i] = dbops.Value{
				Val: nil,
				Err: err,
			}
		} else {
			values[i] = dbops.Value{
				Val: val,
				Err: nil,
			}
		}

	}
	return values, nil
}

// Exists checks if a key exists in the key-value store.
// It acquires a read lock on the database to ensure thread safety.
//
// Parameters:
// - key: The key to be checked. It must be a non-empty string.
//
// Returns:
//   - A boolean value indicating whether the key exists in the store.
//     Returns true if the key exists, false otherwise.
func (hts *HashTableStorage) Exists(key string) bool {
	hts.RLock()
	defer hts.RUnlock()
	_, err := hts.keyLocationIndex.Get(key)
	return err == nil
}

// Set inserts or updates a key-value pair in the key-value store.
// It acquires a write lock on the database to ensure thread safety.
//
// Parameters:
// - key: The key to be inserted or updated. It must be a non-empty string.
// - value: The value to be associated with the given key. It can be an empty slice.
//
// Returns:
//   - An error if the key or value is invalid or if there is an issue writing to the database.
//     Otherwise, it returns nil.
func (hts *HashTableStorage) Set(key string, value []byte) error {
	hts.Lock()
	defer hts.Unlock()

	if err := validateEntry(key, value); err != nil {
		return fmt.Errorf("invalid key or value")
	}

	return hts.set(hts.ActiveDataFile, key, value, nil)
}

// Delete removes a key-value pair from the key-value store.
// It acquires a write lock on the database to ensure thread safety.
//
// Parameters:
// - key: The key to be deleted. It must be a non-empty string.
//
// Returns:
//   - An error if there is an issue writing to the database or if the key is missing.
//     Otherwise, it returns nil.
func (hts *HashTableStorage) Delete(key string) error {
	hts.Lock()
	defer hts.Unlock()

	// write a tombstone to the database
	if err := hts.set(hts.ActiveDataFile, key, []byte{}, nil); err != nil {
		return err
	}

	// delete the value from in-memory index
	hts.keyLocationIndex.Delete(key)
	return nil
}

func (hts *HashTableStorage) AllKeys() ([]string, error) {
	hts.RLock()
	defer hts.RUnlock()

	return keysMatchingRegex(hts.keyLocationIndex, "*")
}

func (hts *HashTableStorage) Keys(regex string) ([]string, error) {
	hts.RLock()
	defer hts.RUnlock()

	return keysMatchingRegex(hts.keyLocationIndex, regex)
}

func keysMatchingRegex(dbIndex storagecommon.DatabaseIndex, pattern string) ([]string, error) {
	// Compile the regex pattern
	var re *regexp.Regexp
	var err error
	if pattern != "*" {
		re, err = regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %w", err)
		}
	}

	var matchingKeys []string

	// Iterate over the map and add matching keys to the slice
	dbIndex.Map(func(key string, metaData storagecommon.Meta) error {
		if pattern == "*" || re.MatchString(key) {
			matchingKeys = append(matchingKeys, key)
		}
		return nil
	})

	return matchingKeys, nil
}

func (hts *HashTableStorage) Expire(key string, expireTime *time.Time) error {
	hts.Lock()
	defer hts.Unlock()

	record, err := hts.get(key)
	if err != nil {
		return err
	}

	record.Header.SetExpiry(expireTime.UnixNano())
	return hts.set(hts.ActiveDataFile, key, record.Value, expireTime)
}

// Redis-compatible INCR command
func (hts *HashTableStorage) Incr(key string) error {
	hts.Lock()
	defer hts.Unlock()

	value, err := hts.getAndValidateMuLocked(key)
	if err != nil {
		return err
	}

	intValue, err := dataconvutils.BytesToInt(value)
	if err != nil {
		return err
	}

	intValue++
	return hts.Set(key, dataconvutils.IntToBytes(intValue))
}

// Redis-compatible DECR command
func (hts *HashTableStorage) Decr(key string) error {
	hts.Lock()
	defer hts.Unlock()

	value, err := hts.getAndValidateMuLocked(key)
	if err != nil {
		return err
	}

	intValue, err := dataconvutils.BytesToInt(value)
	if err != nil {
		return err
	}

	intValue--
	return hts.Set(key, dataconvutils.IntToBytes(intValue))
}

// Redis-compatible TTL command
func (hts *HashTableStorage) TTL(key string) (int64, error) {
	hts.RLock()
	defer hts.RUnlock()

	record, err := hts.get(key)
	if err != nil {
		return -1, err
	}

	if record.Header.GetExpiry() == 0 {
		return -1, nil
	}

	ttl := record.Header.GetExpiry() - timeutils.CurrentTimeNanos()
	if ttl <= 0 {
		return -1, nil
	}

	return ttl / int64(time.Second), nil
}

// Redis-compatible SETEX command
func (hts *HashTableStorage) SetEx(key string, value []byte, ttlSeconds int64) error {
	hts.Lock()
	defer hts.Unlock()

	expireTime := time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	return hts.set(hts.ActiveDataFile, key, value, &expireTime)
}

// Redis-compatible PERSIST command
func (hts *HashTableStorage) Persist(key string) error {
	hts.Lock()
	defer hts.Unlock()

	record, err := hts.get(key)
	if err != nil {
		return err
	}

	record.Header.SetExpiry(0)
	return hts.set(hts.ActiveDataFile, key, record.Value, nil)
}
