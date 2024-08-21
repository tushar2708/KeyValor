package KeyValor

import (
	"KeyValor/internal/index"
	"KeyValor/internal/timeutils"
	"fmt"
	"regexp"
	"time"
)

type Value struct {
	Val []byte
	Err error
}

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
func (db *KeyValorDatabase) Get(key string) ([]byte, error) {
	db.RLock()
	defer db.RUnlock()

	return db.getAndValidateMuLocked(key)
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
func (db *KeyValorDatabase) MGet(keys []string) ([]Value, error) {
	db.Lock()
	defer db.Unlock()

	values := make([]Value, len(keys))

	for i, key := range keys {
		if val, err := db.getAndValidateMuLocked(key); err != nil {
			values[i] = Value{
				Val: nil,
				Err: err,
			}
		} else {
			values[i] = Value{
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
func (db *KeyValorDatabase) Exists(key string) bool {
	db.RLock()
	defer db.RUnlock()
	_, ok := db.keyLocationIndex[key]
	return ok
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
func (db *KeyValorDatabase) Set(key string, value []byte) error {
	db.Lock()
	defer db.Unlock()

	if err := validateEntry(key, value); err != nil {
		return fmt.Errorf("invalid key or value")
	}

	return db.set(db.activeDataFile, key, value, nil)
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
func (db *KeyValorDatabase) Delete(key string) error {
	db.Lock()
	defer db.Unlock()

	// write a tombstone to the database
	if err := db.set(db.activeDataFile, key, []byte{}, nil); err != nil {
		return err
	}

	// delete the value from in-memory index
	delete(db.keyLocationIndex, key)
	return nil
}

func (db *KeyValorDatabase) AllKeys(key string) ([]string, error) {
	db.RLock()
	defer db.RUnlock()

	return keysMatchingRegex(db.keyLocationIndex, "*")
}

func (db *KeyValorDatabase) Keys(regex string) ([]string, error) {
	db.RLock()
	defer db.RUnlock()

	return keysMatchingRegex(db.keyLocationIndex, regex)
}

func keysMatchingRegex(index index.LogStructuredKeyIndex, pattern string) ([]string, error) {
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
	for key := range index {
		// avoid regex check if we have a wildcard regex
		if pattern == "*" || re.MatchString(key) {
			matchingKeys = append(matchingKeys, key)
		}
	}

	return matchingKeys, nil
}

func (db *KeyValorDatabase) Expire(key string, expireTime *time.Time) error {
	db.Lock()
	defer db.Unlock()

	record, err := db.get(key)
	if err != nil {
		return err
	}

	record.Header.SetExpiry(expireTime.UnixNano())
	return db.set(db.activeDataFile, key, record.Value, expireTime)
}

// Redis-compatible INCR command
func (db *KeyValorDatabase) Incr(key string) error {
	db.Lock()
	defer db.Unlock()

	value, err := db.getAndValidateMuLocked(key)
	if err != nil {
		return err
	}

	intValue, err := bytesToInt(value)
	if err != nil {
		return err
	}

	intValue++
	return db.Set(key, intToBytes(intValue))
}

// Redis-compatible DECR command
func (db *KeyValorDatabase) Decr(key string) error {
	db.Lock()
	defer db.Unlock()

	value, err := db.getAndValidateMuLocked(key)
	if err != nil {
		return err
	}

	intValue, err := bytesToInt(value)
	if err != nil {
		return err
	}

	intValue--
	return db.Set(key, intToBytes(intValue))
}

// Redis-compatible TTL command
func (db *KeyValorDatabase) TTL(key string) (int64, error) {
	db.RLock()
	defer db.RUnlock()

	record, err := db.get(key)
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
func (db *KeyValorDatabase) SetEx(key string, value []byte, ttlSeconds int64) error {
	db.Lock()
	defer db.Unlock()

	expireTime := time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	return db.set(db.activeDataFile, key, value, &expireTime)
}

// Redis-compatible PERSIST command
func (db *KeyValorDatabase) Persist(key string) error {
	db.Lock()
	defer db.Unlock()

	record, err := db.get(key)
	if err != nil {
		return err
	}

	record.Header.SetExpiry(0)
	return db.set(db.activeDataFile, key, record.Value, nil)
}
