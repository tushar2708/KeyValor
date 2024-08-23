package KeyValor

import (
	"time"

	"KeyValor/dbops"
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
func (db *KeyValorDatabase) Get(key string) ([]byte, error) {
	db.RLock()
	defer db.RUnlock()

	return db.storage.Get(key)
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
func (db *KeyValorDatabase) MGet(keys []string) ([]dbops.Value, error) {
	db.Lock()
	defer db.Unlock()

	return db.storage.MGet(keys)
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

	return db.storage.Exists(key)
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

	return db.storage.Set(key, value)
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

	return db.storage.Delete(key)
}

func (db *KeyValorDatabase) AllKeys() ([]string, error) {
	db.RLock()
	defer db.RUnlock()

	return db.storage.AllKeys()
}

func (db *KeyValorDatabase) Keys(regex string) ([]string, error) {
	db.RLock()
	defer db.RUnlock()

	return db.storage.Keys(regex)
}

func (db *KeyValorDatabase) Expire(key string, expireTime *time.Time) error {
	db.Lock()
	defer db.Unlock()

	return db.storage.Expire(key, expireTime)
}

// Redis-compatible INCR command
func (db *KeyValorDatabase) Incr(key string) error {
	db.Lock()
	defer db.Unlock()

	return db.storage.Incr(key)
}

// Redis-compatible DECR command
func (db *KeyValorDatabase) Decr(key string) error {
	db.Lock()
	defer db.Unlock()

	return db.storage.Decr(key)
}

// Redis-compatible TTL command
func (db *KeyValorDatabase) TTL(key string) (int64, error) {
	db.RLock()
	defer db.RUnlock()

	return db.storage.TTL(key)
}

// Redis-compatible SETEX command
func (db *KeyValorDatabase) SetEx(key string, value []byte, ttlSeconds int64) error {
	db.Lock()
	defer db.Unlock()

	return db.storage.SetEx(key, value, ttlSeconds)
}

// Redis-compatible PERSIST command
func (db *KeyValorDatabase) Persist(key string) error {
	db.Lock()
	defer db.Unlock()

	return db.storage.Persist(key)
}
