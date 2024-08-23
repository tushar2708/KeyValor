package storagecommon

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// createLockFile creates a lock file at the specified path and acquires an exclusive lock on it.
// The function returns the created file and any encountered errors.
//
// Parameters:
// - lockFilePath (string): The path where the lock file will be created.
//
// Returns:
// - *os.File: The created lock file.
// - error: Any error encountered during the creation or locking of the lock file.
func AcquireLockFile(lockFilePath string) (*os.File, error) {
	lockFile, err := os.Create(lockFilePath)
	if err != nil {
		return nil, fmt.Errorf("error creating lockfile (%s) error: %w", lockFilePath, err)
	}

	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("error acquiring lock on file (%s) error: %w", lockFilePath, err)
	}
	return lockFile, nil
}

func FreeLockFile(lockFile *os.File) error {
	// unlock the lock file
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("error unlocking lock file, error: %w", err)
	}

	// close te file descriptor
	if err := lockFile.Close(); err != nil {
		return fmt.Errorf("error closing lock file, error: %w", err)
	}

	// delete the lock file
	if err := os.Remove(lockFile.Name()); err != nil {
		return fmt.Errorf("error deleting lock file, error: %w", err)
	}
	return nil
}
