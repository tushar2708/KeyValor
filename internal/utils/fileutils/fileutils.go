package fileutils

import (
	"fmt"
	"os"
	"path/filepath"
)

func SyncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func FileExists(filePath string) bool {
	if _, err := os.Stat(filePath); err != nil {
		return false
	}
	return true
}

// SyncDir fsyncs a directory to durably persist rename and unlink operations.
func SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// AtomicReplaceFile writes content to dst atomically using a unique temp file
// in the same directory, then renames it into place.
// On success, dst contains the data produced by write().
// On any failure, dst is left unchanged and the temp file is removed.
// Uses os.CreateTemp so concurrent calls with the same dst do not race on the temp file.
func AtomicReplaceFile(dst string, write func(f *os.File) error) error {
	dir := filepath.Dir(dst)

	tmp, err := os.CreateTemp(dir, ".tmp-atomic-*")
	if err != nil {
		return fmt.Errorf("error creating temp file: %w", err)
	}
	tmpPath := tmp.Name()

	if err := write(tmp); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("error syncing temp file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("error closing temp file: %w", err)
	}

	if err := os.Rename(tmpPath, dst); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("error renaming temp file to %s: %w", dst, err)
	}

	if err := SyncDir(dir); err != nil {
		return fmt.Errorf("error syncing directory after rename: %w", err)
	}

	return nil
}
