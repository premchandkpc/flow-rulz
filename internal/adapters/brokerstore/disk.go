// Package brokerstore provides local-disk persistence for the embedded broker.
package brokerstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const maxSnapshotBytes = 128 << 20

// Disk atomically replaces a snapshot under an exclusive process lock.
// The data directory must reside on a local filesystem supporting fsync and rename.
type Disk struct {
	dir  string
	lock *os.File
}

// Open acquires the directory lock. A second broker cannot share this directory.
func Open(dir string) (*Disk, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "broker.lock"), os.O_CREATE|os.O_RDWR, 0600) // #nosec G304 -- operator-configured data directory; fixed filename
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(fmt.Errorf("broker directory is locked: %w", err), lock.Close())
	}
	return &Disk{dir: dir, lock: lock}, nil
}

// Load reads a bounded snapshot. An empty directory starts a fresh broker.
func (d *Disk) Load() ([]byte, error) {
	f, err := os.Open(filepath.Join(d.dir, "broker.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, maxSnapshotBytes+1))
	err = errors.Join(readErr, f.Close())
	if len(data) > maxSnapshotBytes {
		return nil, errors.New("broker snapshot exceeds 128 MiB")
	}
	return data, err
}

// Save syncs a new snapshot before atomically replacing and syncing its directory.
func (d *Disk) Save(data []byte) (saveErr error) {
	if len(data) > maxSnapshotBytes {
		return errors.New("broker snapshot exceeds 128 MiB")
	}
	f, err := os.CreateTemp(d.dir, "broker-*.tmp")
	if err != nil {
		return err
	}
	path := f.Name()
	defer func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			saveErr = errors.Join(saveErr, err)
		}
	}()
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	if err := errors.Join(writeErr, syncErr, f.Close()); err != nil {
		return err
	}
	if err := os.Rename(path, filepath.Join(d.dir, "broker.json")); err != nil {
		return err
	}
	dir, err := os.Open(d.dir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// Close releases process ownership. The lock file may safely remain on disk.
func (d *Disk) Close() error {
	return errors.Join(syscall.Flock(int(d.lock.Fd()), syscall.LOCK_UN), d.lock.Close())
}
