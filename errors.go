package stash

import "errors"

var (
	ErrNotFound      = errors.New("not found")
	ErrBadDir        = errors.New("invalid directory")
	ErrBadSize       = errors.New("storage size must be greater then zero")
	ErrBadCap        = errors.New("file number must be greater then zero")
	ErrTooLarge      = errors.New("file size must be less or equal storage size")
	ErrUntagged      = errors.New("file is not tagged")
	ErrAlreadyTagged = errors.New("file is already tagged")
)

// FileError records the storage directory name and key of the that failed to cached.
type FileError struct {
	Dir string
	Key string
	Err error
}

func (e *FileError) Error() string {
	return "stash: " + e.Dir + " " + e.Key + ": " + e.Err.Error()
}
