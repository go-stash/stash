package stash

import "errors"

var (
	// ErrNotFound represents the error encountered when no value found for the provided key.
	ErrNotFound = errors.New("not found")

	// ErrInvalidDir represents the error encountered when no dir value is empty.
	ErrInvalidDir = errors.New("invalid directory")

	// ErrCreateFile represents the error encountered when can't create file.
	ErrCreateFile = errors.New("can't create file")

	// ErrWriteFile represents the error encountered when can't write file.
	ErrWriteFile = errors.New("can't write file")

	// ErrInavlidSize represents the error encountered when sz is less or equal to zero.
	ErrInavlidSize = errors.New("storage size must be greater then zero")

	// ErrInavlidCap represents the error encountered when c is less or equal to zero.
	ErrInavlidCap = errors.New("file number must be greater then zero")

	// ErrFileSizeExceedsStorageSize represents the error encountered when file size more then storage size.
	ErrFileSizeExceedsStorageSize = errors.New("file size must be less or equal storage size")
)
