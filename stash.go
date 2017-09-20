package stash

import (
	"errors"
	"container/list"
	"sync"
)

var (
	// ErrNotFound represents the error encountered when no value found for the provided key.
	ErrNotFound = errors.New("not found")

	// ErrEmptyDir represents the error encountered when no dir value is empty.
	ErrEmptyDir = errors.New("empty directory")

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

type ItemMeta struct {
	Key string //key of blob
	Size int64 //Size of blob
	Path string //Path of blob
}

type Cache struct {
	Dir                   	string 					//Directory Path where files will be saved
	StorageSize           	int64  					//Total Storage Size for file to write
	TotalFilesToBeWritten 	int64  					//Total Number of files to be written

	StorageSizeUsed   		int64 					//Total Storage used
	TotalFilesWritten 		int64 					//Total Files currently written

	ItemsList 				*list.List               //ItemsList will hold the list of entries
	Items     				map[string]*list.Element //Items/Files saved in storage

	Lock 					sync.RWMutex			 //Read Write sync
}