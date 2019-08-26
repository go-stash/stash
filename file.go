package stash

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sync"
)

type exoBuffer struct {
	bytes []byte
}

var s3FsPool = sync.Pool{
	New: func() interface{} {
		return &exoBuffer{make([]byte, 4*1024*1024)}
	},
}

// writeFile writes a new file to the cache storage.
func writeFile(dir, key string, r io.Reader) (string, int64, error) {
	path := realFilePath(dir, key)
	f, err := os.Create(path)
	defer f.Close()
	if err != nil {
		return "", 0, &FileError{dir, key, err}
	}
	n, err := io.Copy(f, r)
	if err != nil {
		return "", 0, &FileError{dir, key, err}
	}

	return path, n, nil
}

func writeFileValidate(c *Cache,
	dir, key string, r io.Reader) (string, int64, error) {

	path := realFilePath(dir, key)
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	defer f.Close()
	if err != nil {
		return "", 0, &FileError{dir, key, err}
	}
	var total int64
	chunkSize := 1024 * 1024
	exoBuf := s3FsPool.Get().(*exoBuffer)
	defer s3FsPool.Put(exoBuf)

	for {
		// validate
		if err := c.validate(tmpPath, int64(chunkSize)); err != nil {
			return tmpPath, 0, err
		}

		// copy
		n, err := r.Read(exoBuf.bytes)
		if err != nil {
			return tmpPath, 0, &FileError{dir, key, err}
		}

		w, err := f.WriteAt(exoBuf.bytes[0:n], total)
		if err != nil {
			return tmpPath, 0, &FileError{dir, key, err}
		}

		total += int64(w)
		if n < chunkSize {
			break
		}
	}
	err = os.Rename(tmpPath, path)
	if err != nil {
		return tmpPath, 0, &FileError{dir, key, err}
	}
	return path, total, nil
}

func filepath(dir, name string) string {
	return dir + string(os.PathSeparator) + name
}

func shasum(v string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(v)))
}

func realFilePath(dir, key string) string {
	name := shasum(key)
	return filepath(dir, name)
}
