package stash

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/gofrs/uuid"
)

type exoBuffer struct {
	bytes []byte
}

const chunkSize = 4 * 1024 * 1024

var s3FsPool = sync.Pool{
	New: func() interface{} {
		return &exoBuffer{make([]byte, chunkSize)}
	},
}

// writeFile writes a new file to the cache storage.
func writeTmpFile(dir, key string, r io.Reader) (string, int64, error) {

	ukey, err := uuid.NewV4()
	if err != nil {
		return "", 0, &FileError{dir, key, err}
	}

	path := path.Join(dir, ukey.String())

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
	if err != nil {
		return "", 0, &FileError{dir, key, err}
	}

	var total int64
	exoBuf := s3FsPool.Get().(*exoBuffer)
	defer s3FsPool.Put(exoBuf)

	for {
		// validate
		c.l.Lock()
		if err := c.validate(tmpPath, int64(chunkSize)); err != nil {
			c.l.Unlock()
			return tmpPath, 0, err
		}
		c.l.Unlock()

		// copy
		n, errRead := r.Read(exoBuf.bytes)
		if n == 0 && errRead == io.EOF {
			break
		} else if n == 0 && errRead != nil {
			_ = f.Close()
			return tmpPath, 0, &FileError{dir, key, err}
		}

		w, err := f.WriteAt(exoBuf.bytes[0:n], total)
		if err != nil {
			_ = f.Close()
			return tmpPath, 0, &FileError{dir, key, err}
		}

		total += int64(w)
	}

	err = f.Close()
	if err != nil {
		return tmpPath, total, &FileError{dir, key, err}
	}

	err = os.Rename(tmpPath, path)
	if err != nil {
		return tmpPath, total, &FileError{dir, key, err}
	}

	return path, total, nil
}

func shasum(v string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(v)))
}

func realFilePath(dir, key string) string {
	name := shasum(key) + ".cache"
	return path.Join(dir, name)
}
