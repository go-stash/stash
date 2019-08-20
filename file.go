package stash

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

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
	f, err := os.Create(path)
	defer f.Close()
	if err != nil {
		return "", 0, &FileError{dir, key, err}
	}
	var total int64
	chunkSize := int64(1024)
	for {
		// validate
		if err := c.validate(path, chunkSize); err != nil {
			return path, 0, err
		}

		// copy
		n, err := io.CopyN(f, r, 1024)
		if err != nil {
			return "", 0, &FileError{dir, key, err}
		}

		total += n
		if n <= chunkSize {
			break
		}
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
