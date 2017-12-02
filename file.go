package stash

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

// writeFile writes a new file to the cache storage.
func writeFile(dir, key string, r io.Reader) (string, int64, error) {
	name := shasum(key)
	path := filepath(dir, name)

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

func filepath(dir, name string) string {
	return dir + string(os.PathSeparator) + name
}

func shasum(v string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(v)))
}
