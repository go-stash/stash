package stash

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/gofrs/uuid"
)

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

func shasum(v string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(v)))
}

func realFilePath(dir, key string) string {
	name := shasum(key) + ".cache"
	//name := key + ".cache"
	return path.Join(dir, name)
}
