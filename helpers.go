package stash

import (
	"fmt"
	"crypto/md5"
	"os"
	"bufio"
	"io"
	"errors"
)

func getFileName(key string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}

//write container content to file
func writeToFile(dir, key string, container interface{}) (path string, length int64, error error)  {
	fileName := getFileName(key)
	path = dir + string(os.PathSeparator) + fileName
	f, e := os.Create(path)
	defer f.Close()
	if e != nil {
		error = ErrCreateFile
		return
	}
	w := bufio.NewWriter(f)
	switch contentGetter := container.(type) {
	case []byte:
		var lInt int
		lInt, error = w.WriteString(string(contentGetter))
		if error != nil {
			return path, length, ErrWriteFile
		}
		length = int64(lInt)
	case io.Reader:
		length, error = w.ReadFrom(contentGetter)
		if error != nil {
			return path, length, ErrWriteFile
		}
	default:
		return path, length, errors.New("invalid type")
	}
	error = w.Flush()
	if error != nil {
		return
	}
	return
}