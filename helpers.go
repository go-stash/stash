package stash

import (
	"fmt"
	"crypto/md5"
	"os"
	"bufio"
	"io"
)

func getFileName(key string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}

//write container content to file
func writeToFile(dir, key string, container io.Reader) (path string, length int64, error error)  {
	fileName := getFileName(key)
	path = dir + string(os.PathSeparator) + fileName
	f, e := os.Create(path)
	defer f.Close()
	if e != nil {
		error = ErrCreateFile
		return
	}
	w := bufio.NewWriter(f)
	length, error = w.ReadFrom(container)
	if error != nil {
		return path, length, ErrWriteFile
	}
	error = w.Flush()
	if error != nil {
		return
	}
	return
}