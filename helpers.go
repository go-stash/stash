package stash

import (
	"fmt"
	"crypto/md5"
)

func getFileName(key string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}
