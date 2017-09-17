package stash

import (
	"testing"
	"fmt"
	"crypto/md5"
	"io/ioutil"
	"log"
	"os"
	"io"
	"bufio"
)

var (
	key string = "a"
)

func TestGetFileName(t *testing.T)  {
	fileName := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	returnFileName := getFileName(key)

	if fileName != returnFileName {
		t.Fatalf("FileName not equal (%s!=%s)", fileName, returnFileName)
	}
}

func getCacheDir(prefix string) string {
	dir, err := ioutil.TempDir("", prefix)

	if err != nil {
		log.Fatal(err)
	}

	return dir
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func deleteCacheDir(dir string) {
	checkError(os.RemoveAll(dir))
}

func deleteFile(path string) {
	checkError(os.Remove(path))
}

func getFile() (io.Reader, string) {
	dir := getCacheDir("stash_cache_file_write_test")
	path := dir + string(os.PathSeparator) + getFileName(key)
	f, e := os.Create(path)
	checkError(e)
	w := bufio.NewWriter(f)
	_, e = w.WriteString(key)
	checkError(e)
	e = w.Flush()
	checkError(e)
	f, _ = os.Open(path)
	return f, dir
}

func TestWriteToFileContentString(t *testing.T)  {
	dir := getCacheDir("stash_cache")
	defer deleteCacheDir(dir)

	path, length, err := writeToFile(dir, key, []byte(key))
	defer deleteFile(path)
	if err != nil {
		t.Fatalf("Can't write to file in path %s %#v", dir, err)
	}
	pathNameShouldBe := dir + string(os.PathSeparator) + getFileName(key)
	if path != pathNameShouldBe {
		t.Fatalf("Path Name not equal (%s!=%s)", path, pathNameShouldBe)
	}
	if length != 1 {
		t.Fatalf("Number of bytes is %d should be 1 ", length)
	}
}

func TestWriteToFileContentFileReader(t *testing.T)  {
	dir := getCacheDir("stash_cache")
	defer deleteCacheDir(dir)

	fi, p := getFile()
	path, length, err := writeToFile(dir, key, fi)
	defer deleteCacheDir(p)
	if err != nil {
		t.Fatalf("Can't write to file in path %s %#v", dir, err)
	}
	pathNameShouldBe := dir + string(os.PathSeparator) + getFileName(key)
	if path != pathNameShouldBe {
		t.Fatalf("Path Name not equal (%s!=%s)", path, pathNameShouldBe)
	}
	if length != 1 {
		t.Fatalf("Number of bytes is %d should be 1 ", length)
	}
}