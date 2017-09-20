package stash

import (
	"testing"
	"time"
	"math/rand"
	"os"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func TestGet(t *testing.T) {
	//here we create a directory to hold the temp file
	dir := getCacheDir("stash_cache_file_write_test")
	defer deleteCacheDir(dir)
	path := dir + string(os.PathSeparator) + getFileName(key)
	f, e := os.Create(path)
	checkError(e)
	var l int64 = 10000000
	e = f.Truncate(l)//we create a 10MB file
	checkError(e)
	//create stash object
	c := getCacheDir("stash_cache_file_read_write_test")
	defer deleteCacheDir(c)
	s, _ := New(c, l, 1)
	if _, e := s.Get(key); e != ErrNotFound {
		t.Fatal("Error not equal ErrNotFound")
	}
}

func TestAdd(t *testing.T) {
	//here we create a directory to hold the temp file
	dir := getCacheDir("stash_cache_file_write_test")
	defer deleteCacheDir(dir)
	path := dir + string(os.PathSeparator) + getFileName(key)
	f, e := os.Create(path)
	checkError(e)
	var l int64 = 10000000
	e = f.Truncate(l)//we create a 10MB file
	checkError(e)
	//create stash object
	c := getCacheDir("stash_cache_file_read_write_test")
	defer deleteCacheDir(c)
	s, _ := New(c, l, 1)
	if e := s.AddFrom(key, f); e == nil {
		if _, e := s.Get(key); e == ErrNotFound {
			t.Fatalf("Key not in %#v", s)
		}
		if k := s.Keys(); len(k) != 1 {
			t.Fatal("number of keys mismatch")
		}
	} else {
		t.Fatal(e)
	}
	secondKey := "b"
	path = dir + string(os.PathSeparator) + getFileName(secondKey)
	f, e = os.Create(path)
	checkError(e)
	var l2 int64 = 100000000
	e = f.Truncate(l2)//we create a 10MB file
	checkError(e)
	if e := s.AddFrom(secondKey, f); e != ErrFileSizeExceedsStorageSize {
		t.Fatalf("Error not equal (%#v!=%#v)", e, ErrFileSizeExceedsStorageSize)
	} else {
		if k := s.Keys(); len(k) != 1 {
			t.Fatal("number of keys mismatch")
		}
	}

	str := String(256)
	if e := s.Add(key, []byte(str)); e == nil {
		if _, e := s.Get(key); e == ErrNotFound {
			t.Fatalf("Key not in %#v", s)
		}
		if k := s.Keys(); len(k) != 1 {
			t.Fatal("number of keys mismatch")
		}
	} else {
		t.Fatal(e)
	}
}

func TestNew(t *testing.T) {
	if _, err := New("", 0, 0); err != ErrEmptyDir {
		t.Fatal("Error not equal ErrEmptyDir")
	}
	if _, err := New("some-rand-dir", 0, 0); err != ErrInavlidSize {
		t.Fatal("Error not equal ErrInavlidSize")
	}
	if _, err := New("some-rand-dir", 1000, 0); err != ErrInavlidCap {
		t.Fatal("Error not equal ErrInavlidCap")
	}
}