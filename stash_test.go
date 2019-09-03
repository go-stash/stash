package stash

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"path"
	"reflect"
	"testing"
)

var storageDir string

func capStorage() int {
	dir, err := os.Open(storageDir)
	defer dir.Close()

	if err != nil {
		return -1
	}

	if fs, err := dir.Readdirnames(-1); err == nil {
		return len(fs)
	}

	return -1
}

// clearStorage empties the temporary storage directory
func clearStorage() {
	err := os.RemoveAll(storageDir)
	if err != nil {
		panic(err)
	}

	err = os.Mkdir(storageDir, 0777)
	if err != nil {
		panic(err)
	}
}

func TestNew(t *testing.T) {
	for i, c := range []struct {
		dir string
		sz  int64
		c   int64
		err error
	}{
		{
			dir: "",
			sz:  2048,
			c:   4,
			err: ErrBadDir,
		},
		{
			dir: storageDir,
			sz:  0,
			c:   0,
			err: ErrBadSize,
		},
		{
			dir: storageDir,
			sz:  2048,
			c:   0,
			err: ErrBadCap,
		},
	} {
		clearStorage()

		_, err := New(c.dir, c.sz, c.c)
		if err != c.err {
			t.Fatalf("#%d: Expected err == %q, got %q", i+1, c.err, err)
		}
	}
}

func TestClear(t *testing.T) {
	clearStorage()
	s, err := New(storageDir, 2048000, 40)
	catch(err)
	for k, b := range blobs {
		err := s.Put(k, b)
		catch(err)
	}
	cl := s.NumEntries()
	if cl != int64(len(blobs)) {
		t.Fatalf("Expected cap == %d, got %d", len(blobs), cl)
	}

	cs := capStorage()
	if cs != len(blobs) {
		t.Fatalf("Expected capStorage == %d, got %d", len(blobs), cs)
	}

	s.Clear()

	cs = capStorage()
	if cs != 0 {
		t.Fatalf("Expected capStorage == 0, got %d", cs)
	}
}

func TestCachePut(t *testing.T) {
	clearStorage()

	s, err := New(storageDir, 2048000, 40)
	catch(err)
	for k, b := range blobs {
		err := s.Put(k, b)
		catch(err)
	}

	for k, b := range blobs {
		path := path.Join(storageDir, shasum(k))
		v, err := ioutil.ReadFile(path)
		catch(err)
		if !bytes.Equal(b, v) {
			t.Fatalf("Expected v == %q, got %q", b, v)
		}
	}
}

func TestDeleteIf(t *testing.T) {
	clearStorage()

	s, err := New(storageDir, 2048000, 40)
	catch(err)
	err = s.PutWithTag("test1", []byte("tag1"), []byte("content"))
	catch(err)
	err = s.PutWithTag("test2", []byte("tag2"), []byte("content"))
	catch(err)

	if sz := s.NumEntries(); sz != 2 {
		t.Fatalf("Expected size == 2, got %d", sz)
	}

	_, err = s.DeleteIf("test1", func(tag []byte) bool {
		return bytes.Equal(tag, []byte("tag1"))
	})
	catch(err)

	_, err = s.DeleteIf("test2", func(tag []byte) bool {
		return bytes.Equal(tag, []byte("tag1"))
	})
	catch(err)

	if sz := s.NumEntries(); sz != 1 {
		t.Fatalf("Expected size == 1, got %d", sz)
	}
}

func TestAlreadyTagged(t *testing.T) {
	clearStorage()

	s, err := New(storageDir, 2048000, 40)
	catch(err)
	err = s.PutWithTag("test", []byte("test"), []byte("content"))
	catch(err)
	err = s.SetTag("test", []byte("test2"))
	if err != ErrAlreadyTagged {
		t.Fatalf("Expected error == ErrAlreadyTagged, got error '%s'", err)
	}
}

func TestCachePutAndGetWithTag(t *testing.T) {
	clearStorage()

	s, err := New(storageDir, 2048000, 40)
	catch(err)
	for k, b := range blobs {
		err := s.PutWithTag(k, []byte(k), b)
		catch(err)
	}

	for k, _ := range blobs {
		_, tag, err := s.GetWithTag(k)
		catch(err)
		if !bytes.Equal([]byte(k), tag) {
			t.Fatalf("Expected tag == %q, got %q", k, tag)
		}
	}
}

func TestCacheSetAndGetTag(t *testing.T) {
	clearStorage()

	s, err := New(storageDir, 2048000, 40)
	catch(err)
	for k, b := range blobs {
		err := s.Put(k, b)
		s.SetTag(k, []byte("tag"))
		catch(err)
	}

	for k, _ := range blobs {
		tag, _ := s.GetTag(k)
		if !bytes.Equal(tag, []byte("tag")) {
			t.Fatalf("Expected tag == \"tag\", got %q", tag)
		}
	}
}

func TestCacheDeleteAndStats(t *testing.T) { // cache
	clearStorage()

	s, err := New(storageDir, 2048000, 40)
	catch(err)
	for k, b := range blobs {
		err := s.Put(k, b)
		catch(err)
	}

	if _, err := s.Get("missing"); err == nil {
		t.Fatalf("Miss Expected!")
	}
	_, c, h, m := s.Stats()
	if c != int64(len(blobs)) {
		t.Fatalf("Expected cap == %v, got %v", len(blobs), c)
	}

	if h != 0 {
		t.Fatalf("Expected hit == 0, got %v", m)
	}

	s.Get("gopher")
	s.Get("gopher")

	if _, _, h, _ := s.Stats(); h != 2 {
		t.Fatalf("Expected hit == 2, got %v", h)
	}

	if m != 1 {
		t.Fatalf("Expected miss == 1, got %v", m)
	}

	for k, _ := range blobs {
		if err := s.Delete(k); err != nil {
			t.Fatalf("Unexpected error when deleting a file!")
		}
	}

	if err := s.Delete("missing"); err == nil {
		t.Fatalf("Expected error when deleting a missing file!")
	}

	if !s.Empty() {
		t.Fatalf("Expected empty cache!")
	}

	if s.Size() != 0 {
		t.Fatalf("Expected cache with size == 0!")
	}
}
func TestSizeEviction(t *testing.T) {
	clearStorage()

	s, err := New(storageDir, 10, 40)
	catch(err)

	err = s.Put("a", []byte("abcdefgh"))
	catch(err)
	err = s.Put("b", []byte("ij"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"a", "b"})

	err = s.Put("c", []byte("k"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"b", "c"})

	err = s.Put("d", []byte("l"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"b", "c", "d"})

	err = s.Put("e", []byte("m"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"b", "c", "d", "e"})

	err = s.Put("f", []byte("nopqrstuvw"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"f"})
}

func TestCapEviction(t *testing.T) {
	clearStorage()

	s, err := New(storageDir, 2048, 3)
	catch(err)

	err = s.Put("a", []byte("abcdefg"))
	catch(err)
	err = s.Put("b", []byte("hi"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"a", "b"})

	err = s.Put("c", []byte("k"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"a", "b", "c"})

	err = s.Put("d", []byte("l"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"b", "c", "d"})

	err = s.Put("e", []byte("m"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"c", "d", "e"})

	err = s.Put("f", []byte("nopqrstuv"))
	catch(err)
	assertKeys(t, s.Keys(), []string{"d", "e", "f"})
}

func TestMain(m *testing.M) {
	// Create a temporary storage directory for tests
	name, err := ioutil.TempDir("", "stash-")
	if err != nil {
		log.Fatal(err)
	}
	storageDir = name
	defer os.RemoveAll(name)

	os.Exit(m.Run())
}

func assertKeys(t *testing.T, keys []string, expected []string) {
	if len(keys) != len(expected) {
		t.Fatalf("Expected %d key(s), got %d", len(expected), len(keys))
	}
	if !reflect.DeepEqual(keys, expected) {
		t.Fatalf("Expected keys == %q, got %q", expected, keys)
	}
}

func catch(err error) {
	if err != nil {
		panic(err)
	}
}

var blobs = map[string][]byte{
	"gopher":      []byte(`The Go gopher is an iconic mascot and one of the most distinctive features of the Go project. In this post we'll talk about its origins, evolution, and behavior.`),
	"io/ioutil":   []byte(`Package ioutil implements some I/O utility functions.`),
	"testing.go":  []byte(`Package testing provides support for automated testing of Go packages.`),
	"empty.txt":   []byte(``),
	"hello-world": []byte(`Hello, world!`),
	"null":        []byte{0},
}
