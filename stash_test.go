package stash

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

var storageDir string

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

func TestCacheAdd(t *testing.T) {
	clearStorage()

	s, _ := New(storageDir, 2048000, 40)
	for k, b := range blobs {
		err := s.Add(k, b)
		catch(err)
	}

	for k, b := range blobs {
		path := filepath(storageDir, shasum(k))
		v, err := ioutil.ReadFile(path)
		catch(err)
		if !bytes.Equal(b, v) {
			t.Fatalf("Expected v == %q, got %q", b, v)
		}
	}
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
