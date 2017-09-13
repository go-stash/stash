package stash

import (
	"container/list"
	"os"
	"strings"
	"bufio"
	"io"
)

// New creates an Cache of the given Directory, StorageSize & TotalFilesToBeWritten
func New(dir string, sz, c int64) (*Cache, error) {
	//check dir value empty
	if dir == "" {
		return nil, ErrEmptyDir
	}

	//@TODO check Directory readable & writable
	//@TODO check StorageSize greater then zero
	//@TODO check TotalFilesToBeWritten greater then zero

	dir = strings.TrimRight(dir, "\\/") //trim the right directory separator
	return &Cache{
		Dir: dir,
		StorageSize: sz,
		TotalFilesToBeWritten: c,
		ItemsList: list.New(),
		Items: make(map[string]*list.Element),
	}, nil
}

// Add adds a byte slice as a blob to the cache against the given key.
func (c *Cache) Add(key string, val []byte) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	//@TODO check key
	//@TODO check val

	//@TODO check available space
	//@TODO check available file count

	content := string(val)
	fileName := getFileName(key)
	f, e := os.Create(c.Dir + string(os.PathSeparator) + fileName)
	defer f.Close()
	if e != nil {
		return ErrCreateFile
	}
	w := bufio.NewWriter(f)
	if _, e := w.WriteString(content); e != nil {
		return ErrCreateFile
	}
	if e := w.Flush(); e != nil {
		return e
	}
	return nil
}

// AddFrom adds the contents of a reader as a blob to the cache against the given key.
func (c *Cache) AddFrom(key string, r *io.Reader) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	//@TODO check key
	//@TODO check r

	//@TODO check available space
	//@TODO check available file count

	fileName := getFileName(key)
	f, e := os.Create(c.Dir + string(os.PathSeparator) + fileName)
	defer f.Close()
	if e != nil {
		return ErrCreateFile
	}
	w := bufio.NewWriter(f)
	if _, e := w.ReadFrom(r); e != nil {
		return ErrCreateFile
	}
	if e := w.Flush(); e != nil {
		return e
	}
	return nil
}