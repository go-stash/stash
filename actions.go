package stash

import (
	"container/list"
	"strings"
	"io"
	"os"
	"errors"
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

	if path, l, e := writeToFile(key, c.Dir, val); e != nil {
		return e
	} else {
		c.onAdd(key, path, l)
		return nil
	}
}

// AddFrom adds the contents of a reader as a blob to the cache against the given key.
func (c *Cache) AddFrom(key string, r io.Reader) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	//@TODO check key
	//@TODO check r

	//@TODO check available space
	//@TODO check available file count

	if path, l, e := writeToFile(key, c.Dir, r); e != nil {
		return e
	} else {
		c.onAdd(key, path, l)
		return nil
	}
}

// Update the cache.
func (c *Cache) onAdd(key, path string, length int64) {
	c.StorageSizeUsed += length
	c.TotalFilesWritten += 1
	if item, ok := c.Items[key]; ok {
		c.ItemsList.MoveToFront(item)
		item.Value.(*ItemMeta) = &ItemMeta {
			Size: length,
			Path: path,
		}
	} else {
		item := &ItemMeta {
			Size: length,
			Path: path,
		}
		listElement := c.ItemsList.PushFront(item)
		c.Items[key] = listElement
	}
}

// Get returns a reader for a blob in the cache, or ErrNotFound otherwise.
func (c *Cache) Get(key string) (io.ReadCloser, error) {
	c.Lock.RLock()
	defer c.Lock.RUnlock()

	if item, ok := c.Items[key]; ok {
		c.ItemsList.MoveToFront(item)
		path := item.Value.(*ItemMeta).Path
		if f, err := os.Open(path); err != nil {
			return nil, err
		} else {
			return f, nil
		}
	} else {
		return nil, ErrNotFound
	}
}