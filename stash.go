package stash

import (
	"bytes"
	"container/list"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

type Meta struct {
	Key  string
	Size int64
	Path string
}

type Cache struct {
	dir     string // Path to storage directory
	maxSize int64  // Total size of files allowed
	maxCap  int64  // Total number of files allowed
	size    int64  // Total size of files added
	cap     int64  // Total number of files added
	hit     int64  // Cache hit
	miss    int64  // Cache miss...

	list *list.List               // List of items in cache
	m    map[string]*list.Element // Map of items in list

	l sync.Mutex
}

// New creates a Cache backed by dir on disk. The cache allows at most c files of total size sz.
func New(dir string, sz, c int64) (*Cache, error) {
	if !validDir(dir) {
		return nil, ErrBadDir
	}
	if sz <= 0 {
		return nil, ErrBadSize
	}
	if c <= 0 {
		return nil, ErrBadCap
	}

	dir = filepath.Clean(dir)

	return &Cache{
		dir:     dir,
		maxSize: sz,
		maxCap:  c,
		list:    list.New(),
		m:       make(map[string]*list.Element),
	}, nil
}

// Put adds a byte slice as a blob to the cache against the given key.
func (c *Cache) Put(key string, val []byte) error {
	return c.PutReader(key, bytes.NewReader(val))
}
func (c *Cache) UnlockedPut(key string, val []byte) error {
	return c.UnlockedPutReader(key, bytes.NewReader(val))
}

// PutReader adds the contents of a reader as a blob to the cache against the given key.
func (c *Cache) PutReader(key string, r io.Reader) error {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedPutReader(key, r)
}

func (c *Cache) UnlockedPutReader(key string, r io.Reader) error {

	path, n, err := writeFile(c.dir, key, r)
	if err != nil {
		return err
	}
	if err := c.validate(path, n); err != nil {
		return err
	}

	c.addMeta(key, path, n)
	return nil
}

// PutReaderChunked adds the contents of a reader, validating size for single chunk
func (c *Cache) PutReaderChunked(key string, r io.Reader) error {
	return c.LockablePutReaderChunked(key, r, &c.l)
}
func (c *Cache) UnlockedPutReaderChunked(key string, r io.Reader) error {
	return c.LockablePutReaderChunked(key, r, nil)
}

func (c *Cache) LockablePutReaderChunked(key string, r io.Reader, m *sync.Mutex) error {
	path, n, err := writeFileValidate(c, c.dir, key, r)
	if err != nil {
		return errors.WithStack(os.Remove(path))
	}

	if m != nil {
		m.Lock()
		defer m.Unlock()
	}

	c.addMeta(key, path, n)
	return nil
}

// Get returns a reader for a blob in the cache, or ErrNotFound otherwise.
func (c *Cache) Get(key string) (io.ReadCloser, error) {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedGet(key)
}
func (c *Cache) UnlockedGet(key string) (io.ReadCloser, error) {
	if item, ok := c.m[key]; ok {
		c.list.MoveToFront(item)
		path := item.Value.(*Meta).Path
		if f, err := os.Open(path); err != nil {
			return nil, err
		} else {
			c.hit++
			return f, nil
		}
	} else {
		c.miss++
		return nil, ErrNotFound
	}
}

// Delete a key from the cache, return error in case of key not present.
func (c *Cache) Delete(key string) error {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedDelete(key)
}
func (c *Cache) UnlockedDelete(key string) error {
	elem, ok := c.m[key]
	if !ok {
		return ErrNotFound
	}

	item := elem.Value.(*Meta)
	c.size -= item.Size
	c.cap--
	os.Remove(item.Path)
	delete(c.m, item.Key)
	c.list.Remove(elem)
	return nil
}

// Return Cache stats.
func (c *Cache) Stats() (int64, int64, int64, int64) {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedStats()
}
func (c *Cache) UnlockedStats() (int64, int64, int64, int64) {
	return c.size, c.cap, c.hit, c.miss
}

func (c *Cache) Empty() bool {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedEmpty()
}
func (c *Cache) UnlockedEmpty() bool {
	return c.cap == 0
}

func (c *Cache) Cap() int64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedCap()
}
func (c *Cache) UnlockedCap() int64 {
	return c.cap
}

func (c *Cache) Size() int64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedSize()
}
func (c *Cache) UnlockedSize() int64 {
	return c.size
}

// Keys returns a list of keys in the cache.
func (c *Cache) Keys() []string {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedKeys()
}
func (c *Cache) UnlockedKeys() []string {
	keys := make([]string, len(c.m))
	i := 0
	for item := c.list.Back(); item != nil; item = item.Prev() {
		keys[i] = item.Value.(*Meta).Key
		i++
	}
	sort.Strings(keys)
	return keys
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

// validate ensures the file satisfies the constraints of the cache.

func (c *Cache) validate(path string, n int64) error {
	if n > c.maxSize {
		os.Remove(path) // XXX(hjr265): We should not suppress this error even if it is very unlikely.
		return &FileError{c.dir, "", ErrTooLarge}
	}

	for n+c.size > c.maxSize {
		err := c.evictLast()
		if err != nil {
			return err
		}
	}

	if c.cap+1 > c.maxCap {
		err := c.evictLast()
		if err != nil {
			return err
		}
	}

	return nil
}

// evitcLast removes the last file following the LRU policy.
func (c *Cache) evictLast() error {
	if last := c.list.Back(); last != nil {
		item := last.Value.(*Meta)
		if e := os.Remove(item.Path); e == nil {
			c.size -= item.Size
			c.cap--
			delete(c.m, item.Key)
			c.list.Remove(last)
			return nil
		} else {
			return e
		}
	}

	return nil
}

// addMeta adds meta information to the cache.
func (c *Cache) addMeta(key, path string, length int64) {
	c.size += length
	c.cap++
	if item, ok := c.m[key]; ok {
		c.list.Remove(item)
	}

	item := &Meta{
		Key:  key,
		Size: length,
		Path: path,
	}
	listElement := c.list.PushFront(item)
	c.m[key] = listElement
}

func validDir(dir string) bool {
	// XXX(hjr265): We need to ensure the disk is either empty, or contains a valid cache storage.
	return dir != ""
}
