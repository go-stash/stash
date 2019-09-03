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

// Meta is the information about the cache entry.
type Meta struct {
	Key  string
	Size int64
	Path string
	Tag  []byte // user annotation
}

type Cache struct {
	dir        string // Path to storage directory
	maxSize    int64  // Total size of files allowed
	maxEntries int64  // Total number of files allowed
	size       int64  // Total size of files added
	numEntries int64  // Total number of files added
	hit        int64  // Cache hit
	miss       int64  // Cache miss...

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

	cache := &Cache{
		dir:        dir,
		maxSize:    sz,
		maxEntries: c,
		list:       list.New(),
		m:          make(map[string]*list.Element),
	}

	if err := cache.UnlockedClear(); err != nil {
		return nil, err
	}
	return cache, nil
}

// Clear resets the cache and erases the files from the cache directory.
func (c *Cache) Clear() error {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedClear()
}

// UnlockedClear is the concurrency-unsafe version of Clear function.
func (c *Cache) UnlockedClear() error {
	c.size = 0
	c.numEntries = 0
	c.UnlockedResetStats()
	c.list = list.New()
	c.m = make(map[string]*list.Element)

	d, err := os.Open(c.dir)
	if err != nil {
		return err
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err == nil {
		for _, name := range names {
			os.RemoveAll(filepath.Join(c.dir, name))
		}
	}

	return nil
}

// SetTag simple sets a binary Tag to the cached key element.
func (c *Cache) SetTag(key string, tag []byte) error {
	c.l.Lock()
	defer c.l.Unlock()
	return c.SetTagUnlocked(key, tag)
}

// SetTagUnlocked is the concurrency-unsafe version of SetTag.
func (c *Cache) SetTagUnlocked(key string, tag []byte) error {
	if item, ok := c.m[key]; ok {
		meta := item.Value.(*Meta)
		switch {
		case meta.Tag == nil:
			item.Value.(*Meta).Tag = tag
			return nil
		case bytes.Equal(meta.Tag, tag):
			return nil
		}

		return ErrAlreadyTagged
	}
	return ErrNotFound
}

// GetTag retrieves the tag associated with the key.
func (c *Cache) GetTag(key string) ([]byte, error) {
	c.l.Lock()
	defer c.l.Unlock()
	return c.GetTagUnlocked(key)
}

// GetTagUnlocked is the concurrency-unsafe version of GetTag.
func (c *Cache) GetTagUnlocked(key string) ([]byte, error) {
	if item, ok := c.m[key]; ok {
		return item.Value.(*Meta).Tag, nil
	}
	return nil, ErrNotFound
}

// Put adds a byte slice as a blob to the cache against the given key.
func (c *Cache) Put(key string, val []byte) error {
	return c.PutReader(key, bytes.NewReader(val))
}

// Put like Put, adds a byte slice as a blob along with a tag annotation.
func (c *Cache) PutWithTag(key string, tag, val []byte) error {
	return c.PutReaderWithTag(key, tag, bytes.NewReader(val))
}

// UnlockedPutWithTag is the concurrency-unsafe version of PutWithTag.
func (c *Cache) UnlockedPutWithTag(key string, tag, val []byte) error {
	return c.UnlockedPutReaderWithTag(key, tag, bytes.NewReader(val))
}

// PutReader adds the contents of a reader as a blob to the cache against the given key.
func (c *Cache) PutReader(key string, r io.Reader) error {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedPutReaderWithTag(key, nil, r)
}

// PutReaderWithTag like PutReader, adds the contents of a reader as blog along with a tag annotation against the given key.
func (c *Cache) PutReaderWithTag(key string, tag []byte, r io.Reader) error {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedPutReaderWithTag(key, tag, r)
}

// UnlockedPutReaderWithTag is the concurrency-unsafe version of PutReaderWithTag.
func (c *Cache) UnlockedPutReaderWithTag(key string, tag []byte, r io.Reader) error {

	path, n, err := writeFile(c.dir, key, r)
	if err != nil {
		return err
	}
	if err := c.validate(path, n); err != nil {
		return err
	}

	c.addMeta(key, tag, path, n)
	return nil
}

// PutReaderChunked adds the contents of a reader, validating size chunk.
func (c *Cache) PutReaderChunked(key string, r io.Reader) error {
	return c.LockablePutReaderChunkedWithTag(key, nil, r, &c.l)
}

// PutReaderChunkedWithTag, like PutReaderChunked, adds the contents of a reader along with a tag annotation.
func (c *Cache) PutReaderChunkedWithTag(key string, tag []byte, r io.Reader) error {
	return c.LockablePutReaderChunkedWithTag(key, tag, r, &c.l)
}

// UnlockedPutReaderChunkedWithTag is the concurrency-unsafe version of PutReaderChunkedWithTag.
func (c *Cache) UnlockedPutReaderChunkedWithTag(key string, tag []byte, r io.Reader) error {
	return c.LockablePutReaderChunkedWithTag(key, tag, r, nil)
}

// LockablePutReaderChunkedWithTag, like PutReaderChunkedWithTag, only with an extra optional pointer to mutex to lock.
// If nil is passed, this version is concurrency-unsafe.
func (c *Cache) LockablePutReaderChunkedWithTag(key string, tag []byte, r io.Reader, m *sync.Mutex) error {
	path, n, err := writeFileValidate(c, c.dir, key, r)
	if err != nil {
		return errors.WithStack(os.Remove(path))
	}

	if m != nil {
		m.Lock()
		defer m.Unlock()
	}

	c.addMeta(key, tag, path, n)
	return nil
}

// Get returns a reader for a blob in the cache, or ErrNotFound otherwise.
func (c *Cache) Get(key string) (io.ReadCloser, error) {
	c.l.Lock()
	defer c.l.Unlock()
	r, _, e := c.UnlockedGetWithTag(key)
	return r, e
}

// GetWithTag returns a reader for a blob in the cache along with the associated tag, or ErrNotFound otherwise.
func (c *Cache) GetWithTag(key string) (io.ReadCloser, []byte, error) {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedGetWithTag(key)
}

// UnlockedGetWithTag is the concurrency-unsafe version of GetWithTag.
func (c *Cache) UnlockedGetWithTag(key string) (io.ReadCloser, []byte, error) {
	if item, ok := c.m[key]; ok {
		c.list.MoveToFront(item)
		path := item.Value.(*Meta).Path
		if f, err := os.Open(path); err != nil {
			return nil, nil, err
		} else {
			c.hit++
			return f, item.Value.(*Meta).Tag, nil
		}
	} else {
		c.miss++
		return nil, nil, ErrNotFound
	}
}

// Delete a key from the cache, return error in case of key not present.
func (c *Cache) Delete(key string) error {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedDelete(key)
}

// UnlockedDelete is the concurrency-unsafe version of Delete.
func (c *Cache) UnlockedDelete(key string) error {
	elem, ok := c.m[key]
	if !ok {
		return ErrNotFound
	}

	item := elem.Value.(*Meta)
	c.size -= item.Size
	c.numEntries--
	os.Remove(item.Path)
	delete(c.m, item.Key)
	c.list.Remove(elem)
	return nil
}

// Stats returns the Cache stats.
func (c *Cache) Stats() (int64, int64, int64, int64) {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedStats()
}

// UnlockedStats is the concurrency-unsafe version of Stats.
func (c *Cache) UnlockedStats() (int64, int64, int64, int64) {
	return c.size, c.numEntries, c.hit, c.miss
}

// ResetStats resets the statistics of the cache.
func (c *Cache) ResetStats() {
	c.l.Lock()
	defer c.l.Unlock()
	c.UnlockedResetStats()
}

// UnlockedResetStats is the concurrency-unsafe version of ResetStats.
func (c *Cache) UnlockedResetStats() {
	c.hit = 0
	c.miss = 0
}

// Empty returns true if the cache is empty.
func (c *Cache) Empty() bool {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedEmpty()
}

// UnlockedEmpty is true if the cache is the concurrency-unsafe version of Empty.
func (c *Cache) UnlockedEmpty() bool {
	return c.numEntries == 0
}

// numEntries returns the number of entries in the cache.
func (c *Cache) NumEntries() int64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedNumEntries()
}

// UnlockedNumEntries the concurrency-unsafe version of NumEntries.
func (c *Cache) UnlockedNumEntries() int64 {
	return c.numEntries
}

// Size returns the size of the cache in bytes.
func (c *Cache) Size() int64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedSize()
}

// UnlockedSize is the concurrency-unsafe version of Size.
func (c *Cache) UnlockedSize() int64 {
	return c.size
}

// Keys returns a list of keys in the cache.
func (c *Cache) Keys() []string {
	c.l.Lock()
	defer c.l.Unlock()
	return c.UnlockedKeys()
}

// UnlockedKeys is the concurrency-unsafe version of Keys.
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

	if c.numEntries+1 > c.maxEntries {
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
			c.numEntries--
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
func (c *Cache) addMeta(key string, tag []byte, path string, length int64) {
	c.size += length
	c.numEntries++
	if item, ok := c.m[key]; ok {
		c.list.Remove(item)
	}

	item := &Meta{
		Key:  key,
		Tag:  tag,
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
