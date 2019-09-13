package stash

import (
	"bytes"
	"container/list"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// Stats is the cache stats.
type Stats struct {
	Size    int64
	Entries int64
	Hit     int64
	Miss    int64
}

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

	if _, err := os.Stat(dir); os.IsNotExist(err) {
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

	if err := cache.Clear(); err != nil {
		return nil, err
	}
	return cache, nil
}

// Dump the content on the cache for debugging.
func (c *Cache) Dump() {
	c.l.Lock()
	defer c.l.Unlock()
	fmt.Printf("Stash: -------\n")
	fmt.Printf("   dir        : %s\n", c.dir)
	fmt.Printf("   max size   : %d\n", c.maxSize)
	fmt.Printf("   max entries: %d\n", c.maxEntries)
	fmt.Printf("   size       : %d\n", c.size)
	fmt.Printf("   entries    : %d\n", c.numEntries)
	fmt.Printf("   hit        : %d\n", c.hit)
	fmt.Printf("   miss       : %d\n", c.miss)

	for k, v := range c.m {
		fmt.Printf("   key        : %s -> %+v\n", k, v.Value.(*Meta))
	}

	fmt.Printf("--------------\n")
}

// Clear resets the cache and erases the files from the cache directory.
func (c *Cache) Clear() error {

	c.l.Lock()
	defer c.l.Unlock()

	c.size = 0
	c.numEntries = 0

	c.unlockedResetStats()
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
			if filepath.Ext(name) == ".cache" {
				os.RemoveAll(filepath.Join(c.dir, name))
			}
		}
	}

	return nil
}

// SetTag simple sets a binary Tag to the cached key element.
func (c *Cache) SetTag(key string, tag []byte) error {

	c.l.Lock()
	defer c.l.Unlock()

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

// PutReader adds the contents of a reader as a blob to the cache against the given key.
func (c *Cache) PutReader(key string, r io.Reader) error {
	return c.PutReaderWithTag(key, nil, r)
}

// PutReaderWithTag like PutReader, adds the contents of a reader as blog along with a tag annotation against the given key.
func (c *Cache) PutReaderWithTag(key string, tag []byte, r io.Reader) error {

	c.l.Lock()
	if item, ok := c.m[key]; ok {
		if bytes.Equal(tag, item.Value.(*Meta).Tag) {
			fmt.Printf("File already present in cache!!!!!\n")
			c.l.Unlock()
			return nil
		}
	}
	c.l.Unlock()

	tmpPath, bytes, err := writeTmpFile(c.dir, key, r)
	if err != nil {
		return err
	}

	path := realFilePath(c.dir, key)

	c.l.Lock()
	defer c.l.Unlock()

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if err := c.validate(path, bytes); err != nil {
		return err
	}

	c.addMeta(key, tag, path, bytes)
	return nil
}

// PutReaderChunked adds the contents of a reader, validating size chunk.
func (c *Cache) PutReaderChunked(key string, r io.Reader) error {
	return c.PutReaderChunkedWithTag(key, nil, r)
}

// PutReaderChunkedWithTag, like PutReaderChunked, adds the contents of a reader along with a tag annotation.
func (c *Cache) PutReaderChunkedWithTag(key string, tag []byte, r io.Reader) error {
	path, n, err := writeFileValidate(c, c.dir, key, r)
	if err != nil {
		return errors.WithStack(os.Remove(path))
	}

	c.l.Lock()
	defer c.l.Unlock()

	c.addMeta(key, tag, path, n)
	return nil
}

// Get returns a reader for a blob in the cache, or ErrNotFound otherwise.
func (c *Cache) Get(key string) (ReadSeekCloser, int64, error) {
	r, _, s, e := c.GetWithTag(key)
	return r, s, e
}

// GetWithTag returns a reader for a blob in the cache along with the associated tag, or ErrNotFound otherwise.
func (c *Cache) GetWithTag(key string) (ReadSeekCloser, []byte, int64, error) {
	c.l.Lock()
	defer c.l.Unlock()
	if item, ok := c.m[key]; ok {
		c.list.MoveToFront(item)
		path := item.Value.(*Meta).Path
		if f, err := os.Open(path); err != nil {
			return nil, nil, 0, err
		} else {
			c.hit++
			return f, item.Value.(*Meta).Tag, item.Value.(*Meta).Size, nil
		}
	} else {
		c.miss++
		return nil, nil, 0, ErrNotFound
	}
}

// Delete a key from the cache if the given lambda returns true, do nothing otherwise.
func (c *Cache) DeleteIf(key string, removeTest func(tag []byte) bool) (bool, error) {
	c.l.Lock()
	defer c.l.Unlock()
	elem, ok := c.m[key]
	if !ok {
		return false, ErrNotFound
	}
	if item := elem.Value.(*Meta); removeTest(item.Tag) {
		c.size -= item.Size
		c.numEntries--
		os.Remove(item.Path)
		delete(c.m, item.Key)
		c.list.Remove(elem)
		return true, nil
	}

	return false, nil
}

// Delete a key from the cache, return error in case of key not present.
func (c *Cache) Delete(key string) error {
	c.l.Lock()
	defer c.l.Unlock()
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

// GetStats returns the Cache stats.
func (c *Cache) GetStats() Stats {
	c.l.Lock()
	defer c.l.Unlock()
	return Stats{
		Size:    c.size,
		Entries: c.numEntries,
		Hit:     c.hit,
		Miss:    c.miss,
	}
}

// ResetStats resets the statistics of the cache.
func (c *Cache) ResetStats() {
	c.l.Lock()
	defer c.l.Unlock()
	c.unlockedResetStats()
}

// Empty returns true if the cache is empty.
func (c *Cache) Empty() bool {
	c.l.Lock()
	defer c.l.Unlock()
	return c.numEntries == 0
}

// numEntries returns the number of entries in the cache.
func (c *Cache) NumEntries() int64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.numEntries
}

// Size returns the size of the cache in bytes.
func (c *Cache) Size() int64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.size
}

// Keys returns a list of keys in the cache.
func (c *Cache) Keys() []string {
	c.l.Lock()
	defer c.l.Unlock()
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

// UnlockedResetStats is the concurrency-unsafe version of ResetStats.
func (c *Cache) unlockedResetStats() {
	c.hit = 0
	c.miss = 0
}

// validate ensures the file satisfies the constraints of the cache.
func (c *Cache) validate(path string, n int64) error {
	if n > c.maxSize {
		os.Remove(path)
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
	oldlength := int64(0)

	if item, ok := c.m[key]; ok {
		oldlength = item.Value.(*Meta).Size
		c.list.Remove(item)
	} else {
		c.numEntries++
	}

	c.size += (length - oldlength)

	item := &Meta{
		Key:  key,
		Tag:  tag,
		Size: length,
		Path: path,
	}
	listElement := c.list.PushFront(item)
	c.m[key] = listElement
}
