package stash

import (
	"bytes"
	"container/list"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type EntryStatus int64

const (
	EntryBusy EntryStatus = iota
	EntryReady
	EntryDeleted
)

type LazyReader func() (io.ReadCloser, error)

func NewLazyReader(r io.ReadCloser) LazyReader {
	return func() (io.ReadCloser, error) {
		return r, nil
	}
}

func NewLazyReaderFromBuffer(buf []byte) LazyReader {
	return NewLazyReader(ioutil.NopCloser(bytes.NewReader(buf)))
}

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
	Key    string
	Size   int64
	Path   string
	Tag    []byte // user annotation
	Status EntryStatus
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

	l *sync.Mutex
	c *sync.Cond
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

	lock := sync.Mutex{}

	cache := &Cache{
		dir:        dir,
		maxSize:    sz,
		maxEntries: c,
		list:       list.New(),
		m:          make(map[string]*list.Element),
		l:          &lock,
		c:          sync.NewCond(&lock),
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
		elem := item.Value.(*Meta)
		switch {
		case elem.Tag == nil:
			elem.Tag = tag
			return nil
		case bytes.Equal(elem.Tag, tag):
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
	_, err := c.PutReader(key, NewLazyReaderFromBuffer(val))
	return err
}

// Put like Put, adds a byte slice as a blob along with a tag annotation.
func (c *Cache) PutWithTag(key string, tag, val []byte) error {
	_, err := c.PutReaderWithTag(key, tag, NewLazyReaderFromBuffer(val))
	return err
}

// PutReader adds the contents of a lazy reader as a blob to the cache against the given key.
func (c *Cache) PutReader(key string, r LazyReader) (io.ReadCloser, error) {
	return c.PutReaderWithTag(key, nil, r)
}

// PutReaderWithTag like PutReader, adds the contents of a reader as blog along with a tag annotation against the given key.
func (c *Cache) PutReaderWithTag(key string, tag []byte, lr LazyReader) (io.ReadCloser, error) {

	path := realFilePath(c.dir, key)

	c.l.Lock()

	if item, ok := c.m[key]; ok {
		status := c.waitStatus(item)
		if status == EntryReady {
			if bytes.Equal(tag, item.Value.(*Meta).Tag) {
				c.l.Unlock()
				return nil, nil
			}
		}
	}

	// replace or add a new element...
	//

	item := c.addElement(key, tag, path, 0, EntryBusy)

	reader, err := lr()
	if err != nil {
		c.l.Lock()
		defer c.l.Unlock()
		_, _ = c.removeElement(item)
		c.c.Broadcast()
		return reader, err
	}

	c.l.Unlock()

	tmpPath, bytes, err := writeTmpFile(c.dir, key, reader)

	c.l.Lock()
	defer c.l.Unlock()

	if err != nil {
		_ = os.Remove(tmpPath)
		goto Err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		goto Err
	}

	if err := c.validate(path, bytes); err != nil {
		_ = os.Remove(path)
		goto Err
	}

	_, _ = c.updateElement(key, tag, path, bytes, EntryReady)
	c.c.Broadcast()
	return reader, nil

Err:
	_, _ = c.removeElement(item)
	c.c.Broadcast()
	return reader, err
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
		status := c.waitStatus(item)

		if status == EntryReady {
			c.list.MoveToFront(item)
			elem := item.Value.(*Meta)

			if f, err := os.Open(elem.Path); err != nil {
				return nil, nil, 0, err
			} else {
				c.hit++
				return f, elem.Tag, elem.Size, nil
			}
		}
	}

	c.miss++
	return nil, nil, 0, ErrNotFound
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

	if c.numEntries > c.maxEntries {
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

// addElement adds meta information to the cache.
func (c *Cache) addElement(key string, tag []byte, path string, length int64, s EntryStatus) *list.Element {

	if item, ok := c.m[key]; ok {

		elem := item.Value.(*Meta)

		c.size += (length - elem.Size)

		elem.Tag = tag
		elem.Size = length
		elem.Path = path
		elem.Status = s

		c.list.MoveToFront(item)
		return item
	}

	c.numEntries++
	c.size += length

	item := &Meta{
		Key:    key,
		Tag:    tag,
		Size:   length,
		Path:   path,
		Status: s,
	}
	listElement := c.list.PushFront(item)
	c.m[key] = listElement

	return listElement
}

// updateElement updates meta information of a file.
func (c *Cache) updateElement(key string, tag []byte, path string, length int64, s EntryStatus) (*list.Element, error) {

	if item, ok := c.m[key]; ok {

		elem := item.Value.(*Meta)

		c.size += (length - elem.Size)

		elem.Key = key
		elem.Tag = tag
		elem.Size = length
		elem.Path = path
		elem.Status = s

		return item, nil
	}

	return nil, ErrNotFound
}

// removeElement removes an element from the stash.
func (c *Cache) removeElement(item *list.Element) (*list.Element, error) {

	elem := item.Value.(*Meta)

	if _, ok := c.m[elem.Key]; ok {
		c.size -= elem.Size
		c.numEntries--
		delete(c.m, elem.Key)
		c.list.Remove(item)

		elem.Status = EntryDeleted
		return item, nil
	}

	return nil, ErrNotFound
}

// waitStatus waits for the status to become READY or DELETED
func (c *Cache) waitStatus(item *list.Element) EntryStatus {

	elem := item.Value.(*Meta)

	for elem.Status == EntryBusy {
		c.c.Wait()
	}

	return elem.Status
}
