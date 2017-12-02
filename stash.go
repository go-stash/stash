package stash

import (
	"bytes"
	"container/list"
	"io"
	"os"
	"strings"
	"sync"
)

type Meta struct {
	Key  string //key of blob
	Size int64  //Size of blob
	Path string //Path of blob
}

type Cache struct {
	Dir                   string //Directory Path where files will be saved
	StorageSize           int64  //Total Storage Size for file to write
	TotalFilesToBeWritten int64  //Total Number of files to be written

	StorageSizeUsed   int64 //Total Storage used
	TotalFilesWritten int64 //Total Files currently written

	ItemsList *list.List               //ItemsList will hold the list of entries
	Items     map[string]*list.Element //Items/Files saved in storage

	Lock sync.RWMutex //Read Write sync
}

// New creates an Cache of the given Directory, StorageSize & TotalFilesToBeWritten
func New(dir string, sz, c int64) (*Cache, error) {
	//check dir value empty
	if dir == "" {
		return nil, ErrBadDir
	}
	//check StorageSize greater then zero
	if sz <= 0 {
		return nil, ErrBadSize
	}
	//check TotalFilesToBeWritten greater then zero
	if c <= 0 {
		return nil, ErrBadCap
	}

	dir = strings.TrimRight(dir, "\\/") //trim the right directory separator
	return &Cache{
		Dir:                   dir,
		StorageSize:           sz,
		TotalFilesToBeWritten: c,
		ItemsList:             list.New(),
		Items:                 make(map[string]*list.Element),
	}, nil
}

// Add adds a byte slice as a blob to the cache against the given key.
func (c *Cache) Add(key string, val []byte) error {
	return c.AddFrom(key, bytes.NewReader(val))
}

// AddFrom adds the contents of a reader as a blob to the cache against the given key.
func (c *Cache) AddFrom(key string, r io.Reader) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if path, l, e := writeFile(c.Dir, key, r); e != nil {
		return e
	} else {
		if e := c.validate(path, l); e != nil { // XXX(hjr265): We should validate before storing the file.
			return e
		}
		c.onAdd(key, path, l)
		return nil
	}
}

// Validate the file.
func (c *Cache) validate(path string, length int64) error {
	if length > c.StorageSize {
		if e := os.Remove(path); e == nil {
			return &FileError{c.Dir, "", ErrTooLarge}
		} else {
			return e
		}
	}
	if length+c.StorageSizeUsed <= c.StorageSize && c.TotalFilesWritten+1 <= c.TotalFilesToBeWritten {
		return nil
	} else if length+c.StorageSizeUsed >= c.StorageSize {
		if e := c.removeLast(); e != nil {
			return e
		}
		c.validate(path, length)
	} else if c.TotalFilesWritten+1 >= c.TotalFilesToBeWritten {
		if e := c.removeLast(); e != nil {
			return e
		}
		c.validate(path, length)
	}
	return nil
}

// Removes the last file.
func (c *Cache) removeLast() error {
	if last := c.ItemsList.Back(); last != nil {
		item := last.Value.(*Meta)
		if e := os.Remove(item.Path); e == nil {
			c.StorageSizeUsed -= item.Size
			c.TotalFilesWritten--
			delete(c.Items, item.Key)
			c.ItemsList.Remove(last)
			return nil
		} else {
			return e
		}
	}

	return nil
}

// Update the cache.
func (c *Cache) onAdd(key, path string, length int64) {
	c.StorageSizeUsed += length
	c.TotalFilesWritten++
	if item, ok := c.Items[key]; ok {
		c.ItemsList.Remove(item)
	}

	item := &Meta{
		Key:  key,
		Size: length,
		Path: path,
	}
	listElement := c.ItemsList.PushFront(item)
	c.Items[key] = listElement
}

// Get returns a reader for a blob in the cache, or ErrNotFound otherwise.
func (c *Cache) Get(key string) (io.ReadCloser, error) {
	c.Lock.RLock()
	defer c.Lock.RUnlock()

	if item, ok := c.Items[key]; ok {
		c.ItemsList.MoveToFront(item)
		path := item.Value.(*Meta).Path
		if f, err := os.Open(path); err != nil {
			return nil, err
		} else {
			return f, nil
		}
	} else {
		return nil, ErrNotFound
	}
}

// Keys returns a list of keys in the cache.
func (c *Cache) Keys() []string {
	keys := make([]string, len(c.Items))
	i := 0
	for item := c.ItemsList.Back(); item != nil; item = item.Prev() {
		keys[i] = item.Value.(*Meta).Key
		i++
	}
	return keys
}
