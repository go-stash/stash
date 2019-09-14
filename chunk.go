package stash

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

type FileEntry struct {
	Offset    map[int64]struct{}
	CommonTag []byte
	wg        sync.WaitGroup
}

type ChunkCache struct {
	Cache  Cache
	Chunks map[string]*FileEntry
	L      sync.Mutex
}

func NewChunkCache(dir string, sz, c int64) (*ChunkCache, error) {
	cache, err := NewCache(dir, sz, c)
	if err != nil {
		return nil, err
	}
	return &ChunkCache{
		Chunks: make(map[string]*FileEntry),
		Cache:  *cache,
	}, nil
}

func (c *ChunkCache) GetTag(key string, offset int64) ([]byte, error) {

	c.L.Lock()
	defer c.L.Unlock()

	elem := c.Chunks[key]
	if elem == nil {
		return nil, ErrNotFound
	}

	tag, err := c.Cache.GetTag(chunkKey(key, offset))
	if err != nil {
		delete(elem.Offset, offset)
		if len(elem.Offset) == 0 {
			delete(c.Chunks, key)
		}
		return nil, ErrChunkNotFound
	}

	return tag, nil
}

func (c *ChunkCache) GetCommonTag(key string) ([]byte, error) {
	c.L.Lock()
	defer c.L.Unlock()

	elem := c.Chunks[key]
	if elem == nil {
		return nil, ErrNotFound
	}
	return elem.CommonTag, nil
}

func (c *ChunkCache) SetTag(key string, offset int64, tag []byte) error {
	c.L.Lock()
	defer c.L.Unlock()
	return c.unlockedSetTag(key, offset, tag)
}

func (c *ChunkCache) unlockedSetTag(key string, offset int64, tag []byte) error {

	var err error
	var ptag []byte

	elem := c.Chunks[key]
	if elem == nil {
		return ErrNotFound
	}
	if _, ok := elem.Offset[offset]; !ok {
		return ErrChunkNotFound
	}

	keyfile := chunkKey(key, offset)

	ptag, err = c.Cache.GetTag(keyfile)
	if err != nil {
		delete(elem.Offset, offset)
		if len(elem.Offset) == 0 {
			delete(c.Chunks, key)
		}
		return err
	}

	switch {
	case ptag == nil:
		if err := c.Cache.SetTag(keyfile, tag); err != nil {
			return err
		}
	case !bytes.Equal(ptag, tag):
		return ErrAlreadyTagged
	}

	// keyfile is tagged with tag!
	//

	if tag != nil {
		elem.CommonTag = tag
	}

	// dismiss the unsafe entries, that is entries untagged or tagged with a different tag.

	for off, _ := range elem.Offset {
		if off != offset {
			res, err := c.Cache.DeleteIf(chunkKey(key, off), func(t []byte) bool {
				return !bytes.Equal(t, tag)
			})
			if res && err == nil {
				delete(elem.Offset, off)
			}
		}
	}
	return nil
}

func (c *ChunkCache) SetUntaggedChunks(key string, tag []byte) (int, error) {

	var err error
	var ptag []byte
	tagged := 0

	c.L.Lock()
	defer c.L.Unlock()

	elem := c.Chunks[key]
	if elem == nil {
		return 0, ErrNotFound
	}

	if elem.CommonTag != nil && !bytes.Equal(elem.CommonTag, tag) {
		return 0, ErrIncoherentTag
	}

	// set all untagged chunks with the given tag
	//

	for offset, _ := range elem.Offset {

		keyfile := chunkKey(key, offset)
		ptag, err = c.Cache.GetTag(keyfile)
		if err != nil {
			delete(elem.Offset, offset)
			if len(elem.Offset) == 0 {
				delete(c.Chunks, key)
			}
			continue
		}

		switch {
		case ptag == nil:
			if err := c.Cache.SetTag(keyfile, tag); err != nil {
				panic(err)
			}
			tagged++
		case !bytes.Equal(ptag, tag):
			panic("Tag mismatch with commonTag")
		}
	}

	if tag != nil {
		elem.CommonTag = tag
	}
	return tagged, nil
}

func (c *ChunkCache) Clear() error {

	c.L.Lock()
	defer c.L.Unlock()

	err := c.Cache.Clear()

	if err == nil {
		c.Chunks = make(map[string]*FileEntry)
	}
	return err
}

func (c *ChunkCache) Put(key string, offset int64, val []byte) error {
	return c.PutWithTag(key, offset, nil, val)
}

func (c *ChunkCache) PutWithTag(key string, offset int64, tag []byte, val []byte) error {

	c.L.Lock()
	file := c.getFileEntry(key)
	c.L.Unlock()

	file.wg.Add(1)
	defer file.wg.Done()

	err := c.Cache.PutWithTag(chunkKey(key, offset), tag, val)
	if err != nil {
		return err
	}
	c.L.Lock()
	defer c.L.Unlock()
	c.addChunkOffset(key, offset)
	if tag == nil {
		return nil
	}
	return c.unlockedSetTag(key, offset, tag)
}

func (c *ChunkCache) PutReader(key string, offset int64, r LazyReader) (io.ReadCloser, error) {
	return c.PutReaderWithTag(key, offset, nil, r)
}

func (c *ChunkCache) PutReaderWithTag(key string, offset int64, tag []byte, lz LazyReader) (io.ReadCloser, error) {

	c.L.Lock()
	file := c.getFileEntry(key)
	c.L.Unlock()

	file.wg.Add(1)
	defer file.wg.Done()

	r, err := c.Cache.PutReaderWithTag(chunkKey(key, offset), tag, lz)
	if err != nil {
		return r, err
	}
	c.L.Lock()
	defer c.L.Unlock()
	c.addChunkOffset(key, offset)
	if tag == nil {
		return r, nil
	}
	return r, c.unlockedSetTag(key, offset, tag)
}

func (c *ChunkCache) WaitPut(key string) {
	c.L.Lock()
	file := c.getFileEntry(key)
	c.L.Unlock()

	file.wg.Wait()
}

func (c *ChunkCache) Get(key string, offset int64) (ReadSeekCloser, int64, error) {
	rd, tag, size, err := c.GetWithTag(key, offset)
	if tag == nil {
		return rd, 0, ErrUntagged
	}
	return rd, size, err
}

func (c *ChunkCache) GetWithTag(key string, offset int64) (ReadSeekCloser, []byte, int64, error) {
	rd, tag, size, err := c.Cache.GetWithTag(chunkKey(key, offset))
	if err != nil {
		c.L.Lock()
		defer c.L.Unlock()
		c.delChunkOffset(key, offset)
	}
	return rd, tag, size, err
}

func (c *ChunkCache) Delete(key string) error {
	c.L.Lock()
	defer c.L.Unlock()
	var err error
	if elem, ok := c.Chunks[key]; ok {
		for offset, _ := range elem.Offset {
			if e := c.unlockedDeleteChunk(key, offset); e != nil {
				err = e
			}
		}
	} else {
		return ErrNotFound
	}
	return err
}

func (c *ChunkCache) DeleteChunk(key string, offset int64) error {
	c.L.Lock()
	defer c.L.Unlock()
	return c.unlockedDeleteChunk(key, offset)
}

func (c *ChunkCache) unlockedDeleteChunk(key string, offset int64) error {
	if c.delChunkOffset(key, offset) {
		return c.Cache.Delete(chunkKey(key, offset))
	}
	return ErrChunkNotFound
}

func (c *ChunkCache) GetStats() Stats {
	return c.Cache.GetStats()
}

func (c *ChunkCache) ResetStats() {
	c.Cache.ResetStats()
}

func (c *ChunkCache) Empty() bool {
	return c.Cache.Empty()
}

func (c *ChunkCache) Shrink() {
	c.L.Lock()
	defer c.L.Unlock()
	for key, elem := range c.Chunks {
		for offset, _ := range elem.Offset {
			_, err := c.Cache.GetTag(chunkKey(key, offset))
			if err != nil {
				delete(elem.Offset, offset)
			}
		}
		if len(elem.Offset) == 0 {
			delete(c.Chunks, key)
		}
	}
}

func (c *ChunkCache) NumEntries() int64 {
	return int64(len(c.Chunks))
}

func (c *ChunkCache) NumChunksOf(key string) (int64, error) {
	c.L.Lock()
	defer c.L.Unlock()

	elem := c.Chunks[key]
	if elem == nil {
		return 0, ErrNotFound
	}

	return int64(len(elem.Offset)), nil
}

func (c *ChunkCache) NumTotalChunks() int64 {
	return c.Cache.NumEntries()
}

func (c *ChunkCache) Size() int64 {
	return c.Cache.Size()
}

func (c *ChunkCache) Keys() []string {
	return c.Cache.Keys()
}

////////////////////////////////////////////////////////////////

func chunkKey(key string, offset int64) string {
	return fmt.Sprintf("%s#%d", key, offset)
}

func (c *ChunkCache) getFileEntry(key string) *FileEntry {
	if elem := c.Chunks[key]; elem != nil {
		return elem
	}
	elem := FileEntry{
		Offset: make(map[int64]struct{}),
	}
	c.Chunks[key] = &elem
	return &elem
}

func (c *ChunkCache) addChunkOffset(key string, offset int64) {
	elem := c.getFileEntry(key)
	elem.Offset[offset] = struct{}{}
}

func (c *ChunkCache) delChunkOffset(key string, offset int64) bool {

	if elem, ok := c.Chunks[key]; ok {
		if _, ok := elem.Offset[offset]; ok {
			delete(elem.Offset, offset)
			if len(elem.Offset) == 0 {
				delete(c.Chunks, key)
			}
			return true
		}
	}

	return false
}
