package stash

import (
	"bytes"
	"reflect"
	"testing"
)

func TestGetUntagged(t *testing.T) {
	clearStorage()

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)
	err = s.Put("test", 0, []byte("content"))
	catch(err)
	_, _, err = s.Get("test", 0)
	if err != ErrUntagged {
		t.Fatalf("Expected error == ErrUntagged, got error '%s'", err)
	}
}

func TestGet(t *testing.T) {
	clearStorage()

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)
	err = s.Put("test", 0, []byte("content"))
	catch(err)
	err = s.SetTag("test", 0, []byte("TAG"))
	catch(err)
	_, _, err = s.Get("test", 0)
	catch(err)
}

func TestSetTag(t *testing.T) {

	clearStorage()

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)
	err = s.Put("test", 0, []byte("content"))
	catch(err)
	err = s.SetTag("test", 0, []byte("TAG"))
	catch(err)
	err = s.SetTag("missing-file", 0, []byte("TAG2"))
	if err != ErrNotFound {
		t.Fatalf("Expected error == ErrNotFound, got error '%s'", err)
	}
	err = s.SetTag("test", 42, []byte("TAG"))
	if err != ErrChunkNotFound {
		t.Fatalf("Expected error == ErrChunkNotFound, got error '%s'", err)
	}

	tag, err := s.GetCommonTag("test")
	catch(err)

	if !bytes.Equal(tag, []byte("TAG")) {
		t.Fatalf("Expected Commontag == 'tag', got error '%s'", tag)
	}

	err = s.SetTag("test", 0, []byte("TAG2"))
	if err != ErrAlreadyTagged {
		t.Fatalf("Expected error == ErrAlreadyTagged, got error '%s'", err)
	}

	// add another chunk with a valid tag...
	//

	err = s.Put("test", 42, []byte("content"))
	catch(err)

	if n, err := s.NumChunksOf("test"); err != nil || n != 2 {
		t.Fatalf("Expected 2 chunks, got error '%s' numChunks = %d", err, n)
	}

	err = s.SetTag("test", 42, []byte("TAG"))
	catch(err)

	if ct, _ := s.GetCommonTag("test"); !bytes.Equal(ct, []byte("TAG")) {
		t.Fatalf("Expected tag == 'tag'; got '%s'", ct)
	}

	if n, err := s.NumChunksOf("test"); err != nil || n != 2 {
		t.Fatalf("Expected 2 chunks, got error '%s' numChunks = %d", err, n)
	}

	// add new chunks with different tag values.
	//

	err = s.Put("test", 100, []byte("content"))
	catch(err)

	err = s.Put("test", 150, []byte("content"))
	catch(err)

	err = s.SetTag("test", 100, []byte("NEW_TAG"))
	catch(err)

	if n, err := s.NumChunksOf("test"); err != nil || n != 1 {
		t.Fatalf("Expected 1 chunk, got error '%s' numChunks = %d", err, n)
	}

	if n := s.NumEntries(); n != 1 {
		t.Fatalf("Expected 1 underlining entry, got %d", n)
	}

	if ct, _ := s.GetCommonTag("test"); !bytes.Equal(ct, []byte("NEW_TAG")) {
		t.Fatalf("Expected tag == NEW_TAG; got '%s'", ct)
	}
}

func TestSetUntaggedChunks(t *testing.T) {

	clearStorage()

	var n int

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.Put("test", 0, []byte("content"))
	catch(err)

	err = s.Put("test", 10, []byte("content"))
	catch(err)

	err = s.Put("test", 20, []byte("content"))
	catch(err)

	n, err = s.SetUntaggedChunks("test", []byte("tag"))
	catch(err)
	if n != 3 {
		t.Fatalf("expected return value == 3, got error %d", n)
	}

	// add additional chunks...

	err = s.Put("test", 30, []byte("content"))
	catch(err)

	err = s.Put("test", 40, []byte("content"))
	catch(err)

	n, err = s.SetUntaggedChunks("test", []byte("tag"))
	catch(err)
	if n != 2 {
		t.Fatalf("expected return value == 2, got error %d", n)
	}

	// add a new chunk with a new tag...

	err = s.Put("test", 50, []byte("content"))
	catch(err)

	_, err = s.SetUntaggedChunks("test", []byte("new_tag"))
	if err != ErrIncoherentTag {
		t.Fatalf("expected errincoherenttag, got '%s'", err)
	}

	if ct, _ := s.GetCommonTag("test"); !bytes.Equal(ct, []byte("tag")) {
		t.Fatalf("expected tag == _tag; got '%s'", ct)
	}

	if n, err := s.NumChunksOf("test"); err != nil || n != 6 {
		t.Fatalf("expected 6 chunks, got error '%s' numchunks = %d", err, n)
	}

}

func TestGets(t *testing.T) {

	clearStorage()

	var tag []byte

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.Put("test", 0, []byte("content"))
	catch(err)

	err = s.Put("test", 10, []byte("content"))
	catch(err)

	err = s.Put("test", 20, []byte("content"))
	catch(err)

	_, _, err = s.Get("test", 0)
	if err != ErrUntagged {
		t.Fatalf("Expected error ErrUntagged, got '%s'", err)
	}

	_, err = s.SetUntaggedChunks("test", []byte("tag"))
	catch(err)

	tag, err = s.GetCommonTag("test")
	catch(err)

	if !bytes.Equal(tag, []byte("tag")) {
		t.Fatalf("expected tag == 'tag'; got '%s'", tag)
	}

	tag, err = s.GetTag("test", 0)
	catch(err)
	if !bytes.Equal(tag, []byte("tag")) {
		t.Fatalf("expected tag == 'tag'; got '%s'", tag)
	}
	tag, err = s.GetTag("test", 10)
	catch(err)
	if !bytes.Equal(tag, []byte("tag")) {
		t.Fatalf("expected tag == 'tag'; got '%s'", tag)
	}
	tag, err = s.GetTag("test", 20)
	catch(err)
	if !bytes.Equal(tag, []byte("tag")) {
		t.Fatalf("expected tag == 'tag'; got '%s'", tag)
	}
}

func TestGetTag(t *testing.T) {

	clearStorage()

	var tag []byte

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.Put("test", 0, []byte("content"))
	catch(err)

	_, err = s.SetUntaggedChunks("test", []byte("tag"))
	catch(err)

	_, err = s.GetTag("unknown", 0)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound; got '%s'", err)
	}

	_, err = s.GetTag("test", 10)
	if err != ErrChunkNotFound {
		t.Fatalf("expected ErrChunkNotFound; got '%s'", err)
	}

	tag, err = s.GetTag("test", 0)
	if err != nil {
		t.Fatalf("expected error; got '%s'", err)
	}

	if !bytes.Equal(tag, []byte("tag")) {
		t.Fatalf("expected tag == 'tag'; got '%s'", tag)
	}
}

func TestGetCommonTag(t *testing.T) {

	clearStorage()

	var tag []byte

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.Put("test", 0, []byte("content"))
	catch(err)

	tag, err = s.GetCommonTag("test")
	catch(err)

	if tag != nil {
		t.Fatalf("expected nil tag; got '%s'", tag)
	}

	_, err = s.SetUntaggedChunks("test", []byte("tag"))
	catch(err)

	tag, err = s.GetCommonTag("test")
	catch(err)

	if !bytes.Equal(tag, []byte("tag")) {
		t.Fatalf("expected tag == 'tag'; got '%s'", tag)
	}

	_, err = s.GetCommonTag("unknown")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound; got '%s'", err)
	}
}

func TestChunkClear(t *testing.T) {

	clearStorage()

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.Put("test", 0, []byte("content"))
	catch(err)

	err = s.Put("test", 10, []byte("content"))
	catch(err)

	err = s.Put("test", 20, []byte("content"))
	catch(err)

	_, err = s.SetUntaggedChunks("test", []byte("tag"))
	catch(err)

	s.Clear()

	if !s.Cache.Empty() {
		t.Fatalf("stash expected to be empty!")
	}

	if !s.Empty() {
		t.Fatalf("cache expected to be empty!")
	}
}

func TestPutWithTag(t *testing.T) {

	clearStorage()

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("content"))
	catch(err)

	err = s.PutWithTag("test", 10, []byte("tag"), []byte("content"))
	catch(err)

	err = s.PutWithTag("test2", 0, []byte("tag2"), []byte("content"))
	catch(err)

	err = s.PutWithTag("test2", 10, []byte("tag2"), []byte("content"))
	catch(err)

	if n := s.NumEntries(); n != 2 {
		t.Fatalf("Expected 2 entries, got %d", n)
	}

	if n := s.NumTotalChunks(); n != 4 {
		t.Fatalf("Expected 4 total chunks, got %d", n)
	}

	err = s.PutWithTag("test", 100, []byte("xxx"), []byte("content"))
	catch(err)

	if n := s.NumEntries(); n != 2 {
		t.Fatalf("Expected 2 entries, got %d", n)
	}

	if n := s.NumTotalChunks(); n != 3 {
		t.Fatalf("Expected 3 total chunks, got %d", n)
	}
}

func TestDelete(t *testing.T) {
	clearStorage()

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.Delete("test")
	if err != ErrNotFound {
		t.Fatalf("Expected error ErrNotFound, got '%s'", err)
	}

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("content"))
	catch(err)

	err = s.Delete("test")
	catch(err)

	if !s.Empty() {
		t.Fatalf("Expected empty cache!")
	}
}

func TestDeleteChunk(t *testing.T) {
	clearStorage()

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("content"))
	catch(err)

	err = s.DeleteChunk("test", 10)
	if err != ErrChunkNotFound {
		t.Fatalf("Expected error ErrChunkNotFound, got '%s'", err)
	}

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("content"))
	catch(err)

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("content"))
	catch(err)

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("content"))
	catch(err)

	err = s.DeleteChunk("test", 0)
	catch(err)

	if !s.Empty() {
		t.Fatalf("Expected empty cache, size = %d, stash_size: %d", s.NumEntries(), s.Cache.NumEntries())
	}
}

func TestStats(t *testing.T) {

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("1234"))
	catch(err)

	stats := s.GetStats()
	if res := []int64{stats.Size, stats.Entries, stats.Hit, stats.Miss}; !reflect.DeepEqual(res, []int64{4, 1, 0, 0}) {
		t.Fatalf("Expected Stats = [4, 1, 0, 0], got %v", res)
	}

	// overwriting the content when an entry is already in the cache with the same tag is a null operation.

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("12345678"))
	catch(err)

	stats = s.GetStats()
	if res := []int64{stats.Size, stats.Entries, stats.Hit, stats.Miss}; !reflect.DeepEqual(res, []int64{4, 1, 0, 0}) {
		t.Fatalf("Expected Stats = [4, 1, 0, 0], got %v", res)
	}

	err = s.PutWithTag("test", 10, []byte("tag"), []byte("1234"))
	catch(err)

	err = s.PutWithTag("test2", 0, []byte("tag"), []byte("1234"))
	catch(err)

	stats = s.GetStats()
	if res := []int64{stats.Size, stats.Entries, stats.Hit, stats.Miss}; !reflect.DeepEqual(res, []int64{12, 3, 0, 0}) {
		t.Fatalf("Expected Stats = [12, 3, 0, 0], got %v", res)
	}

	s.GetWithTag("test", 0)

	stats = s.GetStats()
	if res := []int64{stats.Size, stats.Entries, stats.Hit, stats.Miss}; !reflect.DeepEqual(res, []int64{12, 3, 1, 0}) {
		t.Fatalf("Expected Stats = [12, 3, 1, 0], got %v", res)
	}

	s.GetWithTag("test", 10)

	stats = s.GetStats()
	if res := []int64{stats.Size, stats.Entries, stats.Hit, stats.Miss}; !reflect.DeepEqual(res, []int64{12, 3, 2, 0}) {
		t.Fatalf("Expected Stats = [12, 3, 2, 0], got %v", res)
	}

	s.GetWithTag("test", 20)

	stats = s.GetStats()
	if res := []int64{stats.Size, stats.Entries, stats.Hit, stats.Miss}; !reflect.DeepEqual(res, []int64{12, 3, 2, 1}) {
		t.Fatalf("Expected Stats = [12, 3, 2, 1], got %v", res)
	}

	s.GetWithTag("unknown", 20)

	stats = s.GetStats()
	if res := []int64{stats.Size, stats.Entries, stats.Hit, stats.Miss}; !reflect.DeepEqual(res, []int64{12, 3, 2, 2}) {
		t.Fatalf("Expected Stats = [12, 3, 2, 2], got %v", res)
	}

	s.ResetStats()
	stats = s.GetStats()
	if res := []int64{stats.Size, stats.Entries, stats.Hit, stats.Miss}; !reflect.DeepEqual(res, []int64{12, 3, 0, 0}) {
		t.Fatalf("Expected Stats = [12, 3, 0, 0], got %v", res)
	}
}

func TestShrink(t *testing.T) {

	s, err := NewChunkCache(storageDir, 2048000, 40)
	catch(err)

	err = s.PutWithTag("test", 0, []byte("tag"), []byte("1234"))
	catch(err)

	err = s.PutWithTag("test", 10, []byte("tag"), []byte("1234"))
	catch(err)

	err = s.PutWithTag("test", 20, []byte("tag"), []byte("1234"))
	catch(err)

	s.Cache.Clear()

	nc, err := s.NumChunksOf("test")
	catch(err)

	if nc == s.NumTotalChunks() {
		t.Fatalf("Expected number of chunks mismatch, got equal instead!")
	}

	s.Shrink()

	nc, err = s.NumChunksOf("test")
	if err != ErrNotFound {
		t.Fatalf("Expected error == ErrNotFound, got error '%s'", err)
	}

	if nc != s.NumTotalChunks() {
		t.Fatalf("Expected equal number of chunks, got different values: %d - %d!", nc, s.NumTotalChunks())
	}
}
