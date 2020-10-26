package datastore

import (
	"bytes"
	"sort"
	"testing"
	"yuyi-go/datastore/chunk"
)

func TestIndexPage(t *testing.T) {
	entries := randomPutKVPairs(20)
	pageForDump := &pageForDump{
		page:      *NewPage(Index, entries),
		dirty:     true,
		valid:     false,
		size:      0,
		shadowKey: nil,
	}
	content := pageForDump.buildCompressedBytes()

	page := &page{
		content: content,
		entries: nil,
		addr:    chunk.Address{Chunk: file, Offset: 0, Length: 0},
	}

	if page.Type() != Index {
		t.Error("type of page mismatch")
	}
	for i := 0; i < len(entries); i++ {
		if bytes.Compare(entries[i].Key, page.Key(i)) != 0 {
			t.Error("key invalid", page.Key(i))
		}
		if bytes.Compare(entries[i].Value, page.Value(i)) != 0 {
			t.Error("value invalid")
		}
	}
}

func randomPutKVPairs(count int) []*KVPair {
	res := make([]*KVPair, count)
	for i := 0; i < count; i++ {
		key := randomBytes(keyLen, defaultLetters)

		// create new address and cache it.
		addr := chunk.Address{Chunk: file, Offset: off, Length: 8192}
		off += 8192
		res[i] = &KVPair{
			Key:   key,
			Value: addr.Bytes(),
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return bytes.Compare(res[i].Key, res[j].Key) <= 0
	})
	return res
}
