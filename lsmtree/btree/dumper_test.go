// Copyright 2015 The yuyi Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"yuyi-go/lsmtree/memtable"
)

var entriesCount = 2000

func TestPutEntries(t *testing.T) {
	btree := &BTree{}
	allEntries := make([]*memtable.KVEntry, 0)
outer:
	for i := 0; i < 1000; i++ {
		entries := randomPutKVEntries(entriesCount)
		allEntries = mergeEntries(allEntries, entries)
		dumper := buildDumperInstance(btree)
		btree.lastTreeInfo = dumper.Dump(entries)
		if !validateBTree(btree.lastTreeInfo.root) {
			t.Error("tree invalid\n")
			break outer
		}

		index := 0
		// do list
		var start memtable.Key
		for {
			listRes := btree.List(start, nil, 1000)
			for _, pair := range listRes.pairs {
				if bytes.Compare(allEntries[index].Key, pair.Key) != 0 {
					t.Error("key invalid", "\n", allEntries[index].Key, "\n", pair.Key)
					break outer
				}
				index++
			}
			start = *listRes.next
			if start == nil {
				break
			}
		}
	}
}

func TestPutAndRemoveEntries(t *testing.T) {
	btree := &BTree{}
	allEntries := make([]*memtable.KVEntry, 0)

	// init with 2000 put entries
	entries := randomPutKVEntries(entriesCount)
	allEntries = mergeEntries(allEntries, entries)
	dumper := buildDumperInstance(btree)
	btree.lastTreeInfo = dumper.Dump(entries)
outer:
	for i := 0; i < 200; i++ {
		entries := randomPutAndRemoveKVEntries(allEntries, entriesCount, 20)
		allEntries = mergeEntries(allEntries, entries)
		dumper := buildDumperInstance(btree)
		btree.lastTreeInfo = dumper.Dump(entries)
		if !validateBTree(btree.lastTreeInfo.root) {
			t.Error("tree invalid\n")
			break outer
		}

		index := 0
		// do list
		var start memtable.Key
		for {
			listRes := btree.List(start, nil, 1000)
			for _, pair := range listRes.pairs {
				if bytes.Compare(allEntries[index].Key, pair.Key) != 0 {
					t.Error("key invalid", "\n", allEntries[index].Key, "\n", pair.Key)
					break outer
				}
				index++
			}
			start = *listRes.next
			if start == nil {
				break
			}
		}
	}
}

func buildDumperInstance(btree *BTree) *dumper {
	var root pageForDump
	var depth int
	if btree.lastTreeInfo != nil && btree.lastTreeInfo.root != nil {
		page := btree.lastTreeInfo.root
		root = pageForDump{
			page:      *page,
			dirty:     false,
			valid:     true,
			size:      len(page.content),
			shadowKey: nil,
		}
		depth = btree.lastTreeInfo.depth
		return &dumper{
			btree:         btree,
			root:          &root,
			filter:        &dummyFilter{},
			cache:         map[address]*pageForDump{},
			treeDepth:     depth,
			leafPageSize:  8192,
			indexPageSize: 8192,
		}
	}
	return &dumper{
		btree:         btree,
		root:          nil,
		filter:        &dummyFilter{},
		cache:         map[address]*pageForDump{},
		treeDepth:     0,
		leafPageSize:  8192,
		indexPageSize: 8192,
	}
}

var keyLen = 100
var valueLen = 200

func randomPutKVEntries(count int) []*memtable.KVEntry {
	res := make([]*memtable.KVEntry, count)
	for i := 0; i < count; i++ {
		key := randomBytes(keyLen, defaultLetters)
		value := randomBytes(valueLen, defaultLetters)
		res[i] = &memtable.KVEntry{
			Key: key,
			TableValue: memtable.TableValue{
				Operation: memtable.Put,
				Value:     value,
			},
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return bytes.Compare(res[i].Key, res[j].Key) <= 0
	})
	return res
}

func randomPutAndRemoveKVEntries(entries []*memtable.KVEntry, count int, removePer int) []*memtable.KVEntry {
	res := make([]*memtable.KVEntry, count)
	deleted := map[int]bool{}

	length := len(entries)
	for i := 0; i < count; i++ {
		if rand.Intn(100) < removePer {
			var key memtable.Key
			for {
				deleteIndex := rand.Intn(length)
				if deleted[deleteIndex] == false {
					key = entries[deleteIndex].Key
					deleted[deleteIndex] = true
					break
				} else {
					fmt.Errorf("Hit duplicated key for delete")
				}
			}
			res[i] = &memtable.KVEntry{
				Key: key,
				TableValue: memtable.TableValue{
					Operation: memtable.Remove,
					Value:     nil,
				},
			}
		} else {
			key := randomBytes(keyLen, defaultLetters)
			value := randomBytes(valueLen, defaultLetters)
			res[i] = &memtable.KVEntry{
				Key: key,
				TableValue: memtable.TableValue{
					Operation: memtable.Put,
					Value:     value,
				},
			}
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return bytes.Compare(res[i].Key, res[j].Key) <= 0
	})
	return res
}

func mergeEntries(allEntries []*memtable.KVEntry, newEntries []*memtable.KVEntry) []*memtable.KVEntry {
	mergedEntries := make([]*memtable.KVEntry, 0)

	i := 0
	j := 0
	for {
		if i >= len(allEntries) || j >= len(newEntries) {
			break
		}
		res := allEntries[i].Key.Compare(newEntries[j].Key)
		if res == 0 {
			if newEntries[j].TableValue.Operation != memtable.Remove {
				mergedEntries = append(mergedEntries, newEntries[j])
			}
			i++
			j++
		} else if res > 0 {
			if newEntries[j].TableValue.Operation != memtable.Remove {
				mergedEntries = append(mergedEntries, newEntries[j])
			}
			j++
		} else {
			mergedEntries = append(mergedEntries, allEntries[i])
			i++
		}
	}
	if i < len(allEntries) {
		mergedEntries = append(mergedEntries, allEntries[i:]...)
	}
	if j < len(newEntries) {
		mergedEntries = append(mergedEntries, newEntries[j:]...)
	}
	return mergedEntries
}

var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandomString returns a random string with a fixed length
func randomBytes(n int, allowedChars ...[]rune) []byte {
	var letters []rune

	if len(allowedChars) == 0 {
		letters = defaultLetters
	} else {
		letters = allowedChars[0]
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return []byte(string(b))
}

func validateBTree(root *page) bool {
	queue := make([]*page, 1)
	queue[0] = root

	for {
		if len(queue) == 0 {
			break
		}
		head := queue[0]
		queue = queue[1:]

		// check order of keys in head
		for i := 0; i < head.KVPairsCount()-1; i++ {
			if head.Key(i).Compare(head.Key(i+1)) >= 0 {
				return false
			}
		}
		if head.Type() == Root || head.Type() == Index {
			// read child pages to do validation
			for _, entry := range head.AllEntries() {
				addr := newAddress(entry.Value)
				page := readPage(addr)
				if page.content == nil {
					return false
				}
				// check mapping key
				if entry.Key.Compare(page.Key(0)) != 0 {
					return false
				}
				queue = append(queue, page)
			}
		}
	}
	return true
}
