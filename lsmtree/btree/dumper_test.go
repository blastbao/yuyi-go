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
	for i := 0; i < 30; i++ {
		entries := randomPutKVEntries(entriesCount)
		allEntries = mergeEntries(allEntries, entries)
		dumper := buildDumperInstance(btree)
		btree.lastTreeInfo = dumper.Dump(entries)

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

	for i := 0; i < 10; i++ {
		entries := randomPutAndRemoveKVEntries(allEntries, entriesCount, 20)
		allEntries = mergeEntries(allEntries, entries)
		dumper := buildDumperInstance(btree)
		btree.lastTreeInfo = dumper.Dump(entries)

		index := 0
		// do list
		var start memtable.Key
		for {
			listRes := btree.List(start, nil, 1000)
			for _, pair := range listRes.pairs {
				if bytes.Compare(allEntries[index].Key, pair.Key) != 0 {
					t.Error("key invalid", "\n", allEntries[index].Key, "\n", pair.Key)
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

	for i := 0; i < count; i++ {
		if removePer < rand.Intn(100) {
			key := entries[rand.Intn(len(entries))].Key
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
			if newEntries[i].TableValue.Operation != memtable.Remove {
				mergedEntries = append(mergedEntries, newEntries[j])
			}
			i++
			j++
		} else if res > 0 {
			mergedEntries = append(mergedEntries, newEntries[j])
			j++
		} else {
			mergedEntries = append(mergedEntries, allEntries[i])
			i++
		}
	}
	if i < len(allEntries) {
		mergedEntries = append(mergedEntries, allEntries[i:len(allEntries)]...)
	}
	if j < len(newEntries) {
		mergedEntries = append(mergedEntries, newEntries[j:len(newEntries)]...)
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
