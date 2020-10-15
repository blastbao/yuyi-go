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

func TestDumper(t *testing.T) {
	btree := &BTree{}
	allEntries := make([]*memtable.KVEntry, 0)
	for i := 0; i < 10; i++ {
		entries := randomPutKVEntries(entriesCount)
		allEntries = append(allEntries, entries...)
		sort.Slice(allEntries, func(i, j int) bool {
			return bytes.Compare(allEntries[i].Key, allEntries[j].Key) <= 0
		})

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
