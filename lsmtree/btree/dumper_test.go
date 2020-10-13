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
	dumper := buildDumperInstance()

	for i := 0; i < 10; i++ {
		entries := randomPutKVEntries(entriesCount)
		c := make(chan TreeInfo, 1)
		dumper.internalDump(entries, c)
	}
}

func buildDumperInstance() *dumper {
	btree := BTree{}
	return &dumper{
		btree:         &btree,
		root:          nil,
		filter:        &dummyFilter{},
		cache:         map[address]*pageForDump{},
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
