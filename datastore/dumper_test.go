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

package datastore

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/blastbao/yuyi-go/datastore/chunk"

	"go.uber.org/zap"
)

var entriesCount = 2000

func TestBTreePutEntries(t *testing.T) {
	cfg, err := setupCfg()
	if err != nil {
		t.Error("Failed to initialize config")
		return
	}

	btree, err := NewBTree(zap.NewExample(), nil, cfg)
	if err != nil {
		t.Error("Create empty btree failed")
		return
	}
	allEntries := make([]*KVEntry, 0)
outer:
	for i := 0; i < 20; i++ {
		entries := randomPutKVEntries(entriesCount)
		allEntries = mergeEntries(allEntries, entries)
		dumper, err := newDumper(zap.NewExample(), btree, cfg)
		if err != nil {
			t.Error("new dumper failed")
			return
		}
		treeInfo, err := dumper.Dump(entries, 0)
		if err != nil {
			t.Error("btree dump failed")
			break outer
		}
		btree.lastTreeInfo = treeInfo
		if !validateBTree(btree) {
			t.Error("tree invalid\n")
			break outer
		}
		fmt.Printf("Finish dump round %d\n", i)

		index := 0
		// do list
		var start Key
		for {
			listRes, err := btree.List(start, nil, 1000)
			if err != nil {
				t.Error("btree list failed")
				break outer
			}
			for _, pair := range listRes.pairs {
				if bytes.Compare(allEntries[index].Key, pair.Key) != 0 {
					t.Error("key invalid", "\n", allEntries[index].Key, "\n", pair.Key)
					break outer
				}
				index++
			}
			start = listRes.next
			if start == nil {
				break
			}
		}
	}
}

func TestBTreePutAndRemoveEntries(t *testing.T) {
	cfg, err := setupCfg()
	if err != nil {
		t.Error("Failed to initialize config")
		return
	}

	btree, err := NewBTree(zap.NewExample(), nil, cfg)
	if err != nil {
		t.Error("Create empty btree failed")
		return
	}
	allEntries := make([]*KVEntry, 0)

	// init with 2000 put entries
	entries := randomPutKVEntries(entriesCount)
	allEntries = mergeEntries(allEntries, entries)
	dumper, err := newDumper(zap.NewExample(), btree, cfg)
	if err != nil {
		t.Error("new dumper failed")
		return
	}
	treeInfo, err := dumper.Dump(entries, 0)
	if err != nil {
		t.Error("btree dump failed")
		return
	}
	btree.lastTreeInfo = treeInfo
outer:
	for i := 0; i < 20; i++ {
		entries := randomPutAndRemoveKVEntries(allEntries, entriesCount, 20)
		allEntries = mergeEntries(allEntries, entries)
		dumper, err := newDumper(zap.NewExample(), btree, cfg)
		if err != nil {
			t.Error("new dumper failed")
			return
		}
		treeInfo, err := dumper.Dump(entries, 0)
		if err != nil {
			t.Error("btree dump failed ")
			break outer
		}
		btree.lastTreeInfo = treeInfo
		if !validateBTree(btree) {
			t.Error("tree invalid")
			break outer
		}
		fmt.Printf("Finish dump round %d\n", i)

		index := 0
		// do list
		var start Key
		for {
			listRes, err := btree.List(start, nil, 1000)
			if err != nil {
				t.Error("btree list failed")
				break outer
			}
			for _, pair := range listRes.pairs {
				if bytes.Compare(allEntries[index].Key, pair.Key) != 0 {
					t.Error("key invalid", "\n", allEntries[index].Key, "\n", pair.Key)
					break outer
				}
				index++
			}
			start = listRes.next
			if start == nil {
				break
			}
		}
	}
}

func TestBTreePutAndRemoveAll(t *testing.T) {
	cfg, err := setupCfg()
	if err != nil {
		t.Error("Failed to initialize config")
		return
	}

	btree, err := NewBTree(zap.NewExample(), nil, cfg)
	if err != nil {
		t.Error("Create empty btree failed")
		return
	}

	for i := 0; i < 20; i++ {
		// put entries
		entries := randomPutKVEntries(entriesCount)
		dumper, err := newDumper(zap.NewExample(), btree, cfg)
		if err != nil {
			t.Error("new dumper failed")
			return
		}
		treeInfo, err := dumper.Dump(entries, 0)
		if err != nil {
			t.Error("btree dump failed")
			return
		}
		btree.lastTreeInfo = treeInfo

		// remove entries
		for _, entry := range entries {
			entry.TableValue = TableValue{
				Operation: Remove,
				Value:     nil,
			}
		}
		dumper, err = newDumper(zap.NewExample(), btree, cfg)
		if err != nil {
			t.Error("new dumper failed")
			return
		}
		treeInfo, err = dumper.Dump(entries, 0)
		if err != nil {
			t.Error("btree dump failed")
			return
		}
		btree.lastTreeInfo = treeInfo

		listRes, err2 := btree.List(nil, nil, 1000)
		if err2 != nil {
			t.Error("btree list failed")
			return
		}
		if len(listRes.pairs) != 0 {
			t.Error("tree is not empty")
			break
		}
	}
}

var keyLen = 100
var valueLen = 200

func randomPutKVEntries(count int) []*KVEntry {
	res := make([]*KVEntry, count)
	for i := 0; i < count; i++ {
		key := randomBytes(keyLen, defaultLetters)
		value := randomBytes(valueLen, defaultLetters)
		res[i] = &KVEntry{
			Key: key,
			TableValue: TableValue{
				Operation: Put,
				Value:     value,
			},
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return bytes.Compare(res[i].Key, res[j].Key) <= 0
	})
	return res
}

func randomPutAndRemoveKVEntries(entries []*KVEntry, count int, removePer int) []*KVEntry {
	res := make([]*KVEntry, count)
	deleted := map[int]bool{}

	length := len(entries)
	for i := 0; i < count; i++ {
		if rand.Intn(100) < removePer {
			var key Key
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
			res[i] = &KVEntry{
				Key: key,
				TableValue: TableValue{
					Operation: Remove,
					Value:     nil,
				},
			}
		} else {
			key := randomBytes(keyLen, defaultLetters)
			value := randomBytes(valueLen, defaultLetters)
			res[i] = &KVEntry{
				Key: key,
				TableValue: TableValue{
					Operation: Put,
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

func mergeEntries(allEntries []*KVEntry, newEntries []*KVEntry) []*KVEntry {
	mergedEntries := make([]*KVEntry, 0)

	i := 0
	j := 0
	for {
		if i >= len(allEntries) || j >= len(newEntries) {
			break
		}
		res := allEntries[i].Key.Compare(newEntries[j].Key)
		if res == 0 {
			if newEntries[j].TableValue.Operation != Remove {
				mergedEntries = append(mergedEntries, newEntries[j])
			}
			i++
			j++
		} else if res > 0 {
			if newEntries[j].TableValue.Operation != Remove {
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

func validateBTree(btree *BTree) bool {
	root := btree.lastTreeInfo.root

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
				addr := chunk.ParseAddress(entry.Value)
				page, err := btree.readPage(addr)
				if err != nil {
					return false
				}
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
