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

package lsmtree

import (
	"yuyi-go/lsmtree/btree"
	"yuyi-go/lsmtree/memtable"
)

var (
	minSeq = uint64(0)
	maxSeq = uint64(0xFFFFFFFFFFFFFFFF)
)

type DataStore struct {
	// activeMemTable current using memory table for writing data to.
	activeMemTable memtable.MemTable

	// sealedMemTables the memory table instances that already reached
	//max limit size and need be persisted
	sealedMemTables []*memtable.MemTable

	// btree the structure for persisting data from memory table
	btree *btree.BTree

	// seq the sequence of write operation. When read started, current
	// sequence will be acquired and make sure that no later committed
	// kv will be read
	seq uint64
}

func (store *DataStore) Put(key memtable.Key, value memtable.Value) {
	return
}

func (store *DataStore) Remove(key memtable.Key) {
	return
}

func (store *DataStore) Has(key memtable.Key) bool {
	seq := store.seq
	value := store.activeMemTable.Get(key, seq)
	if value != nil {
		if value.Operation == memtable.Remove {
			return false
		}
		return true
	}
	for _, table := range store.sealedMemTables {
		value := table.Get(key, maxSeq)
		if value != nil {
			if value.Operation == memtable.Remove {
				return false
			}
			return true
		}
	}
	return store.btree.Has(&key)
}

func (store *DataStore) Get(key memtable.Key) memtable.Value {
	seq := store.seq
	value := store.activeMemTable.Get(key, seq)
	if value != nil {
		if value.Operation == memtable.Remove {
			return nil
		}
		return value.Value
	}
	for _, table := range store.sealedMemTables {
		value := table.Get(key, maxSeq)
		if value != nil {
			if value.Operation == memtable.Remove {
				return nil
			}
			return value.Value
		}
	}
	return store.btree.Get(&key)
}

func (store *DataStore) List(start memtable.Key, end memtable.Key, max int) []*memtable.KVPair {
	// seq := store.seq
	// activeIter := store.activeMemTable.List(start, end, seq)
	// sealedIter := make([]*memtable.Iterator, 0)
	// for _, memtable := range store.sealedMemTables {
	// 	sealedIter = append(sealedIter, memtable.List(start, end, seq))
	// }
	// resFromTree := store.btree.List(start, end, max)

	// res := make([]*memtable.KVPair, 0)
	// found := 0
	// Todo: implement min heap for merging result together
	return nil
}

func ReverseList(start memtable.Key, end memtable.Value, max uint16) []memtable.Value {
	return nil
}
