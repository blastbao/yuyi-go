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

type DataStore struct {
	// activeMemTable current using memory table for writing data to.
	activeMemTable memtable.MemTable

	// sealedMemTables the memory table instances that already reached
	//max limit size and need be persisted
	sealedMemTables []*memtable.MemTable

	// btree the structure for persisting data from memory table
	btree *btree.BTree
}

func (store *DataStore) Put(key memtable.Key, value memtable.Value) {
	return
}

func (store *DataStore) Remove(key memtable.Key) {
	return
}

func (store *DataStore) Has(key *memtable.Key) bool {
	if store.activeMemTable.Has(key) {
		return true
	}
	for _, table := range store.sealedMemTables {
		if table.Has(key) {
			return true
		}
	}
	return store.btree.Has(key)
}

func (store *DataStore) Get(key *memtable.Key) memtable.Value {
	var value []byte
	value = store.activeMemTable.GetIfExists(key)
	if value != nil {
		return value
	}
	for _, table := range store.sealedMemTables {
		value = table.GetIfExists(key)
		if value != nil {
			return nil
		}
	}
	return store.btree.Get(key)
}

func List(start memtable.Key, end memtable.Key, max uint16) []memtable.Value {
	return nil
}

func ReverseList(start memtable.Key, end memtable.Value, max uint16) []memtable.Value {
	return nil
}

func (store *DataStore) ListOnPrefix(prefix memtable.Key, start memtable.Key, end memtable.Key, max uint16) []memtable.Value {
	return nil
}

func (store *DataStore) ReverseListOnPrefix(prefix memtable.Key, max uint16) []memtable.Value {
	return nil
}
