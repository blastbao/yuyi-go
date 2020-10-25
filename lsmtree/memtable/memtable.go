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

package memtable

type MemTable struct {
	skipList *SkipList
	sealed   bool
	capacity int32
}

func NewMemTable() *MemTable {
	skipList := NewSkipList()
	return &MemTable{
		skipList: skipList,
		sealed:   false,
		capacity: 0,
	}
}

func (memTable *MemTable) Has(key Key, seq uint64) bool {
	return memTable.skipList.Get(key, seq) != nil
}

func (memTable *MemTable) Get(key Key, seq uint64) *TableValue {
	return memTable.skipList.Get(key, seq)
}

func (memTable *MemTable) List(start Key, end Key, seq uint64) *Iterator {
	return memTable.skipList.NewIterator(start, end, seq)
}
