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
	"testing"
)

func TestPutEntries(t *testing.T) {
	datastore, err := New()
	if err != nil {
		t.Error("datastore create failed")
		return
	}
	allEntries := make([]*KVEntry, 0)
outer:
	for i := 0; i < 20; i++ {
		entries := randomPutKVEntries(entriesCount)
		allEntries = mergeEntries(allEntries, entries)

		for _, entry := range entries {
			datastore.Put(entry.Key, entry.TableValue.Value)
		}

		index := 0
		// do list
		var start Key
		for {
			listRes, err := datastore.List(start, nil, 1000)
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
			start = *listRes.next
			if start == nil {
				break
			}
		}
	}
}
