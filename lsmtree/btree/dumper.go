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
	"yuyi-go/lsmtree/memtable"
)

type Dumper struct {
	// btree the b+ tree instance for dumping
	btree *BTree

	// root the working root for dumping
	root PageBuffer

	// filter the filter of btree
	filter Filter

	// tables the memory tables slice to be persisted
	tables []*memtable.MemTable

	// cache the cache for page buffer that may be modified during dump
	cache map[Address]*PageBuffer

	// leafPageSize the size limit for leaf page
	leafPageSize int

	// indexPageSize the size limit for index page
	indexPageSize int
}

func (dumper Dumper) Dump() *TreeInfo {
	entries := toSlice(dumper.tables)
	go dumper.internalDump(dumper.btree, entries)
	return nil
}

func toSlice(tables []*memtable.MemTable) []*memtable.KVEntry {
	return nil
}

func (dumper Dumper) internalDump(btree *BTree, entries []*memtable.KVEntry) {
	context := DumpContext{
		oldPath:   nil,
		committed: map[Address]*[]PageBuffer{},
	}
	lastIndex := 0
	for i, entry := range entries {
		if entry.TableValue.Operation == memtable.Put {
			continue
		}
		if entry.TableValue.Operation == memtable.Remove {
			if i > lastIndex {
				putEntries := entries[lastIndex : i+1 : i+1]
				dumper.putEntries(putEntries, context)
			}
			dumper.removeEntry(entry, context)
			lastIndex = i + 1
		}
	}
	if lastIndex < len(entries) {
		putEntries := entries[lastIndex:len(entries):len(entries)]
		dumper.putEntries(putEntries, context)
	}
}

func (dumper *Dumper) putEntries(putEntries []*memtable.KVEntry, context DumpContext) {
	var rightSibling memtable.Key
	var path []*PathItemForDump
	for _, entry := range putEntries {
		if path == nil || (rightSibling != nil && rightSibling.Compare(&entry.Key) <= 0) {
			// check if need split pages
			dumper.checkForSplit(context.oldPath, context)

			// Need fetch path cause origin path is nil/invalid
			path = dumper.fetchPageForDumper(&dumper.root, &entry.Key)
			rightSibling = dumper.findRightSibling(path)
		}

		leafPage := path[len(path)-1].page
		index := leafPage.Search(&entry.Key)
		if index < 0 {
			// key not exists, add it to filter first
			dumper.filter.Put(&entry.Key)
		}
		leafPage.AddKVEntryToIndex(entry, index)
	}
	// check if need split pages
	dumper.checkForSplit(path, context)
}

func (dumper *Dumper) removeEntry(entry *memtable.KVEntry, context DumpContext) {
	if !dumper.filter.MightContains(&entry.Key) {
		return
	}
	path := dumper.fetchPageForDumper(&dumper.root, &entry.Key)
	leafPage := path[len(path)-1].page
	index := leafPage.Search(&entry.Key)
	if index < 0 {
		return
	} else {
		// key exists update filter first
		dumper.filter.Remove(&entry.Key)

		// modify page's content
		leafPage.RemoveKVEntry(index)
	}
	// check if need merge page for balance
	path = dumper.checkForMerge(path)
}

func (dumper *Dumper) fetchPageForDumper(root *PageBuffer, key *memtable.Key) []*PathItemForDump {
	// Todo: Should consider pages that already cached in dumper cache
	path := dumper.btree.FindPathToLeaf(root, key)
	pathForDump := make([]*PathItemForDump, len(path))
	for i, item := range path {
		// cache pages in dumper cache
		dumper.cache[item.page.Address] = &item.page

		pathForDump[i] = &PathItemForDump{page: PageBufferForDump{PageBuffer: item.page, dirty: false}, index: item.index}
	}
	return pathForDump
}

func (dumper *Dumper) findRightSibling(path []*PathItemForDump) memtable.Key {
	// iterate from index to index
	for i := len(path) - 2; i >= 0; i-- {
		page := path[i].page
		nextIndex := path[i+1].index + 1
		if nextIndex >= page.PageBuffer.KVPairsCount() {
			continue
		}
		return page.PageBuffer.Key(nextIndex)
	}
	return nil
}

func (dumper *Dumper) checkForSplit(path []*PathItemForDump, context DumpContext) {
	if path == nil {
		return
	}
	// root should not split here
	for i := len(path) - 1; i > 0; i-- {
		pathItem := path[i]
		if pathItem.page.size < dumper.leafPageSize {
			return
		}
		// split current page
		splitPoints := calculateSplitPoints(&pathItem.page)
		if len(splitPoints) == 0 || len(splitPoints) == 1 {
			return
		}
		// remove old page from cache
		dumper.cache[pathItem.page.Address] = nil

		// create new page and add to cache
		startPoint := 0
		for splitPoint := range splitPoints {
			newPage := splitPage(&pathItem.page, startPoint, splitPoint)
			path[i-1].page.AddKVPair(&memtable.KVPair{
				Key:   newPage.Key(0),
				Value: newPage.Address.ToValue(),
			})
			dumper.cache[newPage.Address] = newPage

			// ToDo: Need recalculate path item's index in i - 1
			if startPoint <= index && index < splitPoint {
				pathItem.page.PageBuffer = *newPage
			}
			startPoint = splitPoint
		}
	}
}

func calculateSplitPoints(page *PageBufferForDump) []int {
	return nil
}

func splitPage(page *PageBufferForDump, start int, end int) *PageBuffer {
	return nil
}

func (dumper *Dumper) checkForMerge(path []*PathItemForDump) []*PathItemForDump {
	return nil
}

type DumpContext struct {
	// oldPath the path found in last round put/remove entries
	oldPath []*PathItemForDump
	// committed the map to save parent/child relationship for the pages
	// that are writable
	committed map[Address]*[]PageBuffer
}
