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
	cache map[Address]*PageBufferForDump

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
		path:      nil,
		committed: map[int]map[Address][]*PageBufferForDump{},
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
		dumper.checkForCommit(context, nil)
	}
}

func (dumper *Dumper) putEntries(putEntries []*memtable.KVEntry, ctx DumpContext) {
	var rightSibling memtable.Key
	path := ctx.path
	for _, entry := range putEntries {
		// check path
		if path == nil || (rightSibling != nil && rightSibling.Compare(entry.Key) <= 0) {
			// need fetch path cause origin path is nil/invalid
			path = dumper.fetchPageForDumper(&dumper.root, &entry.Key)

			// check with new path to commit pages that is writable
			dumper.checkForCommit(ctx, path)

			ctx.path = path
			// update right sibling for checking if need find new path
			rightSibling = dumper.findRightSibling(path)
		}

		leafPage := path[len(path)-1].page
		index := leafPage.Search(&entry.Key)
		if index < 0 {
			// key not exists, add it to filter first
			dumper.filter.Put(&entry.Key)
		}
		leafPage.addKVEntryToIndex(entry, index)
	}
}

func (dumper *Dumper) removeEntry(entry *memtable.KVEntry, ctx DumpContext) {
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
		leafPage.removeKVEntry(index)
	}
	// check if need merge page for balance
	path = dumper.checkForMerge(path)
}

func (dumper *Dumper) fetchPageForDumper(root *PageBuffer, key *memtable.Key) []*PathItemForDump {
	// Todo: Should consider pages that already cached in dumper cache
	path := dumper.btree.FindPathToLeaf(root, key)
	pathForDump := make([]*PathItemForDump, len(path))
	for i, item := range path {
		pathForDump[i] = &PathItemForDump{page: PageBufferForDump{PageBuffer: item.page, dirty: false}, index: item.index}

		// cache pages in dumper cache
		dumper.cache[item.page.Address] = &pathForDump[i].page
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

func (dumper *Dumper) checkForCommit(ctx DumpContext, newPath []*PathItemForDump) {
	path := ctx.path
	if path == nil {
		return
	}
	for i := len(path) - 1; i > 0; i-- {
		// check if page is committable
		if newPath != nil && &path[i].page == &newPath[i].page {
			break
		}
		pathItem := path[i]
		if pathItem.page.size > dumper.leafPageSize {
			// page size over threshold. Try to split this page
			splitPoints := dumper.calculateSplitPoints(&pathItem.page)
			if len(splitPoints) == 0 || len(splitPoints) == 1 {
				return
			}
			// should flush writable pages before splitting index page
			if pathItem.page.Type() == Index {
				dumper.flush()
			}

			// split current page, decommission current page
			dumper.decommissionPage(&pathItem.page)

			// create new pages based on the splitPoint
			startPoint := 0
			for splitPoint := range splitPoints {
				newPage := dumper.splitPage(&pathItem.page, startPoint, splitPoint)
				// add new page's reference in it's parent
				path[i-1].page.addKVPair(&memtable.KVPair{
					Key:   newPage.Key(0),
					Value: newPage.Address.ToValue(),
				})
				// commit this page for writing
				ctx.commitPage(newPage, path[i-1].page.Address, i)

				startPoint = splitPoint
			}
		} else if pathItem.page.size < dumper.leafPageSize/2 {
			// page size under threshold. Try to merge this page with it's right page
		} else {
			// commit this page for writing
			ctx.commitPage(&pathItem.page, path[i-1].page.Address, i)
		}
	}
}

func (dumper *Dumper) decommissionPage(page *PageBufferForDump) {
	page.valid = false
	dumper.cache[page.Address] = nil
}

func (dumper *Dumper) calculateSplitPoints(page *PageBufferForDump) []int {
	return nil
}

func (dumper *Dumper) splitPage(page *PageBufferForDump, start int, end int) *PageBufferForDump {
	//dumper.cache[newPage.Address] = newPage
	// Todo: add the created page in write cache
	return nil
}

func (dumper *Dumper) checkForMerge(path []*PathItemForDump) []*PathItemForDump {
	return nil
}

func (dumper *Dumper) flush() {
	return
}

type DumpContext struct {
	// path the path found in last round put/remove entries
	path []*PathItemForDump
	// committed the map to save parent/child relationship for the pages
	// that are writable
	committed map[int]map[Address][]*PageBufferForDump
}

func (ctx *DumpContext) commitPage(page *PageBufferForDump, parentAddr Address, level int) {
	committedOnLevel := ctx.committedOnLevel(level)
	if committedOnLevel[parentAddr] == nil {
		committedOnLevel[parentAddr] = make([]*PageBufferForDump, 10)
	}
	committedOnLevel[parentAddr] = append(committedOnLevel[parentAddr], page)
}

func (ctx *DumpContext) committedOnLevel(level int) map[Address][]*PageBufferForDump {
	if ctx.committed[level] == nil {
		ctx.committed[level] = map[Address][]*PageBufferForDump{}
	}
	return ctx.committed[level]
}
