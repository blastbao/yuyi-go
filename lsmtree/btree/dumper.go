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

type dumper struct {
	// btree the b+ tree instance for dumping
	btree *BTree

	// root the working root for dumping
	root *pageForDump

	// filter the filter of btree
	filter Filter

	// cache the cache for page buffer that may be modified during dump
	cache map[address]*pageForDump

	// treeDepth the depth of the tree during dump
	treeDepth int

	// leafPageSize the size limit for leaf page
	leafPageSize int

	// indexPageSize the size limit for index page
	indexPageSize int
}

func (dumper *dumper) Dump(tables []*memtable.MemTable) *TreeInfo {
	entries := toSlice(tables)
	c := make(chan TreeInfo, 1)
	go dumper.internalDump(entries, c)
	res := <-c
	return &res
}

func toSlice(tables []*memtable.MemTable) []*memtable.KVEntry {
	return nil
}

func (dumper *dumper) internalDump(entries []*memtable.KVEntry, c chan TreeInfo) {
	var treeInfo TreeInfo
	defer func() {
		c <- treeInfo
	}()

	ctx := &context{
		path:      nil,
		committed: map[int]map[address][]*pageForDump{},
	}
	lastIndex := 0
	for i, entry := range entries {
		if entry.TableValue.Operation == memtable.Put {
			continue
		}
		if entry.TableValue.Operation == memtable.Remove {
			if i > lastIndex {
				putEntries := entries[lastIndex : i+1 : i+1]
				dumper.putEntries(putEntries, ctx)
			}
			dumper.removeEntry(entry, ctx)
			lastIndex = i + 1
		}
	}
	if lastIndex < len(entries) {
		putEntries := entries[lastIndex:len(entries):len(entries)]
		dumper.putEntries(putEntries, ctx)
		dumper.checkForCommit(ctx, nil)
	}
	dumper.flush(ctx)

	// handle root split and sync
	treeInfo = *dumper.sync(ctx)
}

func (dumper *dumper) sync(ctx *context) *TreeInfo {
	for {
		if dumper.root.size > dumper.indexPageSize {
			splitPoints := dumper.calculateSplitPoints(dumper.root)
			if len(splitPoints) > 1 {
				dumper.decommissionPage(dumper.root)
				newRoot := &pageForDump{
					page:      *NewPage(Root, nil),
					dirty:     false,
					valid:     true,
					size:      0,
					shadowKey: nil,
				}

				startPoint := 0
				for _, splitPoint := range splitPoints {
					newPage := dumper.splitPage(dumper.root, Index, startPoint, splitPoint)
					// add new page's reference in root
					newRoot.addKVPair(&memtable.KVPair{
						Key:   newPage.Key(0),
						Value: newPage.addr.ToValue(),
					})
					// commit this page for writing
					ctx.commitPage(newPage, newRoot.addr, 0)
					startPoint = splitPoint
				}
				dumper.root = newRoot
				dumper.treeDepth++
				dumper.flush(ctx)
			}
		} else {
			break
		}
	}

	// flush root
	dumper.root.addr = dumper.writePage(dumper.root)

	return &TreeInfo{
		root:   readPage(dumper.root.addr),
		filter: &dummyFilter{},
	}
}

func (dumper *dumper) putEntries(putEntries []*memtable.KVEntry, ctx *context) {
	var rightSibling memtable.Key
	path := ctx.path
	for _, entry := range putEntries {
		// check path
		if path == nil || (rightSibling != nil && rightSibling.Compare(entry.Key) <= 0) {
			// need fetch path cause origin path is nil/invalid
			path = dumper.fetchPathForDumper(dumper.root, &entry.Key)

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

func (dumper *dumper) removeEntry(entry *memtable.KVEntry, ctx *context) {
	if dumper.root == nil || !dumper.filter.MightContains(&entry.Key) {
		return
	}
	path := dumper.fetchPathForDumper(dumper.root, &entry.Key)

	leafPage := path[len(path)-1].page
	index := leafPage.Search(&entry.Key)
	if index < 0 {
		return
	}
	// key exists update filter first
	dumper.filter.Remove(&entry.Key)

	// modify page's content
	leafPage.removeKVEntry(index)
}

func (dumper *dumper) fetchPathForDumper(root *pageForDump, key *memtable.Key) []*pathItemForDump {
	if root == nil {
		res := make([]*pathItemForDump, 2, 2)

		// create empty leaf
		leaf := pageForDump{
			page:      *NewPage(Leaf, nil),
			dirty:     true,
			valid:     false,
			size:      0,
			shadowKey: *key,
		}
		res[1] = &pathItemForDump{&leaf, 0}
		dumper.cache[leaf.addr] = &leaf
		// create new root with leaf reference in root
		root := pageForDump{
			page:      *NewPage(Root, nil),
			dirty:     true,
			valid:     true,
			size:      0,
			shadowKey: nil,
		}
		root.addKVToIndex(*key, leaf.addr.ToValue(), -1)
		res[0] = &pathItemForDump{&root, 0}
		dumper.cache[root.addr] = &root

		dumper.root = &root
		dumper.treeDepth = 2
		return res
	}
	res := make([]*pathItemForDump, dumper.treeDepth, dumper.treeDepth)
	res[0] = &pathItemForDump{root, 0}

	parent := root
	depth := 1
	for {
		index := parent.Search(key)
		if index < 0 {
			index = -index - 2
		}
		// find child page with the found index and push it in result.
		childAddr := parent.ChildAddress(index)
		var childPage *pageForDump
		if dumper.cache[childAddr] != nil {
			childPage = dumper.cache[childAddr]
		} else {
			page := *readPage(childAddr)
			childPage = &pageForDump{
				page:      page,
				dirty:     false,
				valid:     true,
				size:      len(page.content),
				shadowKey: nil,
			}
			dumper.cache[childAddr] = childPage
		}
		res[depth] = &pathItemForDump{
			page:  childPage,
			index: index,
		}

		if childPage.Type() == Leaf {
			// found leaf page, search end
			break
		} else {
			parent = childPage
			depth++
		}
	}
	return res
}

func (dumper *dumper) fetchPageForDumper(addr address) *pageForDump {
	content := ReadFrom(addr)
	page := page{content: content, addr: addr}
	return &pageForDump{
		page:      page,
		dirty:     false,
		valid:     true,
		size:      len(content),
		shadowKey: nil,
	}
}

func (dumper *dumper) findRightSibling(path []*pathItemForDump) memtable.Key {
	// iterate from index to index
	for i := len(path) - 2; i >= 0; i-- {
		page := path[i].page
		nextIndex := path[i+1].index + 1
		if nextIndex >= page.page.KVPairsCount() {
			continue
		}
		return page.page.Key(nextIndex)
	}
	return nil
}

func (dumper *dumper) checkForCommit(ctx *context, newPath []*pathItemForDump) {
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
			splitPoints := dumper.calculateSplitPoints(pathItem.page)
			if len(splitPoints) == 0 || len(splitPoints) == 1 {
				return
			}
			// should flush writable pages before splitting index page
			if pathItem.page.Type() == Index {
				dumper.flush(ctx)
			}

			// split current page, decommission current page
			dumper.decommissionPage(pathItem.page)

			// create new pages based on the splitPoint
			startPoint := 0
			for _, splitPoint := range splitPoints {
				newPage := dumper.splitPage(pathItem.page, pathItem.page.Type(), startPoint, splitPoint)
				// add new page's reference in it's parent
				path[i-1].page.addKVPair(&memtable.KVPair{
					Key:   newPage.Key(0),
					Value: newPage.addr.ToValue(),
				})
				// commit this page for writing
				ctx.commitPage(newPage, path[i-1].page.addr, i)

				startPoint = splitPoint
			}
		} else if pathItem.page.size < dumper.leafPageSize/2 {
			// page size under threshold. Try to merge this page with it's right page
		} else {
			// commit this page for writing
			ctx.commitPage(pathItem.page, path[i-1].page.addr, i)
		}
	}
}

func (dumper *dumper) decommissionPage(page *pageForDump) {
	page.valid = false
	dumper.cache[page.addr] = nil
}

func (dumper *dumper) calculateSplitPoints(page *pageForDump) []int {
	// get pages' size after split based on page type
	var pageSize int
	if page.Type() == Leaf {
		pageSize = dumper.leafPageSize
	} else {
		pageSize = dumper.indexPageSize
	}

	limit := pageSize
	if page.size < 2*pageSize {
		limit = page.size / 2
	}

	res := make([]int, 0)
	cur := 0
	curSize := 0
	total := 0
	for i, pair := range page.entries {
		size := len(pair.Key) + len(pair.Value)
		total += size

		if curSize+size > limit {
			if cur == i {
				res = append(res, i+1)
				cur = i + 1
				curSize = 0
			} else {
				res = append(res, i)
				cur = i
				curSize = size
			}
			if page.size-total < pageSize {
				break
			} else if page.size-total > pageSize && page.size-total < 2*pageSize {
				limit = (page.size - total) / 2
			}
		} else {
			curSize += size
		}
	}
	res = append(res, len(page.entries))
	return res
}

func (dumper *dumper) splitPage(page *pageForDump, pageType PageType, start int, end int) *pageForDump {
	var size int
	for i := start; i < end; i++ {
		size += len(page.entries[i].Key) + len(page.entries[i].Value)
	}
	page = &pageForDump{
		page:      *NewPage(pageType, page.entries[start:end:end]),
		dirty:     true,
		valid:     false,
		size:      size,
		shadowKey: nil,
	}
	dumper.cache[page.addr] = page
	return page
}

func (dumper *dumper) checkForMerge(path []*pathItemForDump) []*pathItemForDump {
	return nil
}

func (dumper *dumper) flush(ctx *context) {
	// flush writable page from bottom to up, leaf to index
	depth := dumper.treeDepth
	for level := depth - 1; level >= 0; level-- {
		for parentAddr, pages := range ctx.committed[level] {
			parent := dumper.cache[parentAddr]
			if parent == nil && parentAddr.equals(dumper.root.addr) {
				parent = dumper.root
			}
			for i, addr := range dumper.writePages(pages) {
				page := pages[i]
				if page.shadowKey != nil {
					// remove shadow key and replace with current first key
					parent.removeKVEntry(parent.Search(&page.shadowKey))
					page.shadowKey = nil
				}
				parent.addKV(page.mappingKey(), addr.ToValue())

				dumper.decommissionPage(page)
			}
			ctx.committed[level][parentAddr] = nil
		}
	}
	return
}

func (dumper *dumper) writePage(page *pageForDump) address {
	return writeTo(page.buildCompressedBytes())
}

func (dumper *dumper) writePages(pages []*pageForDump) []address {
	addresses := make([]address, 0)
	for _, page := range pages {
		addresses = append(addresses, writeTo(page.buildCompressedBytes()))
	}
	return addresses
}

// context the context for b+ tree dump progress
type context struct {
	// path the path found in last round put/remove entries
	path []*pathItemForDump
	// committed the map to save parent/child relationship for the pages
	// that are writable
	committed map[int]map[address][]*pageForDump
}

func (ctx *context) commitPage(page *pageForDump, parentAddr address, level int) {
	committedOnLevel := ctx.committedOnLevel(level)
	if committedOnLevel[parentAddr] == nil {
		committedOnLevel[parentAddr] = make([]*pageForDump, 0)
	}
	committedOnLevel[parentAddr] = append(committedOnLevel[parentAddr], page)
}

func (ctx *context) committedOnLevel(level int) map[address][]*pageForDump {
	if ctx.committed[level] == nil {
		ctx.committed[level] = map[address][]*pageForDump{}
	}
	return ctx.committed[level]
}
