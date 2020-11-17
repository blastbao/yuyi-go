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
	"fmt"

	"yuyi-go/datastore/chunk"
	"yuyi-go/shared"

	"go.uber.org/zap"
)

type dumper struct {
	// lg the logger instance
	lg *zap.Logger

	// btree the b+ tree instance for dumping
	btree *BTree

	// root the working root for dumping
	root *pageForDump

	// filter the filter of btree
	filter Filter

	// writer the writer for btree pages
	writer *chunk.BtreeWriter

	// cache the cache for page buffer that may be modified during dump
	cache map[chunk.Address]*pageForDump

	// treeDepth the depth of the tree during dump
	treeDepth int

	// leafPageSize the size limit for leaf page
	leafPageSize int

	// indexPageSize the size limit for index page
	indexPageSize int
}

func newDumper(lg *zap.Logger, btree *BTree, cfg *shared.Config) (*dumper, error) {
	if lg == nil {
		lg = zap.NewNop()
	}

	var root pageForDump
	var depth int
	writer, err := chunk.NewBtreeWriter(cfg)
	if err != nil {
		return nil, err
	}
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
			lg:            lg,
			btree:         btree,
			root:          &root,
			filter:        &dummyFilter{},
			writer:        writer,
			cache:         map[chunk.Address]*pageForDump{},
			treeDepth:     depth,
			leafPageSize:  8192,
			indexPageSize: 8192,
		}, nil
	}
	return &dumper{
		lg:            lg,
		btree:         btree,
		root:          nil,
		filter:        &dummyFilter{},
		writer:        writer,
		cache:         map[chunk.Address]*pageForDump{},
		treeDepth:     0,
		leafPageSize:  8192,
		indexPageSize: 8192,
	}, nil
}

func (dumper *dumper) Dump(entries []*KVEntry) (*TreeInfo, error) {
	ctx := &context{
		path:      nil,
		committed: map[int]map[chunk.Address][]*pageForDump{},
	}
	lastIndex := 0
	for i, entry := range entries {
		if entry.TableValue.Operation == Put {
			continue
		}
		if entry.TableValue.Operation == Remove {
			if i > lastIndex {
				putEntries := entries[lastIndex:i:i]
				dumper.putEntries(putEntries, ctx)
			}
			dumper.removeEntry(entry, ctx)
			lastIndex = i + 1
		}
	}
	if lastIndex < len(entries) {
		putEntries := entries[lastIndex:len(entries):len(entries)]
		dumper.putEntries(putEntries, ctx)
	}
	dumper.checkForCommit(ctx, nil)
	dumper.flush(ctx)

	for _, page := range dumper.cache {
		if page != nil && page.Type() != Root && page.dirty == true {
			fmt.Printf("Found dirty page in cache\n")
		}
	}

	// handle root split and sync
	return dumper.sync(ctx)
}

func (dumper *dumper) sync(ctx *context) (*TreeInfo, error) {
	for {
		if dumper.root.size > dumper.indexPageSize {
			splitPoints := dumper.calculateSplitPoints(dumper.root, dumper.indexPageSize)
			if len(splitPoints) > 1 {
				oldRoot := dumper.root
				dumper.decommissionPage(dumper.root)

				newRoot := NewPageForDump(Root, nil)
				dumper.root = newRoot

				startPoint := 0
				for _, splitPoint := range splitPoints {
					newPage := dumper.splitPage(oldRoot, Index, startPoint, splitPoint)
					// add new page's reference in root
					newRoot.addKVPair(&KVPair{
						Key:   newPage.Key(0),
						Value: newPage.addr.Bytes(),
					})
					// commit this page for writing
					ctx.commitPage(newPage, newRoot.addr, 0)
					startPoint = splitPoint
				}
				dumper.treeDepth++
				dumper.flush(ctx)
			}
		} else {
			break
		}
	}

	// flush root
	var err error
	dumper.root.addr, err = dumper.writePage(dumper.root)
	if err != nil {
		return nil, err
	}
	if dumper.root.KVPairsCount() == 0 {
		dumper.treeDepth = 1
	}

	rootPage, err := dumper.btree.readPage(dumper.root.addr)
	if err != nil {
		return nil, err
	}
	return &TreeInfo{
		root:   rootPage,
		depth:  dumper.treeDepth,
		filter: &dummyFilter{},
	}, nil
}

func (dumper *dumper) putEntries(putEntries []*KVEntry, ctx *context) error {
	var rightSibling Key
	var path []*pathItemForDump
	var err error
	for _, entry := range putEntries {
		// check path
		if path == nil || (rightSibling != nil && rightSibling.Compare(entry.Key) <= 0) {
			// need fetch path cause origin path is nil/invalid
			path, err = dumper.fetchPathForDumper(dumper.root, entry.Key)
			if err != nil {
				return err
			}

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
	return nil
}

func (dumper *dumper) removeEntry(entry *KVEntry, ctx *context) error {
	if dumper.root == nil || !dumper.filter.MightContains(&entry.Key) {
		return nil
	}
	path, err := dumper.fetchPathForDumper(dumper.root, entry.Key)
	if err != nil {
		return err
	}
	if path != nil {
		// check with new path to commit pages that is writable
		dumper.checkForCommit(ctx, path)
		ctx.path = path
	}

	leafPage := path[len(path)-1].page
	index := leafPage.Search(&entry.Key)
	if index < 0 {
		return err
	}
	// key exists update filter first
	dumper.filter.Remove(&entry.Key)

	// modify page's content
	leafPage.removeKV(entry.Key)
	return nil
}

func (dumper *dumper) fetchPathForDumper(root *pageForDump, key Key) ([]*pathItemForDump, error) {
	if root == nil || root.KVPairsCount() == 0 {
		res := make([]*pathItemForDump, 2, 2)

		// create empty leaf with the shadow key that first key to insert
		leaf := pageForDump{
			page:      *NewPage(Leaf, nil),
			dirty:     true,
			valid:     false,
			size:      0,
			shadowKey: key,
		}
		res[1] = &pathItemForDump{&leaf, 0}
		dumper.cache[leaf.addr] = &leaf

		if root == nil {
			// create new root with leaf reference in root
			root = &pageForDump{
				page:      *NewPage(Root, nil),
				dirty:     true,
				valid:     true,
				size:      0,
				shadowKey: nil,
			}
		}

		root.addKVToIndex(key, leaf.addr.Bytes(), -1)
		res[0] = &pathItemForDump{root, 0}
		dumper.cache[root.addr] = root

		dumper.root = root
		dumper.treeDepth = 2
		return res, nil
	}
	res := make([]*pathItemForDump, dumper.treeDepth, dumper.treeDepth)
	res[0] = &pathItemForDump{root, 0}

	parent := root
	depth := 1
	for {
		index := parent.Search(&key)
		if index < 0 {
			if index == -1 {
				index = 0
			} else {
				index = -index - 2
			}
		}
		// find child page with the found index and push it in result.
		childAddr := parent.ChildAddress(index)
		var childPage *pageForDump
		var err error
		if dumper.cache[childAddr] != nil {
			childPage = dumper.cache[childAddr]
		} else {
			childPage, err = dumper.fetchPageForDumper(childAddr)
			if err != nil {
				return nil, err
			}
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
	return res, nil
}

func (dumper *dumper) fetchPageForDumper(addr chunk.Address) (*pageForDump, error) {
	content, err := dumper.btree.reader.Read(addr)
	if err != nil {
		return nil, err
	}
	page := page{content: content, addr: addr}
	pageForDump := &pageForDump{
		page:      page,
		dirty:     false,
		valid:     true,
		size:      len(content),
		shadowKey: nil,
	}
	dumper.cache[addr] = pageForDump
	return pageForDump, nil
}

func (dumper *dumper) findRightSibling(path []*pathItemForDump) Key {
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

func (dumper *dumper) pageSizeThreshold(pageType PageType) int {
	var threshold int
	if pageType == Leaf {
		threshold = dumper.leafPageSize
	} else {
		threshold = dumper.indexPageSize
	}
	return threshold
}

func (dumper *dumper) checkForCommit(ctx *context, newPath []*pathItemForDump) {
	path := ctx.path
	if path == nil {
		return
	}
	for i := len(path) - 1; i > 0; i-- {
		// check if page is committable
		if newPath != nil && path[i].page == newPath[i].page {
			break
		}
		pathItem := path[i]
		threshold := dumper.pageSizeThreshold(pathItem.page.Type())
		if pathItem.page.size > threshold {
			dumper.checkForSplit(ctx, i, threshold, newPath)
		} else if pathItem.page.size < threshold/2 {
			dumper.checkForMerge(ctx, i, threshold, newPath)
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

func (dumper *dumper) checkForSplit(ctx *context, level int, threshold int, newPath []*pathItemForDump) {
	path := ctx.path
	pathItem := path[level]
	// page size over threshold. Try to split this page
	splitPoints := dumper.calculateSplitPoints(pathItem.page, threshold)
	if len(splitPoints) < 2 {
		// can't split page, just commit this page for write
		ctx.commitPage(pathItem.page, path[level-1].page.addr, level)
		return
	}
	// should flush writable pages before splitting index page
	if pathItem.page.Type() == Index {
		dumper.flush(ctx)
	}

	// split current page, decommission current page
	dumper.decommissionPage(pathItem.page)
	path[level-1].page.removeKV(pathItem.page.mappingKey())

	// create new pages based on the splitPoint
	startPoint := 0
	for _, splitPoint := range splitPoints {
		newPage := dumper.splitPage(pathItem.page, pathItem.page.Type(), startPoint, splitPoint)
		// add new page's reference in it's parent
		path[level-1].page.addKVPair(&KVPair{
			Key:   newPage.mappingKey(),
			Value: newPage.addr.Bytes(),
		})
		// commit this page for writing
		ctx.commitPage(newPage, path[level-1].page.addr, level)

		startPoint = splitPoint
	}
	if newPath != nil && path[level-1].page == newPath[level-1].page {
		newPath[level].index = newPath[level].index - 1 + len(splitPoints)
	}
}

func (dumper *dumper) checkForMerge(ctx *context, level int, threshold int, newPath []*pathItemForDump) error {
	path := ctx.path
	pathItem := path[level]
	// page size under threshold. Try to merge this page with it's right page of old path
	if pathItem.index+1 < path[level-1].page.KVPairsCount() {
		nextAddr := path[level-1].page.ChildAddress(pathItem.index + 1)
		if newPath != nil && nextAddr.Equals(newPath[level].page.addr) {
			// page of new path is the right page of old path, merge them together
			leftCount := pathItem.page.KVPairsCount()
			pathItem.page.appendKVEntries(newPath[level].page.AllEntries())
			path[level-1].page.removeKV(newPath[level].page.mappingKey())
			dumper.decommissionPage(newPath[level].page)

			newPath[level].page = pathItem.page
			newPath[level].index = pathItem.index
			if pathItem.page.Type() == Index {
				// update new path item index
				newPath[level+1].index += leftCount
			}
		} else {
			// read right page of old path and merge them together
			nextPage, err := dumper.btree.readPage(nextAddr)
			if err != nil {
				return err
			}

			pathItem.page.appendKVEntries(nextPage.AllEntries())
			path[level-1].page.removeKV(nextPage.Key(0))
			if newPath != nil && path[level-1].page == newPath[level-1].page {
				newPath[level].index = newPath[level].index - 1
			}

			if pathItem.page.size > threshold {
				dumper.checkForSplit(ctx, level, threshold, newPath)
			} else {
				ctx.commitPage(pathItem.page, path[level-1].page.addr, level)
			}
		}
		return nil
	} else if pathItem.index > 1 {
		// can't merge with right page of old path. Try left page
		prevAddr := path[level-1].page.ChildAddress(pathItem.index - 1)
		if dumper.cache[prevAddr] != nil {
			// prevPage already in cache, and must already committed, should not change it.
			ctx.commitPage(pathItem.page, path[level-1].page.addr, level)
			return nil
		}
		prevPage, err := dumper.fetchPageForDumper(prevAddr)
		if err != nil {
			return err
		}
		prevPage.appendKVEntries(pathItem.page.AllEntries())
		// remove reference for the merged page
		if pathItem.page.Type() == Index {
			// change parent reference for pages that already committed
			committedChildPages := ctx.committed[level+1][pathItem.page.addr]
			if committedChildPages != nil {
				ctx.commitPages(committedChildPages, prevAddr, level+1)
				ctx.committed[level+1][pathItem.page.addr] = nil
			}
		}
		dumper.decommissionPage(pathItem.page)

		path[level-1].page.removeKV(pathItem.page.mappingKey())
		pathItem.page = prevPage
		pathItem.index = path[level].index - 1

		if prevPage.size > threshold {
			dumper.checkForSplit(ctx, level, threshold, newPath)
		} else {
			ctx.commitPage(prevPage, path[level-1].page.addr, level)
		}
		return nil
	} else {
		// commit this page for writing
		ctx.commitPage(pathItem.page, path[level-1].page.addr, level)
	}
	return nil
}

func (dumper *dumper) calculateSplitPoints(page *pageForDump, threshold int) []int {
	limit := threshold
	if page.size < 2*threshold {
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
			if page.size-total < threshold {
				break
			} else if page.size-total > threshold && page.size-total < 2*threshold {
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
	page = NewPageForDump(pageType, page.entries[start:end:end])
	dumper.cache[page.addr] = page
	return page
}

func (dumper *dumper) flush(ctx *context) error {
	// flush writable page from bottom to up, leaf to index
	depth := dumper.treeDepth
	for level := depth - 1; level >= 0; level-- {
		for parentAddr, pages := range ctx.committed[level] {
			var parent *pageForDump
			if parentAddr.Equals(dumper.root.addr) {
				parent = dumper.root
			} else {
				parent = dumper.cache[parentAddr]
			}
			pagesForWrite := make([]*pageForDump, 0, len(pages))
			// filter pages that are empty
			for _, page := range pages {
				if page.KVPairsCount() != 0 {
					pagesForWrite = append(pagesForWrite, page)
				} else {
					parent.removeKV(page.mappingKey())
					dumper.decommissionPage(page)
				}
			}
			addrs, err := dumper.writePages(pagesForWrite)
			if err != nil {
				return err
			}
			for i, addr := range addrs {
				page := pages[i]
				if page.shadowKey != nil {
					// remove shadow key and replace with current first key
					parent.removeKV(page.shadowKey)
					page.shadowKey = nil
				}
				parent.addKV(page.mappingKey(), addr.Bytes())

				dumper.decommissionPage(page)
			}
			ctx.committed[level][parentAddr] = nil
		}
	}
	return nil
}

func (dumper *dumper) writePage(page *pageForDump) (chunk.Address, error) {
	return dumper.writer.Write(page.buildBytes())
}

func (dumper *dumper) writePages(pages []*pageForDump) ([]chunk.Address, error) {
	addrs := make([]chunk.Address, 0)
	for _, page := range pages {
		addr, err := dumper.writer.Write(page.buildBytes())
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// context the context for b+ tree dump progress
type context struct {
	// path the path found in last round put/remove entries
	path []*pathItemForDump
	// committed the map to save parent/child relationship for the pages
	// that are writable
	committed map[int]map[chunk.Address][]*pageForDump
}

func (ctx *context) commitPage(page *pageForDump, parentAddr chunk.Address, level int) {
	committedOnLevel := ctx.committedOnLevel(level)
	if committedOnLevel[parentAddr] == nil {
		committedOnLevel[parentAddr] = make([]*pageForDump, 0)
	}
	committedOnLevel[parentAddr] = append(committedOnLevel[parentAddr], page)
}

func (ctx *context) commitPages(pages []*pageForDump, parentAddr chunk.Address, level int) {
	committedOnLevel := ctx.committedOnLevel(level)
	if committedOnLevel[parentAddr] == nil {
		committedOnLevel[parentAddr] = make([]*pageForDump, 0, len(pages))
	}
	committedOnLevel[parentAddr] = append(committedOnLevel[parentAddr], pages...)
}

func (ctx *context) committedOnLevel(level int) map[chunk.Address][]*pageForDump {
	if ctx.committed[level] == nil {
		ctx.committed[level] = map[chunk.Address][]*pageForDump{}
	}
	return ctx.committed[level]
}
