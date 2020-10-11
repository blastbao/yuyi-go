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

var treeDepth int8

func init() {
	// find path from root to leaf with empty key to determine current tree depth
	treeDepth = 0
}

type BTree struct {
	// lastTreeInfo last tree info after Cow synced from memory table.
	lastTreeInfo TreeInfo
}

type TreeInfo struct {
	// root the root page of the copy-on-write b+ tree.
	root page

	// filter the filter for quick filter key
	filter Filter
}

type pathItem struct {
	page  page
	index int
}

type pathItemForDump struct {
	page  pageForDump
	index int
}

func (tree *BTree) Has(key *memtable.Key) bool {
	treeInfo := tree.lastTreeInfo
	skipRead := !mightContains(&treeInfo, key)
	if skipRead {
		return false
	}

	// fetch the path with the specified key
	path := tree.FindPathToLeaf(&treeInfo.root, key)
	leaf := path[len(path)-1].page
	return leaf.Search(key) >= 0
}

func (tree *BTree) Get(key *memtable.Key) memtable.Value {
	treeInfo := tree.lastTreeInfo
	skipRead := !mightContains(&treeInfo, key)
	if skipRead {
		return nil
	}

	// fetch the path with the specified key
	path := tree.FindPathToLeaf(&treeInfo.root, key)

	leaf := path[len(path)-1].page
	index := leaf.Search(key)
	if index >= 0 {
		return leaf.Value(index)
	} else {
		return nil
	}
}

func (tree *BTree) List(start memtable.Key, end memtable.Key, max uint16) []memtable.Value {
	return nil
}

func (tree *BTree) ReverseList(start memtable.Key, end memtable.Key, max uint16) []memtable.Value {
	return nil
}

func (tree *BTree) ListOnPrefix(prefix memtable.Key, start memtable.Key, end memtable.Key, max uint16) []*memtable.Value {
	return nil
}

func (tree *BTree) ReverseListOnPrefix(prefix memtable.Key, max uint16) []*memtable.Value {
	return nil
}

// FindPathToLeaf find the path from root page to leaf page with the specified key
func (tree *BTree) FindPathToLeaf(root *page, key *memtable.Key) []*pathItem {
	// create result slice and put root to the result
	res := make([]*pathItem, treeDepth, treeDepth)
	res[0] = &pathItem{*root, 0}

	parent := root
	depth := 1
	for {
		index := parent.Search(key)
		if index < 0 {
			index = -index - 1
		}
		// find child page with the found index and push it in result.
		childAddress := parent.ChildAddress(index)
		childPage := readPage(childAddress)
		res[depth] = &pathItem{*childPage, index}

		if childPage.Type() == Leaf {
			// found leaf page, search end
			break
		}
	}
	return res
}

func mightContains(treeInfo *TreeInfo, key *memtable.Key) bool {
	if treeInfo == nil || !treeInfo.filter.MightContains(key) {
		return false
	}
	return true
}

func readPage(addr address) *page {
	content := ReadFrom(addr)
	return &page{
		content: content,
		entries: nil,
		addr:    addr,
	}
}
