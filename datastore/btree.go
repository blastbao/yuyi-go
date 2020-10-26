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

import "yuyi-go/datastore/chunk"

type BTree struct {
	// lastTreeInfo last tree info after Cow synced from memory table.
	lastTreeInfo *TreeInfo
}

type TreeInfo struct {
	// root the root page of the copy-on-write b+ tree.
	root *page

	// depth the depth of the tree
	depth int

	// filter the filter for quick filter key
	filter Filter
}

type pathItem struct {
	page  *page
	index int
}

type pathItemForDump struct {
	page  *pageForDump
	index int
}

func (tree *BTree) Has(key *Key) bool {
	treeInfo := tree.lastTreeInfo
	skipRead := !mightContains(treeInfo, key)
	if skipRead {
		return false
	}

	// fetch the path with the specified key
	path := tree.findPath(treeInfo, key)
	leaf := path[len(path)-1].page
	return leaf.Search(key) >= 0
}

func (tree *BTree) Get(key *Key) Value {
	treeInfo := tree.lastTreeInfo
	skipRead := !mightContains(treeInfo, key)
	if skipRead {
		return nil
	}

	// fetch the path with the specified key
	path := tree.findPath(treeInfo, key)

	leaf := path[len(path)-1].page
	index := leaf.Search(key)
	if index >= 0 {
		return leaf.Value(index)
	} else {
		return nil
	}
}

type ListResult struct {
	pairs []*KVPair
	next  *Key
}

func (tree *BTree) List(start Key, end Key, max int) *ListResult {
	treeInfo := tree.lastTreeInfo
	if treeInfo == nil || treeInfo.root == nil || treeInfo.root.KVPairsCount() == 0 {
		return &ListResult{
			pairs: []*KVPair{},
			next:  nil,
		}
	}

	res := make([]*KVPair, 0, max)
	if start == nil {
		start = []byte{}
	}

	path := tree.findPath(tree.lastTreeInfo, &start)
	leaf := path[len(path)-1].page
	pairsCount := leaf.KVPairsCount()
	if pairsCount == 0 {
		return &ListResult{make([]*KVPair, 0), nil}
	}

	startIndex := leaf.Search(&start)
	if startIndex < 0 {
		startIndex = -startIndex - 1
	}

	keyFound := 0
	var next Key
outer:
	for {
		for i := startIndex; i < pairsCount; i++ {
			if end != nil && leaf.Key(i).Compare(end) >= 0 {
				break outer
			}
			if keyFound == max {
				// search for kv pairs finished, found next key
				next = leaf.Key(i)
				break outer
			}
			res = append(res, leaf.KVPair(i))
			keyFound++
		}
		// try find next leaf page
		path = tree.findNextLeaf(path)
		if path == nil {
			// reach end of the tree
			break
		}
		leaf = path[len(path)-1].page
		pairsCount = leaf.KVPairsCount()
		startIndex = 0
	}
	return &ListResult{res, &next}
}

func (tree *BTree) ReverseList(start Key, end Key, max int) []KVPair {
	return nil
}

// findPath find the path from root page to leaf page with the specified key
func (tree *BTree) findPath(treeInfo *TreeInfo, key *Key) []*pathItem {
	root := treeInfo.root
	// create result slice and put root to the result
	res := make([]*pathItem, treeInfo.depth, treeInfo.depth)
	res[0] = &pathItem{root, 0}

	parent := root
	depth := 1
	for {
		index := parent.Search(key)
		if index < 0 {
			if index == -1 {
				index = 0
			} else {
				index = -index - 2
			}
		}
		// find child page with the found index and push it in result.
		childAddress := parent.ChildAddress(index)
		childPage := readPage(childAddress)
		res[depth] = &pathItem{childPage, index}

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

func (tree *BTree) findNextLeaf(curPath []*pathItem) []*pathItem {
	depth := len(curPath)
	// recusive find next index
	i := depth - 1
	for ; i > 0; i-- {
		if curPath[i].index+1 < curPath[i-1].page.KVPairsCount() {
			nextLeaf := readPage(curPath[i-1].page.ChildAddress(curPath[i].index + 1))
			curPath[i] = &pathItem{nextLeaf, curPath[i].index + 1}
			break
		} else {
			curPath[i] = nil
		}
	}
	if curPath[1] == nil {
		// reach end of the tree
		return nil
	}
	for {
		if curPath[i].page.Type() == Leaf {
			break
		}
		// find child page with the found index and push it in result.
		childAddress := curPath[i].page.ChildAddress(0)
		childPage := readPage(childAddress)
		curPath[i+1] = &pathItem{childPage, 0}
		i++
	}
	return curPath
}

func mightContains(treeInfo *TreeInfo, key *Key) bool {
	if treeInfo == nil || !treeInfo.filter.MightContains(key) {
		return false
	}
	return true
}

func readPage(addr chunk.Address) *page {
	content := ReadFrom(addr)
	return &page{
		content: content,
		entries: nil,
		addr:    addr,
	}
}
